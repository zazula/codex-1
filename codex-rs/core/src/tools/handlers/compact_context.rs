use serde::Deserialize;
use serde::Serialize;

use crate::compact::COMPACT_USER_MESSAGE_MAX_TOKENS;
use crate::compact::SUMMARY_PREFIX;
use crate::compact::build_compacted_history_with_limit;
use crate::compact::build_compaction_checkpoint;
use crate::compact::collect_user_messages;
use crate::compact::select_recent_user_messages;
use crate::function_tool::FunctionCallError;
use crate::session::session::Session;
use crate::session::turn::get_last_assistant_message_from_turn;
use crate::session::turn_context::TurnContext;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::tools::registry::ToolHandler;
use crate::tools::registry::ToolKind;
use codex_protocol::models::ContentItem;
use codex_protocol::models::FunctionCallOutputBody;
use codex_protocol::models::FunctionCallOutputPayload;
use codex_protocol::models::ResponseInputItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::CompactedItem;
use codex_protocol::protocol::ContextCompactedEvent;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::WarningEvent;
use std::sync::Arc;

pub struct CompactContextHandler;

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CompactionStrategy {
    Inspect,
    ExtractRecent,
    Checkpoint,
    Truncate,
    RemoveByRole,
    RemoveByContentType,
    RemoveUntilTokenBudget,
}

#[derive(Debug, Deserialize)]
struct CompactContextArgs {
    strategy: CompactionStrategy,
    summary: Option<String>,
    max_user_message_tokens: Option<usize>,
    max_total_tokens: Option<usize>,
    role: Option<String>,
    content_type: Option<String>,
    keep_last_items: Option<usize>,
    include_summary_prefix: Option<bool>,
    include_recent_user_messages: Option<bool>,
    include_ghost_snapshots: Option<bool>,
    apply: Option<bool>,
}

#[derive(Debug, Serialize)]
struct CompactionToolResult {
    strategy: CompactionStrategy,
    applied: bool,
    history_items: usize,
    user_message_count: usize,
    estimated_total_tokens: Option<i64>,
    last_assistant_message: Option<String>,
    max_user_message_tokens: usize,
    keep_last_items: Option<usize>,
    trimmed_history_items: Option<usize>,
    removed_items: Option<usize>,
    kept_items: Option<usize>,
    selected_user_messages: Option<Vec<String>>,
    summary_used: Option<String>,
    checkpoint: Option<String>,
}

pub struct CompactContextOutput {
    content: String,
}

impl ToolOutput for CompactContextOutput {
    fn log_preview(&self) -> String {
        self.content.clone()
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn to_response_item(&self, call_id: &str, _payload: &ToolPayload) -> ResponseInputItem {
        let output = FunctionCallOutputPayload {
            body: FunctionCallOutputBody::Text(self.content.clone()),
            success: Some(true),
        };
        ResponseInputItem::FunctionCallOutput {
            call_id: call_id.to_string(),
            output,
        }
    }
}

impl ToolHandler for CompactContextHandler {
    type Output = CompactContextOutput;

    fn kind(&self) -> ToolKind {
        ToolKind::Function
    }

    async fn is_mutating(&self, invocation: &ToolInvocation) -> bool {
        let ToolInvocation { payload, .. } = invocation;
        let arguments = match payload {
            ToolPayload::Function { arguments } => arguments,
            _ => return false,
        };

        let args: CompactContextArgs = match serde_json::from_str(arguments) {
            Ok(args) => args,
            Err(_) => return false,
        };

        args.apply.unwrap_or(false)
    }

    async fn handle(&self, invocation: ToolInvocation) -> Result<Self::Output, FunctionCallError> {
        let ToolInvocation {
            payload,
            session,
            turn,
            ..
        } = invocation;

        let arguments = match payload {
            ToolPayload::Function { arguments } => arguments,
            _ => {
                return Err(FunctionCallError::RespondToModel(
                    "compact_context handler received unsupported payload".to_string(),
                ));
            }
        };

        let args: CompactContextArgs = serde_json::from_str(&arguments).map_err(|err| {
            FunctionCallError::RespondToModel(format!(
                "failed to parse function arguments: {err:?}"
            ))
        })?;

        let history = session.clone_history().await;
        let history_snapshot: Vec<_> = history.raw_items().to_vec();
        let user_messages = collect_user_messages(&history_snapshot);
        let user_message_count = user_messages.len();
        let history_items = history_snapshot.len();
        let estimated_total_tokens = history.estimate_token_count(&turn);
        let last_assistant_message = get_last_assistant_message_from_turn(&history_snapshot);
        let max_user_message_tokens = args
            .max_user_message_tokens
            .unwrap_or(COMPACT_USER_MESSAGE_MAX_TOKENS);
        let include_recent_user_messages = args.include_recent_user_messages.unwrap_or(true);
        let include_summary_prefix = args.include_summary_prefix.unwrap_or(true);
        let include_ghost_snapshots = args.include_ghost_snapshots.unwrap_or(true);
        let apply = args.apply.unwrap_or(false);

        let mut result = CompactionToolResult {
            strategy: args.strategy,
            applied: false,
            history_items,
            user_message_count,
            estimated_total_tokens,
            last_assistant_message: last_assistant_message.clone(),
            max_user_message_tokens,
            keep_last_items: args.keep_last_items,
            trimmed_history_items: None,
            removed_items: None,
            kept_items: None,
            selected_user_messages: None,
            summary_used: None,
            checkpoint: None,
        };

        match args.strategy {
            CompactionStrategy::Inspect => {}
            CompactionStrategy::ExtractRecent => {
                let selected = if include_recent_user_messages {
                    select_recent_user_messages(&user_messages, max_user_message_tokens)
                } else {
                    Vec::new()
                };
                result.selected_user_messages = Some(selected);
            }
            CompactionStrategy::Checkpoint => {
                let summary = args.summary.or(last_assistant_message).ok_or_else(|| {
                    FunctionCallError::RespondToModel(
                        "summary must be provided or available from the last assistant message"
                            .to_string(),
                    )
                })?;
                let summary_text = if include_summary_prefix {
                    format!("{SUMMARY_PREFIX}\n{summary}")
                } else {
                    summary
                };
                let user_messages_for_checkpoint = if include_recent_user_messages {
                    user_messages.clone()
                } else {
                    Vec::new()
                };
                let (checkpoint, selected) = build_compaction_checkpoint(
                    &user_messages_for_checkpoint,
                    &summary_text,
                    max_user_message_tokens,
                );
                result.selected_user_messages = Some(selected);
                result.summary_used = Some(summary_text.clone());
                result.checkpoint = Some(checkpoint.clone());

                if apply {
                    apply_checkpoint(
                        &session,
                        &turn,
                        &user_messages_for_checkpoint,
                        &summary_text,
                        max_user_message_tokens,
                        include_ghost_snapshots,
                    )
                    .await?;
                    result.applied = true;
                }
            }
            CompactionStrategy::Truncate => {
                let keep_last_items = args.keep_last_items.ok_or_else(|| {
                    FunctionCallError::RespondToModel(
                        "keep_last_items is required when strategy is truncate".to_string(),
                    )
                })?;
                if keep_last_items == 0 {
                    return Err(FunctionCallError::RespondToModel(
                        "keep_last_items must be greater than zero".to_string(),
                    ));
                }

                let (trimmed_count, kept_items) =
                    truncate_history(&history_snapshot, keep_last_items);
                result.trimmed_history_items = Some(trimmed_count);
                result.kept_items = Some(kept_items.len());

                if apply {
                    apply_truncation(
                        &session,
                        &turn,
                        kept_items,
                        &history_snapshot,
                        include_ghost_snapshots,
                    )
                    .await?;
                    result.applied = true;
                }
            }
            CompactionStrategy::RemoveByRole => {
                let role = args.role.as_deref().ok_or_else(|| {
                    FunctionCallError::RespondToModel(
                        "role is required when strategy is remove_by_role".to_string(),
                    )
                })?;
                let filtered = filter_by_role(&history_snapshot, role);
                result.removed_items = Some(history_snapshot.len().saturating_sub(filtered.len()));
                result.kept_items = Some(filtered.len());

                if apply {
                    apply_filtered_history(
                        &session,
                        &turn,
                        filtered,
                        &history_snapshot,
                        include_ghost_snapshots,
                    )
                    .await?;
                    result.applied = true;
                }
            }
            CompactionStrategy::RemoveByContentType => {
                let content_type = args.content_type.as_deref().ok_or_else(|| {
                    FunctionCallError::RespondToModel(
                        "content_type is required when strategy is remove_by_content_type"
                            .to_string(),
                    )
                })?;
                let filtered = filter_by_content_type(&history_snapshot, content_type)?;
                result.removed_items = Some(history_snapshot.len().saturating_sub(filtered.len()));
                result.kept_items = Some(filtered.len());

                if apply {
                    apply_filtered_history(
                        &session,
                        &turn,
                        filtered,
                        &history_snapshot,
                        include_ghost_snapshots,
                    )
                    .await?;
                    result.applied = true;
                }
            }
            CompactionStrategy::RemoveUntilTokenBudget => {
                let max_total_tokens = args.max_total_tokens.ok_or_else(|| {
                    FunctionCallError::RespondToModel(
                        "max_total_tokens is required when strategy is remove_until_token_budget"
                            .to_string(),
                    )
                })?;
                let (removed, kept_items) =
                    remove_oldest_until_token_budget(history, max_total_tokens, &turn);
                result.removed_items = Some(removed);
                result.kept_items = Some(kept_items.len());

                if apply {
                    apply_filtered_history(
                        &session,
                        &turn,
                        kept_items,
                        &history_snapshot,
                        include_ghost_snapshots,
                    )
                    .await?;
                    result.applied = true;
                }
            }
        }

        let content = serde_json::to_string_pretty(&result).map_err(|err| {
            FunctionCallError::RespondToModel(format!(
                "failed to serialize compaction result: {err}"
            ))
        })?;

        Ok(CompactContextOutput { content })
    }
}

fn truncate_history(items: &[ResponseItem], keep_last_items: usize) -> (usize, Vec<ResponseItem>) {
    let filtered: Vec<ResponseItem> = items.to_vec();
    if keep_last_items >= filtered.len() {
        return (0, filtered);
    }
    let start = filtered.len().saturating_sub(keep_last_items);
    let kept = filtered[start..].to_vec();
    (start, kept)
}

fn filter_by_role(items: &[ResponseItem], role: &str) -> Vec<ResponseItem> {
    items
        .iter()
        .filter(|item| !matches!(item, ResponseItem::Message { role: r, .. } if r == role))
        .cloned()
        .collect()
}

fn filter_by_content_type(
    items: &[ResponseItem],
    content_type: &str,
) -> Result<Vec<ResponseItem>, FunctionCallError> {
    let normalized_type = content_type.trim().to_lowercase();
    if !matches!(
        normalized_type.as_str(),
        "input_text" | "output_text" | "input_image"
    ) {
        return Err(FunctionCallError::RespondToModel(format!(
            "unsupported content_type: {content_type}"
        )));
    }

    let matches_type = |item: &ContentItem| {
        matches!(
            (normalized_type.as_str(), item),
            ("input_text", ContentItem::InputText { .. })
                | ("output_text", ContentItem::OutputText { .. })
                | ("input_image", ContentItem::InputImage { .. })
        )
    };

    Ok(items
        .iter()
        .filter_map(|item| match item {
            ResponseItem::Message {
                id, role, content, ..
            } => {
                let filtered: Vec<ContentItem> = content
                    .iter()
                    .filter(|ci| !matches_type(ci))
                    .cloned()
                    .collect();
                if filtered.is_empty() {
                    None
                } else {
                    Some(ResponseItem::Message {
                        id: id.clone(),
                        role: role.clone(),
                        content: filtered,
                        phase: None,
                    })
                }
            }
            other => Some(other.clone()),
        })
        .collect())
}

fn remove_oldest_until_token_budget(
    mut history: crate::context_manager::ContextManager,
    max_total_tokens: usize,
    turn: &TurnContext,
) -> (usize, Vec<ResponseItem>) {
    let mut removed = 0usize;
    loop {
        let estimate = history.estimate_token_count(turn).unwrap_or(0);
        if estimate <= max_total_tokens as i64 {
            break;
        }
        if history.raw_items().is_empty() {
            break;
        }
        history.remove_first_item();
        removed = removed.saturating_add(1);
    }
    (removed, history.raw_items().to_vec())
}

async fn apply_checkpoint(
    session: &Arc<Session>,
    turn: &Arc<TurnContext>,
    user_messages: &[String],
    summary_text: &str,
    max_user_message_tokens: usize,
    include_ghost_snapshots: bool,
) -> Result<(), FunctionCallError> {
    let initial_context = session.build_initial_context(turn.as_ref()).await;
    let new_history = build_compacted_history_with_limit(
        initial_context,
        user_messages,
        summary_text,
        max_user_message_tokens,
    );
    let _ = include_ghost_snapshots;
    session.replace_history(new_history, None).await;
    session.recompute_token_usage(turn).await;
    record_compaction(session, turn, summary_text).await;
    Ok(())
}

async fn apply_truncation(
    session: &Arc<Session>,
    turn: &Arc<TurnContext>,
    kept_items: Vec<ResponseItem>,
    history_snapshot: &[ResponseItem],
    include_ghost_snapshots: bool,
) -> Result<(), FunctionCallError> {
    let mut new_history = session.build_initial_context(turn.as_ref()).await;
    new_history.extend(kept_items);
    let _ = (history_snapshot, include_ghost_snapshots);
    session.replace_history(new_history, None).await;
    session.recompute_token_usage(turn).await;
    record_compaction(session, turn, "").await;
    Ok(())
}

async fn apply_filtered_history(
    session: &Arc<Session>,
    turn: &Arc<TurnContext>,
    kept_items: Vec<ResponseItem>,
    history_snapshot: &[ResponseItem],
    include_ghost_snapshots: bool,
) -> Result<(), FunctionCallError> {
    let mut new_history = session.build_initial_context(turn.as_ref()).await;
    new_history.extend(kept_items);
    let _ = (history_snapshot, include_ghost_snapshots);
    session.replace_history(new_history, None).await;
    session.recompute_token_usage(turn).await;
    record_compaction(session, turn, "").await;
    Ok(())
}

async fn record_compaction(session: &Arc<Session>, turn: &Arc<TurnContext>, summary_text: &str) {
    let compacted_item = CompactedItem {
        message: summary_text.to_string(),
        replacement_history: None,
    };
    session
        .persist_rollout_items(&[RolloutItem::Compacted(compacted_item)])
        .await;

    let event = EventMsg::ContextCompacted(ContextCompactedEvent {});
    session.send_event(turn, event).await;

    let warning = EventMsg::Warning(WarningEvent {
        message: "Heads up: Long conversations and multiple compactions can cause the model to be less accurate. Start a new conversation when possible to keep conversations small and targeted.".to_string(),
    });
    session.send_event(turn, warning).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn filter_by_role_removes_matching_messages() {
        let items = vec![
            ResponseItem::Message {
                id: None,
                role: "user".to_string(),
                content: vec![ContentItem::InputText {
                    text: "hi".to_string(),
                }],
                phase: None,
            },
            ResponseItem::Message {
                id: None,
                role: "assistant".to_string(),
                content: vec![ContentItem::OutputText {
                    text: "hello".to_string(),
                }],
                phase: None,
            },
        ];

        let filtered = filter_by_role(&items, "user");
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered, vec![items[1].clone()]);
    }

    #[test]
    fn filter_by_content_type_removes_matching_items() {
        let items = vec![ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![
                ContentItem::InputText {
                    text: "keep".to_string(),
                },
                ContentItem::InputImage {
                    image_url: "file:///tmp/image.png".to_string(),
                    detail: None,
                },
            ],
            phase: None,
        }];

        let filtered = filter_by_content_type(&items, "input_image").expect("filter");
        assert_eq!(filtered.len(), 1);
        let ResponseItem::Message { content, .. } = &filtered[0] else {
            panic!("expected message");
        };
        assert_eq!(content.len(), 1);
        assert!(matches!(content[0], ContentItem::InputText { .. }));
    }
}
