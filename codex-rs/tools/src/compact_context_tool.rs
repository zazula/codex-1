use crate::JsonSchema;
use crate::ResponsesApiTool;
use crate::ToolSpec;
use std::collections::BTreeMap;

pub fn create_compact_context_tool() -> ToolSpec {
    let properties = BTreeMap::from([
        (
            "strategy".to_string(),
            JsonSchema::string(Some(
                "Compaction strategy: inspect, extract_recent, checkpoint, truncate, remove_by_role, remove_by_content_type, remove_until_token_budget.".to_string(),
            )),
        ),
        (
            "summary".to_string(),
            JsonSchema::string(Some("Summary for checkpoint strategy.".to_string())),
        ),
        (
            "max_user_message_tokens".to_string(),
            JsonSchema::number(Some("Max tokens for user messages.".to_string())),
        ),
        (
            "max_total_tokens".to_string(),
            JsonSchema::number(Some("Target max tokens.".to_string())),
        ),
        (
            "role".to_string(),
            JsonSchema::string(Some("Role for remove_by_role.".to_string())),
        ),
        (
            "content_type".to_string(),
            JsonSchema::string(Some(
                "Content type: input_text, output_text, input_image.".to_string(),
            )),
        ),
        (
            "keep_last_items".to_string(),
            JsonSchema::number(Some("Items to keep for truncate.".to_string())),
        ),
        (
            "include_summary_prefix".to_string(),
            JsonSchema::boolean(Some("Include summary prefix.".to_string())),
        ),
        (
            "include_recent_user_messages".to_string(),
            JsonSchema::boolean(Some("Include recent user messages.".to_string())),
        ),
        (
            "include_ghost_snapshots".to_string(),
            JsonSchema::boolean(Some("Preserve ghost snapshots.".to_string())),
        ),
        (
            "apply".to_string(),
            JsonSchema::boolean(Some("Apply the compaction.".to_string())),
        ),
    ]);

    ToolSpec::Function(ResponsesApiTool {
        name: "compact_context".to_string(),
        description:
            "Compact conversation context to manage tokens. Set apply=true to make changes."
                .to_string(),
        strict: false,
        defer_loading: None,
        parameters: JsonSchema::object(
            properties,
            Some(vec!["strategy".to_string()]),
            Some(false.into()),
        ),
        output_schema: None,
    })
}
