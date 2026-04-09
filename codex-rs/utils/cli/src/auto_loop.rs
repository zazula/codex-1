use std::collections::VecDeque;
use std::time::Duration;
use std::time::Instant;

use serde::Deserialize;

pub const AUTO_LOOP_CONTROL_MARKER: &str = "[[CODEX-CONTROL]]";
pub const LEGACY_AUTO_LOOP_CONTROL_MARKER: &str = "[CODEX-CONTROL]";
pub const AUTO_LOOP_HINT_HEADER: &str = "[auto-loop control]";

/// Hard ceiling on the number of auto-loop continuations allowed per session.
pub const DEFAULT_AUTO_LOOP_LIMIT: u32 = 25;

/// Rate limit on auto-loop continuations to avoid tight spin-loops.
pub const DEFAULT_AUTO_LOOP_RATE_LIMIT_PER_MINUTE: u32 = 10;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct AutoLoopControl {
    #[serde(default)]
    pub wants_continue: bool,
    #[serde(default)]
    pub user_message: Option<String>,
    #[serde(default)]
    pub note: Option<String>,
    #[serde(default)]
    pub delay_ms: Option<u64>,
    #[serde(default)]
    pub rebase: Option<bool>,
}

impl AutoLoopControl {
    pub fn desired_user_message(&self) -> String {
        self.user_message
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| "continue".to_string())
    }

    pub fn wants_rebase(&self) -> bool {
        self.rebase.unwrap_or(false)
    }
}

pub fn ensure_auto_loop_hint(existing: Option<&str>) -> String {
    match existing {
        Some(current) => {
            if current.is_empty() {
                auto_loop_hint_body()
            } else if current.contains(AUTO_LOOP_HINT_HEADER) {
                current.to_string()
            } else {
                let hint = auto_loop_hint_body();
                format!("{current}\n\n{hint}")
            }
        }
        None => auto_loop_hint_body(),
    }
}

fn auto_loop_hint_body() -> String {
    format!(
        "{AUTO_LOOP_HINT_HEADER}\nOnly emit `{AUTO_LOOP_CONTROL_MARKER} {{...}}` when the next concrete step in your existing plan still needs to run. When you need another automatic turn, append `{AUTO_LOOP_CONTROL_MARKER} {{\"wants_continue\": true, \"user_message\": \"<next user text>\", \"note\": \"status update\"}}` at the very end of your final response."
    )
}

#[derive(Debug, Clone)]
pub struct AutoLoopBudget {
    remaining: u32,
    per_minute: u32,
    recent: VecDeque<Instant>,
}

impl AutoLoopBudget {
    pub fn new(remaining: u32, per_minute: u32) -> Self {
        Self {
            remaining,
            per_minute,
            recent: VecDeque::new(),
        }
    }

    pub fn consume(&mut self, now: Instant) -> Result<(), String> {
        if self.remaining == 0 {
            return Err("continuation limit reached".to_string());
        }

        let window = Duration::from_secs(60);
        while let Some(front) = self.recent.front().copied() {
            if now.duration_since(front) > window {
                self.recent.pop_front();
            } else {
                break;
            }
        }

        if self.per_minute > 0 && self.recent.len() >= self.per_minute as usize {
            return Err("rate limit reached".to_string());
        }

        self.remaining -= 1;
        self.recent.push_back(now);
        Ok(())
    }
}

/// Returns a tuple of `(cleaned_message, control)`.
///
/// If a control stanza is present it is removed from the returned message, and
/// the parsed control payload is returned (when valid).
pub fn sanitize_final_message(
    message: Option<String>,
) -> (Option<String>, Option<AutoLoopControl>) {
    let Some(message) = message else {
        return (None, None);
    };

    let Some((marker_idx, marker)) = find_control_marker(&message) else {
        return (Some(message), None);
    };

    let (prefix, suffix) = message.split_at(marker_idx);
    let cleaned = prefix.trim_end().to_string();
    let suffix = suffix[marker.len()..].trim();
    if suffix.is_empty() {
        return (Some(cleaned), None);
    }

    let control = match parse_control_payload(suffix) {
        Some(control) => control,
        None => return (Some(cleaned), None),
    };

    if !control.wants_continue {
        return (Some(cleaned), None);
    }

    (Some(cleaned), Some(control))
}

fn find_control_marker(message: &str) -> Option<(usize, &'static str)> {
    let modern = message.rfind(AUTO_LOOP_CONTROL_MARKER);
    let legacy = find_last_standalone_legacy_marker(message);

    match (modern, legacy) {
        (Some(modern_idx), Some(legacy_idx)) => {
            if modern_idx >= legacy_idx {
                Some((modern_idx, AUTO_LOOP_CONTROL_MARKER))
            } else {
                Some((legacy_idx, LEGACY_AUTO_LOOP_CONTROL_MARKER))
            }
        }
        (Some(modern_idx), None) => Some((modern_idx, AUTO_LOOP_CONTROL_MARKER)),
        (None, Some(legacy_idx)) => Some((legacy_idx, LEGACY_AUTO_LOOP_CONTROL_MARKER)),
        (None, None) => None,
    }
}

fn find_last_standalone_legacy_marker(message: &str) -> Option<usize> {
    let mut search_end = message.len();

    while let Some(rel_idx) = message[..search_end].rfind(LEGACY_AUTO_LOOP_CONTROL_MARKER) {
        if !is_legacy_marker_nested_in_modern(message, rel_idx) {
            return Some(rel_idx);
        }
        search_end = rel_idx;
    }

    None
}

fn is_legacy_marker_nested_in_modern(message: &str, marker_idx: usize) -> bool {
    let Some(before) = marker_idx
        .checked_sub(1)
        .and_then(|idx| message.as_bytes().get(idx))
    else {
        return false;
    };
    let Some(after) = message
        .as_bytes()
        .get(marker_idx + LEGACY_AUTO_LOOP_CONTROL_MARKER.len())
    else {
        return false;
    };

    *before == b'[' && *after == b']'
}

fn parse_control_payload(payload: &str) -> Option<AutoLoopControl> {
    let json_start = payload.find('{')?;
    let json_end = payload.rfind('}')?;
    let json = payload.get(json_start..=json_end)?;
    serde_json::from_str(json).ok()
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn sanitize_final_message_without_control_is_unchanged() {
        let input = Some("hello".to_string());
        let (cleaned, control) = sanitize_final_message(input.clone());
        assert_eq!(cleaned, input);
        assert_eq!(control, None);
    }

    #[test]
    fn sanitize_final_message_strips_modern_control_and_parses_json() {
        let input = Some(
            "done\n\n[[CODEX-CONTROL]] {\"wants_continue\":true,\"user_message\":\"next\"}"
                .to_string(),
        );
        let (cleaned, control) = sanitize_final_message(input);
        assert_eq!(cleaned, Some("done".to_string()));
        assert_eq!(
            control,
            Some(AutoLoopControl {
                wants_continue: true,
                user_message: Some("next".to_string()),
                note: None,
                delay_ms: None,
                rebase: None,
            })
        );
    }

    #[test]
    fn sanitize_final_message_supports_legacy_marker() {
        let input = Some(
            "done\n\n[CODEX-CONTROL] {\"wants_continue\":true,\"user_message\":\"next\"}"
                .to_string(),
        );
        let (cleaned, control) = sanitize_final_message(input);
        assert_eq!(cleaned, Some("done".to_string()));
        assert_eq!(
            control.map(|c| c.desired_user_message()),
            Some("next".to_string())
        );
    }

    #[test]
    fn sanitize_final_message_tolerates_outer_brackets() {
        let input = Some(
            "done\n\n[[CODEX-CONTROL]] [{\"wants_continue\":true,\"user_message\":\"next\"}]"
                .to_string(),
        );
        let (cleaned, control) = sanitize_final_message(input);
        assert_eq!(cleaned, Some("done".to_string()));
        assert!(control.is_some());
    }

    #[test]
    fn find_control_marker_ignores_legacy_embedded_in_modern_marker() {
        let input = "done\n\n[[CODEX-CONTROL]] {\"wants_continue\":true}";
        let marker = find_control_marker(input);
        assert_eq!(marker, Some((6, AUTO_LOOP_CONTROL_MARKER)));
    }

    #[test]
    fn desired_user_message_defaults_to_continue() {
        let control = AutoLoopControl {
            wants_continue: true,
            user_message: Some("   ".to_string()),
            note: None,
            delay_ms: None,
            rebase: None,
        };

        assert_eq!(control.desired_user_message(), "continue");
    }

    #[test]
    fn ensure_auto_loop_hint_is_idempotent() {
        let once = ensure_auto_loop_hint(None);
        let twice = ensure_auto_loop_hint(Some(&once));

        assert_eq!(twice, once);
        assert!(once.contains(AUTO_LOOP_HINT_HEADER));
    }
}
