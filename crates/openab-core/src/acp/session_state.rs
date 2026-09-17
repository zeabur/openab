//! Runtime-owned session facts. Transports and clients only read this projection.
use serde_json::{json, Value};
use std::sync::Mutex;

pub struct SessionState(Mutex<Value>);

impl Default for SessionState {
    fn default() -> Self {
        Self(Mutex::new(json!({
            "epoch": uuid::Uuid::new_v4().to_string(), "revision": 0,
            "state": "unknown", "providerState": "unknown", "operation": "none",
            "tools": {}, "asyncTasks": {}, "providerDetails": null, "permissionWaits": 0, "activity": "working"
        })))
    }
}

impl SessionState {
    /// A successful ACP creation/load acknowledges a session ready for its first
    /// prompt. Do not overwrite a lifecycle notification already received.
    pub fn initialized(&self) {
        self.change(|snapshot| {
            if snapshot["state"] == "unknown" && snapshot["providerState"] == "unknown" {
                snapshot["state"] = json!("idle");
                snapshot["providerState"] = json!("idle");
                snapshot["providerDetails"] = json!({"source":"session_initialized"});
            }
        });
    }
    pub fn snapshot(&self) -> Value {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn change(&self, update: impl FnOnce(&mut Value)) {
        let mut snapshot = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let previous = snapshot.clone();
        update(&mut snapshot);
        if *snapshot != previous {
            snapshot["revision"] = json!(previous["revision"].as_u64().unwrap_or(0) + 1);
        }
    }

    pub fn set(&self, state: &str) {
        self.change(|snapshot| {
            snapshot["state"] = json!(state);
            if state == "interrupted" {
                snapshot["operation"] = json!("none");
                snapshot["permissionWaits"] = json!(0);
            }
            snapshot["providerState"] = json!(state);
        });
    }

    pub fn operation(&self, operation: &str) {
        self.change(|snapshot| {
            snapshot["operation"] = json!(operation);
            if operation == "prompt" {
                // Keep background tools visible across turns, but reset model activity.
                snapshot["activity"] = json!("working");
            }
        });
    }

    pub fn permission_wait(&self, waiting: bool) {
        self.change(|snapshot| {
            let count = snapshot["permissionWaits"].as_u64().unwrap_or(0);
            snapshot["permissionWaits"] = json!(if waiting {
                count + 1
            } else {
                count.saturating_sub(1)
            });
        });
    }

    /// Stamp provenance at the native reader, before prompt/idle routes can change.
    pub fn observe_and_stamp(&self, params: Option<&mut Value>) {
        let Some(params) = params else { return };
        let previous = params
            .pointer("/update/toolCallId")
            .and_then(Value::as_str)
            .map(|id| self.snapshot()["tools"][id].clone());
        self.observe(Some(params));
        let Some(update) = params.get_mut("update") else {
            return;
        };
        let snapshot = self.snapshot();
        let (record, background) = match update["sessionUpdate"].as_str() {
            Some("async_task_spawned" | "async_task_state_update") => {
                let Some(id) = update["asyncTaskId"].as_str() else {
                    return;
                };
                (snapshot["asyncTasks"][id].clone(), false)
            }
            Some("tool_call" | "tool_call_update") => {
                let Some(id) = update["toolCallId"].as_str() else {
                    return;
                };
                let current = &snapshot["tools"][id];
                let record = if current.is_null() {
                    previous.unwrap_or(Value::Null)
                } else {
                    current.clone()
                };
                let explicit_task = snapshot["asyncTasks"]
                    .as_object()
                    .is_some_and(|tasks| tasks.values().any(|task| task["toolCallId"] == id));
                let background = record["background"] == true && !explicit_task;
                (record, background)
            }
            _ => return,
        };
        // Never trust provenance supplied by the subprocess itself.
        if !update["_meta"].is_object() {
            update["_meta"] = json!({});
        }
        update["_meta"]["dev.openab/taskToken"] = record["ownershipToken"].clone();
        update["_meta"]["dev.openab/taskEpoch"] = snapshot["epoch"].clone();
        update["_meta"]["dev.openab/backgroundTool"] = json!(background);
    }

    pub fn observe(&self, params: Option<&Value>) {
        let Some(update) = params.and_then(|p| p.get("update")) else {
            return;
        };
        self.change(|snapshot| {
            match update["sessionUpdate"].as_str() {
                Some("session_info_update") => {
                    let provider = update
                        .pointer("/_meta/ai.nuphos~1sessionState/state")
                        .or_else(|| update.pointer("/_meta/codex/threadStatus/type"))
                        .and_then(Value::as_str);
                    if let Some(details) = update
                        .pointer("/_meta/codex/threadStatus")
                        .or_else(|| update.pointer("/_meta/ai.nuphos~1sessionState"))
                    {
                        snapshot["providerDetails"] = details.clone();
                    }
                    if let Some(provider) = provider {
                        // Preserve unfamiliar provider states instead of silently hiding them.
                        snapshot["providerState"] = json!(provider);
                        if provider == "idle" {
                            for tool in snapshot["tools"].as_object_mut().unwrap().values_mut() {
                                tool["background"] = json!(true);
                            }
                        }
                        if provider == "idle" && snapshot["operation"] == "cancelling" {
                            snapshot["operation"] = json!("none");
                        }
                        snapshot["state"] = json!(match provider {
                            "active" => "active",
                            "idle" => "idle",
                            "notLoaded" | "systemError" | "interrupted" => "interrupted",
                            _ => "unknown",
                        });
                    }
                }
                Some("async_task_spawned" | "async_task_state_update") => {
                    if let Some(id) = update["asyncTaskId"].as_str() {
                        let tasks = snapshot["asyncTasks"].as_object_mut().unwrap();
                        let task = tasks.entry(id).or_insert_with(|| json!({"id":id}));
                        for key in ["state", "name", "toolCallId"] {
                            if let Some(v) = update[key].as_str() {
                                task[key] = json!(v);
                            }
                        }
                        if update["sessionUpdate"] == "async_task_spawned" {
                            if task["state"] != "running" || !task["ownershipToken"].is_string() {
                                task["ownershipToken"] = json!(uuid::Uuid::new_v4().to_string());
                            }
                            task["state"] = json!("running");
                        }
                    }
                }
                Some("agent_thought_chunk") => snapshot["activity"] = json!("thinking"),
                Some("agent_message_chunk") => snapshot["activity"] = json!("responding"),
                Some("tool_call" | "tool_call_update") => {
                    if let Some(id) = update["toolCallId"].as_str() {
                        let status = update["status"].as_str();
                        if matches!(
                            status,
                            Some("completed" | "failed" | "cancelled" | "stopped")
                        ) {
                            snapshot["tools"].as_object_mut().unwrap().remove(id);
                        } else {
                            let tools = snapshot["tools"].as_object_mut().unwrap();
                            // Only open calls and updates for known calls describe live work.
                            if update["sessionUpdate"] == "tool_call" || tools.contains_key(id) {
                                let tool = tools
                                    .entry(id)
                                    .or_insert_with(|| json!({"id": id, "status": "pending", "ownershipToken":uuid::Uuid::new_v4().to_string()}));
                                for field in ["status", "title", "kind"] {
                                    if let Some(value) = update[field].as_str() {
                                        tool[field] = json!(value);
                                    }
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn initialization_ack_does_not_override_an_earlier_native_lifecycle() {
        let new_session = SessionState::default();
        new_session.initialized();
        assert_eq!(new_session.snapshot()["state"], "idle");
        let resumed = SessionState::default();
        resumed.set("active");
        let before = resumed.snapshot();
        resumed.initialized();
        assert_eq!(resumed.snapshot(), before);
    }
    #[test]
    fn background_tools_remain_visible_without_reopening_execution() {
        let state = SessionState::default();
        state.set("active");
        state.observe(Some(&json!({"update":{"sessionUpdate":"tool_call","toolCallId":"bg","status":"in_progress"}})));
        state.set("idle");
        assert_eq!(state.snapshot()["state"], "idle");
        assert_eq!(state.snapshot()["tools"]["bg"]["status"], "in_progress");
        state.observe(Some(&json!({"update":{"sessionUpdate":"tool_call_update","toolCallId":"bg","status":"completed"}})));
        assert_eq!(state.snapshot()["state"], "idle");
        assert!(state.snapshot()["tools"].as_object().unwrap().is_empty());
    }
    #[test]
    fn provider_idle_does_not_hide_a_pending_prompt_or_permission() {
        let state = SessionState::default();
        state.operation("prompt");
        state.permission_wait(true);
        state.set("idle");
        let waiting = state.snapshot();
        assert_eq!(waiting["operation"], "prompt");
        assert_eq!(waiting["permissionWaits"], 1);
        state.permission_wait(false);
        state.operation("none");
        assert!(state.snapshot()["revision"].as_u64() > waiting["revision"].as_u64());
    }
    #[test]
    fn unfamiliar_provider_state_is_retained_and_revision_is_monotonic() {
        let state = SessionState::default();
        let first = state.snapshot();
        state.observe(Some(&json!({"update":{"sessionUpdate":"session_info_update","_meta":{"codex":{"threadStatus":{"type":"waiting_for_input"}}}}})));
        let next = state.snapshot();
        assert_eq!(next["providerState"], "waiting_for_input");
        assert_eq!(next["state"], "unknown");
        assert_eq!(next["revision"], 1);
        assert_eq!(first["epoch"], next["epoch"]);
        assert_ne!(next["epoch"], SessionState::default().snapshot()["epoch"]);
    }
    #[test]
    fn native_task_provenance_survives_turns_and_changes_on_id_reuse() {
        let state = SessionState::default();
        let mut spawned = json!({"update":{"sessionUpdate":"async_task_spawned","asyncTaskId":"t","_meta":{"dev.openab/taskToken":"forged"}}});
        state.observe_and_stamp(Some(&mut spawned));
        let token = spawned["update"]["_meta"]["dev.openab/taskToken"].clone();
        assert!(token.is_string());
        assert_ne!(token, "forged");
        state.operation("prompt");
        let mut done = json!({"update":{"sessionUpdate":"async_task_state_update","asyncTaskId":"t","state":"completed"}});
        state.observe_and_stamp(Some(&mut done));
        assert_eq!(done["update"]["_meta"]["dev.openab/taskToken"], token);
        state.observe_and_stamp(Some(&mut spawned));
        assert_ne!(spawned["update"]["_meta"]["dev.openab/taskToken"], token);
        let mut unknown = json!({"update":{"sessionUpdate":"async_task_state_update","asyncTaskId":"unknown","state":"completed","_meta":{"dev.openab/taskToken":"forged"}}});
        state.observe_and_stamp(Some(&mut unknown));
        assert!(unknown["update"]["_meta"]["dev.openab/taskToken"].is_null());
    }
    #[test]
    fn native_background_tool_completion_keeps_provenance_after_removal() {
        let state = SessionState::default();
        let mut start = json!({"update":{"sessionUpdate":"tool_call","toolCallId":"exec","status":"in_progress"}});
        state.observe_and_stamp(Some(&mut start));
        let token = start["update"]["_meta"]["dev.openab/taskToken"].clone();
        assert!(token.is_string());
        state.observe(Some(&json!({"update":{"sessionUpdate":"session_info_update","_meta":{"codex":{"threadStatus":{"type":"idle"}}}}})));
        state.operation("prompt");
        let mut done = json!({"update":{"sessionUpdate":"tool_call_update","toolCallId":"exec","status":"completed"}});
        state.observe_and_stamp(Some(&mut done));
        assert_eq!(done["update"]["_meta"]["dev.openab/taskToken"], token);
        assert_eq!(done["update"]["_meta"]["dev.openab/backgroundTool"], true);
        assert!(state.snapshot()["tools"].as_object().unwrap().is_empty());
        state.observe_and_stamp(Some(&mut done));
        assert!(done["update"]["_meta"]["dev.openab/taskToken"].is_null());
        assert_eq!(done["update"]["_meta"]["dev.openab/backgroundTool"], false);
    }
}
