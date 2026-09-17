//! Execution state belongs to the provider process, never to a WebSocket or tool stream.
use serde_json::{json, Value};
use std::sync::Mutex;

pub struct SessionState(Mutex<Value>);

impl Default for SessionState {
    fn default() -> Self {
        Self(Mutex::new(json!({
            "epoch": uuid::Uuid::new_v4().to_string(), "revision": 0,
            "state": "unknown"
        })))
    }
}

impl SessionState {
    pub fn snapshot(&self) -> Value {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub fn set(&self, state: &str) {
        let mut snapshot = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if snapshot["state"] != state {
            let revision = snapshot["revision"].as_u64().unwrap_or(0) + 1;
            snapshot["revision"] = json!(revision);
            snapshot["state"] = json!(state);
        }
    }

    pub fn observe(&self, params: Option<&Value>) {
        let Some(update) = params.and_then(|p| p.get("update")) else {
            return;
        };
        if update["sessionUpdate"] != "session_info_update" {
            return;
        }
        let state = update
            .pointer("/_meta/ai.nuphos~1sessionState/state")
            .or_else(|| update.pointer("/_meta/codex/threadStatus/type"))
            .and_then(Value::as_str);
        match state {
            Some("active") => self.set("active"),
            Some("idle") => self.set("idle"),
            Some("notLoaded" | "systemError" | "interrupted") => self.set("interrupted"),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn late_tool_results_cannot_create_or_extend_execution() {
        let state = SessionState::default();
        state.observe(Some(&json!({"update":{"sessionUpdate":"session_info_update", "_meta":{"codex":{"threadStatus":{"type":"idle"}}}}})));
        let idle = state.snapshot();
        state.observe(Some(
            &json!({"update":{"sessionUpdate":"tool_call_update","status":"failed"}}),
        ));
        assert_eq!(state.snapshot(), idle);
    }
    #[test]
    fn provider_state_has_a_process_epoch_and_monotonic_revision() {
        let state = SessionState::default();
        let first = state.snapshot();
        state.set("active");
        state.set("idle");
        let last = state.snapshot();
        assert_eq!(first["epoch"], last["epoch"]);
        assert_eq!(last["revision"], 2);
        assert_ne!(last["epoch"], SessionState::default().snapshot()["epoch"]);
    }
}
