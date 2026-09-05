//! Control requests must reach the existing agent and never fall back to prompts.
#![cfg(unix)]
use openab_core::acp::SessionPool;
use openab_core::config::AgentConfig;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn strict_control_is_session_scoped_and_acknowledged() {
    let tmp = tempfile::tempdir().unwrap();
    // This integration-test binary contains one test; isolate pool persistence.
    std::env::set_var("HOME", tmp.path());
    let script = tmp.path().join("agent.sh");
    let record = tmp.path().join("calls");
    std::fs::write(&script, r##"#!/bin/sh
while IFS= read -r line; do
  printf '%s\n' "$line" >> "$RECORD"
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"initialize"'*) result='{"agentInfo":{"name":"fixture"},"agentCapabilities":{}}';;
    *'"session/new"'*) result='{"sessionId":"inner","configOptions":[{"id":"model","name":"Model","type":"select","currentValue":"a","options":[{"name":"A","value":"a"},{"name":"B","value":"b"},{"name":"Reject","value":"reject"}]}]}';;
    *'"session/set_config_option"'*'"value":"reject"'*) printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32602,"message":"rejected"}}\n' "$id"; continue;;
    *'"session/set_config_option"'*) result='{"configOptions":[{"id":"model","name":"Model","type":"select","currentValue":"b","options":[{"name":"B","value":"b"},{"name":"Reject","value":"reject"}]}]}';;
    *) result='{}';;
  esac
  printf '{"jsonrpc":"2.0","id":%s,"result":%s}\n' "$id" "$result"
done
"##).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    let config = AgentConfig {
        command: script.to_string_lossy().into(),
        args: vec![],
        working_dir: tmp.path().to_string_lossy().into(),
        env: HashMap::from([("RECORD".into(), record.to_string_lossy().into())]),
        inherit_env: vec![],
        command_explicit: true,
    };
    let pool = Arc::new(SessionPool::new(config, 4, 60, HashMap::new()));
    assert_eq!(
        pool.session_config_options("missing", None)
            .await
            .unwrap_err()
            .0,
        -32004
    );
    pool.get_or_create("first", None, &[], None).await.unwrap();
    pool.get_or_create("second", None, &[], None).await.unwrap();
    let value = pool
        .session_config_options("first", Some(("model", "b")))
        .await
        .unwrap();
    assert_eq!(value["configOptions"][0]["currentValue"], "b");
    assert_eq!(
        pool.session_config_options("first", None).await.unwrap(),
        value
    );
    assert_eq!(
        pool.session_config_options("second", None).await.unwrap()["configOptions"][0]
            ["currentValue"],
        "a"
    );
    assert_eq!(
        pool.session_config_options("first", Some(("model", "invalid")))
            .await
            .unwrap_err()
            .0,
        -32602
    );
    let (started, start) = tokio::sync::oneshot::channel();
    let (release, finish) = tokio::sync::oneshot::channel();
    let busy_pool = pool.clone();
    let busy = tokio::spawn(async move {
        busy_pool
            .with_connection("first", |_conn| {
                Box::pin(async move {
                    started.send(()).unwrap();
                    finish.await.unwrap();
                    Ok(())
                })
            })
            .await
    });
    start.await.unwrap();
    let result = tokio::time::timeout(
        Duration::from_millis(200),
        pool.session_config_options("first", Some(("model", "b"))),
    )
    .await
    .unwrap();
    assert_eq!(result.unwrap_err().0, -32005);
    release.send(()).unwrap();
    busy.await.unwrap().unwrap();
    assert_eq!(
        pool.session_config_options("first", Some(("model", "reject")))
            .await
            .unwrap_err()
            .0,
        -32603
    );
    assert_eq!(
        pool.session_config_options("first", None).await.unwrap()["configOptions"],
        serde_json::json!([])
    );
    let calls = std::fs::read_to_string(record).unwrap();
    assert!(!calls.contains("session/prompt"));
    assert!(!calls.contains("invalid"));
}
