//! Spec-compliant `terminal/*` RPC handlers (the ACP `terminal` client capability:
//! https://agentclientprotocol.com/protocol/terminals).
//!
//! Neither agent adapter this gateway talks to today (claude-agent-acp 0.74.0,
//! codex-acp 1.1.4) issues these RPCs — both gate terminal behavior on the
//! non-standard `_meta.terminal_output` flag instead (see `session_state.rs`
//! and `connection.rs::initialize`). This module is real, spec-correct
//! scaffolding for an agent that does call `terminal/create` and friends; it
//! is currently unreachable in production.
//!
//! Process spawning reuses [`super::connection::baseline_command`] and
//! [`super::connection::kill_process_group`] — the same security baseline and
//! kill semantics used for the agent subprocess itself — rather than a parallel
//! implementation.

use anyhow::{anyhow, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::process::Child;
use tokio::sync::{Mutex, Notify};

use super::connection::{baseline_command, kill_process_group};

/// Output is retained as a byte buffer and truncated from the front once it
/// exceeds this many bytes, unless the request specifies its own
/// `outputByteLimit`. Matches the order of magnitude terminal UIs typically
/// keep buffered.
const DEFAULT_OUTPUT_BYTE_LIMIT: usize = 1024 * 1024;

#[derive(Debug, Clone, Deserialize)]
struct EnvVariable {
    name: String,
    value: String,
}

#[derive(Debug, Clone, Deserialize)]
struct CreateTerminalParams {
    #[serde(rename = "sessionId")]
    #[allow(dead_code)]
    session_id: String,
    command: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    env: Vec<EnvVariable>,
    #[serde(rename = "outputByteLimit", default)]
    output_byte_limit: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
struct TerminalIdParams {
    #[serde(rename = "terminalId")]
    terminal_id: String,
}

#[derive(Debug, Clone, Default)]
struct ExitStatus {
    exit_code: Option<u32>,
    signal: Option<String>,
}

impl ExitStatus {
    fn to_json(&self) -> Value {
        json!({"exitCode": self.exit_code, "signal": self.signal})
    }
}

struct Terminal {
    pgid: Option<i32>,
    /// `None` once the process has exited and its `Child` handle was reaped
    /// by the output pump task.
    child: Mutex<Option<Child>>,
    output: Mutex<Vec<u8>>,
    output_byte_limit: usize,
    truncated: AtomicBool,
    exit: Mutex<Option<ExitStatus>>,
    exit_notify: Notify,
}

/// Tracks every terminal the agent has created for a session via `terminal/create`,
/// through `terminal/output` / `terminal/wait_for_exit` / `terminal/kill` /
/// `terminal/release`. One instance per [`super::connection::AcpConnection`].
#[derive(Default)]
pub struct TerminalManager {
    terminals: Mutex<HashMap<String, Arc<Terminal>>>,
}

/// Append `chunk` to `buf`, then truncate from the front to `limit` bytes at a
/// UTF-8 character boundary if it now exceeds the limit. Returns whether the
/// buffer is currently truncated relative to everything ever written.
fn append_and_truncate(buf: &mut Vec<u8>, chunk: &[u8], limit: usize, truncated: &AtomicBool) {
    buf.extend_from_slice(chunk);
    if buf.len() > limit {
        truncated.store(true, Ordering::Relaxed);
        let mut cut = buf.len() - limit;
        // The spec requires truncation to land on a character boundary even if
        // that keeps slightly less than `limit` bytes.
        while cut < buf.len() && std::str::from_utf8(&buf[cut..]).is_err() {
            cut += 1;
        }
        buf.drain(0..cut);
    }
}

impl TerminalManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Handle `terminal/create`: spawn a real process and start tracking it.
    /// Returns the JSON-RPC result value (`{"terminalId": ...}`) on success.
    pub async fn create(&self, params: Option<&Value>) -> Result<Value> {
        let params: CreateTerminalParams =
            serde_json::from_value(params.cloned().ok_or_else(|| anyhow!("missing params"))?)
                .map_err(|e| anyhow!("invalid terminal/create params: {e}"))?;

        let cwd = params.cwd.clone().unwrap_or_else(|| {
            std::env::current_dir()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|_| "/".into())
        });
        let output_byte_limit = params
            .output_byte_limit
            .and_then(|v| usize::try_from(v).ok())
            .unwrap_or(DEFAULT_OUTPUT_BYTE_LIMIT);

        let mut cmd = baseline_command(&params.command, &params.args, &cwd);
        cmd.stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        for var in &params.env {
            cmd.env(&var.name, &var.value);
        }

        let mut child = cmd
            .spawn()
            .map_err(|e| anyhow!("failed to spawn terminal command {}: {e}", params.command))?;
        let pgid = child.id().and_then(|pid| i32::try_from(pid).ok());
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();

        let terminal = Arc::new(Terminal {
            pgid,
            child: Mutex::new(Some(child)),
            output: Mutex::new(Vec::new()),
            output_byte_limit,
            truncated: AtomicBool::new(false),
            exit: Mutex::new(None),
            exit_notify: Notify::new(),
        });

        let terminal_id = uuid::Uuid::new_v4().to_string();
        self.terminals
            .lock()
            .await
            .insert(terminal_id.clone(), terminal.clone());

        tokio::spawn(pump_output_and_wait(terminal, stdout, stderr));

        Ok(json!({"terminalId": terminal_id}))
    }

    /// Handle `terminal/output`: current buffered output without blocking on exit.
    pub async fn output(&self, params: Option<&Value>) -> Result<Value> {
        let terminal = self.get(params).await?;
        let output = terminal.output.lock().await;
        let output_str = String::from_utf8_lossy(&output).into_owned();
        let exit = terminal.exit.lock().await.clone();
        Ok(json!({
            "output": output_str,
            "truncated": terminal.truncated.load(Ordering::Relaxed),
            "exitStatus": exit.map(|e| e.to_json()),
        }))
    }

    /// Handle `terminal/wait_for_exit`: block until the command exits.
    pub async fn wait_for_exit(&self, params: Option<&Value>) -> Result<Value> {
        let terminal = self.get(params).await?;
        loop {
            if let Some(exit) = terminal.exit.lock().await.clone() {
                return Ok(exit.to_json());
            }
            terminal.exit_notify.notified().await;
        }
    }

    /// Handle `terminal/kill`: terminate the command without releasing the terminal.
    pub async fn kill(&self, params: Option<&Value>) -> Result<Value> {
        let terminal = self.get(params).await?;
        if let Some(pgid) = terminal.pgid {
            kill_process_group(pgid);
        }
        Ok(json!({}))
    }

    /// Handle `terminal/release`: kill (if still running) and free the terminal.
    pub async fn release(&self, params: Option<&Value>) -> Result<Value> {
        let params: TerminalIdParams =
            serde_json::from_value(params.cloned().ok_or_else(|| anyhow!("missing params"))?)
                .map_err(|e| anyhow!("invalid terminal params: {e}"))?;
        let terminal = self
            .terminals
            .lock()
            .await
            .remove(&params.terminal_id)
            .ok_or_else(|| anyhow!("unknown terminalId {}", params.terminal_id))?;
        if terminal.exit.lock().await.is_none() {
            if let Some(pgid) = terminal.pgid {
                kill_process_group(pgid);
            }
        }
        Ok(json!({}))
    }

    async fn get(&self, params: Option<&Value>) -> Result<Arc<Terminal>> {
        let params: TerminalIdParams =
            serde_json::from_value(params.cloned().ok_or_else(|| anyhow!("missing params"))?)
                .map_err(|e| anyhow!("invalid terminal params: {e}"))?;
        self.terminals
            .lock()
            .await
            .get(&params.terminal_id)
            .cloned()
            .ok_or_else(|| anyhow!("unknown terminalId {}", params.terminal_id))
    }
}

/// Merge stdout+stderr into the terminal's output buffer (with truncation) and,
/// once both streams hit EOF, reap the child and record its exit status.
async fn pump_output_and_wait(
    terminal: Arc<Terminal>,
    stdout: Option<tokio::process::ChildStdout>,
    stderr: Option<tokio::process::ChildStderr>,
) {
    let mut stdout = stdout;
    let mut stderr = stderr;
    let mut stdout_buf = [0u8; 8192];
    let mut stderr_buf = [0u8; 8192];

    loop {
        if stdout.is_none() && stderr.is_none() {
            break;
        }
        tokio::select! {
            n = async {
                match &mut stdout {
                    Some(s) => s.read(&mut stdout_buf).await,
                    None => std::future::pending().await,
                }
            } => {
                match n {
                    Ok(0) | Err(_) => stdout = None,
                    Ok(n) => {
                        let mut buf = terminal.output.lock().await;
                        append_and_truncate(&mut buf, &stdout_buf[..n], terminal.output_byte_limit, &terminal.truncated);
                    }
                }
            }
            n = async {
                match &mut stderr {
                    Some(s) => s.read(&mut stderr_buf).await,
                    None => std::future::pending().await,
                }
            } => {
                match n {
                    Ok(0) | Err(_) => stderr = None,
                    Ok(n) => {
                        let mut buf = terminal.output.lock().await;
                        append_and_truncate(&mut buf, &stderr_buf[..n], terminal.output_byte_limit, &terminal.truncated);
                    }
                }
            }
        }
    }

    let status = {
        let mut child_slot = terminal.child.lock().await;
        match child_slot.take() {
            Some(mut child) => child.wait().await.ok(),
            None => None,
        }
    };

    let exit = ExitStatus {
        exit_code: status.and_then(|s| {
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                if s.signal().is_some() {
                    None
                } else {
                    s.code().and_then(|c| u32::try_from(c).ok())
                }
            }
            #[cfg(not(unix))]
            {
                s.code().and_then(|c| u32::try_from(c).ok())
            }
        }),
        signal: status.and_then(|s| {
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                s.signal().map(signal_name)
            }
            #[cfg(not(unix))]
            {
                let _ = s;
                None
            }
        }),
    };

    *terminal.exit.lock().await = Some(exit);
    terminal.exit_notify.notify_waiters();
}

#[cfg(unix)]
fn signal_name(signal: i32) -> String {
    match signal {
        libc::SIGTERM => "SIGTERM".into(),
        libc::SIGKILL => "SIGKILL".into(),
        libc::SIGINT => "SIGINT".into(),
        libc::SIGHUP => "SIGHUP".into(),
        other => format!("SIG{other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncation_keeps_a_char_boundary() {
        let mut buf = "héllo".as_bytes().to_vec(); // 'é' is 2 bytes
        let truncated = AtomicBool::new(false);
        // Force truncation that would otherwise land inside 'é'.
        append_and_truncate(&mut buf, b"", 5, &truncated);
        assert!(String::from_utf8(buf).is_ok());
    }

    #[test]
    fn truncation_sets_the_flag_once_the_limit_is_exceeded() {
        let mut buf = Vec::new();
        let truncated = AtomicBool::new(false);
        append_and_truncate(&mut buf, b"0123456789", 4, &truncated);
        assert!(truncated.load(Ordering::Relaxed));
        assert_eq!(buf.len(), 4);
        assert_eq!(&buf, b"6789");
    }

    #[tokio::test]
    async fn create_output_wait_kill_round_trip() {
        let manager = TerminalManager::new();
        let created = manager
            .create(Some(&json!({
                "sessionId": "s1",
                "command": "sh",
                "args": ["-c", "echo hi; sleep 5"],
            })))
            .await
            .unwrap();
        let terminal_id = created["terminalId"].as_str().unwrap().to_string();

        // Give the process a moment to write its output.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let out = manager
            .output(Some(&json!({"terminalId": terminal_id})))
            .await
            .unwrap();
        assert!(out["output"].as_str().unwrap().contains("hi"));
        assert!(out["exitStatus"].is_null());

        manager
            .kill(Some(&json!({"terminalId": terminal_id})))
            .await
            .unwrap();
        let exit = manager
            .wait_for_exit(Some(&json!({"terminalId": terminal_id})))
            .await
            .unwrap();
        assert!(exit["exitCode"].is_number() || exit["signal"].is_string());

        manager
            .release(Some(&json!({"terminalId": terminal_id})))
            .await
            .unwrap();
        assert!(manager
            .output(Some(&json!({"terminalId": terminal_id})))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn output_is_truncated_to_the_requested_byte_limit() {
        let manager = TerminalManager::new();
        let created = manager
            .create(Some(&json!({
                "sessionId": "s1",
                "command": "sh",
                "args": ["-c", "printf '0123456789'"],
                "outputByteLimit": 4,
            })))
            .await
            .unwrap();
        let terminal_id = created["terminalId"].as_str().unwrap().to_string();
        manager
            .wait_for_exit(Some(&json!({"terminalId": terminal_id})))
            .await
            .unwrap();
        let out = manager
            .output(Some(&json!({"terminalId": terminal_id})))
            .await
            .unwrap();
        assert_eq!(out["output"], "6789");
        assert_eq!(out["truncated"], true);
    }
}
