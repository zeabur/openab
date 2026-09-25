//! Operator-driven device sign-in for a runtime nobody provisioned.
//!
//! A runtime an operator started by hand has no pod template, no projected Secret and no
//! exec channel, so the credential its provider CLI mints interactively cannot be seeded
//! from outside. `_openab/runtime/login` runs a configured command inside this container
//! and streams its NDJSON stdout back to the operator as notifications, so a remote
//! application can render the device prompt without ever holding the credential.
//!
//! OpenAB stays provider-agnostic: it knows only how to run
//! `OPENAB_RUNTIME_LOGIN_COMMAND`, relay whatever JSON objects it prints, and hand it
//! the lines the operator sends back through `_openab/runtime/login/input`.

use serde_json::{json, Value};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tracing::{info, warn};

/// Cap on one NDJSON frame from the login command, matching the operator-side parser.
const MAX_LOGIN_FRAME_BYTES: usize = 128 * 1024;
/// A device prompt nobody answers must release the slot rather than hold it forever.
const LOGIN_TIMEOUT_SECS: u64 = 15 * 60;
/// How long the pipe may still be drained after the command's group has been killed.
const RELAY_DRAIN_SECS: u64 = 5;
/// Cap on one line of operator input, such as an authorization code pasted back.
const MAX_LOGIN_INPUT_BYTES: usize = 4096;
/// Lines accepted ahead of a command that is not reading them.
const LOGIN_INPUT_QUEUE: usize = 4;
/// Notification carrying one frame the login command printed.
pub const LOGIN_FRAME_METHOD: &str = "_openab/runtime/login/frame";

/// Another sign-in already owns the runtime's single login slot.
pub const LOGIN_BUSY: i32 = -32005;
/// No `OPENAB_RUNTIME_LOGIN_COMMAND` is configured for this runtime.
pub const LOGIN_UNSUPPORTED: i32 = -32601;
/// The command could not run, or ended without signing in.
pub const LOGIN_FAILED: i32 = -32006;
/// No sign-in with this `attemptId` is running, so there is nothing to hand input to.
pub const LOGIN_NOT_RUNNING: i32 = -32008;

/// The program and arguments `OPENAB_RUNTIME_LOGIN_COMMAND` names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoginCommand {
    pub program: String,
    pub args: Vec<String>,
}

/// Whitespace-separated argv. Deliberately not a shell: the value is operator
/// configuration for a fixed command, and a shell would turn it into an execution
/// surface for anything that can write the environment.
pub fn parse_login_command(raw: &str) -> Option<LoginCommand> {
    let mut parts = raw.split_whitespace().map(str::to_string);
    let program = parts.next()?;
    Some(LoginCommand {
        program,
        args: parts.collect(),
    })
}

/// An `attemptId` is echoed back to the operator on every frame, so it must be safe to
/// place in a log line and small enough not to be a payload of its own.
pub fn valid_attempt_id(attempt: &str) -> bool {
    !attempt.is_empty()
        && attempt.len() <= 128
        && attempt
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

struct InFlight {
    attempt: String,
    connection: String,
    pgid: Option<i32>,
    input: Option<Sender<String>>,
    cancel: Option<tokio::sync::oneshot::Sender<()>>,
}

/// One sign-in at a time for the whole runtime. Two concurrent device flows would
/// race to write the same credential file, and the loser would silently win.
static IN_FLIGHT: parking_lot::Mutex<Option<InFlight>> = parking_lot::Mutex::new(None);

/// The slot is runtime-wide by design, so every test that drives a sign-in — here and in
/// the ACP server's WebSocket suite — has to take this first.
#[cfg(test)]
pub static TEST_GUARD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn claim(attempt: &str, connection: &str) -> Option<tokio::sync::oneshot::Receiver<()>> {
    let mut slot = IN_FLIGHT.lock();
    if slot.is_some() {
        return None;
    }
    let (tx, rx) = tokio::sync::oneshot::channel();
    *slot = Some(InFlight {
        attempt: attempt.to_string(),
        connection: connection.to_string(),
        pgid: None,
        input: None,
        cancel: Some(tx),
    });
    Some(rx)
}

fn record_child(attempt: &str, pgid: Option<i32>, input: Option<Sender<String>>) {
    let mut slot = IN_FLIGHT.lock();
    if let Some(entry) = slot.as_mut() {
        if entry.attempt == attempt {
            entry.pgid = pgid;
            entry.input = input;
        }
    }
}

/// One line of input for the command: bounded, and without a line break of its own.
pub fn valid_input(text: &str) -> bool {
    !text.is_empty() && text.len() <= MAX_LOGIN_INPUT_BYTES && !text.contains(['\n', '\r'])
}

/// Write one line to the running sign-in's stdin — for a provider whose browser flow
/// ends in a code the user pastes back. Never logged, for the same reason frames are not.
pub fn send_input(attempt: &str, text: &str) -> Result<(), (i32, String)> {
    if !valid_input(text) {
        return Err((-32602, "Invalid input".to_string()));
    }
    let slot = IN_FLIGHT.lock();
    let Some(input) = slot
        .as_ref()
        .filter(|entry| entry.attempt == attempt)
        .and_then(|entry| entry.input.as_ref())
    else {
        return Err((
            LOGIN_NOT_RUNNING,
            "No runtime sign-in with this attemptId is running".to_string(),
        ));
    };
    match input.try_send(format!("{text}\n")) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(_)) => Err((
            LOGIN_BUSY,
            "The sign-in command is not reading its input".to_string(),
        )),
        Err(TrySendError::Closed(_)) => Err((
            LOGIN_NOT_RUNNING,
            "No runtime sign-in with this attemptId is running".to_string(),
        )),
    }
}

fn release(attempt: &str) {
    let mut slot = IN_FLIGHT.lock();
    if slot.as_ref().is_some_and(|entry| entry.attempt == attempt) {
        *slot = None;
    }
}

/// Stop the in-flight sign-in. `attempt` of `None` cancels whichever one is running.
/// Returns whether an attempt was cancelled.
///
/// The slot is freed here rather than by the driving task, because a cancel that arrives
/// with a closing connection races that task's own teardown: if the task is dropped
/// before it can release, every later sign-in is refused until the process restarts. The
/// kill below is synchronous and unconditional, so the command is already gone by the
/// time the slot is free for someone else to claim.
pub fn cancel(attempt: Option<&str>) -> bool {
    let mut slot = IN_FLIGHT.lock();
    let Some(entry) = slot.as_ref() else {
        return false;
    };
    if attempt.is_some_and(|wanted| wanted != entry.attempt) {
        return false;
    }
    let Some(entry) = slot.take() else {
        return false;
    };

    kill_group(entry.pgid);
    // A send failure means the driving task has already stopped; the kill is what
    // actually ends the command either way.
    if let Some(tx) = entry.cancel {
        let _ = tx.send(());
    }
    true
}

/// The exec channel this replaces died with its request. A sign-in whose operator is
/// gone has nobody to answer the device prompt, so it must not outlive the connection.
pub fn cancel_for_connection(connection: &str) {
    let owned = {
        let slot = IN_FLIGHT.lock();
        slot.as_ref()
            .filter(|entry| entry.connection == connection)
            .map(|entry| entry.attempt.clone())
    };
    if let Some(attempt) = owned {
        cancel(Some(&attempt));
    }
}

/// The provider CLI launches its own children (a browser opener, a native helper) and
/// they keep the pipes open, so only the whole group going away ends the sign-in.
pub(super) fn kill_group(pgid: Option<i32>) {
    #[cfg(unix)]
    if let Some(pgid) = pgid {
        // SAFETY: `kill` on a process-group id we created ourselves; an already-exited
        // group returns ESRCH, which is the outcome we want anyway.
        unsafe {
            libc::kill(-pgid, libc::SIGKILL);
        }
    }
    #[cfg(not(unix))]
    let _ = pgid;
}

fn frame_notification(attempt: &str, frame: Value) -> String {
    json!({
        "jsonrpc": "2.0",
        "method": LOGIN_FRAME_METHOD,
        "params": {"attemptId": attempt, "frame": frame},
    })
    .to_string()
}

/// Run the configured login command, relaying its NDJSON stdout to `out_tx`.
///
/// Frame contents are never logged: they carry a device code and, for some providers,
/// the credential itself.
pub async fn run(
    command: &LoginCommand,
    attempt: &str,
    connection: &str,
    out_tx: &UnboundedSender<String>,
    on_success: Option<Arc<dyn Fn() + Send + Sync>>,
) -> Result<Value, (i32, String)> {
    let Some(cancelled) = claim(attempt, connection) else {
        return Err((
            LOGIN_BUSY,
            "A runtime sign-in is already in progress".to_string(),
        ));
    };
    let outcome = drive(command, attempt, out_tx, cancelled).await;
    release(attempt);
    if matches!(&outcome, Ok(value) if value["exitCode"] == 0) {
        if let Some(hook) = on_success {
            hook();
        }
    }
    outcome
}

async fn drive(
    command: &LoginCommand,
    attempt: &str,
    out_tx: &UnboundedSender<String>,
    cancelled: tokio::sync::oneshot::Receiver<()>,
) -> Result<Value, (i32, String)> {
    let mut builder = tokio::process::Command::new(&command.program);
    builder
        .args(&command.args)
        .stdin(std::process::Stdio::piped())
        // Provider output can carry secrets and is not ours to interpret; only the
        // command's own NDJSON frames leave this process.
        .stderr(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    builder.process_group(0);
    let mut child = builder.spawn().map_err(|error| {
        warn!(error = %error, "runtime login command could not start");
        (
            LOGIN_FAILED,
            "Runtime login command could not start".to_string(),
        )
    })?;
    let pgid = child.id().and_then(|pid| i32::try_from(pid).ok());
    // The writer ends when the slot drops its sender, which every exit path does.
    let (input_tx, mut input_rx) = tokio::sync::mpsc::channel::<String>(LOGIN_INPUT_QUEUE);
    let mut stdin = child.stdin.take().expect("stdin is piped");
    tokio::spawn(async move {
        while let Some(line) = input_rx.recv().await {
            if stdin.write_all(line.as_bytes()).await.is_err() || stdin.flush().await.is_err() {
                break;
            }
        }
    });
    record_child(attempt, pgid, Some(input_tx));
    info!(attempt = %attempt, "runtime sign-in started");

    let stdout = child.stdout.take().expect("stdout is piped");
    let relay = {
        let out_tx = out_tx.clone();
        let attempt = attempt.to_string();
        tokio::spawn(async move { relay_frames(stdout, &attempt, &out_tx).await })
    };

    let status = tokio::select! {
        status = child.wait() => Ok(status),
        _ = cancelled => Err("Runtime sign-in was cancelled"),
        _ = tokio::time::sleep(std::time::Duration::from_secs(LOGIN_TIMEOUT_SECS)) => {
            Err("Runtime sign-in timed out")
        }
    };
    // However the command ended, nothing reads stdin any more, so no later line may be
    // acknowledged — not while stdout drains, nor while a stopped command is killed.
    record_child(attempt, pgid, None);
    let status = match status {
        Ok(status) => status,
        Err(reason) => {
            kill_group(pgid);
            let _ = child.kill().await;
            relay.abort();
            return Err((LOGIN_FAILED, reason.to_string()));
        }
    };
    // End the group before waiting for EOF. A descendant that inherited stdout can hold
    // the pipe open long after the command itself exits, and by here the cancellation and
    // timeout arms are gone — so an unbounded drain would outlive both guarantees. Bytes
    // already written stay readable once the writers are dead, so no frame is lost.
    kill_group(pgid);
    let relayed = tokio::time::timeout(std::time::Duration::from_secs(RELAY_DRAIN_SECS), relay)
        .await
        .unwrap_or_else(|_| Ok(Err(())))
        .unwrap_or(Err(()));
    if relayed.is_err() {
        return Err((
            LOGIN_FAILED,
            "Runtime login produced an unreadable response".to_string(),
        ));
    }
    let code = status
        .map(|status| status.code().unwrap_or(-1))
        .map_err(|error| {
            warn!(error = %error, "runtime login command did not report an exit status");
            (LOGIN_FAILED, "Runtime sign-in did not complete".to_string())
        })?;
    info!(attempt = %attempt, exit_code = code, "runtime sign-in finished");
    Ok(json!({"exitCode": code}))
}

/// Split stdout into NDJSON frames and forward each JSON object. A line over the cap
/// fails the sign-in rather than growing a buffer the remote end controls.
async fn relay_frames(
    mut stdout: tokio::process::ChildStdout,
    attempt: &str,
    out_tx: &UnboundedSender<String>,
) -> Result<(), ()> {
    let mut pending = Vec::new();
    let mut chunk = [0_u8; 8192];
    loop {
        let read = match stdout.read(&mut chunk).await {
            Ok(0) => return Ok(()),
            Ok(read) => read,
            Err(_) => return Err(()),
        };
        pending.extend_from_slice(&chunk[..read]);
        if pending.len() > MAX_LOGIN_FRAME_BYTES {
            return Err(());
        }
        while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
            let line: Vec<u8> = pending.drain(..=newline).take(newline).collect();
            let Ok(text) = std::str::from_utf8(&line) else {
                return Err(());
            };
            if text.trim().is_empty() {
                continue;
            }
            let Ok(frame @ Value::Object(_)) = serde_json::from_str::<Value>(text) else {
                return Err(());
            };
            if out_tx.send(frame_notification(attempt, frame)).is_err() {
                return Err(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_command_is_argv_not_a_shell_line() {
        assert_eq!(
            parse_login_command("  node /opt/login.mjs --device  "),
            Some(LoginCommand {
                program: "node".into(),
                args: vec!["/opt/login.mjs".into(), "--device".into()],
            })
        );
        assert_eq!(parse_login_command("   "), None);
    }

    #[test]
    fn an_attempt_id_stays_loggable() {
        assert!(valid_attempt_id("a-b_C9"));
        assert!(!valid_attempt_id(""));
        assert!(!valid_attempt_id("has space"));
        assert!(!valid_attempt_id(&"x".repeat(129)));
    }

    #[tokio::test]
    async fn frames_reach_the_operator_and_the_slot_is_released() {
        let _serialized = TEST_GUARD.lock().await;
        let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel();
        let command = LoginCommand {
            program: "/bin/sh".into(),
            args: vec![
                "-c".into(),
                "printf '{\"type\":\"device\"}\\n{\"type\":\"authenticated\"}\\n'".into(),
            ],
        };
        let result = run(&command, "attempt-1", "conn-1", &out_tx, None)
            .await
            .unwrap();
        assert_eq!(result["exitCode"], 0);
        let first: Value = serde_json::from_str(&out_rx.recv().await.unwrap()).unwrap();
        assert_eq!(first["method"], LOGIN_FRAME_METHOD);
        assert_eq!(first["params"]["attemptId"], "attempt-1");
        assert_eq!(first["params"]["frame"]["type"], "device");
        let second: Value = serde_json::from_str(&out_rx.recv().await.unwrap()).unwrap();
        assert_eq!(second["params"]["frame"]["type"], "authenticated");
        assert!(IN_FLIGHT.lock().is_none());
    }

    #[tokio::test]
    async fn operator_input_reaches_the_running_command_as_one_line() {
        let _serialized = TEST_GUARD.lock().await;
        let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel();
        let echo = LoginCommand {
            program: "/bin/sh".into(),
            args: vec![
                "-c".into(),
                "read line; printf '{\"type\":\"got\",\"line\":\"%s\"}\\n' \"$line\"".into(),
            ],
        };
        let running = {
            let out_tx = out_tx.clone();
            tokio::spawn(async move { run(&echo, "attempt-i", "conn-1", &out_tx, None).await })
        };
        while IN_FLIGHT
            .lock()
            .as_ref()
            .is_none_or(|entry| entry.input.is_none())
        {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        assert_eq!(send_input("attempt-i", "two\nlines").unwrap_err().0, -32602);
        assert_eq!(send_input("attempt-i", "").unwrap_err().0, -32602);
        assert_eq!(
            send_input("someone-else", "code#state").unwrap_err().0,
            LOGIN_NOT_RUNNING
        );
        send_input("attempt-i", "code#state").unwrap();

        assert_eq!(running.await.unwrap().unwrap()["exitCode"], 0);
        let frame: Value = serde_json::from_str(&out_rx.recv().await.unwrap()).unwrap();
        assert_eq!(frame["params"]["frame"]["line"], "code#state");
        assert_eq!(
            send_input("attempt-i", "late").unwrap_err().0,
            LOGIN_NOT_RUNNING
        );
    }

    #[tokio::test]
    async fn input_for_a_command_that_does_not_read_it_is_bounded() {
        let _serialized = TEST_GUARD.lock().await;
        let (out_tx, _out_rx) = tokio::sync::mpsc::unbounded_channel();
        let deaf = LoginCommand {
            program: "/bin/sh".into(),
            args: vec!["-c".into(), "sleep 30".into()],
        };
        let running = {
            let out_tx = out_tx.clone();
            tokio::spawn(async move { run(&deaf, "attempt-q", "conn-1", &out_tx, None).await })
        };
        while IN_FLIGHT
            .lock()
            .as_ref()
            .is_none_or(|entry| entry.input.is_none())
        {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let line = "x".repeat(MAX_LOGIN_INPUT_BYTES);
        let mut refused = None;
        // The pipe buffer absorbs some lines before the queue itself fills.
        for _ in 0..1000 {
            if let Err((code, _)) = send_input("attempt-q", &line) {
                refused = Some(code);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }

        assert_eq!(refused, Some(LOGIN_BUSY));
        assert!(cancel(Some("attempt-q")));
        let _ = running.await.unwrap();
    }

    #[tokio::test]
    async fn an_exited_command_acknowledges_no_more_input() {
        let _serialized = TEST_GUARD.lock().await;
        assert!(claim("attempt-x", "conn-1").is_some());
        let (input, _reader) = tokio::sync::mpsc::channel(LOGIN_INPUT_QUEUE);
        record_child("attempt-x", None, Some(input));
        send_input("attempt-x", "code#state").unwrap();

        // What `drive` does the moment the command exits, before it drains stdout.
        record_child("attempt-x", None, None);
        assert_eq!(
            send_input("attempt-x", "code#state").unwrap_err().0,
            LOGIN_NOT_RUNNING
        );
        release("attempt-x");
    }

    #[tokio::test]
    async fn a_second_sign_in_is_refused_while_one_runs() {
        let _serialized = TEST_GUARD.lock().await;
        let (out_tx, _out_rx) = tokio::sync::mpsc::unbounded_channel();
        let slow = LoginCommand {
            program: "/bin/sh".into(),
            args: vec!["-c".into(), "sleep 30".into()],
        };
        let first = {
            let out_tx = out_tx.clone();
            let slow = slow.clone();
            tokio::spawn(async move { run(&slow, "attempt-a", "conn-1", &out_tx, None).await })
        };
        // Wait for the slot rather than a fixed delay; the spawn above is not ordered.
        while IN_FLIGHT.lock().is_none() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let refused = run(&slow, "attempt-b", "conn-1", &out_tx, None).await;
        assert_eq!(refused.unwrap_err().0, LOGIN_BUSY);
        assert!(cancel(Some("attempt-a")));
        let _ = first.await.unwrap();
        assert!(IN_FLIGHT.lock().is_none());
    }

    #[tokio::test]
    async fn a_closed_connection_ends_its_sign_in() {
        let _serialized = TEST_GUARD.lock().await;
        let (out_tx, _out_rx) = tokio::sync::mpsc::unbounded_channel();
        let slow = LoginCommand {
            program: "/bin/sh".into(),
            args: vec!["-c".into(), "sleep 30".into()],
        };
        let running = {
            let out_tx = out_tx.clone();
            let slow = slow.clone();
            tokio::spawn(async move { run(&slow, "attempt-c", "conn-9", &out_tx, None).await })
        };
        while IN_FLIGHT.lock().is_none() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        cancel_for_connection("someone-else");
        assert!(IN_FLIGHT.lock().is_some());
        cancel_for_connection("conn-9");
        assert_eq!(running.await.unwrap().unwrap_err().0, LOGIN_FAILED);
        assert!(IN_FLIGHT.lock().is_none());
    }

    /// A helper that inherited stdout can outlive the command that spawned it. Before the
    /// group was killed ahead of the drain, the pipe stayed open and the runtime-wide slot
    /// with it — long past the sign-in's own bound.
    #[tokio::test]
    async fn a_descendant_holding_stdout_cannot_hold_the_slot() {
        let _serialized = TEST_GUARD.lock().await;
        let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel();
        let lingering = LoginCommand {
            program: "/bin/sh".into(),
            args: vec![
                "-c".into(),
                "sleep 120 & printf '{\"type\":\"authenticated\"}\\n'".into(),
            ],
        };
        let started = std::time::Instant::now();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            run(&lingering, "attempt-g", "conn-1", &out_tx, None),
        )
        .await
        .expect("the sign-in must not wait on a descendant's copy of stdout")
        .unwrap();

        assert_eq!(result["exitCode"], 0);
        assert!(started.elapsed() < std::time::Duration::from_secs(RELAY_DRAIN_SECS + 5));
        // The frame the command did print still arrives: killing the writers does not
        // discard what is already in the pipe.
        let frame: Value = serde_json::from_str(&out_rx.recv().await.unwrap()).unwrap();

        assert_eq!(frame["params"]["frame"]["type"], "authenticated");
        assert!(IN_FLIGHT.lock().is_none());
    }

    #[tokio::test]
    async fn an_oversized_line_fails_the_sign_in() {
        let _serialized = TEST_GUARD.lock().await;
        let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel();
        let flood = LoginCommand {
            program: "/bin/sh".into(),
            args: vec![
                "-c".into(),
                format!("printf '%0{}d' 0", MAX_LOGIN_FRAME_BYTES + 1),
            ],
        };
        assert_eq!(
            run(&flood, "attempt-d", "conn-1", &out_tx, None)
                .await
                .unwrap_err()
                .0,
            LOGIN_FAILED
        );
        assert!(out_rx.try_recv().is_err());
        assert!(IN_FLIGHT.lock().is_none());
    }

    #[tokio::test]
    async fn a_completed_sign_in_runs_the_success_hook_once() {
        let _serialized = TEST_GUARD.lock().await;
        let (out_tx, _out_rx) = tokio::sync::mpsc::unbounded_channel();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = calls.clone();
        let hook: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        });
        let ok = LoginCommand {
            program: "/bin/sh".into(),
            args: vec!["-c".into(), "true".into()],
        };
        let fail = LoginCommand {
            program: "/bin/sh".into(),
            args: vec!["-c".into(), "exit 3".into()],
        };
        run(&ok, "attempt-e", "conn-1", &out_tx, Some(hook.clone()))
            .await
            .unwrap();
        let failed = run(&fail, "attempt-f", "conn-1", &out_tx, Some(hook))
            .await
            .unwrap();
        assert_eq!(failed["exitCode"], 3);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
