//! Short-lived credentials a client hands a session through `_meta`, kept in
//! per-session files the agent reads at call time. Process env is fixed at
//! spawn, so env alone cannot carry a credential that outlives its TTL.
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::warn;

static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

pub const SESSION_CREDENTIALS_META_KEY: &str = "dev.openab/credentials";
pub const SESSION_CREDENTIALS_DIR_ENV: &str = "OPENAB_CREDENTIALS_DIR";

fn valid_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn credentials(meta: Option<&serde_json::Value>) -> Vec<(&str, &str)> {
    meta.and_then(|m| m.get(SESSION_CREDENTIALS_META_KEY))
        .and_then(serde_json::Value::as_object)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|(name, value)| Some((name.as_str(), value.as_str()?)))
                .filter(|(name, _)| valid_name(name))
                .collect()
        })
        .unwrap_or_default()
}

pub fn session_credentials_dir(root: &Path, thread_id: &str) -> PathBuf {
    let digest = Sha256::digest(thread_id.as_bytes());
    root.join(
        digest[..16]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>(),
    )
}

fn write_private(dir: &Path, name: &str, value: &str) -> std::io::Result<()> {
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let tmp = dir.join(format!(".{name}.{}.{seq}.tmp", std::process::id()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let written = options.open(&tmp).and_then(|mut file| {
        file.write_all(value.as_bytes())?;
        file.sync_all()
    });
    let result = written.and_then(|()| std::fs::rename(&tmp, dir.join(name)));
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

/// Writes the meta's credentials into the session's private directory and
/// returns it, or `None` when the meta carries none. Values are never logged.
pub fn write_session_credentials(
    root: &Path,
    thread_id: &str,
    meta: Option<&serde_json::Value>,
) -> Option<PathBuf> {
    let entries = credentials(meta);
    if entries.is_empty() {
        return None;
    }
    let dir = session_credentials_dir(root, thread_id);
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    if let Err(error) = builder.create(&dir) {
        warn!(%error, "could not create session credentials directory");
        return None;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700));
    }
    for (name, value) in entries {
        if let Err(error) = write_private(&dir, name, value) {
            warn!(credential = name, %error, "could not write session credential");
        }
    }
    Some(dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn writes_only_named_string_credentials_privately() {
        let root = tempfile::tempdir().unwrap();
        assert!(write_session_credentials(root.path(), "t", None).is_none());
        assert!(write_session_credentials(root.path(), "t", Some(&json!({"x": 1}))).is_none());

        let meta = json!({SESSION_CREDENTIALS_META_KEY: {
            "TOKEN": "v1", "../escape": "x", "N": 3, "_OK_2": "ok"
        }});
        let dir = write_session_credentials(root.path(), "acp:a", Some(&meta)).unwrap();
        assert_eq!(dir, session_credentials_dir(root.path(), "acp:a"));
        assert_ne!(dir, session_credentials_dir(root.path(), "acp:b"));
        assert_eq!(std::fs::read_to_string(dir.join("TOKEN")).unwrap(), "v1");
        assert_eq!(std::fs::read_to_string(dir.join("_OK_2")).unwrap(), "ok");
        let mut names: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert_eq!(names, ["TOKEN", "_OK_2"]);

        let meta = json!({SESSION_CREDENTIALS_META_KEY: {"TOKEN": "v2"}});
        write_session_credentials(root.path(), "acp:a", Some(&meta)).unwrap();
        assert_eq!(std::fs::read_to_string(dir.join("TOKEN")).unwrap(), "v2");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode(&dir), 0o700);
            assert_eq!(mode(&dir.join("TOKEN")), 0o600);
        }
    }
}
