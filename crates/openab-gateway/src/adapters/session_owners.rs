//! Which runtime binding owns each ACP session, so one binding can neither see nor act on
//! another binding's sessions. Persisted as `session-owners.json` beside `bindings.json`:
//! otherwise, after a restart, the first binding to resume a session would own it.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::error;

use super::runtime_credentials::write_private_atomic;

const OWNERS_FILE: &str = "session-owners.json";

#[derive(Default)]
pub struct SessionOwners {
    path: Option<PathBuf>,
    owners: Mutex<HashMap<String, String>>,
}

impl SessionOwners {
    /// A corrupt file is an error: starting empty would let any binding claim every session.
    pub fn open(dir: &Path) -> anyhow::Result<Self> {
        let path = dir.join(OWNERS_FILE);
        let owners = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(e) => return Err(e.into()),
        };
        Ok(Self {
            path: Some(path),
            owners: Mutex::new(owners),
        })
    }

    pub fn owner(&self, channel: &str) -> Option<String> {
        self.owners.lock().get(channel).cloned()
    }

    /// Records `binding` as the owner of an unowned channel. Returns whether `binding` owns it.
    pub fn claim(&self, channel: &str, binding: &str) -> bool {
        let mut owners = self.owners.lock();
        if let Some(existing) = owners.get(channel) {
            return existing == binding;
        }
        owners.insert(channel.to_string(), binding.to_string());
        self.save(&owners);
        true
    }

    /// A revoked binding's sessions become unowned, so the application can resume them after
    /// pairing again.
    pub fn release_binding(&self, binding: &str) {
        let mut owners = self.owners.lock();
        let before = owners.len();
        owners.retain(|_, owner| owner != binding);
        if owners.len() != before {
            self.save(&owners);
        }
    }

    fn save(&self, owners: &HashMap<String, String>) {
        let Some(path) = &self.path else {
            return;
        };
        let written = serde_json::to_vec(owners)
            .map_err(std::io::Error::other)
            .and_then(|json| write_private_atomic(path, &json));
        if let Err(e) = written {
            error!(error = %e, "runtime credentials: could not write session owners");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_claim_wins_and_survives_a_restart() {
        let dir = std::env::temp_dir().join(format!("openab-owners-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let owners = SessionOwners::open(&dir).unwrap();
        assert!(owners.claim("acp_1", "a"));
        assert!(owners.claim("acp_1", "a"));
        assert!(!owners.claim("acp_1", "b"));

        let reopened = SessionOwners::open(&dir).unwrap();
        assert_eq!(reopened.owner("acp_1").as_deref(), Some("a"));
        assert!(!reopened.claim("acp_1", "b"));

        reopened.release_binding("a");
        assert!(SessionOwners::open(&dir).unwrap().claim("acp_1", "b"));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_corrupt_file_refuses_to_open() {
        let dir = std::env::temp_dir().join(format!("openab-owners-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(OWNERS_FILE), b"{not json").unwrap();
        assert!(SessionOwners::open(&dir).is_err());
        let _ = std::fs::remove_dir_all(dir);
    }
}
