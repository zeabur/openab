//! Which runtime binding owns each ACP session, so one binding can neither see nor act on
//! another binding's sessions. Persisted as `session-owners.json` beside `bindings.json`:
//! otherwise, after a restart, the first binding to resume a session would own it.

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use tracing::error;

use super::runtime_credentials::write_private_atomic;

const OWNERS_FILE: &str = "session-owners.json";
/// Beyond this a binding's oldest claim is dropped, so a binding cannot grow the ledger
/// without bound by resuming made-up session ids.
pub const MAX_SESSIONS_PER_BINDING: usize = 4096;

#[derive(Serialize, Deserialize)]
struct Entry {
    session: String,
    binding: String,
}

#[derive(Default)]
struct Ledger {
    owners: HashMap<String, String>,
    /// Each binding's channels, oldest claim first.
    claims: HashMap<String, VecDeque<String>>,
}

impl Ledger {
    fn insert(&mut self, channel: String, binding: String, cap: usize) {
        let claims = self.claims.entry(binding.clone()).or_default();
        claims.push_back(channel.clone());
        while claims.len() > cap {
            if let Some(evicted) = claims.pop_front() {
                self.owners.remove(&evicted);
            }
        }
        self.owners.insert(channel, binding);
    }

    fn entries(&self) -> Vec<Entry> {
        self.claims
            .iter()
            .flat_map(|(binding, channels)| {
                channels.iter().map(|channel| Entry {
                    session: channel.clone(),
                    binding: binding.clone(),
                })
            })
            .collect()
    }
}

pub struct SessionOwners {
    path: Option<PathBuf>,
    cap: usize,
    ledger: Mutex<Ledger>,
}

impl Default for SessionOwners {
    fn default() -> Self {
        Self {
            path: None,
            cap: MAX_SESSIONS_PER_BINDING,
            ledger: Mutex::default(),
        }
    }
}

impl SessionOwners {
    /// A corrupt file is an error: starting empty would let any binding claim every session.
    pub fn open(dir: &Path) -> anyhow::Result<Self> {
        let path = dir.join(OWNERS_FILE);
        let entries: Vec<Entry> = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        let mut ledger = Ledger::default();
        for entry in entries {
            if !ledger.owners.contains_key(&entry.session) {
                ledger.insert(entry.session, entry.binding, MAX_SESSIONS_PER_BINDING);
            }
        }
        Ok(Self {
            path: Some(path),
            cap: MAX_SESSIONS_PER_BINDING,
            ledger: Mutex::new(ledger),
        })
    }

    pub fn owner(&self, channel: &str) -> Option<String> {
        self.ledger.lock().owners.get(channel).cloned()
    }

    /// Records `binding` as the owner of an unowned channel. Returns whether `binding` owns it.
    pub fn claim(&self, channel: &str, binding: &str) -> bool {
        let mut ledger = self.ledger.lock();
        if let Some(existing) = ledger.owners.get(channel) {
            return existing == binding;
        }
        ledger.insert(channel.to_string(), binding.to_string(), self.cap);
        self.save(&ledger);
        true
    }

    /// A revoked binding's sessions become unowned, so the application can resume them after
    /// pairing again.
    pub fn release_binding(&self, binding: &str) {
        let mut ledger = self.ledger.lock();
        let Some(channels) = ledger.claims.remove(binding) else {
            return;
        };
        for channel in channels {
            ledger.owners.remove(&channel);
        }
        self.save(&ledger);
    }

    fn save(&self, ledger: &Ledger) {
        let Some(path) = &self.path else {
            return;
        };
        let written = serde_json::to_vec(&ledger.entries())
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

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("openab-owners-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn the_first_claim_wins_and_survives_a_restart() {
        let dir = temp_dir();
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
    fn a_binding_cannot_grow_the_ledger_past_its_cap() {
        let owners = SessionOwners {
            cap: 3,
            ..SessionOwners::default()
        };
        assert!(owners.claim("keep", "b"));
        for n in 0..10 {
            assert!(owners.claim(&format!("acp_{n}"), "a"));
        }
        let ledger = owners.ledger.lock();
        assert_eq!(ledger.claims["a"].len(), 3);
        assert_eq!(ledger.owners.len(), 4);
        assert_eq!(ledger.owners.get("keep").map(String::as_str), Some("b"));
        assert_eq!(ledger.owners.get("acp_9").map(String::as_str), Some("a"));
        assert!(!ledger.owners.contains_key("acp_0"));
    }

    #[test]
    fn a_corrupt_file_refuses_to_open() {
        let dir = temp_dir();
        std::fs::write(dir.join(OWNERS_FILE), b"{not json").unwrap();
        assert!(SessionOwners::open(&dir).is_err());
        let _ = std::fs::remove_dir_all(dir);
    }
}
