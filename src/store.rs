//! The keyspace: a sharded hash map with per-key expiry.
//!
//! Keys are spread across N independently locked shards (N is a power of two),
//! so clients touching different keys rarely contend on the same lock. This is
//! the main reason ferrodb scales across CPU cores while real Redis executes
//! commands on a single thread.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::lock;

/// Wall-clock time in Unix milliseconds. Expiry deadlines are stored as
/// absolute timestamps so they survive restarts via the AOF.
pub fn clock_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Data {
    Str(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Hash(HashMap<Vec<u8>, Vec<u8>>),
    Set(HashSet<Vec<u8>>),
}

impl Data {
    pub fn type_name(&self) -> &'static str {
        match self {
            Data::Str(_) => "string",
            Data::List(_) => "list",
            Data::Hash(_) => "hash",
            Data::Set(_) => "set",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub data: Data,
    /// Absolute expiry deadline in Unix ms, if any.
    pub expires_at: Option<u64>,
}

impl Entry {
    pub fn new(data: Data) -> Self {
        Entry {
            data,
            expires_at: None,
        }
    }

    pub fn is_expired(&self, now: u64) -> bool {
        matches!(self.expires_at, Some(t) if t <= now)
    }
}

pub type Shard = HashMap<Vec<u8>, Entry>;

pub struct Store {
    shards: Box<[Mutex<Shard>]>,
}

/// FNV-1a: tiny, fast, and good enough for picking a shard.
fn fnv1a(key: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

impl Store {
    pub fn new(shards: usize) -> Self {
        let n = shards.clamp(1, 4096).next_power_of_two();
        Store {
            shards: (0..n).map(|_| Mutex::new(HashMap::new())).collect(),
        }
    }

    pub fn num_shards(&self) -> usize {
        self.shards.len()
    }

    pub fn shard_of(&self, key: &[u8]) -> usize {
        (fnv1a(key) as usize) & (self.shards.len() - 1)
    }

    pub fn lock(&self, key: &[u8]) -> MutexGuard<'_, Shard> {
        self.lock_index(self.shard_of(key))
    }

    pub fn lock_index(&self, i: usize) -> MutexGuard<'_, Shard> {
        lock(&self.shards[i])
    }

    /// Lock every shard, always in index order so this can never deadlock with
    /// another caller doing the same. Used for FLUSHALL and AOF rewrites.
    pub fn lock_all(&self) -> Vec<MutexGuard<'_, Shard>> {
        self.shards.iter().map(lock).collect()
    }

    /// Remove expired keys from one shard. Returns how many were removed.
    pub fn sweep_expired(&self, i: usize, now: u64) -> usize {
        let mut shard = self.lock_index(i);
        let before = shard.len();
        shard.retain(|_, e| !e.is_expired(now));
        before - shard.len()
    }

    /// (total keys, keys with an expiry set)
    pub fn stats(&self) -> (usize, usize) {
        let mut keys = 0;
        let mut expires = 0;
        for i in 0..self.shards.len() {
            let s = self.lock_index(i);
            keys += s.len();
            expires += s.values().filter(|e| e.expires_at.is_some()).count();
        }
        (keys, expires)
    }
}

/// Look up a key, lazily deleting it if it has expired.
pub fn live<'a>(shard: &'a mut Shard, key: &[u8], now: u64) -> Option<&'a mut Entry> {
    if shard.get(key).is_some_and(|e| e.is_expired(now)) {
        shard.remove(key);
        return None;
    }
    shard.get_mut(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lazy_expiry_removes_key() {
        let mut shard = Shard::new();
        shard.insert(
            b"k".to_vec(),
            Entry {
                data: Data::Str(b"v".to_vec()),
                expires_at: Some(100),
            },
        );
        assert!(live(&mut shard, b"k", 50).is_some());
        assert!(live(&mut shard, b"k", 100).is_none());
        assert!(shard.is_empty());
    }

    #[test]
    fn shard_count_is_power_of_two_and_keys_spread() {
        let s = Store::new(50);
        assert_eq!(s.num_shards(), 64);
        let used: HashSet<usize> = (0..10_000)
            .map(|i| s.shard_of(format!("key:{i}").as_bytes()))
            .collect();
        assert_eq!(used.len(), 64);
    }
}
