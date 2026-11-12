//! Publish/subscribe channels.
//!
//! Each subscribed connection registers a shared handle to its socket writer.
//! PUBLISH snapshots the subscriber list, releases the registry lock, and then
//! writes the message frame to every subscriber, so one slow subscriber never
//! blocks SUBSCRIBE/UNSUBSCRIBE for everyone else.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

use crate::lock;
use crate::resp::Value;

pub type Writer = Arc<Mutex<TcpStream>>;

#[derive(Default)]
pub struct PubSub {
    channels: Mutex<HashMap<Vec<u8>, HashMap<u64, Writer>>>,
}

impl PubSub {
    /// Register `client` on `channel` and write `confirmation` to it while the
    /// registry lock is held. That ordering guarantees the client always sees
    /// its subscribe confirmation before any message published to the channel.
    pub fn subscribe(&self, channel: &[u8], client: u64, w: &Writer, confirmation: &[u8]) {
        let mut reg = lock(&self.channels);
        reg.entry(channel.to_vec())
            .or_default()
            .insert(client, w.clone());
        let _ = lock(w).write_all(confirmation);
    }

    pub fn unsubscribe(&self, channel: &[u8], client: u64) {
        let mut reg = lock(&self.channels);
        if let Some(subs) = reg.get_mut(channel) {
            subs.remove(&client);
            if subs.is_empty() {
                reg.remove(channel);
            }
        }
    }

    pub fn unsubscribe_all(&self, client: u64, channels: &HashSet<Vec<u8>>) {
        for ch in channels {
            self.unsubscribe(ch, client);
        }
    }

    /// Returns the number of clients that received the message.
    pub fn publish(&self, channel: &[u8], message: &[u8]) -> i64 {
        let targets: Vec<Writer> = match lock(&self.channels).get(channel) {
            Some(subs) => subs.values().cloned().collect(),
            None => return 0,
        };
        let frame = Value::Array(vec![
            Value::bulk("message"),
            Value::bulk(channel),
            Value::bulk(message),
        ])
        .to_bytes();
        targets
            .iter()
            .filter(|w| lock(w).write_all(&frame).is_ok())
            .count() as i64
    }

    pub fn num_channels(&self) -> usize {
        lock(&self.channels).len()
    }
}
