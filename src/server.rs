//! TCP server, per-connection protocol loop, and background maintenance.

use std::collections::HashSet;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::aof::{self, Aof, Fsync, encode_command};
use crate::commands::{self, Ctx, upper};
use crate::pubsub::{PubSub, Writer};
use crate::resp::{self, Value};
use crate::store::{Data, Entry, Store, clock_ms};
use crate::{lock, log};

#[derive(Debug, Clone)]
pub struct Config {
    pub bind: String,
    pub port: u16,
    pub shards: usize,
    pub aof_path: Option<PathBuf>,
    pub fsync: Fsync,
    /// Don't auto-rewrite the AOF until it is at least this big.
    pub aof_rewrite_min_size: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            bind: "127.0.0.1".into(),
            port: 6379,
            shards: 64,
            aof_path: None,
            fsync: Fsync::EverySec,
            aof_rewrite_min_size: 64 * 1024 * 1024,
        }
    }
}

#[derive(Default)]
pub struct Stats {
    pub commands: AtomicU64,
    pub connections: AtomicU64,
    pub connected: AtomicUsize,
    pub expired_keys: AtomicU64,
}

pub struct Server {
    pub store: Store,
    pub aof: Option<Aof>,
    pub pubsub: PubSub,
    pub stats: Stats,
    pub config: Config,
    pub started: Instant,
    next_client_id: AtomicU64,
}

impl Server {
    /// Build a server, replaying the AOF first if persistence is enabled.
    pub fn new(config: Config) -> io::Result<Arc<Server>> {
        let mut srv = Server {
            store: Store::new(config.shards),
            aof: None,
            pubsub: PubSub::default(),
            stats: Stats::default(),
            config: config.clone(),
            started: Instant::now(),
            next_client_id: AtomicU64::new(1),
        };

        if let Some(path) = &config.aof_path {
            let t = Instant::now();
            let mut failed = 0usize;
            let report = aof::load(path, |args| {
                let ctx = Ctx {
                    srv: &srv,
                    loading: true,
                };
                if let Value::Error(e) = commands::execute(&ctx, args) {
                    failed += 1;
                    if failed <= 5 {
                        log(&format!("AOF replay: command failed: {e}"));
                    }
                }
            })?;
            if report.truncated_bytes > 0 {
                log(&format!(
                    "AOF had an incomplete trailing command ({} bytes); truncated it",
                    report.truncated_bytes
                ));
            }
            if report.commands > 0 {
                log(&format!(
                    "AOF loaded: {} commands replayed in {:.1?} ({} keys)",
                    report.commands,
                    t.elapsed(),
                    srv.store.stats().0
                ));
            }
            srv.aof = Some(Aof::open(path, config.fsync)?);
        }
        Ok(Arc::new(srv))
    }

    /// Compact the AOF down to the minimal commands that recreate the
    /// current dataset. Holds every shard lock for the duration, so the
    /// snapshot is exactly consistent with the log that follows it.
    pub fn rewrite_aof(&self) -> io::Result<usize> {
        let Some(aof) = &self.aof else {
            return Err(io::Error::other("AOF is disabled"));
        };
        let t = Instant::now();
        let before = aof.size();
        let shards = self.store.lock_all();
        let now = clock_ms();
        let mut snap = Vec::new();
        let mut keys = 0;
        for shard in &shards {
            for (k, e) in shard.iter().filter(|(_, e)| !e.is_expired(now)) {
                snapshot_entry(k, e, &mut snap);
                keys += 1;
            }
        }
        aof.rewrite(&snap)?;
        drop(shards);
        log(&format!(
            "AOF rewritten: {keys} keys, {before} -> {} bytes in {:.1?}",
            snap.len(),
            t.elapsed()
        ));
        Ok(keys)
    }
}

/// Emit the commands that recreate one key.
fn snapshot_entry(key: &[u8], e: &Entry, out: &mut Vec<u8>) {
    const CHUNK: usize = 64;
    let mut emit = |cmd: &[u8], items: &mut dyn Iterator<Item = &[u8]>| {
        let items: Vec<&[u8]> = items.collect();
        for chunk in items.chunks(CHUNK) {
            let mut argv: Vec<&[u8]> = vec![cmd, key];
            argv.extend_from_slice(chunk);
            encode_command(&argv, out);
        }
    };
    match &e.data {
        Data::Str(v) => {
            let ts;
            let mut argv: Vec<&[u8]> = vec![b"SET", key, v];
            if let Some(t) = e.expires_at {
                ts = t.to_string();
                argv.extend_from_slice(&[b"PXAT", ts.as_bytes()]);
            }
            encode_command(&argv, out);
            return;
        }
        Data::List(l) => emit(b"RPUSH", &mut l.iter().map(Vec::as_slice)),
        Data::Set(s) => emit(b"SADD", &mut s.iter().map(Vec::as_slice)),
        Data::Hash(h) => emit(
            b"HSET",
            &mut h.iter().flat_map(|(f, v)| [f.as_slice(), v.as_slice()]),
        ),
    }
    if let Some(t) = e.expires_at {
        encode_command(&[b"PEXPIREAT", key, t.to_string().as_bytes()], out);
    }
}

/// Accept connections forever. Each client gets its own OS thread.
pub fn run(srv: Arc<Server>, listener: TcpListener) -> io::Result<()> {
    spawn_background_tasks(&srv);
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let srv = srv.clone();
                let spawned = thread::Builder::new()
                    .name("client".into())
                    .spawn(move || handle_client(srv, stream));
                if let Err(e) = spawned {
                    log(&format!("failed to spawn client thread: {e}"));
                }
            }
            Err(e) => log(&format!("accept failed: {e}")),
        }
    }
    Ok(())
}

fn spawn_background_tasks(srv: &Arc<Server>) {
    // Active expiry: every 100ms sweep a few shards, round-robin, so keys
    // that are never read again still get their memory reclaimed.
    let s = srv.clone();
    thread::Builder::new()
        .name("expiry".into())
        .spawn(move || {
            let n = s.store.num_shards();
            let per_tick = (n / 16).max(1);
            let mut next = 0;
            loop {
                thread::sleep(Duration::from_millis(100));
                let now = clock_ms();
                for _ in 0..per_tick {
                    let removed = s.store.sweep_expired(next, now);
                    s.stats.expired_keys.fetch_add(removed as u64, Relaxed);
                    next = (next + 1) % n;
                }
            }
        })
        .expect("spawn expiry thread");

    // AOF: flush/fsync once a second and auto-compact when the log doubles.
    if srv.aof.is_some() {
        let s = srv.clone();
        thread::Builder::new()
            .name("aof".into())
            .spawn(move || {
                let aof = s.aof.as_ref().unwrap();
                loop {
                    thread::sleep(Duration::from_secs(1));
                    if let Err(e) = aof.sync() {
                        log(&format!("AOF fsync failed: {e}"));
                    }
                    if aof.needs_rewrite(s.config.aof_rewrite_min_size)
                        && let Err(e) = s.rewrite_aof()
                    {
                        log(&format!("AOF auto-rewrite failed: {e}"));
                    }
                }
            })
            .expect("spawn aof thread");
    }
}

struct Conn {
    id: u64,
    writer: Writer,
    out: Vec<u8>,
    subs: HashSet<Vec<u8>>,
    quit: bool,
}

impl Conn {
    fn reply(&mut self, v: Value) {
        v.encode(&mut self.out);
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.out.is_empty() {
            lock(&self.writer).write_all(&self.out)?;
            self.out.clear();
        }
        Ok(())
    }

    fn dispatch(&mut self, srv: &Server, args: Vec<Vec<u8>>) {
        let cmd = upper(&args[0]);
        let subscribed = !self.subs.is_empty();
        match cmd.as_str() {
            "QUIT" => {
                self.reply(Value::ok());
                self.quit = true;
            }
            "SUBSCRIBE" => self.subscribe(srv, &args[1..]),
            "UNSUBSCRIBE" => self.unsubscribe(srv, &args[1..]),
            "PING" if subscribed => {
                let msg = args.get(1).cloned().unwrap_or_default();
                self.reply(Value::Array(vec![Value::bulk("pong"), Value::Bulk(msg)]));
            }
            _ if subscribed => self.reply(Value::err(format!(
                "ERR Can't execute '{}': only SUBSCRIBE / UNSUBSCRIBE / PING / QUIT are allowed in this context",
                cmd.to_ascii_lowercase()
            ))),
            "PUBLISH" if args.len() == 3 => {
                let n = srv.pubsub.publish(&args[1], &args[2]);
                self.reply(Value::Integer(n));
            }
            "PUBLISH" => self.reply(Value::err("ERR wrong number of arguments for 'publish' command")),
            _ => {
                let v = commands::execute_named(&Ctx { srv, loading: false }, &cmd, &args);
                self.reply(v);
            }
        }
    }

    fn subscribe(&mut self, srv: &Server, channels: &[Vec<u8>]) {
        if channels.is_empty() {
            self.reply(Value::err(
                "ERR wrong number of arguments for 'subscribe' command",
            ));
            return;
        }
        // Earlier pipelined replies must reach the client before the
        // subscribe confirmations (which are written directly).
        let _ = self.flush();
        for ch in channels {
            self.subs.insert(ch.clone());
            let confirmation = Value::Array(vec![
                Value::bulk("subscribe"),
                Value::Bulk(ch.clone()),
                Value::Integer(self.subs.len() as i64),
            ])
            .to_bytes();
            srv.pubsub
                .subscribe(ch, self.id, &self.writer, &confirmation);
        }
    }

    fn unsubscribe(&mut self, srv: &Server, channels: &[Vec<u8>]) {
        let targets: Vec<Vec<u8>> = if channels.is_empty() {
            self.subs.iter().cloned().collect()
        } else {
            channels.to_vec()
        };
        if targets.is_empty() {
            self.reply(Value::Array(vec![
                Value::bulk("unsubscribe"),
                Value::Null,
                Value::Integer(0),
            ]));
            return;
        }
        for ch in targets {
            self.subs.remove(&ch);
            srv.pubsub.unsubscribe(&ch, self.id);
            let count = self.subs.len() as i64;
            self.reply(Value::Array(vec![
                Value::bulk("unsubscribe"),
                Value::Bulk(ch),
                Value::Integer(count),
            ]));
        }
    }
}

fn handle_client(srv: Arc<Server>, stream: TcpStream) {
    let _ = stream.set_nodelay(true);
    let _ = stream.set_write_timeout(Some(Duration::from_secs(10)));
    let Ok(write_half) = stream.try_clone() else {
        return;
    };

    srv.stats.connections.fetch_add(1, Relaxed);
    srv.stats.connected.fetch_add(1, Relaxed);
    let mut conn = Conn {
        id: srv.next_client_id.fetch_add(1, Relaxed),
        writer: Arc::new(Mutex::new(write_half)),
        out: Vec::with_capacity(16 * 1024),
        subs: HashSet::new(),
        quit: false,
    };

    let mut reader = stream;
    let mut buf: Vec<u8> = Vec::with_capacity(16 * 1024);
    let mut chunk = vec![0u8; 64 * 1024];

    while !conn.quit {
        let n = match reader.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        };
        buf.extend_from_slice(&chunk[..n]);

        // Drain every complete command in the buffer (pipelining), batching
        // all their replies into a single write.
        let mut pos = 0;
        let mut processed = 0u64;
        while !conn.quit {
            match resp::parse_command(&buf[pos..]) {
                Ok(Some((args, used))) => {
                    pos += used;
                    if args.is_empty() {
                        continue;
                    }
                    processed += 1;
                    conn.dispatch(&srv, args);
                    if conn.out.len() > 1024 * 1024 {
                        if let Some(aof) = &srv.aof {
                            aof.flush_to_os();
                        }
                        if conn.flush().is_err() {
                            conn.quit = true;
                        }
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    conn.reply(Value::err(format!("ERR Protocol error: {}", e.0)));
                    conn.quit = true;
                }
            }
        }
        buf.drain(..pos);
        srv.stats.commands.fetch_add(processed, Relaxed);
        if let Some(aof) = &srv.aof {
            aof.flush_to_os();
        }
        if conn.flush().is_err() {
            break;
        }
    }

    srv.pubsub.unsubscribe_all(conn.id, &conn.subs);
    srv.stats.connected.fetch_sub(1, Relaxed);
}
