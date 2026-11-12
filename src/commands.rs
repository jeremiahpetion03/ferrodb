//! Command implementations.
//!
//! Every write command appends itself (or a deterministic rewrite of itself)
//! to the AOF *while still holding the shard lock* of the key it modified.
//! That guarantees the log order for each key equals the order in which the
//! mutations were actually applied, even with many concurrent clients.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::glob::glob_match;
use crate::resp::Value;
use crate::server::Server;
use crate::store::{Data, Entry, clock_ms, live};

pub const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
const NOT_INT: &str = "ERR value is not an integer or out of range";
const SYNTAX: &str = "ERR syntax error";

pub struct Ctx<'a> {
    pub srv: &'a Server,
    /// True while replaying the AOF at startup. During loading, nothing is
    /// considered expired (matching Redis), so replay is deterministic no
    /// matter how much later it happens; the background sweeper cleans up
    /// afterwards.
    pub loading: bool,
}

impl Ctx<'_> {
    fn now(&self) -> u64 {
        if self.loading { 0 } else { clock_ms() }
    }

    fn log(&self, args: &[&[u8]]) {
        if let (false, Some(aof)) = (self.loading, &self.srv.aof) {
            aof.append(args);
        }
    }

    fn log_all(&self, args: &[Vec<u8>]) {
        let refs: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
        self.log(&refs);
    }
}

pub fn upper(b: &[u8]) -> String {
    String::from_utf8_lossy(b).to_ascii_uppercase()
}

/// Strict integer parsing, matching Redis (no '+', no whitespace).
pub fn parse_i64(b: &[u8]) -> Option<i64> {
    if b.is_empty() || b.len() > 20 || b[0] == b'+' {
        return None;
    }
    std::str::from_utf8(b).ok()?.parse().ok()
}

fn wrong_args(cmd: &str) -> Value {
    Value::err(format!(
        "ERR wrong number of arguments for '{}' command",
        cmd.to_ascii_lowercase()
    ))
}

fn bulk_array<I: IntoIterator<Item = Vec<u8>>>(items: I) -> Value {
    Value::Array(items.into_iter().map(Value::Bulk).collect())
}

/// Redis-style arity: positive = exact argument count (including the command
/// name), negative = at least that many.
fn arity(cmd: &str) -> Option<i32> {
    Some(match cmd {
        "PING" => -1,
        "ECHO" => 2,
        "GET" | "GETDEL" | "STRLEN" => 2,
        "SET" => -3,
        "MGET" => -2,
        "MSET" => -3,
        "INCR" | "DECR" => 2,
        "INCRBY" | "DECRBY" | "APPEND" => 3,
        "DEL" | "EXISTS" => -2,
        "TYPE" | "KEYS" => 2,
        "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" => 3,
        "TTL" | "PTTL" | "PERSIST" => 2,
        "DBSIZE" | "BGREWRITEAOF" => 1,
        "FLUSHALL" | "FLUSHDB" | "SHUTDOWN" => -1,
        "LPUSH" | "RPUSH" => -3,
        "LPOP" | "RPOP" => -2,
        "LLEN" => 2,
        "LRANGE" => 4,
        "LINDEX" => 3,
        "HSET" => -4,
        "HGET" | "HEXISTS" => 3,
        "HDEL" => -3,
        "HGETALL" | "HLEN" => 2,
        "HINCRBY" => 4,
        "SADD" | "SREM" => -3,
        "SMEMBERS" | "SCARD" => 2,
        "SISMEMBER" => 3,
        "INFO" | "COMMAND" => -1,
        "CONFIG" | "CLIENT" => -2,
        "SELECT" => 2,
        _ => return None,
    })
}

pub fn execute(ctx: &Ctx, args: &[Vec<u8>]) -> Value {
    execute_named(ctx, &upper(&args[0]), args)
}

/// Like `execute`, for callers that have already upper-cased the command name.
pub fn execute_named(ctx: &Ctx, cmd: &str, args: &[Vec<u8>]) -> Value {
    let Some(a) = arity(cmd) else {
        return Value::err(format!(
            "ERR unknown command '{}'",
            String::from_utf8_lossy(&args[0])
        ));
    };
    let n = args.len() as i32;
    if (a > 0 && n != a) || (a < 0 && n < -a) {
        return wrong_args(cmd);
    }

    match cmd {
        // --- connection / server ---------------------------------------
        "PING" => match args.len() {
            1 => Value::Simple("PONG".into()),
            2 => Value::Bulk(args[1].clone()),
            _ => wrong_args(cmd),
        },
        "ECHO" => Value::Bulk(args[1].clone()),
        "SELECT" => match parse_i64(&args[1]) {
            Some(0) => Value::ok(),
            Some(_) => Value::err("ERR DB index is out of range"),
            None => Value::err(NOT_INT),
        },
        "COMMAND" => Value::Array(vec![]),
        "CLIENT" => match upper(&args[1]).as_str() {
            "GETNAME" => Value::Null,
            _ => Value::ok(),
        },
        "CONFIG" => config(ctx, args),
        "INFO" => info(ctx),
        "DBSIZE" => Value::Integer(ctx.srv.store.stats().0 as i64),
        "FLUSHALL" | "FLUSHDB" => {
            let mut shards = ctx.srv.store.lock_all();
            for s in shards.iter_mut() {
                s.clear();
            }
            ctx.log(&[b"FLUSHALL"]);
            Value::ok()
        }
        "BGREWRITEAOF" => match ctx.srv.rewrite_aof() {
            Ok(_) => Value::Simple("Append only file rewritten".into()),
            Err(e) => Value::err(format!("ERR {e}")),
        },
        "SHUTDOWN" => {
            if let Some(aof) = &ctx.srv.aof {
                let _ = aof.sync();
            }
            crate::log("SHUTDOWN requested, exiting");
            std::process::exit(0);
        }

        // --- strings ---------------------------------------------------
        "GET" => get(ctx, &args[1]),
        "GETDEL" => getdel(ctx, &args[1]),
        "SET" => set(ctx, args),
        "MGET" => Value::Array(
            args[1..]
                .iter()
                .map(|k| match get(ctx, k) {
                    Value::Error(_) => Value::Null,
                    v => v,
                })
                .collect(),
        ),
        "MSET" => mset(ctx, args),
        "INCR" => incr_by(ctx, args, 1),
        "DECR" => incr_by(ctx, args, -1),
        "INCRBY" | "DECRBY" => match parse_i64(&args[2]) {
            Some(d) if cmd == "INCRBY" => incr_by(ctx, args, d),
            Some(d) => match d.checked_neg() {
                Some(d) => incr_by(ctx, args, d),
                None => Value::err(NOT_INT),
            },
            None => Value::err(NOT_INT),
        },
        "APPEND" => append(ctx, args),
        "STRLEN" => {
            let mut sh = ctx.srv.store.lock(&args[1]);
            match live(&mut sh, &args[1], ctx.now()) {
                None => Value::Integer(0),
                Some(Entry {
                    data: Data::Str(s), ..
                }) => Value::Integer(s.len() as i64),
                Some(_) => Value::err(WRONGTYPE),
            }
        }

        // --- keyspace --------------------------------------------------
        "DEL" => {
            let mut removed = 0;
            for key in &args[1..] {
                let mut sh = ctx.srv.store.lock(key);
                if live(&mut sh, key, ctx.now()).is_some() {
                    sh.remove(key.as_slice());
                    ctx.log(&[b"DEL", key]);
                    removed += 1;
                }
            }
            Value::Integer(removed)
        }
        "EXISTS" => {
            let mut count = 0;
            for key in &args[1..] {
                let mut sh = ctx.srv.store.lock(key);
                if live(&mut sh, key, ctx.now()).is_some() {
                    count += 1;
                }
            }
            Value::Integer(count)
        }
        "TYPE" => {
            let mut sh = ctx.srv.store.lock(&args[1]);
            let t = live(&mut sh, &args[1], ctx.now()).map_or("none", |e| e.data.type_name());
            Value::Simple(t.into())
        }
        "KEYS" => {
            let now = ctx.now();
            let store = &ctx.srv.store;
            let mut out = Vec::new();
            for i in 0..store.num_shards() {
                let sh = store.lock_index(i);
                out.extend(
                    sh.iter()
                        .filter(|(k, e)| !e.is_expired(now) && glob_match(&args[1], k))
                        .map(|(k, _)| k.clone()),
                );
            }
            bulk_array(out)
        }
        "EXPIRE" => expire(ctx, args, 1000, false),
        "PEXPIRE" => expire(ctx, args, 1, false),
        "EXPIREAT" => expire(ctx, args, 1000, true),
        "PEXPIREAT" => expire(ctx, args, 1, true),
        "TTL" | "PTTL" => {
            let mut sh = ctx.srv.store.lock(&args[1]);
            let now = ctx.now();
            match live(&mut sh, &args[1], now) {
                None => Value::Integer(-2),
                Some(Entry {
                    expires_at: None, ..
                }) => Value::Integer(-1),
                Some(Entry {
                    expires_at: Some(t),
                    ..
                }) => {
                    let ms = t.saturating_sub(now) as i64;
                    Value::Integer(if cmd == "PTTL" { ms } else { (ms + 500) / 1000 })
                }
            }
        }
        "PERSIST" => {
            let mut sh = ctx.srv.store.lock(&args[1]);
            match live(&mut sh, &args[1], ctx.now()) {
                Some(e) if e.expires_at.is_some() => {
                    e.expires_at = None;
                    ctx.log_all(args);
                    Value::Integer(1)
                }
                _ => Value::Integer(0),
            }
        }

        // --- lists -----------------------------------------------------
        "LPUSH" => push(ctx, args, true),
        "RPUSH" => push(ctx, args, false),
        "LPOP" => pop(ctx, args, true),
        "RPOP" => pop(ctx, args, false),
        "LLEN" => with_list(
            ctx,
            &args[1],
            |l| Value::Integer(l.len() as i64),
            Value::Integer(0),
        ),
        "LRANGE" => {
            let (Some(start), Some(stop)) = (parse_i64(&args[2]), parse_i64(&args[3])) else {
                return Value::err(NOT_INT);
            };
            with_list(
                ctx,
                &args[1],
                |l| match norm_range(l.len() as i64, start, stop) {
                    Some((s, e)) => bulk_array(l.range(s..=e).cloned()),
                    None => Value::Array(vec![]),
                },
                Value::Array(vec![]),
            )
        }
        "LINDEX" => {
            let Some(idx) = parse_i64(&args[2]) else {
                return Value::err(NOT_INT);
            };
            with_list(
                ctx,
                &args[1],
                |l| {
                    let i = if idx < 0 { l.len() as i64 + idx } else { idx };
                    if i < 0 {
                        return Value::Null;
                    }
                    l.get(i as usize)
                        .map_or(Value::Null, |v| Value::Bulk(v.clone()))
                },
                Value::Null,
            )
        }

        // --- hashes ----------------------------------------------------
        "HSET" => hset(ctx, args),
        "HGET" => with_hash(
            ctx,
            &args[1],
            |h| {
                h.get(&args[2])
                    .map_or(Value::Null, |v| Value::Bulk(v.clone()))
            },
            Value::Null,
        ),
        "HEXISTS" => with_hash(
            ctx,
            &args[1],
            |h| Value::Integer(h.contains_key(&args[2]) as i64),
            Value::Integer(0),
        ),
        "HLEN" => with_hash(
            ctx,
            &args[1],
            |h| Value::Integer(h.len() as i64),
            Value::Integer(0),
        ),
        "HGETALL" => with_hash(
            ctx,
            &args[1],
            |h| bulk_array(h.iter().flat_map(|(k, v)| [k.clone(), v.clone()])),
            Value::Array(vec![]),
        ),
        "HDEL" => hdel(ctx, args),
        "HINCRBY" => hincrby(ctx, args),

        // --- sets ------------------------------------------------------
        "SADD" => sadd(ctx, args),
        "SREM" => srem(ctx, args),
        "SMEMBERS" => with_set(
            ctx,
            &args[1],
            |s| bulk_array(s.iter().cloned()),
            Value::Array(vec![]),
        ),
        "SCARD" => with_set(
            ctx,
            &args[1],
            |s| Value::Integer(s.len() as i64),
            Value::Integer(0),
        ),
        "SISMEMBER" => with_set(
            ctx,
            &args[1],
            |s| Value::Integer(s.contains(&args[2]) as i64),
            Value::Integer(0),
        ),

        _ => Value::err(format!("ERR unknown command '{cmd}'")),
    }
}

// ---------------------------------------------------------------------------
// read helpers

fn with_list(
    ctx: &Ctx,
    key: &[u8],
    f: impl FnOnce(&VecDeque<Vec<u8>>) -> Value,
    missing: Value,
) -> Value {
    let mut sh = ctx.srv.store.lock(key);
    match live(&mut sh, key, ctx.now()) {
        None => missing,
        Some(Entry {
            data: Data::List(l),
            ..
        }) => f(l),
        Some(_) => Value::err(WRONGTYPE),
    }
}

fn with_hash(
    ctx: &Ctx,
    key: &[u8],
    f: impl FnOnce(&HashMap<Vec<u8>, Vec<u8>>) -> Value,
    missing: Value,
) -> Value {
    let mut sh = ctx.srv.store.lock(key);
    match live(&mut sh, key, ctx.now()) {
        None => missing,
        Some(Entry {
            data: Data::Hash(h),
            ..
        }) => f(h),
        Some(_) => Value::err(WRONGTYPE),
    }
}

fn with_set(
    ctx: &Ctx,
    key: &[u8],
    f: impl FnOnce(&HashSet<Vec<u8>>) -> Value,
    missing: Value,
) -> Value {
    let mut sh = ctx.srv.store.lock(key);
    match live(&mut sh, key, ctx.now()) {
        None => missing,
        Some(Entry {
            data: Data::Set(s), ..
        }) => f(s),
        Some(_) => Value::err(WRONGTYPE),
    }
}

/// Convert Redis-style inclusive (possibly negative) indices into a valid
/// `start..=end` range, or None if the range is empty.
fn norm_range(len: i64, start: i64, stop: i64) -> Option<(usize, usize)> {
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if stop < 0 { len + stop } else { stop };
    if s < 0 {
        s = 0;
    }
    if e >= len {
        e = len - 1;
    }
    if s > e || s >= len {
        None
    } else {
        Some((s as usize, e as usize))
    }
}

// ---------------------------------------------------------------------------
// strings

fn get(ctx: &Ctx, key: &[u8]) -> Value {
    let mut sh = ctx.srv.store.lock(key);
    match live(&mut sh, key, ctx.now()) {
        None => Value::Null,
        Some(Entry {
            data: Data::Str(s), ..
        }) => Value::Bulk(s.clone()),
        Some(_) => Value::err(WRONGTYPE),
    }
}

fn getdel(ctx: &Ctx, key: &[u8]) -> Value {
    let mut sh = ctx.srv.store.lock(key);
    match live(&mut sh, key, ctx.now()) {
        None => return Value::Null,
        Some(Entry {
            data: Data::Str(_), ..
        }) => {}
        Some(_) => return Value::err(WRONGTYPE),
    }
    let Some(Entry {
        data: Data::Str(s), ..
    }) = sh.remove(key)
    else {
        unreachable!()
    };
    ctx.log(&[b"DEL", key]);
    Value::Bulk(s)
}

/// SET key value [NX|XX] [GET] [EX s|PX ms|EXAT ts|PXAT ms-ts|KEEPTTL]
fn set(ctx: &Ctx, args: &[Vec<u8>]) -> Value {
    let (key, val) = (&args[1], &args[2]);
    let (mut nx, mut xx, mut keepttl, mut want_get) = (false, false, false, false);
    let mut expire: Option<u64> = None;

    let mut i = 3;
    while i < args.len() {
        let opt = upper(&args[i]);
        match opt.as_str() {
            "NX" => nx = true,
            "XX" => xx = true,
            "KEEPTTL" => keepttl = true,
            "GET" => want_get = true,
            "EX" | "PX" | "EXAT" | "PXAT" => {
                if expire.is_some() {
                    return Value::err(SYNTAX);
                }
                i += 1;
                let Some(raw) = args.get(i) else {
                    return Value::err(SYNTAX);
                };
                let Some(n) = parse_i64(raw) else {
                    return Value::err(NOT_INT);
                };
                if n <= 0 {
                    return Value::err("ERR invalid expire time in 'set' command");
                }
                let n = n as u64;
                let at = match opt.as_str() {
                    "EX" => n
                        .checked_mul(1000)
                        .and_then(|ms| ms.checked_add(clock_ms())),
                    "PX" => n.checked_add(clock_ms()),
                    "EXAT" => n.checked_mul(1000),
                    _ => Some(n),
                };
                let Some(at) = at else {
                    return Value::err("ERR invalid expire time in 'set' command");
                };
                expire = Some(at);
            }
            _ => return Value::err(SYNTAX),
        }
        i += 1;
    }
    if (nx && xx) || (keepttl && expire.is_some()) {
        return Value::err(SYNTAX);
    }

    let mut sh = ctx.srv.store.lock(key);
    let (exists, old_val, old_exp) = match live(&mut sh, key, ctx.now()) {
        None => (false, None, None),
        Some(Entry {
            data: Data::Str(s),
            expires_at,
        }) => (true, want_get.then(|| s.clone()), *expires_at),
        Some(_) if want_get => return Value::err(WRONGTYPE),
        Some(e) => (true, None, e.expires_at),
    };
    let reply_old = || old_val.clone().map_or(Value::Null, Value::Bulk);

    if (nx && exists) || (xx && !exists) {
        return if want_get { reply_old() } else { Value::Null };
    }

    let final_exp = if keepttl { old_exp } else { expire };
    sh.insert(
        key.clone(),
        Entry {
            data: Data::Str(val.clone()),
            expires_at: final_exp,
        },
    );

    // Log a deterministic form: absolute expiry, no NX/XX/GET flags.
    match final_exp {
        Some(t) => ctx.log(&[b"SET", key, val, b"PXAT", t.to_string().as_bytes()]),
        None => ctx.log(&[b"SET", key, val]),
    }

    if want_get { reply_old() } else { Value::ok() }
}

fn mset(ctx: &Ctx, args: &[Vec<u8>]) -> Value {
    if !(args.len() - 1).is_multiple_of(2) {
        return wrong_args("MSET");
    }
    for pair in args[1..].chunks(2) {
        let (key, val) = (&pair[0], &pair[1]);
        let mut sh = ctx.srv.store.lock(key);
        sh.insert(key.clone(), Entry::new(Data::Str(val.clone())));
        ctx.log(&[b"SET", key, val]);
    }
    Value::ok()
}

fn incr_by(ctx: &Ctx, args: &[Vec<u8>], delta: i64) -> Value {
    let key = &args[1];
    let mut sh = ctx.srv.store.lock(key);
    let result = match live(&mut sh, key, ctx.now()) {
        Some(Entry {
            data: Data::Str(s), ..
        }) => {
            let Some(cur) = parse_i64(s) else {
                return Value::err(NOT_INT);
            };
            let Some(next) = cur.checked_add(delta) else {
                return Value::err("ERR increment or decrement would overflow");
            };
            *s = next.to_string().into_bytes(); // TTL is preserved, as in Redis
            next
        }
        Some(_) => return Value::err(WRONGTYPE),
        None => {
            sh.insert(
                key.clone(),
                Entry::new(Data::Str(delta.to_string().into_bytes())),
            );
            delta
        }
    };
    ctx.log(&[b"INCRBY", key, delta.to_string().as_bytes()]);
    Value::Integer(result)
}

fn append(ctx: &Ctx, args: &[Vec<u8>]) -> Value {
    let key = &args[1];
    let mut sh = ctx.srv.store.lock(key);
    let len = match live(&mut sh, key, ctx.now()) {
        Some(Entry {
            data: Data::Str(s), ..
        }) => {
            s.extend_from_slice(&args[2]);
            s.len()
        }
        Some(_) => return Value::err(WRONGTYPE),
        None => {
            sh.insert(key.clone(), Entry::new(Data::Str(args[2].clone())));
            args[2].len()
        }
    };
    ctx.log_all(args);
    Value::Integer(len as i64)
}

// ---------------------------------------------------------------------------
// expiry

fn expire(ctx: &Ctx, args: &[Vec<u8>], unit_ms: i64, absolute: bool) -> Value {
    let key = &args[1];
    let Some(n) = parse_i64(&args[2]) else {
        return Value::err(NOT_INT);
    };
    let at = n.checked_mul(unit_ms).and_then(|ms| {
        if absolute {
            Some(ms)
        } else {
            ms.checked_add(clock_ms() as i64)
        }
    });
    let Some(at) = at else {
        return Value::err("ERR invalid expire time");
    };

    let mut sh = ctx.srv.store.lock(key);
    if live(&mut sh, key, ctx.now()).is_none() {
        return Value::Integer(0);
    }
    if !ctx.loading && at <= clock_ms() as i64 {
        // A deadline in the past deletes the key immediately.
        sh.remove(key.as_slice());
        ctx.log(&[b"DEL", key]);
        return Value::Integer(1);
    }
    let at = at.max(1) as u64;
    if let Some(e) = sh.get_mut(key.as_slice()) {
        e.expires_at = Some(at);
    }
    ctx.log(&[b"PEXPIREAT", key, at.to_string().as_bytes()]);
    Value::Integer(1)
}

// ---------------------------------------------------------------------------
// lists

fn push(ctx: &Ctx, args: &[Vec<u8>], left: bool) -> Value {
    let key = &args[1];
    let fill = |l: &mut VecDeque<Vec<u8>>| {
        for v in &args[2..] {
            if left {
                l.push_front(v.clone())
            } else {
                l.push_back(v.clone())
            }
        }
        l.len()
    };
    let mut sh = ctx.srv.store.lock(key);
    let len = match live(&mut sh, key, ctx.now()) {
        Some(Entry {
            data: Data::List(l),
            ..
        }) => fill(l),
        Some(_) => return Value::err(WRONGTYPE),
        None => {
            let mut l = VecDeque::new();
            let n = fill(&mut l);
            sh.insert(key.clone(), Entry::new(Data::List(l)));
            n
        }
    };
    ctx.log_all(args);
    Value::Integer(len as i64)
}

fn pop(ctx: &Ctx, args: &[Vec<u8>], left: bool) -> Value {
    if args.len() > 3 {
        return wrong_args(if left { "LPOP" } else { "RPOP" });
    }
    let key = &args[1];
    let count = match args.get(2) {
        None => None,
        Some(raw) => match parse_i64(raw) {
            Some(n) if n >= 0 => Some(n as usize),
            _ => return Value::err("ERR value is out of range, must be positive"),
        },
    };
    let mut sh = ctx.srv.store.lock(key);
    let (popped, now_empty) = match live(&mut sh, key, ctx.now()) {
        None => {
            return if count.is_some() {
                Value::NullArray
            } else {
                Value::Null
            };
        }
        Some(Entry {
            data: Data::List(l),
            ..
        }) => {
            let n = count.unwrap_or(1).min(l.len());
            let popped: Vec<Vec<u8>> = (0..n)
                .filter_map(|_| if left { l.pop_front() } else { l.pop_back() })
                .collect();
            (popped, l.is_empty())
        }
        Some(_) => return Value::err(WRONGTYPE),
    };
    if now_empty {
        sh.remove(key.as_slice());
    }
    if !popped.is_empty() {
        ctx.log_all(args);
    }
    match count {
        Some(_) => bulk_array(popped),
        None => popped.into_iter().next().map_or(Value::Null, Value::Bulk),
    }
}

// ---------------------------------------------------------------------------
// hashes

fn hset(ctx: &Ctx, args: &[Vec<u8>]) -> Value {
    if !(args.len() - 2).is_multiple_of(2) {
        return wrong_args("HSET");
    }
    let key = &args[1];
    let fill = |h: &mut HashMap<Vec<u8>, Vec<u8>>| {
        args[2..]
            .chunks(2)
            .filter(|p| h.insert(p[0].clone(), p[1].clone()).is_none())
            .count()
    };
    let mut sh = ctx.srv.store.lock(key);
    let added = match live(&mut sh, key, ctx.now()) {
        Some(Entry {
            data: Data::Hash(h),
            ..
        }) => fill(h),
        Some(_) => return Value::err(WRONGTYPE),
        None => {
            let mut h = HashMap::new();
            let n = fill(&mut h);
            sh.insert(key.clone(), Entry::new(Data::Hash(h)));
            n
        }
    };
    ctx.log_all(args);
    Value::Integer(added as i64)
}

fn hdel(ctx: &Ctx, args: &[Vec<u8>]) -> Value {
    let key = &args[1];
    let mut sh = ctx.srv.store.lock(key);
    let (removed, now_empty) = match live(&mut sh, key, ctx.now()) {
        None => return Value::Integer(0),
        Some(Entry {
            data: Data::Hash(h),
            ..
        }) => {
            let n = args[2..].iter().filter(|f| h.remove(*f).is_some()).count();
            (n, h.is_empty())
        }
        Some(_) => return Value::err(WRONGTYPE),
    };
    if now_empty {
        sh.remove(key.as_slice());
    }
    if removed > 0 {
        ctx.log_all(args);
    }
    Value::Integer(removed as i64)
}

fn hincrby(ctx: &Ctx, args: &[Vec<u8>]) -> Value {
    let (key, field) = (&args[1], &args[2]);
    let Some(delta) = parse_i64(&args[3]) else {
        return Value::err(NOT_INT);
    };
    let mut sh = ctx.srv.store.lock(key);
    let bump = |h: &mut HashMap<Vec<u8>, Vec<u8>>| -> Result<i64, Value> {
        let cur = match h.get(field) {
            Some(v) => {
                parse_i64(v).ok_or_else(|| Value::err("ERR hash value is not an integer"))?
            }
            None => 0,
        };
        let next = cur
            .checked_add(delta)
            .ok_or_else(|| Value::err("ERR increment or decrement would overflow"))?;
        h.insert(field.clone(), next.to_string().into_bytes());
        Ok(next)
    };
    let result = match live(&mut sh, key, ctx.now()) {
        Some(Entry {
            data: Data::Hash(h),
            ..
        }) => bump(h),
        Some(_) => return Value::err(WRONGTYPE),
        None => {
            let mut h = HashMap::new();
            let r = bump(&mut h);
            if r.is_ok() {
                sh.insert(key.clone(), Entry::new(Data::Hash(h)));
            }
            r
        }
    };
    match result {
        Ok(n) => {
            ctx.log_all(args);
            Value::Integer(n)
        }
        Err(e) => e,
    }
}

// ---------------------------------------------------------------------------
// sets

fn sadd(ctx: &Ctx, args: &[Vec<u8>]) -> Value {
    let key = &args[1];
    let fill =
        |s: &mut HashSet<Vec<u8>>| args[2..].iter().filter(|m| s.insert((*m).clone())).count();
    let mut sh = ctx.srv.store.lock(key);
    let added = match live(&mut sh, key, ctx.now()) {
        Some(Entry {
            data: Data::Set(s), ..
        }) => fill(s),
        Some(_) => return Value::err(WRONGTYPE),
        None => {
            let mut s = HashSet::new();
            let n = fill(&mut s);
            sh.insert(key.clone(), Entry::new(Data::Set(s)));
            n
        }
    };
    if added > 0 {
        ctx.log_all(args);
    }
    Value::Integer(added as i64)
}

fn srem(ctx: &Ctx, args: &[Vec<u8>]) -> Value {
    let key = &args[1];
    let mut sh = ctx.srv.store.lock(key);
    let (removed, now_empty) = match live(&mut sh, key, ctx.now()) {
        None => return Value::Integer(0),
        Some(Entry {
            data: Data::Set(s), ..
        }) => {
            let n = args[2..].iter().filter(|m| s.remove(*m)).count();
            (n, s.is_empty())
        }
        Some(_) => return Value::err(WRONGTYPE),
    };
    if now_empty {
        sh.remove(key.as_slice());
    }
    if removed > 0 {
        ctx.log_all(args);
    }
    Value::Integer(removed as i64)
}

// ---------------------------------------------------------------------------
// server introspection

fn config(ctx: &Ctx, args: &[Vec<u8>]) -> Value {
    match upper(&args[1]).as_str() {
        "GET" if args.len() == 3 => {
            let param = String::from_utf8_lossy(&args[2]).to_ascii_lowercase();
            let value = match param.as_str() {
                "appendonly" => if ctx.srv.aof.is_some() { "yes" } else { "no" }.to_string(),
                "save" => String::new(),
                "port" => ctx.srv.config.port.to_string(),
                "databases" => "1".into(),
                _ => return Value::Array(vec![]),
            };
            Value::Array(vec![Value::bulk(param), Value::bulk(value)])
        }
        "RESETSTAT" => Value::ok(),
        _ => Value::err("ERR CONFIG subcommand not supported"),
    }
}

fn info(ctx: &Ctx) -> Value {
    use std::sync::atomic::Ordering::Relaxed;
    let srv = ctx.srv;
    let (keys, expires) = srv.store.stats();
    let st = &srv.stats;
    let aof = srv.aof.as_ref();
    let text = format!(
        "# Server\r\n\
         ferrodb_version:{}\r\n\
         redis_version:7.0.0-compat\r\n\
         tcp_port:{}\r\n\
         uptime_in_seconds:{}\r\n\
         keyspace_shards:{}\r\n\
         \r\n# Clients\r\n\
         connected_clients:{}\r\n\
         pubsub_channels:{}\r\n\
         \r\n# Persistence\r\n\
         aof_enabled:{}\r\n\
         aof_current_size:{}\r\n\
         \r\n# Stats\r\n\
         total_connections_received:{}\r\n\
         total_commands_processed:{}\r\n\
         expired_keys:{}\r\n\
         \r\n# Keyspace\r\n\
         db0:keys={keys},expires={expires}\r\n",
        env!("CARGO_PKG_VERSION"),
        srv.config.port,
        srv.started.elapsed().as_secs(),
        srv.store.num_shards(),
        st.connected.load(Relaxed),
        srv.pubsub.num_channels(),
        aof.is_some() as u8,
        aof.map_or(0, |a| a.size()),
        st.connections.load(Relaxed),
        st.commands.load(Relaxed),
        st.expired_keys.load(Relaxed),
    );
    Value::Bulk(text.into_bytes())
}
