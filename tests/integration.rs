//! End-to-end tests: real TCP connections against a server running in-process.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use ferrodb::commands::{Ctx, execute};
use ferrodb::resp::{Value, parse_value};
use ferrodb::server::{Config, Server, run};

fn start(config: Config) -> (Arc<Server>, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let srv = Server::new(config).unwrap();
    let s = srv.clone();
    thread::spawn(move || run(s, listener));
    (srv, port)
}

fn temp_aof(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("ferrodb-{}-{name}.aof", std::process::id()));
    let _ = std::fs::remove_file(&p);
    p
}

struct Client {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Client {
    fn connect(port: u16) -> Client {
        let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        Client {
            stream,
            buf: Vec::new(),
        }
    }

    fn send(&mut self, args: &[&str]) {
        let v = Value::Array(args.iter().map(|a| Value::bulk(*a)).collect());
        self.stream.write_all(&v.to_bytes()).unwrap();
    }

    fn read(&mut self) -> Value {
        loop {
            if let Some((v, used)) = parse_value(&self.buf).unwrap() {
                self.buf.drain(..used);
                return v;
            }
            let mut chunk = [0u8; 4096];
            let n = self.stream.read(&mut chunk).expect("read reply");
            assert!(n > 0, "server closed connection");
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }

    fn cmd(&mut self, args: &[&str]) -> Value {
        self.send(args);
        self.read()
    }
}

fn bulk(s: &str) -> Value {
    Value::bulk(s)
}

fn arr(items: &[&str]) -> Value {
    Value::Array(items.iter().map(|s| bulk(s)).collect())
}

#[test]
fn strings_and_options() {
    let (_, port) = start(Config::default());
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&["PING"]), Value::Simple("PONG".into()));
    assert_eq!(c.cmd(&["SET", "k", "v1"]), Value::ok());
    assert_eq!(c.cmd(&["GET", "k"]), bulk("v1"));
    assert_eq!(c.cmd(&["SET", "k", "v2", "NX"]), Value::Null);
    assert_eq!(c.cmd(&["SET", "k", "v2", "XX", "GET"]), bulk("v1"));
    assert_eq!(c.cmd(&["SET", "missing", "x", "XX"]), Value::Null);
    assert_eq!(c.cmd(&["GET", "k"]), bulk("v2"));
    assert_eq!(c.cmd(&["APPEND", "k", "!!"]), Value::Integer(4));
    assert_eq!(c.cmd(&["STRLEN", "k"]), Value::Integer(4));
    assert_eq!(c.cmd(&["INCR", "n"]), Value::Integer(1));
    assert_eq!(c.cmd(&["INCRBY", "n", "41"]), Value::Integer(42));
    assert_eq!(c.cmd(&["DECRBY", "n", "2"]), Value::Integer(40));
    assert_eq!(
        c.cmd(&["INCR", "k"]),
        Value::err("ERR value is not an integer or out of range")
    );
    assert_eq!(c.cmd(&["MSET", "a", "1", "b", "2"]), Value::ok());
    assert_eq!(
        c.cmd(&["MGET", "a", "b", "nope"]),
        Value::Array(vec![bulk("1"), bulk("2"), Value::Null])
    );
    assert_eq!(c.cmd(&["GETDEL", "a"]), bulk("1"));
    assert_eq!(c.cmd(&["EXISTS", "a", "b", "b"]), Value::Integer(2));
    assert_eq!(c.cmd(&["DEL", "a", "b", "k"]), Value::Integer(2));
}

#[test]
fn errors_match_redis() {
    let (_, port) = start(Config::default());
    let mut c = Client::connect(port);
    assert!(matches!(c.cmd(&["NOPE"]), Value::Error(e) if e.starts_with("ERR unknown command")));
    assert_eq!(
        c.cmd(&["GET"]),
        Value::err("ERR wrong number of arguments for 'get' command")
    );
    c.cmd(&["LPUSH", "list", "x"]);
    assert_eq!(
        c.cmd(&["GET", "list"]),
        Value::err(ferrodb::commands::WRONGTYPE)
    );
    assert_eq!(
        c.cmd(&["SET", "k", "v", "EX", "0"]),
        Value::err("ERR invalid expire time in 'set' command")
    );
    assert_eq!(
        c.cmd(&["SET", "k", "v", "NX", "XX"]),
        Value::err("ERR syntax error")
    );
}

#[test]
fn expiry_lazy_and_active() {
    let (srv, port) = start(Config::default());
    let mut c = Client::connect(port);
    c.cmd(&["SET", "short", "x", "PX", "100"]);
    c.cmd(&["SET", "long", "x", "EX", "100"]);
    c.cmd(&["SET", "forever", "x"]);
    assert!(matches!(c.cmd(&["PTTL", "short"]), Value::Integer(n) if n > 0 && n <= 100));
    assert_eq!(c.cmd(&["TTL", "long"]), Value::Integer(100));
    assert_eq!(c.cmd(&["TTL", "forever"]), Value::Integer(-1));
    assert_eq!(c.cmd(&["TTL", "nope"]), Value::Integer(-2));

    // INCR keeps the TTL; PERSIST removes it.
    c.cmd(&["SET", "counter", "1", "EX", "100"]);
    c.cmd(&["INCR", "counter"]);
    assert_eq!(c.cmd(&["TTL", "counter"]), Value::Integer(100));
    assert_eq!(c.cmd(&["PERSIST", "counter"]), Value::Integer(1));
    assert_eq!(c.cmd(&["TTL", "counter"]), Value::Integer(-1));

    // Expire a batch of keys and never touch them: the background sweeper
    // must reclaim them on its own.
    for i in 0..500 {
        c.cmd(&["SET", &format!("tmp:{i}"), "x", "PX", "50"]);
    }
    thread::sleep(Duration::from_millis(2500));
    assert_eq!(c.cmd(&["GET", "short"]), Value::Null);
    let (keys, _) = srv.store.stats();
    assert_eq!(keys, 3, "active expiry should have removed all tmp:* keys");

    // EXPIRE with a past deadline deletes immediately.
    assert_eq!(c.cmd(&["EXPIRE", "forever", "-1"]), Value::Integer(1));
    assert_eq!(c.cmd(&["EXISTS", "forever"]), Value::Integer(0));
}

#[test]
fn lists_hashes_sets() {
    let (_, port) = start(Config::default());
    let mut c = Client::connect(port);
    assert_eq!(c.cmd(&["RPUSH", "l", "a", "b", "c"]), Value::Integer(3));
    assert_eq!(c.cmd(&["LPUSH", "l", "z"]), Value::Integer(4));
    assert_eq!(
        c.cmd(&["LRANGE", "l", "0", "-1"]),
        arr(&["z", "a", "b", "c"])
    );
    assert_eq!(c.cmd(&["LRANGE", "l", "-2", "100"]), arr(&["b", "c"]));
    assert_eq!(c.cmd(&["LINDEX", "l", "-1"]), bulk("c"));
    assert_eq!(c.cmd(&["LPOP", "l"]), bulk("z"));
    assert_eq!(c.cmd(&["RPOP", "l", "5"]), arr(&["c", "b", "a"]));
    assert_eq!(c.cmd(&["TYPE", "l"]), Value::Simple("none".into()));

    assert_eq!(
        c.cmd(&["HSET", "h", "f1", "1", "f2", "2"]),
        Value::Integer(2)
    );
    assert_eq!(c.cmd(&["HSET", "h", "f1", "10"]), Value::Integer(0));
    assert_eq!(c.cmd(&["HGET", "h", "f1"]), bulk("10"));
    assert_eq!(c.cmd(&["HINCRBY", "h", "f2", "5"]), Value::Integer(7));
    assert_eq!(c.cmd(&["HLEN", "h"]), Value::Integer(2));
    assert_eq!(c.cmd(&["HDEL", "h", "f1", "nope"]), Value::Integer(1));
    assert_eq!(c.cmd(&["HGETALL", "h"]), arr(&["f2", "7"]));

    assert_eq!(c.cmd(&["SADD", "s", "a", "b", "a"]), Value::Integer(2));
    assert_eq!(c.cmd(&["SISMEMBER", "s", "a"]), Value::Integer(1));
    assert_eq!(c.cmd(&["SREM", "s", "a"]), Value::Integer(1));
    assert_eq!(c.cmd(&["SMEMBERS", "s"]), arr(&["b"]));
    assert_eq!(c.cmd(&["SCARD", "s"]), Value::Integer(1));
}

#[test]
fn keys_glob() {
    let (_, port) = start(Config::default());
    let mut c = Client::connect(port);
    for k in ["user:1", "user:2", "user:10", "session:1"] {
        c.cmd(&["SET", k, "x"]);
    }
    let Value::Array(mut keys) = c.cmd(&["KEYS", "user:?"]) else {
        panic!()
    };
    keys.sort_by_key(|v| format!("{v:?}"));
    assert_eq!(keys, vec![bulk("user:1"), bulk("user:2")]);
    let Value::Array(all) = c.cmd(&["KEYS", "*"]) else {
        panic!()
    };
    assert_eq!(all.len(), 4);
}

#[test]
fn pipelining_10k_commands_in_one_write() {
    let (_, port) = start(Config::default());
    let mut c = Client::connect(port);
    let mut batch = Vec::new();
    for i in 0..10_000 {
        Value::Array(vec![bulk("RPUSH"), bulk("pipe"), bulk(&i.to_string())]).encode(&mut batch);
    }
    c.stream.write_all(&batch).unwrap();
    for i in 0..10_000 {
        assert_eq!(c.read(), Value::Integer(i + 1));
    }
    assert_eq!(c.cmd(&["LINDEX", "pipe", "9999"]), bulk("9999"));
}

#[test]
fn concurrent_increments_are_atomic() {
    let (_, port) = start(Config::default());
    let handles: Vec<_> = (0..16)
        .map(|_| {
            thread::spawn(move || {
                let mut c = Client::connect(port);
                for _ in 0..1000 {
                    c.cmd(&["INCR", "shared"]);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(Client::connect(port).cmd(&["GET", "shared"]), bulk("16000"));
}

#[test]
fn pubsub_delivers_messages() {
    let (_, port) = start(Config::default());
    let mut sub = Client::connect(port);
    let mut publisher = Client::connect(port);

    sub.send(&["SUBSCRIBE", "news", "sports"]);
    assert_eq!(
        sub.read(),
        Value::Array(vec![bulk("subscribe"), bulk("news"), Value::Integer(1)])
    );
    assert_eq!(
        sub.read(),
        Value::Array(vec![bulk("subscribe"), bulk("sports"), Value::Integer(2)])
    );

    assert_eq!(
        publisher.cmd(&["PUBLISH", "news", "hello"]),
        Value::Integer(1)
    );
    assert_eq!(
        publisher.cmd(&["PUBLISH", "nobody", "x"]),
        Value::Integer(0)
    );
    assert_eq!(sub.read(), arr(&["message", "news", "hello"]));

    // Regular commands are rejected while subscribed.
    assert!(matches!(sub.cmd(&["GET", "x"]), Value::Error(_)));
    assert_eq!(sub.cmd(&["PING"]), arr(&["pong", ""]));

    sub.send(&["UNSUBSCRIBE"]);
    assert!(matches!(sub.read(), Value::Array(a) if a[2] == Value::Integer(1)));
    assert!(matches!(sub.read(), Value::Array(a) if a[2] == Value::Integer(0)));
    assert_eq!(sub.cmd(&["PING"]), Value::Simple("PONG".into()));
    assert_eq!(
        publisher.cmd(&["PUBLISH", "news", "hello"]),
        Value::Integer(0)
    );
}

#[test]
fn aof_survives_restart() {
    let path = temp_aof("restart");
    let cfg = Config {
        aof_path: Some(path.clone()),
        ..Config::default()
    };
    {
        let (srv, port) = start(cfg.clone());
        let mut c = Client::connect(port);
        c.cmd(&["SET", "str", "hello"]);
        c.cmd(&["SET", "ttl", "x", "EX", "1000"]);
        c.cmd(&["SET", "gone", "x", "PX", "1"]);
        c.cmd(&["INCRBY", "n", "5"]);
        c.cmd(&["DECR", "n"]);
        c.cmd(&["RPUSH", "l", "a", "b", "c"]);
        c.cmd(&["LPOP", "l"]);
        c.cmd(&["HSET", "h", "f", "v"]);
        c.cmd(&["SADD", "s", "m1", "m2"]);
        c.cmd(&["SET", "del", "x"]);
        c.cmd(&["DEL", "del"]);
        srv.aof.as_ref().unwrap().sync().unwrap();
    }
    thread::sleep(Duration::from_millis(10));

    // A fresh server replays the log.
    let srv = Server::new(cfg).unwrap();
    let ctx = Ctx {
        srv: &srv,
        loading: false,
    };
    let run = |args: &[&str]| {
        let argv: Vec<Vec<u8>> = args.iter().map(|a| a.as_bytes().to_vec()).collect();
        execute(&ctx, &argv)
    };
    assert_eq!(run(&["GET", "str"]), bulk("hello"));
    assert!(matches!(run(&["TTL", "ttl"]), Value::Integer(n) if (990..=1000).contains(&n)));
    assert_eq!(run(&["GET", "gone"]), Value::Null);
    assert_eq!(run(&["GET", "n"]), bulk("4"));
    assert_eq!(run(&["LRANGE", "l", "0", "-1"]), arr(&["b", "c"]));
    assert_eq!(run(&["HGET", "h", "f"]), bulk("v"));
    assert_eq!(run(&["SCARD", "s"]), Value::Integer(2));
    assert_eq!(run(&["EXISTS", "del"]), Value::Integer(0));
    let _ = std::fs::remove_file(path);
}

#[test]
fn aof_rewrite_compacts_log() {
    let path = temp_aof("rewrite");
    let cfg = Config {
        aof_path: Some(path.clone()),
        ..Config::default()
    };
    let (srv, port) = start(cfg.clone());
    let mut c = Client::connect(port);
    for _ in 0..5000 {
        c.cmd(&["INCR", "counter"]);
    }
    for i in 0..200 {
        c.cmd(&["RPUSH", "list", &i.to_string()]);
    }
    c.cmd(&["SET", "ttl", "x", "EX", "1000"]);
    srv.aof.as_ref().unwrap().sync().unwrap();
    let before = std::fs::metadata(&path).unwrap().len();

    assert!(matches!(c.cmd(&["BGREWRITEAOF"]), Value::Simple(_)));
    // Writes after the rewrite must land in the new file.
    c.cmd(&["INCR", "counter"]);
    srv.aof.as_ref().unwrap().sync().unwrap();
    let after = std::fs::metadata(&path).unwrap().len();
    assert!(
        after * 10 < before,
        "rewrite should shrink the log ({before} -> {after})"
    );

    let srv2 = Server::new(cfg).unwrap();
    let ctx = Ctx {
        srv: &srv2,
        loading: false,
    };
    let get = |args: &[&str]| {
        execute(
            &ctx,
            &args
                .iter()
                .map(|a| a.as_bytes().to_vec())
                .collect::<Vec<_>>(),
        )
    };
    assert_eq!(get(&["GET", "counter"]), bulk("5001"));
    assert_eq!(get(&["LLEN", "list"]), Value::Integer(200));
    assert_eq!(get(&["LINDEX", "list", "199"]), bulk("199"));
    assert!(matches!(get(&["TTL", "ttl"]), Value::Integer(n) if n > 990));
    let _ = std::fs::remove_file(path);
}

#[test]
fn aof_recovers_from_torn_write() {
    let path = temp_aof("torn");
    std::fs::write(
        &path,
        b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n*3\r\n$3\r\nSET\r\n$1\r\nb",
    )
    .unwrap();
    let srv = Server::new(Config {
        aof_path: Some(path.clone()),
        ..Config::default()
    })
    .unwrap();
    let ctx = Ctx {
        srv: &srv,
        loading: false,
    };
    assert_eq!(execute(&ctx, &[b"GET".to_vec(), b"a".to_vec()]), bulk("1"));
    assert_eq!(
        execute(&ctx, &[b"GET".to_vec(), b"b".to_vec()]),
        Value::Null
    );
    // The partial command was trimmed off so future appends stay valid.
    let len = std::fs::metadata(&path).unwrap().len();
    assert_eq!(len, 27);
    let _ = std::fs::remove_file(path);
}
