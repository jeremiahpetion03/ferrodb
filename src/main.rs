use std::net::TcpListener;
use std::path::PathBuf;
use std::process::exit;

use ferrodb::server::{Config, Server, run};

const USAGE: &str = "\
ferrodb — a multi-threaded, Redis-compatible in-memory database

USAGE:
    ferrodb [OPTIONS]

OPTIONS:
    --bind <addr>            Address to listen on        [default: 127.0.0.1]
    --port <port>            Port to listen on           [default: 6379]
    --shards <n>             Keyspace shards (pow of 2)  [default: 64]
    --appendonly <path>      Enable AOF persistence at <path>
    --appendfsync <policy>   always | everysec | no      [default: everysec]
    -h, --help               Show this help";

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Config, String> {
    let mut cfg = Config::default();
    while let Some(flag) = args.next() {
        let mut value = || {
            args.next()
                .ok_or_else(|| format!("missing value for {flag}"))
        };
        match flag.as_str() {
            "--bind" => cfg.bind = value()?,
            "--port" => cfg.port = value()?.parse().map_err(|_| "invalid --port".to_string())?,
            "--shards" => {
                cfg.shards = value()?
                    .parse()
                    .map_err(|_| "invalid --shards".to_string())?
            }
            "--appendonly" => cfg.aof_path = Some(PathBuf::from(value()?)),
            "--appendfsync" => cfg.fsync = value()?.parse()?,
            "-h" | "--help" => {
                println!("{USAGE}");
                exit(0);
            }
            other => return Err(format!("unknown option '{other}'")),
        }
    }
    Ok(cfg)
}

fn main() {
    let cfg = parse_args(std::env::args().skip(1)).unwrap_or_else(|e| {
        eprintln!("error: {e}\n\n{USAGE}");
        exit(2);
    });
    let addr = format!("{}:{}", cfg.bind, cfg.port);
    let persistence = match &cfg.aof_path {
        Some(p) => format!("AOF at {} (fsync {:?})", p.display(), cfg.fsync),
        None => "in-memory only".into(),
    };

    let srv = Server::new(cfg).unwrap_or_else(|e| {
        eprintln!("fatal: could not start: {e}");
        exit(1);
    });
    let listener = TcpListener::bind(&addr).unwrap_or_else(|e| {
        eprintln!("fatal: could not bind {addr}: {e}");
        exit(1);
    });

    ferrodb::log(&format!(
        "ferrodb v{} ready on {addr} — {} shards, {persistence}",
        env!("CARGO_PKG_VERSION"),
        srv.store.num_shards()
    ));
    install_shutdown_handler(srv.clone());
    if let Err(e) = run(srv, listener) {
        eprintln!("fatal: {e}");
        exit(1);
    }
}

/// Graceful shutdown on Ctrl-C / SIGTERM: fsync the AOF, then exit.
/// Implemented with a raw `signal(2)` call to stay dependency-free; the
/// handler only flips an atomic flag (async-signal-safe), and a watcher
/// thread does the real work.
#[cfg(unix)]
fn install_shutdown_handler(srv: std::sync::Arc<Server>) {
    use std::sync::atomic::{AtomicBool, Ordering};
    static STOP: AtomicBool = AtomicBool::new(false);

    unsafe extern "C" {
        fn signal(signum: i32, handler: extern "C" fn(i32)) -> usize;
    }
    extern "C" fn on_signal(_: i32) {
        STOP.store(true, Ordering::SeqCst);
    }
    const SIGINT: i32 = 2;
    const SIGTERM: i32 = 15;
    // SAFETY: `on_signal` only performs an atomic store, which is
    // async-signal-safe.
    unsafe {
        signal(SIGINT, on_signal);
        signal(SIGTERM, on_signal);
    }

    std::thread::spawn(move || {
        while !STOP.load(Ordering::SeqCst) {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        ferrodb::log("shutdown signal received, syncing AOF");
        if let Some(aof) = &srv.aof
            && let Err(e) = aof.sync()
        {
            ferrodb::log(&format!("AOF sync on shutdown failed: {e}"));
        }
        ferrodb::log("bye");
        exit(0);
    });
}

#[cfg(not(unix))]
fn install_shutdown_handler(_srv: std::sync::Arc<Server>) {}
