//! Append-only file persistence.
//!
//! Every write command is appended to the log as a RESP array. On startup the
//! log is replayed to rebuild the dataset. Commands with relative time
//! arguments (`EXPIRE k 10`, `SET k v EX 10`) are rewritten to absolute forms
//! (`PEXPIREAT`, `PXAT`) before logging so that replay is deterministic.
//!
//! The log is compacted by `rewrite`, which replaces it with the minimal set
//! of commands that recreate the current dataset, then atomically renames the
//! new file over the old one so a crash mid-rewrite never loses data.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::{lock, resp};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fsync {
    /// fsync after every write: safest, slowest.
    Always,
    /// fsync once per second from a background thread (Redis default).
    EverySec,
    /// Let the OS decide when to flush.
    No,
}

impl std::str::FromStr for Fsync {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "always" => Ok(Fsync::Always),
            "everysec" => Ok(Fsync::EverySec),
            "no" => Ok(Fsync::No),
            other => Err(format!(
                "invalid appendfsync value '{other}' (always|everysec|no)"
            )),
        }
    }
}

struct Inner {
    w: BufWriter<File>,
    size: u64,
    /// File size right after the last rewrite (or at startup).
    base_size: u64,
    /// Bytes sitting in our userspace buffer, not yet handed to the OS.
    pending: bool,
    /// Bytes handed to the OS but not yet fsynced.
    dirty: bool,
}

pub struct Aof {
    inner: Mutex<Inner>,
    path: PathBuf,
    pub fsync: Fsync,
}

pub fn encode_command(args: &[&[u8]], out: &mut Vec<u8>) {
    let _ = write!(out, "*{}\r\n", args.len());
    for a in args {
        let _ = write!(out, "${}\r\n", a.len());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
}

impl Aof {
    pub fn open(path: &Path, fsync: Fsync) -> io::Result<Aof> {
        let f = OpenOptions::new().create(true).append(true).open(path)?;
        let size = f.metadata()?.len();
        Ok(Aof {
            inner: Mutex::new(Inner {
                w: BufWriter::with_capacity(64 * 1024, f),
                size,
                base_size: size,
                pending: false,
                dirty: false,
            }),
            path: path.to_path_buf(),
            fsync,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append one command. Callers hold the shard lock of the key being
    /// modified, which guarantees the log order matches the order in which
    /// mutations were applied to that key.
    pub fn append(&self, args: &[&[u8]]) {
        let mut buf = Vec::with_capacity(64);
        encode_command(args, &mut buf);
        let mut g = lock(&self.inner);
        if let Err(e) = g.w.write_all(&buf) {
            crate::log(&format!("AOF write failed: {e}"));
            return;
        }
        g.size += buf.len() as u64;
        g.pending = true;
        g.dirty = true;
        if self.fsync == Fsync::Always {
            let _ = g.w.flush().and_then(|_| g.w.get_ref().sync_data());
            g.pending = false;
            g.dirty = false;
        }
    }

    /// Hand buffered log bytes to the OS. Connections call this after a batch
    /// of commands and *before* sending replies, so a client is never told
    /// "OK" for a write that would be lost if the process crashed. (Only an
    /// OS crash / power loss can lose data, bounded by the fsync policy.)
    pub fn flush_to_os(&self) {
        let mut g = lock(&self.inner);
        if g.pending {
            if let Err(e) = g.w.flush() {
                crate::log(&format!("AOF write failed: {e}"));
            }
            g.pending = false;
        }
    }

    /// Flush buffered writes (and fsync, unless the policy is `No`).
    pub fn sync(&self) -> io::Result<()> {
        let mut g = lock(&self.inner);
        if g.dirty {
            g.w.flush()?;
            g.pending = false;
            if self.fsync != Fsync::No {
                g.w.get_ref().sync_data()?;
            }
            g.dirty = false;
        }
        Ok(())
    }

    pub fn size(&self) -> u64 {
        lock(&self.inner).size
    }

    /// Auto-rewrite policy: the log has at least doubled since the last
    /// rewrite and is above a minimum size.
    pub fn needs_rewrite(&self, min_size: u64) -> bool {
        let g = lock(&self.inner);
        g.size >= min_size && g.size >= g.base_size.saturating_mul(2)
    }

    /// Replace the log with `snapshot`. The caller must hold every shard lock
    /// so no writes can slip in between taking the snapshot and the swap.
    pub fn rewrite(&self, snapshot: &[u8]) -> io::Result<()> {
        let mut g = lock(&self.inner);
        g.w.flush()?;
        let tmp = self.path.with_extension("rewrite.tmp");
        {
            let mut f = File::create(&tmp)?;
            f.write_all(snapshot)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &self.path)?;
        let f = OpenOptions::new().append(true).open(&self.path)?;
        g.w = BufWriter::with_capacity(64 * 1024, f);
        g.size = snapshot.len() as u64;
        g.base_size = g.size.max(1);
        g.pending = false;
        g.dirty = false;
        Ok(())
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct LoadReport {
    pub commands: usize,
    /// Bytes of an incomplete trailing command that were discarded (this
    /// happens if the process died mid-write).
    pub truncated_bytes: usize,
}

/// Replay the log at `path`, calling `apply` for each command.
pub fn load(path: &Path, mut apply: impl FnMut(&[Vec<u8>])) -> io::Result<LoadReport> {
    let mut data = Vec::new();
    match File::open(path) {
        Ok(mut f) => {
            f.read_to_end(&mut data)?;
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(LoadReport::default()),
        Err(e) => return Err(e),
    }

    let mut report = LoadReport::default();
    let mut pos = 0;
    while pos < data.len() {
        match resp::parse_command(&data[pos..]) {
            Ok(Some((args, used))) => {
                pos += used;
                if !args.is_empty() {
                    apply(&args);
                    report.commands += 1;
                }
            }
            Ok(None) => {
                report.truncated_bytes = data.len() - pos;
                break;
            }
            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("corrupt AOF at byte {pos}: {}", e.0),
                ));
            }
        }
    }

    if report.truncated_bytes > 0 {
        // Drop the partial command so new appends start on a clean boundary.
        OpenOptions::new()
            .write(true)
            .open(path)?
            .set_len(pos as u64)?;
    }
    Ok(report)
}
