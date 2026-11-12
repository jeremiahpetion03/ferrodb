//! RESP2 (REdis Serialization Protocol) encoding and incremental parsing.
//!
//! The parser is *incremental*: it is handed whatever bytes have arrived on the
//! socket so far and either returns a complete frame plus the number of bytes it
//! consumed, or `Ok(None)` meaning "need more data". This is what makes
//! pipelining work — many commands can arrive in a single read and are drained
//! one after another from the same buffer.

use std::io::Write;

/// Maximum size of a single bulk string (same default as Redis: 512 MB).
const MAX_BULK_LEN: usize = 512 * 1024 * 1024;
/// Maximum number of arguments in a single command.
const MAX_ARGS: usize = 1024 * 1024;
/// Maximum length of an inline command / header line before we give up.
const MAX_INLINE_LEN: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Vec<u8>),
    Null,
    Array(Vec<Value>),
    NullArray,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolError(pub String);

/// A parsed frame plus the number of bytes it consumed, `None` if incomplete.
pub type Parsed<T> = Result<Option<(T, usize)>, ProtocolError>;

impl Value {
    pub fn ok() -> Self {
        Value::Simple("OK".into())
    }

    pub fn err(msg: impl Into<String>) -> Self {
        Value::Error(msg.into())
    }

    pub fn bulk(b: impl Into<Vec<u8>>) -> Self {
        Value::Bulk(b.into())
    }

    /// Serialize this value onto the end of `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Value::Simple(s) => {
                out.push(b'+');
                out.extend_from_slice(s.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Value::Error(s) => {
                out.push(b'-');
                out.extend_from_slice(s.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Value::Integer(i) => {
                let _ = write!(out, ":{i}\r\n");
            }
            Value::Bulk(b) => {
                let _ = write!(out, "${}\r\n", b.len());
                out.extend_from_slice(b);
                out.extend_from_slice(b"\r\n");
            }
            Value::Null => out.extend_from_slice(b"$-1\r\n"),
            Value::Array(items) => {
                let _ = write!(out, "*{}\r\n", items.len());
                for item in items {
                    item.encode(out);
                }
            }
            Value::NullArray => out.extend_from_slice(b"*-1\r\n"),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode(&mut out);
        out
    }
}

fn find_crlf(buf: &[u8], start: usize) -> Option<usize> {
    if start >= buf.len() {
        return None;
    }
    buf[start..]
        .windows(2)
        .position(|w| w == b"\r\n")
        .map(|p| p + start)
}

/// Like `find_crlf`, but errors out if a header line grows unreasonably long
/// without a terminator (protects against memory exhaustion).
fn header_end(buf: &[u8], start: usize) -> Result<Option<usize>, ProtocolError> {
    match find_crlf(buf, start) {
        Some(e) => Ok(Some(e)),
        None if buf.len() - start.min(buf.len()) > MAX_INLINE_LEN => {
            Err(ProtocolError("header line too long".into()))
        }
        None => Ok(None),
    }
}

fn parse_int(bytes: &[u8]) -> Result<i64, ProtocolError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| ProtocolError("invalid integer".into()))
}

/// Parse one client command: either a RESP array of bulk strings (what every
/// real client sends) or an "inline" command (what you type into telnet).
pub fn parse_command(buf: &[u8]) -> Parsed<Vec<Vec<u8>>> {
    if buf.is_empty() {
        return Ok(None);
    }
    if buf[0] != b'*' {
        return parse_inline(buf);
    }

    let Some(end) = header_end(buf, 1)? else {
        return Ok(None);
    };
    let n =
        parse_int(&buf[1..end]).map_err(|_| ProtocolError("invalid multibulk length".into()))?;
    let mut pos = end + 2;
    if n <= 0 {
        return Ok(Some((Vec::new(), pos)));
    }
    let n = n as usize;
    if n > MAX_ARGS {
        return Err(ProtocolError("invalid multibulk length".into()));
    }

    let mut args = Vec::with_capacity(n.min(64));
    for _ in 0..n {
        if pos >= buf.len() {
            return Ok(None);
        }
        if buf[pos] != b'$' {
            return Err(ProtocolError(format!(
                "expected '$', got '{}'",
                buf[pos] as char
            )));
        }
        let Some(end) = header_end(buf, pos + 1)? else {
            return Ok(None);
        };
        let len = parse_int(&buf[pos + 1..end])
            .map_err(|_| ProtocolError("invalid bulk length".into()))?;
        if len < 0 || len as usize > MAX_BULK_LEN {
            return Err(ProtocolError("invalid bulk length".into()));
        }
        let len = len as usize;
        let start = end + 2;
        if buf.len() < start + len + 2 {
            return Ok(None);
        }
        if &buf[start + len..start + len + 2] != b"\r\n" {
            return Err(ProtocolError("bulk string not terminated by CRLF".into()));
        }
        args.push(buf[start..start + len].to_vec());
        pos = start + len + 2;
    }
    Ok(Some((args, pos)))
}

fn parse_inline(buf: &[u8]) -> Parsed<Vec<Vec<u8>>> {
    let Some(nl) = buf.iter().position(|&b| b == b'\n') else {
        if buf.len() > MAX_INLINE_LEN {
            return Err(ProtocolError("too big inline request".into()));
        }
        return Ok(None);
    };
    let mut line = &buf[..nl];
    if line.last() == Some(&b'\r') {
        line = &line[..line.len() - 1];
    }
    let args = line
        .split(|b| b.is_ascii_whitespace())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_vec())
        .collect();
    Ok(Some((args, nl + 1)))
}

/// Parse any RESP value (used for replies, e.g. by tests and clients).
pub fn parse_value(buf: &[u8]) -> Parsed<Value> {
    if buf.is_empty() {
        return Ok(None);
    }
    let Some(end) = header_end(buf, 1)? else {
        return Ok(None);
    };
    let line = &buf[1..end];
    let after = end + 2;
    let text = || String::from_utf8_lossy(line).into_owned();
    match buf[0] {
        b'+' => Ok(Some((Value::Simple(text()), after))),
        b'-' => Ok(Some((Value::Error(text()), after))),
        b':' => Ok(Some((Value::Integer(parse_int(line)?), after))),
        b'$' => {
            let len = parse_int(line)?;
            if len < 0 {
                return Ok(Some((Value::Null, after)));
            }
            let len = len as usize;
            if buf.len() < after + len + 2 {
                return Ok(None);
            }
            Ok(Some((
                Value::Bulk(buf[after..after + len].to_vec()),
                after + len + 2,
            )))
        }
        b'*' => {
            let n = parse_int(line)?;
            if n < 0 {
                return Ok(Some((Value::NullArray, after)));
            }
            let mut pos = after;
            let mut items = Vec::with_capacity((n as usize).min(1024));
            for _ in 0..n {
                match parse_value(&buf[pos..])? {
                    Some((v, used)) => {
                        items.push(v);
                        pos += used;
                    }
                    None => return Ok(None),
                }
            }
            Ok(Some((Value::Array(items), pos)))
        }
        c => Err(ProtocolError(format!(
            "unknown RESP type byte '{}'",
            c as char
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(v: &[&str]) -> Vec<Vec<u8>> {
        v.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    #[test]
    fn parses_full_command() {
        let buf = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let (cmd, used) = parse_command(buf).unwrap().unwrap();
        assert_eq!(cmd, args(&["SET", "key", "value"]));
        assert_eq!(used, buf.len());
    }

    #[test]
    fn incomplete_input_needs_more_data_at_every_split_point() {
        let buf = b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n";
        for i in 0..buf.len() {
            assert_eq!(parse_command(&buf[..i]).unwrap(), None, "split at {i}");
        }
        assert!(parse_command(buf).unwrap().is_some());
    }

    #[test]
    fn pipelined_commands_are_consumed_one_at_a_time() {
        let buf = b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
        let (_, used) = parse_command(buf).unwrap().unwrap();
        assert_eq!(used, 14);
        let (cmd, used2) = parse_command(&buf[used..]).unwrap().unwrap();
        assert_eq!(cmd, args(&["PING"]));
        assert_eq!(used + used2, buf.len());
    }

    #[test]
    fn binary_safe_values() {
        let buf = b"*2\r\n$3\r\nGET\r\n$4\r\na\r\nb\r\n";
        let (cmd, _) = parse_command(buf).unwrap().unwrap();
        assert_eq!(cmd[1], b"a\r\nb");
    }

    #[test]
    fn inline_commands() {
        let (cmd, used) = parse_command(b"SET  foo   bar\r\n").unwrap().unwrap();
        assert_eq!(cmd, args(&["SET", "foo", "bar"]));
        assert_eq!(used, 16);
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_command(b"*1\r\n:5\r\n").is_err());
        assert!(parse_command(b"*x\r\n").is_err());
        assert!(parse_command(b"*1\r\n$-5\r\n").is_err());
        assert!(parse_command(b"*1\r\n$3\r\nabcXY").is_err());
    }

    #[test]
    fn encode_and_parse_roundtrip() {
        let v = Value::Array(vec![
            Value::ok(),
            Value::err("ERR nope"),
            Value::Integer(-42),
            Value::bulk("hi"),
            Value::Null,
            Value::Array(vec![Value::bulk(""), Value::NullArray]),
        ]);
        let bytes = v.to_bytes();
        let (parsed, used) = parse_value(&bytes).unwrap().unwrap();
        assert_eq!(parsed, v);
        assert_eq!(used, bytes.len());
    }
}
