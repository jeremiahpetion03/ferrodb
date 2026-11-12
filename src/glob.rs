//! Redis-style glob matching for `KEYS pattern`.
//!
//! Supports `*`, `?`, `[abc]`, `[a-z]`, `[^x]` and `\` escapes. Uses an
//! iterative backtracking algorithm (O(n*m) worst case, no recursion), so a
//! hostile pattern like `*a*a*a*a*b` cannot blow the stack.

pub fn glob_match(pat: &[u8], s: &[u8]) -> bool {
    let (mut p, mut i) = (0usize, 0usize);
    // Position of the most recent '*' in the pattern, and the string index it
    // is currently "absorbing" up to.
    let mut star: Option<(usize, usize)> = None;

    while i < s.len() {
        if p < pat.len() {
            match pat[p] {
                b'*' => {
                    star = Some((p, i));
                    p += 1;
                    continue;
                }
                b'?' => {
                    p += 1;
                    i += 1;
                    continue;
                }
                b'[' => match match_class(pat, p, s[i]) {
                    Some((true, next)) => {
                        p = next;
                        i += 1;
                        continue;
                    }
                    Some((false, _)) => {}
                    // Unterminated class: treat '[' as a literal.
                    None if s[i] == b'[' => {
                        p += 1;
                        i += 1;
                        continue;
                    }
                    None => {}
                },
                b'\\' if p + 1 < pat.len() => {
                    if pat[p + 1] == s[i] {
                        p += 2;
                        i += 1;
                        continue;
                    }
                }
                c => {
                    if c == s[i] {
                        p += 1;
                        i += 1;
                        continue;
                    }
                }
            }
        }
        // Mismatch: let the last '*' swallow one more character and retry.
        match star {
            Some((sp, si)) => {
                p = sp + 1;
                i = si + 1;
                star = Some((sp, si + 1));
            }
            None => return false,
        }
    }
    while p < pat.len() && pat[p] == b'*' {
        p += 1;
    }
    p == pat.len()
}

/// Match `c` against the character class starting at `pat[start] == '['`.
/// Returns (matched, index just past the closing ']'), or None if unterminated.
fn match_class(pat: &[u8], start: usize, c: u8) -> Option<(bool, usize)> {
    let mut p = start + 1;
    let negate = p < pat.len() && pat[p] == b'^';
    if negate {
        p += 1;
    }
    let mut matched = false;
    loop {
        if p >= pat.len() {
            return None;
        }
        if pat[p] == b']' {
            break;
        }
        let mut lo = pat[p];
        if lo == b'\\' && p + 1 < pat.len() {
            p += 1;
            lo = pat[p];
        }
        if p + 2 < pat.len() && pat[p + 1] == b'-' && pat[p + 2] != b']' {
            let hi = pat[p + 2];
            let (a, b) = if lo <= hi { (lo, hi) } else { (hi, lo) };
            if (a..=b).contains(&c) {
                matched = true;
            }
            p += 3;
        } else {
            if c == lo {
                matched = true;
            }
            p += 1;
        }
    }
    Some((matched != negate, p + 1))
}

#[cfg(test)]
mod tests {
    use super::glob_match;

    fn m(p: &str, s: &str) -> bool {
        glob_match(p.as_bytes(), s.as_bytes())
    }

    #[test]
    fn basics() {
        assert!(m("*", ""));
        assert!(m("*", "anything"));
        assert!(m("user:*", "user:42"));
        assert!(!m("user:*", "session:42"));
        assert!(m("h?llo", "hello"));
        assert!(!m("h?llo", "hllo"));
        assert!(m("*:*:end", "a:b:c:end"));
        assert!(!m("abc", "abcd"));
    }

    #[test]
    fn classes_and_escapes() {
        assert!(m("h[ae]llo", "hallo"));
        assert!(!m("h[ae]llo", "hillo"));
        assert!(m("h[^e]llo", "hallo"));
        assert!(!m("h[^e]llo", "hello"));
        assert!(m("key[0-9]", "key7"));
        assert!(!m("key[0-9]", "keyx"));
        assert!(m("a\\*b", "a*b"));
        assert!(!m("a\\*b", "axb"));
    }

    #[test]
    fn pathological_pattern_is_fast() {
        let s = "a".repeat(5000);
        let p = "*a".repeat(50) + "b";
        assert!(!m(&p, &s));
    }
}
