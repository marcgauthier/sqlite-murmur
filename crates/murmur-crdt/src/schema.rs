//! Stable schema helpers shared by the engine and the extension.
use crate::model::{Column, Table};

pub fn pk_columns(t: &Table) -> Vec<&Column> {
    let mut pk: Vec<_> = t.columns.iter().filter(|c| c.primary_key > 0).collect();
    pk.sort_by_key(|c| c.primary_key);
    pk
}

/// Table named by a plain `CREATE [UNIQUE] INDEX [IF NOT EXISTS] <name> ON
/// [<schema>.]<table> (...)` statement. Quoted identifiers and comments are
/// honored. Returns None when the shape is unrecognized; callers keep such
/// indexes and let SQLite validate them on adopt.
pub fn index_table(sql: &str) -> Option<String> {
    enum T {
        Bare(String),
        Name(String),
        Dot,
    }
    let b = sql.as_bytes();
    let mut toks: Vec<T> = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'\'' {
            i += 1;
            while i < b.len() {
                if b[i] == b'\'' {
                    i += 1;
                    if i < b.len() && b[i] == b'\'' {
                        i += 1;
                    } else {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
        } else if c == b'"' || c == b'`' {
            i += 1;
            let mut raw: Vec<u8> = Vec::new();
            while i < b.len() {
                if b[i] == c {
                    if i + 1 < b.len() && b[i + 1] == c {
                        raw.push(c);
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    raw.push(b[i]);
                    i += 1;
                }
            }
            toks.push(T::Name(String::from_utf8_lossy(&raw).into_owned()));
        } else if c == b'[' {
            i += 1;
            let start = i;
            while i < b.len() && b[i] != b']' {
                i += 1;
            }
            toks.push(T::Name(
                String::from_utf8_lossy(&b[start..i]).into_owned(),
            ));
            i = (i + 1).min(b.len());
        } else if c == b'-' && b[i..].starts_with(b"--") {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
        } else if c == b'/' && i + 1 < b.len() && b[i + 1] == b'*' {
            i += 2;
            while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(b.len());
        } else if c.is_ascii_alphabetic() || c == b'_' {
            let start = i;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$') {
                i += 1;
            }
            toks.push(T::Bare(sql[start..i].to_ascii_uppercase()));
        } else if c == b'.' {
            toks.push(T::Dot);
            i += 1;
        } else {
            i += 1;
        }
    }
    let mut k = 0;
    let eat = |toks: &[T], k: &mut usize, word: &str| -> bool {
        if matches!(&toks.get(*k), Some(T::Bare(w)) if w == word) {
            *k += 1;
            true
        } else {
            false
        }
    };
    if !eat(&toks, &mut k, "CREATE") {
        return None;
    }
    eat(&toks, &mut k, "UNIQUE");
    if !eat(&toks, &mut k, "INDEX") {
        return None;
    }
    if eat(&toks, &mut k, "IF") {
        if !eat(&toks, &mut k, "NOT") || !eat(&toks, &mut k, "EXISTS") {
            return None;
        }
    }
    match toks.get(k) {
        Some(T::Bare(_)) | Some(T::Name(_)) => k += 1,
        _ => return None,
    }
    if !eat(&toks, &mut k, "ON") {
        return None;
    }
    let first = match toks.get(k) {
        Some(T::Bare(n)) | Some(T::Name(n)) => n.clone(),
        _ => return None,
    };
    k += 1;
    if matches!(toks.get(k), Some(T::Dot)) {
        k += 1;
        return match toks.get(k) {
            Some(T::Bare(n)) | Some(T::Name(n)) => Some(n.clone()),
            _ => None,
        };
    }
    // Bare tokens were uppercased for keyword matching; table names compare
    // case-insensitively at use sites.
    Some(first)
}
