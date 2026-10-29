//! Replicated DDL + schema master (point 1, slice A5a).
//!
//! Downstream adapter over the archived schema-manifest design
//! (`SignedSchema`, sequence tracking, adopt/record/stale/current
//! decisions). Policy and transport match the embedded profile:
//!
//! - Signatures are replaced by transport authentication: the QUIC peer
//!   is already mutually authenticated, so the manifest signer must be
//!   the TLS source AND (when designated) the configured schema master.
//!   The signature field stays empty.
//! - Schema bytes are the canonical postcard `Vec<(table, DDL)>` pairs
//!   minus `table_replication=false` tables (the archive likewise
//!   excludes local-only tables before signing).
//! - Adoption is additive-only: new tables and SQLite-accepted added
//!   columns. Anything destructive or incompatible is recorded, never
//!   adopted. DDL text from the wire is untrusted until validated.
//!
//! Reused verbatim from `murmur-tx`: `SignedSchema`, `schema_manifest_key`,
//! `schema_accepted_key`. Ported from archive `replication/src/engine.rs`
//! (`manifest`, `check_schema_manifest`, `record_schema_accepted`) with the
//! transport-auth substitution: `build_manifest`, `check_manifest`,
//! `record_accepted`. New here: the additive-only `diff` gate.

use crate::*;
use murmur_tx::protocol::SignedSchema;
use sha2::{Digest, Sha256};

#[derive(Clone, Debug)]
pub(crate) struct SchemaParams {
    pub accept_remote_schema: bool,
    pub schema_master: Option<murmur_tx::NodeId>,
    pub master: bool,
    pub max_schema_bytes: usize,
}

pub(crate) fn params_from(db: &rusqlite::Connection) -> Result<SchemaParams> {
    let flag = |name: &str| -> Result<bool> {
        match config::setting(db, name)?.as_str() {
            "1" => Ok(true),
            "0" => Ok(false),
            _ => Err(Error::Invalid(format!("invalid {name}"))),
        }
    };
    let raw = config::setting(db, "schema_master")?;
    let schema_master = if raw.is_empty() {
        None
    } else {
        Some(parse_id(&raw)?)
    };
    let max_schema_bytes = config::setting(db, "max_batch_bytes")?
        .parse()
        .map_err(|_| Error::Invalid("invalid max_batch_bytes".into()))?;
    Ok(SchemaParams {
        accept_remote_schema: flag("accept_remote_schema")?,
        schema_master,
        master: flag("schema_master_enabled")?,
        max_schema_bytes,
    })
}

/// Archived `manifest()`: monotonic sequence per digest, stored under
/// `schema_manifest_key`. Unsigned; the master designation is checked
/// against the live node id here (the archive's `bind_signer` rule).
pub(crate) fn build_manifest(
    backend: &dyn murmur_tx::Backend,
    params: &SchemaParams,
    own: murmur_tx::NodeId,
    database: murmur_tx::NodeId,
    schema: Vec<u8>,
) -> murmur_tx::Result<SignedSchema> {
    if params.master && params.schema_master != Some(own) {
        return Err(murmur_tx::Error::Invalid(
            "schema master must designate its own node id".into(),
        ));
    }
    if schema.len() > params.max_schema_bytes {
        return Err(murmur_tx::Error::Overloaded);
    }
    let digest: [u8; 32] = Sha256::digest(&schema).into();
    let key = murmur_tx::protocol::schema_manifest_key();
    let stored: Option<([u8; 32], u64)> =
        backend.get(&key)?.map(|b| murmur_tx::decode(&b)).transpose()?;
    let sequence = match stored {
        Some((d, s)) if d == digest => s,
        Some((_, s)) => s
            .checked_add(1)
            .ok_or_else(|| murmur_tx::Error::Protocol("schema sequence exhausted".into()))?,
        None => 1,
    };
    if stored != Some((digest, sequence)) {
        backend.commit(vec![murmur_tx::Record::put(
            key,
            murmur_tx::encode(&(digest, sequence))?,
        )])?;
    }
    Ok(SignedSchema {
        database,
        signer: own,
        sequence,
        schema,
        signature: Vec::new(),
    })
}

/// Transport-auth replacement for `SignedSchema::verify`: the signer must
/// be the authenticated TLS source. Envelope checks are verbatim.
pub(crate) fn verify_transport(
    signed: &SignedSchema,
    database: murmur_tx::NodeId,
    source: murmur_tx::NodeId,
    params: &SchemaParams,
) -> murmur_tx::Result<()> {
    if signed.database != database
        || signed.signer != source
        || signed.sequence == 0
        || signed.schema.len() > params.max_schema_bytes
    {
        return Err(murmur_tx::Error::Auth(
            "schema envelope identity or size".into(),
        ));
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SchemaDecision {
    Current,
    Adopt,
    Stale,
    Record,
}

/// Archived `record_schema_accepted`, verbatim.
pub(crate) fn record_accepted(
    backend: &dyn murmur_tx::Backend,
    signed: &SignedSchema,
) -> murmur_tx::Result<()> {
    backend.commit(vec![murmur_tx::Record::put(
        murmur_tx::protocol::schema_accepted_key(signed.signer),
        murmur_tx::encode(&(signed.sequence, signed.schema.clone()))?,
    )])
}

/// Archived `check_schema_manifest` with transport auth. `local_schema`
/// is our manifest-filtered bytes (opt-outs excluded, like the sender's).
/// Destructive diffs are recorded, never adopted; that is the only
/// behavioral fork from the archive.
pub(crate) fn check_manifest(
    backend: &dyn murmur_tx::Backend,
    params: &SchemaParams,
    own: murmur_tx::NodeId,
    local_schema: &[u8],
    signed: &SignedSchema,
    source: murmur_tx::NodeId,
) -> murmur_tx::Result<SchemaDecision> {
    let (database, _node) = backend.identity();
    verify_transport(signed, database, source, params)?;
    let adoptable = match params.schema_master {
        Some(m) => signed.signer == m && !(params.master && params.schema_master == Some(own)),
        None => true,
    };
    let key = murmur_tx::protocol::schema_accepted_key(signed.signer);
    let accepted: (u64, Vec<u8>) = backend
        .get(&key)?
        .map(|b| murmur_tx::decode(&b))
        .transpose()?
        .unwrap_or((0, Vec::new()));
    if signed.sequence <= accepted.0 {
        return Ok(SchemaDecision::Stale);
    }
    if signed.schema == local_schema {
        record_accepted(backend, signed)?;
        return Ok(SchemaDecision::Current);
    }
    if !adoptable || !params.accept_remote_schema {
        record_accepted(backend, signed)?;
        return Ok(SchemaDecision::Record);
    }
    match diff(local_schema, &signed.schema)? {
        Diff::Additive(_) => Ok(SchemaDecision::Adopt),
        Diff::Incompatible(_) => {
            record_accepted(backend, signed)?;
            Ok(SchemaDecision::Record)
        }
    }
}

/// Additive schema delta: new tables as (table, DDL) pairs, new columns
/// as (table, original column definition) pairs. Definitions keep their
/// original text for execution (normalization is compare-only); all DDL
/// text stays untrusted until the A5b applier validates each statement.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Additive {
    pub new_tables: Vec<(String, String)>,
    pub add_columns: Vec<(String, String)>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Diff {
    Additive(Additive),
    Incompatible(String),
}

/// Diff canonical pair bytes. Column comparison is whitespace-normalized
/// and keyword case-insensitive, so formatting drift alone is a no-op.
/// Unparseable DDL fails closed to incompatible.
pub(crate) fn diff(local: &[u8], remote: &[u8]) -> murmur_tx::Result<Diff> {
    let local_pairs: Vec<(String, String)> = murmur_tx::decode(local)?;
    let remote_pairs: Vec<(String, String)> = murmur_tx::decode(remote)?;
    let local_map: BTreeMap<&str, &str> = local_pairs
        .iter()
        .map(|(t, d)| (t.as_str(), d.as_str()))
        .collect();
    let mut additive = Additive {
        new_tables: Vec::new(),
        add_columns: Vec::new(),
    };
    for (table, ddl) in &remote_pairs {
        let Some(previous) = local_map.get(table.as_str()) else {
            additive
                .new_tables
                .push((table.clone(), ddl.clone()));
            continue;
        };
        if previous == ddl {
            continue;
        }
        let Some((before, before_constraints)) = columns(previous) else {
            return Ok(Diff::Incompatible(format!("unparseable local DDL: {table}")));
        };
        let Some((after, after_constraints)) = columns(ddl) else {
            return Ok(Diff::Incompatible(format!("unparseable remote DDL: {table}")));
        };
        if before_constraints != after_constraints {
            return Ok(Diff::Incompatible(format!(
                "table constraint change: {table}"
            )));
        }
        for (name, def, _) in &before {
            match after.iter().find(|(n, _, _)| n == name) {
                Some((_, next, _)) if next == def => {}
                Some(_) => {
                    return Ok(Diff::Incompatible(format!("column redefined: {table}.{name}")));
                }
                None => {
                    return Ok(Diff::Incompatible(format!("column dropped: {table}.{name}")));
                }
            }
        }
        for (name, _, original) in &after {
            if !before.iter().any(|(n, _, _)| n == name) {
                additive
                    .add_columns
                    .push((table.clone(), original.clone()));
            }
        }
    }
    for (table, _) in &local_pairs {
        if !remote_pairs.iter().any(|(t, _)| t == table) {
            return Ok(Diff::Incompatible(format!("table retired: {table}")));
        }
    }
    Ok(Diff::Additive(additive))
}

/// Donor-side one-sided pull compatibility. The puller may carry
/// local-extra TABLES (drop-never legacy: it kept a table the donor
/// dropped); the donor's export only carries donor tables, so those
/// apply cleanly. Every donor table must exist on the puller with
/// byte-identical DDL — shared-table drift, extra columns included,
/// stays incompatible (a NOT NULL column without a default would make
/// donor rows uninsertable on the puller). Byte-exact like the digest
/// gate: formatting drift alone still rejects, exactly as before.
pub(crate) fn puller_compatible(local: &[u8], puller: &[u8]) -> murmur_tx::Result<bool> {
    let local_pairs: Vec<(String, String)> = murmur_tx::decode(local)?;
    let puller_pairs: Vec<(String, String)> = murmur_tx::decode(puller)?;
    let puller_map: BTreeMap<&str, &str> = puller_pairs
        .iter()
        .map(|(t, d)| (t.as_str(), d.as_str()))
        .collect();
    for (table, ddl) in &local_pairs {
        match puller_map.get(table.as_str()) {
            Some(puller_ddl) if *puller_ddl == ddl.as_str() => {}
            _ => return Ok(false),
        }
    }
    Ok(true)
}

/// Split a CREATE TABLE body into (name, normalized definition,
/// original element) columns plus normalized table-level constraints.
/// Constraints cannot be added later, so any constraint change is
/// incompatible, not additive. Returns None when the body does not parse.
fn columns(ddl: &str) -> Option<(Vec<(String, String, String)>, Vec<String>)> {
    let start = ddl.find('(')?;
    let body = ddl[start + 1..].rsplit_once(')')?.0;
    let mut cols = Vec::new();
    let mut constraints = Vec::new();
    for element in split_top_level(body)? {
        let element = element.trim();
        // Alphanumeric prefix (UNIQUE(v) has no space after the keyword).
        let first: String = element
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        let first = first.to_ascii_uppercase();
        if matches!(
            first.as_str(),
            "PRIMARY" | "FOREIGN" | "UNIQUE" | "CHECK" | "CONSTRAINT"
        ) {
            constraints.push(normalize(element));
            continue;
        }
        let name = element.split_whitespace().next()?.trim_matches('"').to_string();
        if name.is_empty() || !valid_ident(&name) {
            return None;
        }
        cols.push((name.to_ascii_lowercase(), normalize(element), element.to_string()));
    }
    constraints.sort();
    Some((cols, constraints))
}

/// Split on top-level commas, respecting nested parens and quotes.
fn split_top_level(body: &str) -> Option<Vec<String>> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut quote = None;
    let mut current = String::new();
    let mut chars = body.chars().peekable();
    while let Some(c) = chars.next() {
        if let Some(q) = quote {
            current.push(c);
            if c == q {
                if chars.peek() == Some(&q) {
                    current.push(chars.next()?);
                } else {
                    quote = None;
                }
            }
            continue;
        }
        match c {
            '\'' | '"' | '`' => {
                quote = Some(c);
                current.push(c);
            }
            '(' => {
                depth += 1;
                current.push(c);
            }
            ')' => {
                depth -= 1;
                if depth < 0 {
                    return None;
                }
                current.push(c);
            }
            ',' if depth == 0 => {
                parts.push(std::mem::take(&mut current));
            }
            _ => current.push(c),
        }
    }
    if quote.is_some() || depth != 0 {
        return None;
    }
    parts.push(current);
    Some(parts)
}

/// Normalize for comparison: collapse whitespace, uppercase unquoted
/// keywords, but keep quoted identifiers and string literals
/// case-sensitive (DEFAULT 'a' and DEFAULT 'A' really differ).
fn normalize(element: &str) -> String {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let flush = |current: &mut String, tokens: &mut Vec<String>| {
        if !current.is_empty() {
            tokens.push(std::mem::take(current).to_ascii_uppercase());
        }
    };
    let mut chars = element.chars().peekable();
    while let Some(c) = chars.next() {
        if let Some(q) = quote {
            current.push(c);
            if c == q {
                if chars.peek() == Some(&q) {
                    current.push(chars.next().unwrap_or(q));
                } else {
                    quote = None;
                    tokens.push(std::mem::take(&mut current));
                }
            }
            continue;
        }
        match c {
            '\'' | '"' | '`' => {
                flush(&mut current, &mut tokens);
                quote = Some(c);
                current.push(c);
            }
            _ if c.is_whitespace() => flush(&mut current, &mut tokens),
            _ => current.push(c),
        }
    }
    if quote.is_some() {
        tokens.push(current);
    } else {
        flush(&mut current, &mut tokens);
    }
    tokens.join(" ")
}

pub(crate) fn valid_ident(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
}

/// True when the text holds at most one statement: no semicolon outside
/// quotes except a single trailing one. `sqlite_schema` DDL may or may
/// not carry its semicolon; both pass, anything else fails.
fn single_statement(text: &str) -> bool {
    let mut quote = None;
    let mut semis = 0usize;
    let mut trailing_ok = true;
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if let Some(q) = quote {
            if c == q {
                if chars.get(i + 1) == Some(&q) {
                    i += 1;
                } else {
                    quote = None;
                }
            }
            i += 1;
            continue;
        }
        match c {
            '\'' | '"' | '`' => quote = Some(c),
            ';' => {
                semis += 1;
                trailing_ok = chars[i + 1..].iter().all(|d| d.is_whitespace());
            }
            _ => {}
        }
        i += 1;
    }
    quote.is_none() && (semis == 0 || (semis == 1 && trailing_ok))
}

fn has_unquoted_semicolon(text: &str) -> bool {
    let mut quote = None;
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if let Some(q) = quote {
            if c == q {
                let mut ahead = chars.clone();
                if ahead.next() == Some(q) {
                    chars.next();
                } else {
                    quote = None;
                }
            }
            continue;
        }
        match c {
            '\'' | '"' | '`' => quote = Some(c),
            ';' => return true,
            _ => {}
        }
    }
    false
}

/// Words and quoted idents for shape checks. Quotes keep their marks so
/// callers can distinguish `"name"` from keywords.
fn lex_shape(text: &str) -> Option<Vec<String>> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if let Some(q) = quote {
            current.push(c);
            if c == q {
                if chars.peek() == Some(&q) {
                    current.push(chars.next()?);
                } else {
                    quote = None;
                }
            }
            continue;
        }
        match c {
            '\'' | '"' | '`' => {
                if !current.is_empty() {
                    words.push(std::mem::take(&mut current));
                }
                quote = Some(c);
                current.push(c);
            }
            _ if c.is_whitespace() || c == '(' || c == ')' || c == ',' || c == '.' || c == ';' => {
                if !current.is_empty() {
                    words.push(std::mem::take(&mut current));
                }
                if !c.is_whitespace() {
                    words.push(c.to_string());
                }
            }
            _ => current.push(c),
        }
    }
    if quote.is_some() {
        return None;
    }
    if !current.is_empty() {
        words.push(current);
    }
    Some(words)
}

fn unquote(word: &str) -> Option<String> {
    if word.len() >= 2 {
        let (first, last) = (word.chars().next()?, word.chars().last()?);
        if first == last && matches!(first, '"' | '`') {
            return Some(word[1..word.len() - 1].replace(&format!("{first}{first}"), &first.to_string()));
        }
        if first == '[' && last == ']' {
            return Some(word[1..word.len() - 1].to_string());
        }
    }
    if word.starts_with('\'') {
        return None;
    }
    Some(word.to_string())
}

/// Validate remote CREATE TABLE text before execution: single statement,
/// plain (non-temp, non-virtual, main-schema) table, name matches.
pub(crate) fn check_create_table(table: &str, ddl: &str) -> Result<()> {
    if !valid_ident(table) || !single_statement(ddl) {
        return Err(Error::Invalid("rejected schema DDL".into()));
    }
    let words = lex_shape(ddl).ok_or_else(|| Error::Invalid("rejected schema DDL".into()))?;
    let mut i = 0;
    let eat = |words: &[String], i: &mut usize, keyword: &str| -> bool {
        if words.get(*i).is_some_and(|w| w.eq_ignore_ascii_case(keyword)) {
            *i += 1;
            true
        } else {
            false
        }
    };
    if !eat(&words, &mut i, "CREATE") || !eat(&words, &mut i, "TABLE") {
        return Err(Error::Invalid("rejected schema DDL".into()));
    }
    // Optional IF NOT EXISTS.
    if eat(&words, &mut i, "IF") {
        if !eat(&words, &mut i, "NOT") || !eat(&words, &mut i, "EXISTS") {
            return Err(Error::Invalid("rejected schema DDL".into()));
        }
    }
    let mut name = words.get(i).and_then(|w| unquote(w)).ok_or_else(|| Error::Invalid("rejected schema DDL".into()))?;
    i += 1;
    // Optional main. qualifier; anything else is rejected.
    if words.get(i).is_some_and(|w| w == ".") {
        if !name.eq_ignore_ascii_case("main") {
            return Err(Error::Invalid("rejected schema DDL".into()));
        }
        name = words.get(i + 1).and_then(|w| unquote(w)).ok_or_else(|| Error::Invalid("rejected schema DDL".into()))?;
        i += 2;
    }
    if !valid_ident(&name) || !name.eq_ignore_ascii_case(table) {
        return Err(Error::Invalid("rejected schema DDL".into()));
    }
    if words.get(i).is_some_and(|w| w == "(") {
        Ok(())
    } else {
        Err(Error::Invalid("rejected schema DDL".into()))
    }
}

/// Validate a remote column definition before `ADD COLUMN`. Returns the
/// column name. The definition must be a single bare element: no
/// statement separator outside quotes, balanced parens, a plain
/// leading identifier.
pub(crate) fn check_add_column(coldef: &str) -> Result<String> {
    if has_unquoted_semicolon(coldef) || coldef.trim().is_empty() {
        return Err(Error::Invalid("rejected schema column".into()));
    }
    let elements = split_top_level(coldef).ok_or_else(|| Error::Invalid("rejected schema column".into()))?;
    if elements.len() != 1 {
        return Err(Error::Invalid("rejected schema column".into()));
    }
    let first: String = coldef
        .trim()
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    if first.is_empty()
        || matches!(
            first.to_ascii_uppercase().as_str(),
            "PRIMARY" | "FOREIGN" | "UNIQUE" | "CHECK" | "CONSTRAINT"
        )
    {
        return Err(Error::Invalid("rejected schema column".into()));
    }
    let name = coldef
        .split_whitespace()
        .next()
        .ok_or_else(|| Error::Invalid("rejected schema column".into()))?
        .trim_matches('"')
        .to_string();
    if !valid_ident(&name) {
        return Err(Error::Invalid("rejected schema column".into()));
    }
    Ok(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{backend::CrrBackend, storage::Worker};
    use murmur_tx::Backend as _;

    fn pairs(tables: &[(&str, &str)]) -> Vec<u8> {
        crate::encode(
            &tables
                .iter()
                .map(|(t, d)| (t.to_string(), d.to_string()))
                .collect::<Vec<_>>(),
        )
        .unwrap()
    }

    const DOCS: &str = "CREATE TABLE docs(id INTEGER PRIMARY KEY, v TEXT)";

    #[test]
    fn puller_tolerates_extra_tables_only() {
        let donor = pairs(&[("docs", DOCS)]);
        // Identical schemas are compatible.
        assert!(puller_compatible(&donor, &donor).unwrap());
        // Puller-kept tables after a donor drop (drop-never legacy) serve.
        let legacy = pairs(&[
            ("docs", DOCS),
            ("adopted1", "CREATE TABLE adopted1(id INTEGER PRIMARY KEY)"),
        ]);
        assert!(puller_compatible(&donor, &legacy).unwrap());
        // Puller missing a donor table cannot receive its rows.
        assert!(!puller_compatible(&legacy, &donor).unwrap());
        // Shared-table drift stays incompatible (quarantine, e.g. e's
        // extra column on a replicated table).
        let drifted = pairs(&[(
            "docs",
            "CREATE TABLE docs(id INTEGER PRIMARY KEY, v TEXT, w TEXT)",
        )]);
        assert!(!puller_compatible(&donor, &drifted).unwrap());
        // Byte-exact like the digest gate: formatting drift rejects.
        let pretty = pairs(&[("docs", "create table docs( id integer primary key,v text )")]);
        assert!(!puller_compatible(&donor, &pretty).unwrap());
        // Garbage fails closed at decode.
        assert!(puller_compatible(&donor, &[0x01, 0x02]).is_err());
    }

    #[test]
    fn diff_adds_tables_and_columns() {
        let local = pairs(&[("docs", DOCS)]);
        let remote = pairs(&[
            ("docs", "CREATE TABLE docs(id INTEGER PRIMARY KEY, v TEXT, n INTEGER)"),
            ("newt", "CREATE TABLE newt(id INTEGER PRIMARY KEY)"),
        ]);
        match diff(&local, &remote).unwrap() {
            Diff::Additive(a) => {
                assert_eq!(
                    a.new_tables,
                    vec![("newt".to_string(), "CREATE TABLE newt(id INTEGER PRIMARY KEY)".to_string())]
                );
                assert_eq!(a.add_columns.len(), 1);
                assert_eq!(a.add_columns[0].0, "docs");
            }
            Diff::Incompatible(reason) => panic!("unexpected incompatible: {reason}"),
        }
    }

    #[test]
    fn diff_ignores_formatting_but_not_defaults() {
        let local = pairs(&[("docs", DOCS)]);
        // Keyword case + whitespace drift is a no-op.
        let remote = pairs(&[("docs", "create table docs( id  integer primary key,v text )")]);
        assert!(matches!(diff(&local, &remote).unwrap(), Diff::Additive(a) if a.new_tables.is_empty() && a.add_columns.is_empty()));
        // Literal defaults really differ.
        let local = pairs(&[("docs", "CREATE TABLE docs(id INTEGER PRIMARY KEY, v TEXT DEFAULT 'a')")]);
        let remote = pairs(&[("docs", "CREATE TABLE docs(id INTEGER PRIMARY KEY, v TEXT DEFAULT 'A')")]);
        assert!(matches!(diff(&local, &remote).unwrap(), Diff::Incompatible(_)));
    }

    #[test]
    fn diff_rejects_destructive_changes() {
        let local = pairs(&[("docs", DOCS), ("gone", "CREATE TABLE gone(id INTEGER)")]);
        let remote = pairs(&[("docs", DOCS)]);
        assert!(matches!(
            diff(&local, &remote).unwrap(),
            Diff::Incompatible(reason) if reason.contains("retired")
        ));
        let local = pairs(&[("docs", DOCS)]);
        let remote = pairs(&[("docs", "CREATE TABLE docs(id INTEGER PRIMARY KEY)")]);
        assert!(matches!(
            diff(&local, &remote).unwrap(),
            Diff::Incompatible(reason) if reason.contains("dropped")
        ));
        let remote = pairs(&[("docs", "CREATE TABLE docs(id TEXT PRIMARY KEY, v TEXT)")]);
        assert!(matches!(
            diff(&local, &remote).unwrap(),
            Diff::Incompatible(reason) if reason.contains("redefined")
        ));
        let remote = pairs(&[("docs", "CREATE TABLE docs(id INTEGER PRIMARY KEY, v TEXT, UNIQUE(v))")]);
        assert!(matches!(
            diff(&local, &remote).unwrap(),
            Diff::Incompatible(reason) if reason.contains("constraint")
        ));
        let remote = pairs(&[("docs", "CREATE TABLE docs(id INTEGER PRIMARY KEY, v TEXT")]);
        assert!(matches!(diff(&local, &remote).unwrap(), Diff::Incompatible(_)));
    }

    fn backend(number: u8) -> (tempfile::TempDir, CrrBackend) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db").to_str().unwrap().to_owned();
        let worker = Worker::open_test(path).unwrap();
        let mut node = [0u8; 16];
        node[15] = number;
        (dir, CrrBackend::new_offline(worker, [1; 16], node))
    }

    fn params() -> SchemaParams {
        SchemaParams {
            accept_remote_schema: true,
            schema_master: None,
            master: false,
            max_schema_bytes: 1 << 20,
        }
    }

    #[test]
    fn params_parse_flags_and_master() {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE murmur_config(name TEXT PRIMARY KEY, value TEXT NOT NULL);",
        )
        .unwrap();
        let params = params_from(&db).unwrap();
        assert!(params.accept_remote_schema);
        assert_eq!(params.schema_master, None);
        assert!(!params.master);
        db.execute(
            "INSERT INTO murmur_config VALUES('schema_master','00000000000000000000000000000001'),('schema_master_enabled','1'),('accept_remote_schema','0')",
            [],
        )
        .unwrap();
        let params = params_from(&db).unwrap();
        assert!(!params.accept_remote_schema);
        assert_eq!(params.schema_master, Some([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]));
        assert!(params.master);
        db.execute("UPDATE murmur_config SET value='zz' WHERE name='schema_master'", [])
            .unwrap();
        assert!(params_from(&db).is_err());
    }

    #[test]
    fn validators_accept_plain_ddl_and_reject_smuggling() {
        check_create_table("docs", DOCS).unwrap();
        check_create_table("docs", &format!("{DOCS};")).unwrap();
        check_create_table("docs", "CREATE TABLE IF NOT EXISTS docs(id INTEGER)").unwrap();
        check_create_table("docs", "CREATE TABLE main.docs(id INTEGER)").unwrap();
        assert!(check_create_table("docs", "CREATE TABLE other(id INTEGER)").is_err());
        assert!(check_create_table("docs", "CREATE TEMP TABLE docs(id INTEGER)").is_err());
        assert!(check_create_table("docs", "CREATE VIRTUAL TABLE docs USING fts5(x)").is_err());
        assert!(check_create_table("docs", "DROP TABLE docs").is_err());
        assert!(check_create_table(
            "docs",
            "CREATE TABLE docs(id INTEGER); DROP TABLE docs;"
        )
        .is_err());
        assert!(check_create_table("docs", "CREATE TABLE aux.docs(id INTEGER)").is_err());
        assert!(check_create_table("docs", "CREATE TABLE docs AS SELECT 1").is_err());
        assert_eq!(check_add_column("n INTEGER DEFAULT 0").unwrap(), "n");
        assert_eq!(check_add_column("v TEXT DEFAULT 'a;b'").unwrap(), "v");
        assert!(check_add_column("n INTEGER; DROP TABLE docs").is_err());
        assert!(check_add_column("n INTEGER, m INTEGER").is_err());
        assert!(check_add_column("PRIMARY KEY (id)").is_err());
        assert!(check_add_column("").is_err());
    }

    fn signed(sequence: u64, schema: Vec<u8>) -> SignedSchema {
        SignedSchema {
            database: [1; 16],
            signer: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9],
            sequence,
            schema,
            signature: Vec::new(),
        }
    }

    #[test]
    fn manifest_sequence_tracks_digests() {
        let (_dir, backend) = backend(7);
        let params = params();
        let one = pairs(&[("docs", DOCS)]);
        let m1 = build_manifest(&backend, &params, backend.identity().1, [1; 16], one.clone()).unwrap();
        assert_eq!(m1.sequence, 1);
        // Same bytes retransmit idempotently.
        let m1b = build_manifest(&backend, &params, backend.identity().1, [1; 16], one).unwrap();
        assert_eq!(m1b.sequence, 1);
        let two = pairs(&[("docs", DOCS), ("more", "CREATE TABLE more(id INTEGER)")]);
        let m2 = build_manifest(&backend, &params, backend.identity().1, [1; 16], two).unwrap();
        assert_eq!(m2.sequence, 2);
        // Master must designate its own id.
        let mut master = params.clone();
        master.master = true;
        master.schema_master = Some([9; 16]);
        assert!(build_manifest(&backend, &master, backend.identity().1, [1; 16], pairs(&[])).is_err());
        master.schema_master = Some(backend.identity().1);
        assert!(build_manifest(&backend, &master, backend.identity().1, [1; 16], pairs(&[])).is_ok());
    }

    #[test]
    fn check_decides_stale_current_record_adopt() {
        let (_dir, backend) = backend(8);
        let params = params();
        let own = backend.identity().1;
        let source = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9];
        let local = pairs(&[("docs", DOCS)]);
        // Wrong TLS source is rejected.
        assert!(
            check_manifest(&backend, &params, own, &local, &signed(1, local.clone()), [7; 16]).is_err()
        );
        // New table from a trusted peer adopts in legacy mode.
        let remote = pairs(&[("docs", DOCS), ("more", "CREATE TABLE more(id INTEGER)")]);
        assert_eq!(
            check_manifest(&backend, &params, own, &local, &signed(1, remote), source).unwrap(),
            SchemaDecision::Adopt
        );
        // Same sequence restaged is stale (recorded by the adopt path's
        // caller; record here to simulate it).
        let remote = pairs(&[("docs", DOCS)]);
        let first = signed(1, remote.clone());
        assert_eq!(
            check_manifest(&backend, &params, own, &local, &first, source).unwrap(),
            SchemaDecision::Current
        );
        assert_eq!(
            check_manifest(&backend, &params, own, &local, &signed(1, remote), source).unwrap(),
            SchemaDecision::Stale
        );
        // Destructive diffs record, never adopt.
        let dropped = pairs(&[("docs", "CREATE TABLE docs(id INTEGER PRIMARY KEY)")]);
        assert_eq!(
            check_manifest(&backend, &params, own, &local, &signed(2, dropped), source).unwrap(),
            SchemaDecision::Record
        );
        // Designated master gates adoption.
        let mut gated = params.clone();
        gated.schema_master = Some([5; 16]);
        let remote = pairs(&[("docs", DOCS), ("more", "CREATE TABLE more(id INTEGER)")]);
        assert_eq!(
            check_manifest(&backend, &gated, own, &local, &signed(3, remote), source).unwrap(),
            SchemaDecision::Record
        );
        // Opt-out mirrors the archive's accept flag.
        let mut closed = params.clone();
        closed.accept_remote_schema = false;
        let remote = pairs(&[("docs", DOCS), ("more", "CREATE TABLE more(id INTEGER)")]);
        assert_eq!(
            check_manifest(&backend, &closed, own, &local, &signed(4, remote), source).unwrap(),
            SchemaDecision::Record
        );
    }
}
