//! sqlite-murmur seams for the ported query modules (WP-E, points 15-18).
//!
//! This is the only module the embedding crate (`murmur-replication`, via
//! lead-merged wiring) must know in detail. Contents:
//!
//! - [`FlagStore`] / [`LocalOnly`]: replication-flag seam for schema
//!   inspection guards (WP-A owns the real flag map).
//! - [`default_value`] / [`validate`]: the nine WP-E PRAGMAs (constraint 8),
//!   following `config.rs` conventions. The embedding `config.rs` delegates
//!   these names here.
//! - [`crsql_db_version`]: commit-sequence source for subscriptions (lead
//!   decision: durable, replicated, monotonic).
//! - [`ensure_search_tables`] / [`declared_indexes`] / [`declare_index`] /
//!   [`undeclare_index`]: node-local search-declaration store replacing the
//!   archive's replicated `Column.fts`/`Column.vector` flags.
//! - [`actions`]: SQL-action handlers. The embedding `sql.rs` registers the
//!   SQL functions and delegates to these; each takes the worker
//!   [`Connection`](rusqlite::Connection) plus caller-held state.

use crate::{
    Error, Result, Schema, SearchIndex,
    search::{Manager, SearchOptions, validate_options},
};
use rusqlite::Connection;

// ---- replication-flag seam (WP-A) ----

/// Answers "is this stable table ID replicated?" for migration guards.
/// WP-A provides the map-backed implementation; [`LocalOnly`] is the
/// safe default (every guard permissive, nothing treated as replicated).
pub trait FlagStore {
    fn replicated(&self, table_id: &str) -> bool;
}

/// Default flags: every table local-only.
pub struct LocalOnly;
impl FlagStore for LocalOnly {
    fn replicated(&self, _table_id: &str) -> bool {
        false
    }
}

/// True when `table` is a CRR (owns a `{table}__crsql_clock` table).
/// The embedding flag store uses this as the replicated-fallback until
/// WP-A's per-table flags land: a CRR is by definition replicated state,
/// so destructive migrations against it are guarded from day one.
pub fn is_crr(c: &Connection, table: &str) -> Result<bool> {
    let clock = format!("{table}__crsql_clock");
    let count: i64 = c.query_row(
        "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name=?1",
        [&clock],
        |r| r.get(0),
    )?;
    Ok(count > 0)
}

// ---- PRAGMA spec (constraint 8) ----

/// Default for `PRAGMA murmur_{name}`. `None` means "not a WP-E pragma".
/// The embedding `config.rs` falls through to this after its own names.
pub fn default_value(name: &str) -> Option<&'static str> {
    Some(match name {
        "search_queue_bytes" => "8388608",
        "search_batch_rows" => "256",
        "search_interval_ms" => "100",
        "sub_buffer_size" => "32",
        "sub_max_history" => "1024",
        "sub_emit_unchanged" => "0",
        "migrate_chunk_rows" => "2000",
        "migrate_chunk_bytes" => "2097152",
        "exec_max_statements" => "1024",
        _ => return None,
    })
}

/// Range-check a `PRAGMA murmur_{name}` value. Ranges mirror the archive
/// `Options` validation and Go bounds, tightened to local conventions
/// (byte budgets floor at 64 KiB, intervals in ms like `config.rs`).
pub fn validate(name: &str, value: &str) -> Result<()> {
    default_value(name)
        .ok_or_else(|| Error::Invalid(format!("unknown PRAGMA murmur_{name}")))?;
    if value.len() > 65536 || value.contains('\0') {
        return Err(Error::Invalid("setting too large or contains NUL".into()));
    }
    let range = |lo: usize, hi: usize| -> Result<()> {
        let n = value
            .parse::<usize>()
            .map_err(|_| Error::Invalid(format!("expected integer for {name}")))?;
        if !(lo..=hi).contains(&n) {
            return Err(Error::Invalid(format!("{name} outside {lo}..{hi}")));
        }
        Ok(())
    };
    match name {
        "search_queue_bytes" => range(65536, 134217728)?,
        "search_batch_rows" => range(1, 65536)?,
        "search_interval_ms" => range(10, 86400000)?,
        "sub_buffer_size" => range(1, 4096)?,
        "sub_max_history" => range(16, 100000)?,
        "sub_emit_unchanged" => range(0, 1)?,
        "migrate_chunk_rows" => range(1, 100000)?,
        "migrate_chunk_bytes" => range(1024, 67108864)?,
        "exec_max_statements" => range(1, 100000)?,
        _ => unreachable!("checked above"),
    }
    Ok(())
}

/// Build [`SearchOptions`] from already-validated pragma strings.
pub fn search_options(
    definitions: Vec<SearchIndex>,
    queue_bytes: usize,
    batch_rows: usize,
    interval_ms: u64,
) -> Result<SearchOptions> {
    let options = SearchOptions {
        search_indexes: definitions,
        search_queue_bytes: queue_bytes,
        search_batch_rows: batch_rows,
        search_interval: std::time::Duration::from_millis(interval_ms),
    };
    validate_options(&options)?;
    Ok(options)
}

// ---- commit-sequence source ----

/// Current commit sequence for subscription cursors: cr-sqlite's
/// `crsql_db_version()` (lead decision; durable, replicated, monotonic).
pub fn crsql_db_version(c: &Connection) -> Result<u64> {
    let version: i64 = c.query_row("SELECT crsql_db_version()", [], |r| r.get(0))?;
    Ok(version as u64)
}

// ---- node-local search declarations ----

/// Declaration backing table. The archive persists `Column.fts`/`vector` as
/// replicated schema metadata; sqlite-murmur keeps declarations node-local
/// (index contents were node-local in the archive too). Plain table, never
/// a CRR, so it neither replicates nor disturbs `schema::inspect` (which
/// skips `murmur_*`). `columns` is a JSON array of column names; vector rows
/// carry exactly one column plus dimensions/metric.
pub fn ensure_search_tables(c: &Connection) -> Result<()> {
    c.execute_batch(
        "CREATE TABLE IF NOT EXISTS murmur_search_columns(
            name TEXT PRIMARY KEY, source_table TEXT NOT NULL, kind TEXT NOT NULL,
            columns TEXT NOT NULL, tokenizer TEXT NOT NULL DEFAULT 'unicode61',
            dimensions INTEGER NOT NULL DEFAULT 0, metric TEXT NOT NULL DEFAULT 'l2')",
    )?;
    Ok(())
}

/// Read all declarations as explicit index definitions.
pub fn declared_indexes(c: &Connection) -> Result<Vec<SearchIndex>> {
    ensure_search_tables(c)?;
    let mut q = c.prepare(
        "SELECT name, source_table, kind, columns, tokenizer, dimensions, metric
         FROM murmur_search_columns ORDER BY name",
    )?;
    let rows = q.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, String>(4)?,
            r.get::<_, i64>(5)?,
            r.get::<_, String>(6)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (name, table, kind, columns, tokenizer, dimensions, metric) = row?;
        let columns: Vec<String> =
            serde_json::from_str(&columns).map_err(|e| Error::Corrupt(e.to_string()))?;
        match kind.as_str() {
            "fts" => out.push(
                crate::Fts5Index {
                    name,
                    source_table: table,
                    columns,
                    tokenizer,
                }
                .into(),
            ),
            "vec" => {
                let column = columns
                    .first()
                    .ok_or_else(|| Error::Corrupt("vector declaration without column".into()))?
                    .clone();
                let distance = match metric.as_str() {
                    "l2" => crate::DistanceMetric::L2,
                    "cosine" => crate::DistanceMetric::Cosine,
                    "l1" => crate::DistanceMetric::L1,
                    _ => return Err(Error::Corrupt("unknown vector metric".into())),
                };
                out.push(
                    crate::VectorIndex {
                        name,
                        source_table: table,
                        column,
                        dimensions: dimensions as usize,
                        distance,
                    }
                    .into(),
                );
            }
            _ => return Err(Error::Corrupt("unknown search declaration kind".into())),
        }
    }
    Ok(out)
}

/// Persist (or replace) one declaration. Validation happens when the caller
/// rebuilds the [`Manager`] via [`Manager::set_explicit`].
/// Replace-or-push `index` by name, mirroring the `ON CONFLICT(name)`
/// upsert in [`declare_index`]. Callers build this merged set and pass it to
/// [`Manager::validate_definitions`] BEFORE persisting.
fn upsert_definition(definitions: &mut Vec<SearchIndex>, index: SearchIndex) {
    if let Some(slot) = definitions.iter_mut().find(|d| d.name() == index.name()) {
        *slot = index;
    } else {
        definitions.push(index);
    }
}

pub fn declare_index(c: &Connection, index: &SearchIndex) -> Result<()> {
    ensure_search_tables(c)?;
    match index {
        SearchIndex::Fts5(x) => c.execute(
            "INSERT INTO murmur_search_columns(name,source_table,kind,columns,tokenizer)
             VALUES(?1,?2,'fts',?3,?4)
             ON CONFLICT(name) DO UPDATE SET source_table=excluded.source_table,
               kind='fts', columns=excluded.columns, tokenizer=excluded.tokenizer",
            rusqlite::params![
                x.name,
                x.source_table,
                serde_json::to_string(&x.columns).unwrap(),
                x.tokenizer
            ],
        )?,
        SearchIndex::Vector(x) => {
            let metric = match x.distance {
                crate::DistanceMetric::L2 => "l2",
                crate::DistanceMetric::Cosine => "cosine",
                crate::DistanceMetric::L1 => "l1",
            };
            c.execute(
                "INSERT INTO murmur_search_columns(name,source_table,kind,columns,dimensions,metric)
                 VALUES(?1,?2,'vec',?3,?4,?5)
                 ON CONFLICT(name) DO UPDATE SET source_table=excluded.source_table,
                   kind='vec', columns=excluded.columns, dimensions=excluded.dimensions,
                   metric=excluded.metric",
                rusqlite::params![
                    x.name,
                    x.source_table,
                    serde_json::to_string(&[&x.column]).unwrap(),
                    x.dimensions as i64,
                    metric
                ],
            )?
        }
    };
    Ok(())
}

/// Remove one declaration. Returns true when a row was removed.
pub fn undeclare_index(c: &Connection, name: &str) -> Result<bool> {
    ensure_search_tables(c)?;
    Ok(c.execute("DELETE FROM murmur_search_columns WHERE name=?1", [name])? > 0)
}

/// SQL-action handlers. The embedding `sql.rs` registers one SQL function per
/// handler and delegates here. Every handler runs on the worker connection in
/// autocommit (mirroring the `murmur_*` controls); `manager`/`schema` are the
/// worker's held instances, refreshed by the caller as documented per handler.
pub mod actions {
    use super::*;
    use crate::{
        DistanceMetric, Fts5Index, Migration, VectorIndex,
        primary_key::references_managed_column,
        schema,
        search::SearchIndexStatus,
    };

    fn rebuild(
        c: &Connection,
        manager: &mut Manager,
        s: &Schema,
    ) -> Result<()> {
        let definitions = declared_indexes(c)?;
        manager.set_explicit(c, s, definitions)
    }

    /// `murmur_search_create_fts(name, table, columns_json, tokenizer)`.
    pub fn create_fts(
        c: &Connection,
        manager: &mut Manager,
        s: &Schema,
        name: &str,
        table: &str,
        columns_json: &str,
        tokenizer: &str,
    ) -> Result<()> {
        let columns: Vec<String> =
            serde_json::from_str(columns_json).map_err(|e| Error::Invalid(e.to_string()))?;
        let mut index = Fts5Index::new(name, table, columns);
        index.tokenizer = tokenizer.to_string();
        let index = SearchIndex::from(index);
        let mut definitions = declared_indexes(c)?;
        upsert_definition(&mut definitions, index.clone());
        manager.validate_definitions(&definitions)?;
        declare_index(c, &index)?;
        rebuild(c, manager, s)
    }

    /// `murmur_search_create_vec(name, table, column, dimensions, metric)`.
    /// `metric` is one of `l2`, `cosine`, `l1`.
    pub fn create_vec(
        c: &Connection,
        manager: &mut Manager,
        s: &Schema,
        name: &str,
        table: &str,
        column: &str,
        dimensions: usize,
        metric: &str,
    ) -> Result<()> {
        let distance = match metric {
            "l2" => DistanceMetric::L2,
            "cosine" => DistanceMetric::Cosine,
            "l1" => DistanceMetric::L1,
            _ => return Err(Error::Invalid("unknown vector metric".into())),
        };
        let mut index = VectorIndex::new(name, table, column, dimensions);
        index.distance = distance;
        let index = SearchIndex::from(index);
        let mut definitions = declared_indexes(c)?;
        upsert_definition(&mut definitions, index.clone());
        manager.validate_definitions(&definitions)?;
        declare_index(c, &index)?;
        rebuild(c, manager, s)
    }

    /// `murmur_search_drop(name)`. Returns 1 when an index was dropped.
    pub fn drop_index(
        c: &Connection,
        manager: &mut Manager,
        s: &Schema,
        name: &str,
    ) -> Result<i64> {
        if !undeclare_index(c, name)? {
            return Ok(0);
        }
        rebuild(c, manager, s)?;
        Ok(1)
    }

    /// `murmur_refresh_indexes()`. Blocks until pending index work completes.
    /// Returns the published index names as JSON.
    pub fn refresh(c: &Connection, manager: &mut Manager, s: &Schema) -> Result<String> {
        let published = manager.refresh(c, s)?;
        Ok(serde_json::to_string(&published).unwrap())
    }

    /// `murmur_search_status()`. Per-index readiness as JSON.
    pub fn status(manager: &Manager) -> String {
        serde_json::to_string(&status_rows(manager)).unwrap()
    }

    fn status_rows(manager: &Manager) -> Vec<SearchIndexStatus> {
        manager.status()
    }

    /// `murmur_column_fts(table, column, enabled)`. Archive equivalent:
    /// `set_column_fts`, except the declaration is node-local (see
    /// [`ensure_search_tables`](super::ensure_search_tables)). Enabling
    /// merges the column into the table's `murmur_fts_*` index (archive
    /// naming via [`schema_index_name`](crate::search::schema_index_name));
    /// disabling removes it, dropping the index when empty.
    pub fn set_column_fts(
        c: &Connection,
        manager: &mut Manager,
        s: &Schema,
        table: &str,
        column: &str,
        enabled: bool,
    ) -> Result<()> {
        let t = s
            .tables
            .iter()
            .find(|t| t.name.eq_ignore_ascii_case(table))
            .ok_or_else(|| Error::Invalid(format!("unknown table: {table}")))?;
        if !t.columns.iter().any(|col| col.name.eq_ignore_ascii_case(column)) {
            return Err(Error::Invalid(format!("unknown column: {table}.{column}")));
        }
        let name = crate::search::schema_index_name(t);
        let definitions = declared_indexes(c)?;
        let owned: Vec<String> = definitions
            .iter()
            .filter_map(|d| match d {
                SearchIndex::Fts5(x) if x.name == name => Some(x.columns.clone()),
                _ => None,
            })
            .next()
            .unwrap_or_default();
        let mut columns: Vec<String> = owned
            .into_iter()
            .filter(|existing| !existing.eq_ignore_ascii_case(column))
            .collect();
        if enabled {
            columns.push(column.to_string());
        }
        if !columns.is_empty() {
            let index = Fts5Index {
                name: name.clone(),
                source_table: t.name.clone(),
                columns,
                tokenizer: "unicode61".into(),
            };
            let index = SearchIndex::from(index);
            let mut definitions = declared_indexes(c)?;
            upsert_definition(&mut definitions, index.clone());
            manager.validate_definitions(&definitions)?;
            declare_index(c, &index)?;
        } else {
            undeclare_index(c, &name)?;
        }
        rebuild(c, manager, s)
    }

    /// `murmur_column_vector(table, column, dimensions, metric)`, or
    /// `murmur_column_vector(table, column, NULL, NULL)` to clear. Archive
    /// equivalent: `set_column_vector` (`None` disables), node-local.
    pub fn set_column_vector(
        c: &Connection,
        manager: &mut Manager,
        s: &Schema,
        table: &str,
        column: &str,
        dimensions: Option<usize>,
        metric: Option<&str>,
    ) -> Result<()> {
        let t = s
            .tables
            .iter()
            .find(|t| t.name.eq_ignore_ascii_case(table))
            .ok_or_else(|| Error::Invalid(format!("unknown table: {table}")))?;
        let col = t
            .columns
            .iter()
            .find(|col| col.name.eq_ignore_ascii_case(column))
            .ok_or_else(|| Error::Invalid(format!("unknown column: {table}.{column}")))?;
        let name = crate::search::vector_index_name(t, col);
        match dimensions {
            Some(dimensions) => {
                let distance = match metric {
                    Some("l2") | None => DistanceMetric::L2,
                    Some("cosine") => DistanceMetric::Cosine,
                    Some("l1") => DistanceMetric::L1,
                    _ => return Err(Error::Invalid("unknown vector metric".into())),
                };
                let index = VectorIndex {
                    name,
                    source_table: t.name.clone(),
                    column: col.name.clone(),
                    dimensions,
                    distance,
                };
                let index = SearchIndex::from(index);
                let mut definitions = declared_indexes(c)?;
                upsert_definition(&mut definitions, index.clone());
                manager.validate_definitions(&definitions)?;
                declare_index(c, &index)?;
            }
            None => {
                undeclare_index(c, &name)?;
            }
        }
        rebuild(c, manager, s)
    }

    /// `murmur_migrate(sql, mappings_json)`. Executes the migration SQL on
    /// the worker connection, then inspects the resulting schema (guarding
    /// replicated tables via `replicated`). Runs atomically: a rejected
    /// inspection rolls the SQL back. Returns the new schema history hash
    /// (hex) so the caller can detect change; the caller replaces its held
    /// schema and re-syncs search. `mappings_json` is a JSON array of
    /// `{old_name, new_name, columns: [[old, new], ...]}` (usually `[]`).
    pub fn migrate(
        c: &Connection,
        previous: &Schema,
        sql: &str,
        mappings_json: &str,
        replicated: &dyn Fn(&str) -> bool,
    ) -> Result<String> {
        #[derive(serde::Deserialize)]
        struct Mapping {
            old_name: String,
            new_name: String,
            #[serde(default)]
            columns: Vec<(String, String)>,
        }
        let raw: Vec<Mapping> =
            serde_json::from_str(mappings_json).map_err(|e| Error::Invalid(e.to_string()))?;
        let mut migration = Migration::new(sql);
        migration.mappings = raw
            .into_iter()
            .map(|m| crate::TableMapping {
                old_name: m.old_name,
                new_name: m.new_name,
                columns: m.columns,
            })
            .collect();
        c.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<String> {
            c.execute_batch(sql)?;
            let next = schema::inspect(c, previous, &migration, replicated)?;
            Ok(crate::crdt_compat::hex(&next.history))
        })();
        match result {
            Ok(history) => {
                c.execute_batch("COMMIT")?;
                Ok(history)
            }
            Err(error) => {
                let _ = c.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    /// `murmur_exec_json(statements_json, max_statements)`. Executes a JSON
    /// array of SQL strings as one transaction with managed-key rewriting.
    /// Statements must be value-complete (no bound parameters). Returns
    /// `{"statements": n, "rows_changed": total}`.
    pub fn exec_json(
        c: &Connection,
        s: &Schema,
        statements_json: &str,
        max_statements: usize,
    ) -> Result<String> {
        let statements: Vec<String> =
            serde_json::from_str(statements_json).map_err(|e| Error::Invalid(e.to_string()))?;
        if statements.len() > max_statements {
            return Err(Error::Invalid("statement batch exceeds murmur_exec_max_statements".into()));
        }
        c.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<(usize, u64)> {
            // Per-statement direct write counts: total_changes() would
            // include cr-sqlite trigger bookkeeping on CRR tables, and a
            // single trailing changes() would re-report stale counts
            // after SELECTs. Read-only statements drain without counting.
            let mut changed: u64 = 0;
            for sql in &statements {
                if references_managed_column(sql)? {
                    return Err(Error::Invalid(
                        "id_murmur_primary_key is managed and read-only".into(),
                    ));
                }
                for part in crate::primary_key::statements(sql)? {
                    let rewritten = crate::primary_key::rewrite(part, s)?;
                    let mut stmt = c.prepare(&rewritten)?;
                    if stmt.readonly() {
                        let mut rows = stmt.query([])?;
                        while rows.next()?.is_some() {}
                    } else {
                        changed += stmt.execute([])? as u64;
                    }
                }
            }
            Ok((statements.len(), changed))
        })();
        match result {
            Ok((n, changed)) => {
                c.execute_batch("COMMIT")?;
                Ok(format!("{{\"statements\":{n},\"rows_changed\":{changed}}}"))
            }
            Err(error) => {
                let _ = c.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    /// `murmur_export(since_version)`. Dumps `crsql_changes` rows newer than
    /// `since_version` as JSON (`{"db_version": v, "changes": [...]}`), one
    /// object per row keyed by vtab column name. Read-only; the WP-A merge
    /// engine consumes this shape.
    pub fn export_changes(c: &Connection, since_version: i64) -> Result<String> {
        let mut q = c.prepare(
            "SELECT * FROM crsql_changes WHERE db_version > ?1 ORDER BY db_version, seq",
        )?;
        let names: Vec<String> = q.column_names().iter().map(|s| s.to_string()).collect();
        let mut cursor = q.query([since_version])?;
        let mut changes = Vec::new();
        while let Some(row) = cursor.next()? {
            let mut object = serde_json::Map::new();
            for (i, name) in names.iter().enumerate() {
                let value = crate::Value::read(row.get_ref(i)?)?;
                object.insert(name.clone(), json_value(&value));
            }
            changes.push(serde_json::Value::Object(object));
        }
        let db_version = super::crsql_db_version(c)?;
        Ok(serde_json::json!({"db_version": db_version, "changes": changes}).to_string())
    }

    fn json_value(value: &crate::Value) -> serde_json::Value {
        match value {
            crate::Value::Null => serde_json::Value::Null,
            crate::Value::Integer(n) => serde_json::json!(*n),
            crate::Value::Real(x) => serde_json::json!(*x),
            crate::Value::Text(s) => serde_json::Value::String(s.clone()),
            crate::Value::Blob(b) => {
                serde_json::Value::String(b.iter().map(|byte| format!("{byte:02x}")).collect())
            }
        }
    }

    /// `murmur_import(state_json)` seam. The merge engine arrives with WP-A;
    /// until then this reports unavailable (never silently drops input).
    pub fn merge_state(_c: &Connection, _state_json: &str) -> Result<()> {
        Err(Error::Unavailable(
            "merge engine arrives with WP-A; export via murmur_export only".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Migration, Schema};

    #[test]
    fn pragma_defaults_and_ranges() {
        let cases = [
            ("search_queue_bytes", "8388608", "10", "999999999999"),
            ("search_batch_rows", "256", "0", "70000"),
            ("search_interval_ms", "100", "1", "99999999"),
            ("sub_buffer_size", "32", "0", "5000"),
            ("sub_max_history", "1024", "1", "200000"),
            ("sub_emit_unchanged", "0", "2", "-1"),
            ("migrate_chunk_rows", "2000", "0", "200000"),
            ("migrate_chunk_bytes", "2097152", "10", "99999999999"),
            ("exec_max_statements", "1024", "0", "200000"),
        ];
        for (name, default, too_small, too_big) in cases {
            assert_eq!(default_value(name), Some(default), "{name}");
            validate(name, default).unwrap();
            assert!(validate(name, too_small).is_err(), "{name} {too_small}");
            assert!(validate(name, too_big).is_err(), "{name} {too_big}");
        }
        assert_eq!(default_value("nope"), None);
        assert!(validate("nope", "1").is_err());
        assert!(validate("search_batch_rows", "not-a-number").is_err());
    }

    #[test]
    fn declarations_round_trip() -> Result<()> {
        let c = Connection::open_in_memory()?;
        assert!(declared_indexes(&c)?.is_empty());
        declare_index(
            &c,
            &crate::Fts5Index::new("fts1", "docs", vec!["title".into()]).into(),
        )?;
        let mut vec_index = crate::VectorIndex::new("vec1", "docs", "embedding", 8);
        vec_index.distance = crate::DistanceMetric::Cosine;
        declare_index(&c, &vec_index.into())?;
        let back = declared_indexes(&c)?;
        assert_eq!(back.len(), 2);
        assert!(undeclare_index(&c, "fts1")?);
        assert!(!undeclare_index(&c, "fts1")?);
        let back = declared_indexes(&c)?;
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].name(), "vec1");
        Ok(())
    }

    #[test]
    fn column_fts_merges_and_drops() -> Result<()> {
        let c = Connection::open_in_memory()?;
        c.execute_batch("CREATE TABLE docs(id INTEGER PRIMARY KEY, a TEXT, b TEXT)")?;
        let schema = crate::schema::inspect(
            &c,
            &Schema::default(),
            &Migration::new("CREATE TABLE docs(id INTEGER PRIMARY KEY, a TEXT, b TEXT)"),
            &|_| false,
        )?;
        let mut manager = Manager::new(&SearchOptions::default());
        actions::set_column_fts(&c, &mut manager, &schema, "docs", "a", true)?;
        actions::set_column_fts(&c, &mut manager, &schema, "docs", "b", true)?;
        let definitions = declared_indexes(&c)?;
        assert_eq!(definitions.len(), 1);
        match &definitions[0] {
            SearchIndex::Fts5(x) => assert_eq!(x.columns, vec!["a".to_string(), "b".to_string()]),
            other => panic!("expected FTS, got {other:?}"),
        }
        actions::set_column_fts(&c, &mut manager, &schema, "docs", "a", false)?;
        let definitions = declared_indexes(&c)?;
        match &definitions[0] {
            SearchIndex::Fts5(x) => assert_eq!(x.columns, vec!["b".to_string()]),
            other => panic!("expected FTS, got {other:?}"),
        }
        actions::set_column_fts(&c, &mut manager, &schema, "docs", "b", false)?;
        assert!(declared_indexes(&c)?.is_empty());
        // Unknown table/column rejected.
        assert!(actions::set_column_fts(&c, &mut manager, &schema, "nope", "a", true).is_err());
        assert!(actions::set_column_fts(&c, &mut manager, &schema, "docs", "nope", true).is_err());
        Ok(())
    }

    #[test]
    fn rejected_create_leaves_no_declaration() -> Result<()> {
        let c = Connection::open_in_memory()?;
        c.execute_batch("CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT)")?;
        let schema = crate::schema::inspect(
            &c,
            &Schema::default(),
            &Migration::new("CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT)"),
            &|_| false,
        )?;
        let mut manager = Manager::new(&SearchOptions::default());
        // Invalid tokenizer rejected...
        assert!(actions::create_fts(&c, &mut manager, &schema, "bad", "docs", "[\"title\"]", "nope")
            .is_err());
        // ...and nothing persisted, so later operations see a clean store.
        assert!(declared_indexes(&c)?.is_empty());
        actions::create_fts(&c, &mut manager, &schema, "bad", "docs", "[\"title\"]", "unicode61")?;
        assert_eq!(declared_indexes(&c)?.len(), 1);
        // Replacing a good declaration with a bad one keeps the good one.
        assert!(actions::create_fts(&c, &mut manager, &schema, "bad", "docs", "[\"title\"]", "nope")
            .is_err());
        let back = declared_indexes(&c)?;
        assert_eq!(back.len(), 1);
        match &back[0] {
            SearchIndex::Fts5(x) => assert_eq!(x.tokenizer, "unicode61"),
            other => panic!("expected FTS, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn migrate_inspects_and_guards_replicated_tables() -> Result<()> {
        let c = Connection::open_in_memory()?;
        let empty = Schema::default();
        let history = actions::migrate(
            &c,
            &empty,
            "CREATE TABLE items(id INTEGER PRIMARY KEY, v TEXT)",
            "[]",
            &|_| false,
        )?;
        assert_eq!(history.len(), 64);
        let schema = crate::schema::inspect(
            &c,
            &empty,
            &Migration::new("CREATE TABLE items(id INTEGER PRIMARY KEY, v TEXT)"),
            &|_| false,
        )?;
        assert_eq!(schema.tables.len(), 1);
        // Replicated table: destructive migration rejected…
        let err = actions::migrate(&c, &schema, "DROP TABLE items", "[]", &|_| true)
            .unwrap_err()
            .to_string();
        assert!(err.contains("disable replication first"), "{err}");
        // …while local-only tables drop freely.
        actions::migrate(&c, &schema, "DROP TABLE items", "[]", &|_| false)?;
        let schema = crate::schema::inspect(
            &c,
            &schema,
            &Migration::new("DROP TABLE items"),
            &|_| false,
        )?;
        assert!(schema.tables.is_empty());
        Ok(())
    }

    #[test]
    fn exec_json_runs_one_transaction() -> Result<()> {
        let c = Connection::open_in_memory()?;
        c.execute_batch("CREATE TABLE items(id INTEGER PRIMARY KEY, v TEXT)")?;
        let schema = crate::schema::inspect(
            &c,
            &Schema::default(),
            &Migration::new("CREATE TABLE items(id INTEGER PRIMARY KEY, v TEXT)"),
            &|_| false,
        )?;
        let out = actions::exec_json(
            &c,
            &schema,
            r#"["INSERT INTO items VALUES(1,'a')", "INSERT INTO items VALUES(2,'b')"]"#,
            1024,
        )?;
        assert_eq!(out, r#"{"statements":2,"rows_changed":2}"#);
        // Batch over the cap rejected before touching the database.
        assert!(actions::exec_json(&c, &schema, r#"["SELECT 1", "SELECT 2"]"#, 1).is_err());
        // Failure rolls everything back.
        let before: i64 = c.query_row("SELECT count(*) FROM items", [], |r| r.get(0))?;
        assert!(
            actions::exec_json(&c, &schema, r#"["INSERT INTO items VALUES(3,'c')", "BOGUS SQL"]"#, 1024)
                .is_err()
        );
        let after: i64 = c.query_row("SELECT count(*) FROM items", [], |r| r.get(0))?;
        assert_eq!(before, after);
        // Managed column cannot be written through the batch path.
        assert!(
            actions::exec_json(&c, &schema, r#"["SELECT id_murmur_primary_key FROM items"]"#, 1024)
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn merge_seam_reports_unavailable() {
        let c = Connection::open_in_memory().unwrap();
        let err = actions::merge_state(&c, "{}").unwrap_err();
        assert!(matches!(err, Error::Unavailable(_)));
    }
}
