//! Disposable search indexes. Source rows and their CRR state are authoritative.
//!
//! Ported from MURMUR-RUST `src/search.rs`; sqlite-murmur adaptations (see
//! `PROVENANCE.md`): `Options` replaced by [`SearchOptions`]; the SQLite
//! session-changeset feed (`enqueue`/`decode_pending`) replaced by
//! [`Manager::observe`] driven from CRR changes, so no `session` feature is
//! needed; index tables live in MAIN schema as `murmur_fts_*` /
//! `murmur_vec_*` shadows (archive TEMP tables are per-connection and
//! invisible to app connections); visibility raised to `pub` for the
//! embedding crate.
use crate::{Error, Result, RowState, Schema, Value, model::quote};
use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

pub use crate::crdt_compat::{DistanceMetric, VectorColumnConfig};

/// Search maintenance limits. Archive equivalent: the `search_*` fields of
/// `sqlite-session` `Options` (defaults 8 MiB / 256 rows / 100 ms).
#[derive(Debug, Clone)]
pub struct SearchOptions {
    /// Node-local, rebuildable search indexes over ordinary source tables.
    pub search_indexes: Vec<SearchIndex>,
    /// Byte budget for disposable indexing work; overflow triggers a rebuild.
    pub search_queue_bytes: usize,
    /// Maximum rows processed per maintenance turn.
    pub search_batch_rows: usize,
    pub search_interval: std::time::Duration,
}
impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            search_indexes: Vec::new(),
            search_queue_bytes: 8 << 20,
            search_batch_rows: 256,
            search_interval: std::time::Duration::from_millis(100),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fts5Index {
    pub name: String,
    pub source_table: String,
    pub columns: Vec<String>,
    /// A built-in tokenizer: unicode61 (default), ascii, porter, or trigram.
    pub tokenizer: String,
}
impl Fts5Index {
    pub fn new(
        name: impl Into<String>,
        source_table: impl Into<String>,
        columns: Vec<String>,
    ) -> Self {
        Self {
            name: name.into(),
            source_table: source_table.into(),
            columns,
            tokenizer: "unicode61".into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorIndex {
    pub name: String,
    pub source_table: String,
    /// Source BLOB containing little-endian float32 values; also the index column name.
    pub column: String,
    pub dimensions: usize,
    pub distance: DistanceMetric,
}
impl VectorIndex {
    pub fn new(
        name: impl Into<String>,
        source_table: impl Into<String>,
        column: impl Into<String>,
        dimensions: usize,
    ) -> Self {
        Self {
            name: name.into(),
            source_table: source_table.into(),
            column: column.into(),
            dimensions,
            distance: DistanceMetric::L2,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchIndex {
    Fts5(Fts5Index),
    Vector(VectorIndex),
}
impl From<Fts5Index> for SearchIndex {
    fn from(value: Fts5Index) -> Self {
        Self::Fts5(value)
    }
}
impl From<VectorIndex> for SearchIndex {
    fn from(value: VectorIndex) -> Self {
        Self::Vector(value)
    }
}
impl SearchIndex {
    pub fn name(&self) -> &str {
        match self {
            Self::Fts5(x) => &x.name,
            Self::Vector(x) => &x.name,
        }
    }
    /// Main-schema physical table name. Archive TEMP tables are
    /// per-connection, but population runs on the worker connection while
    /// app connections query — so sqlite-murmur shadows live in main
    /// under the reserved `murmur_fts_*` / `murmur_vec_*` prefix
    /// (excluded from inspect/CRR by the `murmur_*` skip rules).
    /// Idempotent: names already carrying the kind prefix keep it, which
    /// covers archive-derived `murmur_fts_{tableid}` /
    /// `murmur_vec_{tableid}_{columnid}` unchanged.
    pub fn physical_name(&self) -> String {
        let (prefix, declared) = match self {
            Self::Fts5(x) => ("murmur_fts_", x.name.as_str()),
            Self::Vector(x) => ("murmur_vec_", x.name.as_str()),
        };
        if declared.starts_with(prefix) {
            declared.to_string()
        } else {
            format!("{prefix}{declared}")
        }
    }
    pub fn source_table(&self) -> &str {
        match self {
            Self::Fts5(x) => &x.source_table,
            Self::Vector(x) => &x.source_table,
        }
    }
    fn columns(&self) -> Vec<&str> {
        match self {
            Self::Fts5(x) => x.columns.iter().map(String::as_str).collect(),
            Self::Vector(x) => vec![&x.column],
        }
    }
    fn create_sql(&self) -> String {
        let module = match self {
            Self::Fts5(x) => format!(
                "fts5({}, tokenize='{}')",
                x.columns
                    .iter()
                    .map(|c| quote(c))
                    .collect::<Vec<_>>()
                    .join(","),
                x.tokenizer
            ),
            Self::Vector(x) => format!(
                "vec0({} float[{}] distance_metric={})",
                x.column,
                x.dimensions,
                match x.distance {
                    DistanceMetric::L2 => "l2",
                    DistanceMetric::Cosine => "cosine",
                    DistanceMetric::L1 => "l1",
                }
            ),
        };
        format!(
            "CREATE VIRTUAL TABLE {} USING {module}",
            quote(&self.physical_name())
        )
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SearchIndexStatus {
    pub name: String,
    pub ready: bool,
    pub rebuilding: bool,
    pub pending_keys: usize,
    pub queued_bytes: usize,
    pub error: Option<String>,
}

pub fn schema_index_name(table: &crate::Table) -> String {
    format!("murmur_fts_{}", table.id)
}

pub fn vector_index_name(table: &crate::Table, column: &crate::Column) -> String {
    format!("murmur_vec_{}_{}", table.id, column.id)
}

/// Shared by startup, schema maintenance, and explicit-index replacement.
pub fn schema_definitions(explicit: &[SearchIndex], schema: &Schema) -> Result<Vec<SearchIndex>> {
    let mut definitions = explicit.to_vec();
    for table in &schema.tables {
        let columns: Vec<_> = table.columns.iter().filter(|c| c.fts).map(|c| c.name.clone()).collect();
        if !columns.is_empty() {
            definitions.push(Fts5Index::new(schema_index_name(table), &table.name, columns).into());
        }
        for column in &table.columns {
            if let Some(config) = &column.vector {
                if column.name.eq_ignore_ascii_case(crate::primary_key::COLUMN) {
                    return Err(Error::Invalid("vector declarations require an application column".into()));
                }
                definitions.push(VectorIndex {
                    name: vector_index_name(table, column), source_table: table.name.clone(),
                    column: column.name.clone(), dimensions: config.dimensions, distance: config.distance,
                }.into());
            }
        }
    }
    let options = SearchOptions { search_indexes: definitions, ..SearchOptions::default() };
    validate_options(&options)?;
    Ok(options.search_indexes)
}

pub type Guard = Arc<RwLock<BTreeMap<String, bool>>>;

pub fn validate_options(o: &SearchOptions) -> Result<()> {
    if o.search_queue_bytes == 0
        || o.search_queue_bytes > 128 << 20
        || o.search_batch_rows == 0
        || o.search_batch_rows > 65536
        || o.search_interval.is_zero()
    {
        return Err(Error::Invalid("invalid search maintenance limits".into()));
    }
    let mut names = BTreeSet::new();
    for index in &o.search_indexes {
        if index.name().is_empty()
            || index.name().contains('\0')
            || index.source_table().is_empty()
            || index.source_table().contains('\0')
            || index.name().to_ascii_lowercase().starts_with("sqlite_")
            || index.name().to_ascii_lowercase().starts_with("_crdt_")
            || !names.insert(index.name().to_ascii_lowercase())
        {
            return Err(Error::Invalid(
                "invalid or duplicate search index name".into(),
            ));
        }
        let columns = index.columns();
        if columns.is_empty()
            || columns.iter().any(|c| {
                c.is_empty()
                    || c.contains('\0')
                    || c.eq_ignore_ascii_case("rowid")
                    || c.eq_ignore_ascii_case(index.name())
            })
            || columns
                .iter()
                .map(|c| c.to_ascii_lowercase())
                .collect::<BTreeSet<_>>()
                .len()
                != columns.len()
        {
            return Err(Error::Invalid("invalid search index columns".into()));
        }
        match index {
            SearchIndex::Fts5(x)
                if !["unicode61", "ascii", "porter", "trigram"].contains(&x.tokenizer.as_str()) =>
            {
                return Err(Error::Invalid("unsupported FTS5 tokenizer".into()));
            }
            SearchIndex::Vector(x) => {
                if !x
                    .column
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphabetic)
                    || !x
                        .column
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_')
                {
                    return Err(Error::Invalid(
                        "sqlite-vec column names must be ASCII SQL identifiers".into(),
                    ));
                }
                if !cfg!(feature = "vector") {
                    return Err(Error::Invalid(
                        "vector indexes require the vector Cargo feature".into(),
                    ));
                }
                if x.dimensions == 0 || x.dimensions > 8192 {
                    return Err(Error::Invalid("vector dimensions must be 1..=8192".into()));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

#[derive(Clone)]
struct Index {
    definition: SearchIndex,
    owned: BTreeSet<String>,
    installed: bool,
    ready: bool,
    rebuild: bool,
    started: bool,
    last: Option<i64>,
    keys: BTreeSet<i64>,
    error: Option<String>,
}
impl Index {
    fn invalidate(&mut self) {
        self.rebuild = true;
        self.started = false;
        self.last = None;
        self.keys.clear();
        self.ready = false;
    }
}

#[derive(Clone)]
pub struct Manager {
    explicit: Vec<SearchIndex>,
    indexes: Vec<Index>,
    budget: usize,
    batch: usize,
    next: usize,
    publications: BTreeSet<String>,
    pub guard: Guard,
}
impl Manager {
    pub fn new(o: &SearchOptions) -> Self {
        Self {
            explicit: o.search_indexes.clone(),
            indexes: o
                .search_indexes
                .iter()
                .cloned()
                .map(|definition| Index {
                    owned: BTreeSet::from([definition
                        .physical_name()
                        .to_ascii_lowercase()]),
                    definition,
                    installed: false,
                    ready: false,
                    rebuild: false,
                    started: false,
                    last: None,
                    keys: BTreeSet::new(),
                    error: None,
                })
                .collect(),
            budget: o.search_queue_bytes,
            batch: o.search_batch_rows,
            next: 0,
            publications: BTreeSet::new(),
            guard: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
    pub fn register(c: &Connection, definitions: &[SearchIndex]) -> Result<()> {
        if !definitions
            .iter()
            .any(|i| matches!(i, SearchIndex::Vector(_)))
        {
            return Ok(());
        }
        ensure_vec_module(c)
    }
    pub fn validate_schema(&self, s: &Schema, allow_missing: bool) -> Result<()> {
        for index in &self.indexes {
            let d = &index.definition;
            if s.tables
                .iter()
                .any(|t| index.owned.contains(&t.name.to_ascii_lowercase()))
            {
                return Err(Error::Invalid(format!(
                    "source schema collides with search index {}",
                    d.name()
                )));
            }
            let Some(t) = s.tables.iter().find(|t| t.name == d.source_table()) else {
                if allow_missing && !index.installed {
                    continue;
                }
                return Err(Error::Invalid(format!(
                    "missing source table {}",
                    d.source_table()
                )));
            };
            let pk = crate::primary_key::application_columns(t);
            if pk.len() != 1 || !pk[0].declared_type.eq_ignore_ascii_case("INTEGER") {
                return Err(Error::Invalid(format!(
                    "{} requires one INTEGER PRIMARY KEY for search indexing",
                    t.name
                )));
            }
            for name in d.columns() {
                let col = t.columns.iter().find(|c| c.name == name).ok_or_else(|| {
                    Error::Invalid(format!("missing search column {}.{name}", t.name))
                })?;
                let expected = if matches!(d, SearchIndex::Vector(_)) {
                    "BLOB"
                } else {
                    "TEXT"
                };
                if !col.declared_type.eq_ignore_ascii_case(expected) {
                    return Err(Error::Invalid(format!(
                        "{}.{} must be declared {expected}",
                        t.name, col.name
                    )));
                }
            }
        }
        Ok(())
    }
    pub fn sync_schema(
        &mut self,
        c: &Connection,
        s: &Schema,
        allow_missing: bool,
    ) -> Result<()> {
        // Schema-derived definitions are disposable; only the column
        // declarations are authoritative. Reconcile before validating so
        // local drops and renames cannot leave stale derived definitions
        // behind.
        let definitions = schema_definitions(&self.explicit, s)?;
        if definitions != self.indexes.iter().map(|i| i.definition.clone()).collect::<Vec<_>>() {
            let options = SearchOptions { search_indexes: definitions, ..SearchOptions::default() };
            validate_options(&options)?;
            let candidate = Manager::new(&options);
            candidate.validate_schema(s, allow_missing)?;
            Self::register(c, &options.search_indexes)?;
            self.drop_indexes(c)?;
            self.indexes = candidate.indexes;
            self.next = 0;
        }
        self.validate_schema(s, allow_missing)?;
        for index in &mut self.indexes {
            if !s
                .tables
                .iter()
                .any(|t| t.name == index.definition.source_table())
            {
                continue;
            }
            if !index.installed {
                index.owned = Self::install(c, &index.definition)?;
                index.installed = true;
            }
            index.invalidate();
        }
        // Modules may create shadow names that collide with MAIN source tables.
        // Validate actual ownership after creation, before publishing the schema.
        self.validate_schema(s, allow_missing)?;
        self.publish_guard();
        Ok(())
    }
    /// Create-or-adopt one physical shadow table. Shadow definitions and
    /// contents persist in main schema across opens: an existing table
    /// whose stored DDL matches is adopted as-is (contents re-verified
    /// by the caller's rebuild); a same-named table with different DDL
    /// is dropped and recreated; a same-named NON-virtual table is a
    /// user collision and fails without touching it.
    fn install(c: &Connection, definition: &SearchIndex) -> Result<BTreeSet<String>> {
        let physical = definition.physical_name();
        let want = definition.create_sql();
        let have: Option<String> = c
            .query_row(
                "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?1",
                [&physical],
                |r| r.get(0),
            )
            .optional()?;
        match have {
            Some(sql) if sql == want => Ok(discover_owned(c, definition)?),
            Some(sql) if !is_virtual_table_sql(&sql) => Err(Error::Invalid(format!(
                "source schema collides with search index {}",
                definition.name()
            ))),
            Some(_) => {
                c.execute_batch(&format!("DROP TABLE {}", quote(&physical)))?;
                let before = main_tables(c)?;
                c.execute_batch(&want)?;
                let after = main_tables(c)?;
                Ok(after.difference(&before).cloned().collect())
            }
            None => {
                let before = main_tables(c)?;
                c.execute_batch(&want)?;
                let after = main_tables(c)?;
                Ok(after.difference(&before).cloned().collect())
            }
        }
    }

    pub fn drop_indexes(&self, c: &Connection) -> Result<()> {
        for index in &self.indexes {
            if index.installed {
                c.execute_batch(&format!(
                    "DROP TABLE {}",
                    quote(&index.definition.physical_name())
                ))?;
            }
        }
        Ok(())
    }
    pub fn publish_guard(&self) {
        *self.guard.write().unwrap() = self
            .indexes
            .iter()
            .flat_map(|i| i.owned.iter().map(move |name| (name.clone(), i.ready)))
            .collect();
    }
    pub fn status(&self) -> Vec<SearchIndexStatus> {
        self.indexes
            .iter()
            .map(|i| SearchIndexStatus {
                name: i.definition.physical_name(),
                ready: i.ready,
                rebuilding: i.rebuild,
                pending_keys: i.keys.len(),
                // No byte queue in the CRR-driven feed; keys are bounded by
                // `memory()` against the same budget instead.
                queued_bytes: 0,
                error: i.error.clone(),
            })
            .collect()
    }
    /// Live-apply maintenance limits from current PRAGMA values.
    pub fn set_limits(&mut self, queue_bytes: usize, batch_rows: usize) {
        self.budget = queue_bytes;
        self.batch = batch_rows;
    }

    /// Replace explicit index definitions atomically and rebuild them.
    /// Mirrors archive `Database::set_search_indexes` (drop, re-register,
    /// sync, synchronous refresh) minus the spool/checkpoint wrapper, which
    /// the embedding worker owns. Call in autocommit; re-fetch [`Manager::guard`]
    /// afterwards (the guard map is rebuilt with the new indexes).
    /// Maintenance limits of this manager applied to `definitions`.
    fn options_for(&self, definitions: Vec<SearchIndex>) -> SearchOptions {
        let mut options = SearchOptions {
            search_indexes: definitions,
            ..SearchOptions::default()
        };
        options.search_queue_bytes = self.budget;
        options.search_batch_rows = self.batch;
        options
    }

    /// Validate `definitions` without rebuilding. Action entry points call
    /// this BEFORE persisting a new declaration so a rejected definition
    /// never lands in `murmur_search_columns` (a persisted-but-invalid row
    /// would poison every later rebuild, including drops of other indexes).
    pub fn validate_definitions(&self, definitions: &[SearchIndex]) -> Result<()> {
        validate_options(&self.options_for(definitions.to_vec()))
    }

    pub fn set_explicit(
        &mut self,
        c: &Connection,
        s: &Schema,
        definitions: Vec<SearchIndex>,
    ) -> Result<()> {
        let options = self.options_for(definitions);
        validate_options(&options)?;
        let mut search = Manager::new(&options);
        Self::register(c, &options.search_indexes)?;
        c.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            self.drop_indexes(c)?;
            search.sync_schema(c, s, false)?;
            search.refresh(c, s)?;
            c.execute_batch("COMMIT")?;
            Ok(())
        })();
        if let Err(error) = result {
            let _ = c.execute_batch("ROLLBACK");
            return Err(error);
        }
        *self = search;
        Ok(())
    }

    pub fn take_publications(&mut self) -> Vec<String> {
        std::mem::take(&mut self.publications).into_iter().collect()
    }
    fn memory(&self) -> usize {
        self.indexes
            .iter()
            .map(|i| i.keys.len().saturating_mul(32))
            .sum::<usize>()
    }
    fn overflow(&mut self) {
        for index in &mut self.indexes {
            if index.installed && !index.keys.is_empty() {
                index.invalidate();
            }
        }
        self.publish_guard();
    }
    /// Note changed INTEGER PRIMARY KEYs observed in CRR changes for one
    /// source table. sqlite-murmur replacement for the archive's
    /// session-changeset `enqueue`/`decode_pending` feed: the embedding
    /// worker reads changed keys from `crsql_changes` and calls this; keys
    /// are bounded by the same byte budget, with overflow triggering a
    /// rebuild exactly as in the archive.
    pub fn observe(&mut self, table: &str, keys: &[i64]) {
        for key in keys {
            self.key(table, *key);
        }
    }
    pub fn rows(&mut self, s: &Schema, rows: &[RowState]) {
        for row in rows {
            if let Some(t) = s.tables.iter().find(|t| t.id == row.table) {
                if t.application_key.is_some() {
                    for index in &mut self.indexes { if index.definition.source_table() == t.name { index.invalidate(); } }
                } else if let Some(Value::Integer(key)) = row.key.first() { self.key(&t.name, *key); }
            }
        }
    }
    fn key(&mut self, table: &str, key: i64) {
        for index in &mut self.indexes {
            if index.definition.source_table() == table && index.installed {
                index.keys.insert(key);
            }
        }
        if self.memory() > self.budget {
            self.overflow();
        }
    }
    pub fn pending(&self) -> bool {
        self.indexes.iter().any(|i| i.rebuild || !i.keys.is_empty())
    }
    /// A maintenance turn never executes on the application transaction path.
    pub fn tick(&mut self, c: &Connection, s: &Schema) -> Result<Vec<String>> {
        let mut published = Vec::new();
        let mut remaining = self.batch;
        let mut first_error = None;
        let start = self.next;
        let length = self.indexes.len();
        for offset in 0..length {
            if remaining == 0 {
                break;
            }
            let position = (start + offset) % length;
            let index = &mut self.indexes[position];
            if !index.installed || (!index.rebuild && index.keys.is_empty()) {
                continue;
            }
            self.next = (position + 1) % length;
            let saved = (index.rebuild, index.started, index.last);
            let mut completed_keys = Vec::new();
            let result = (|| -> Result<usize> {
                c.execute_batch("SAVEPOINT murmur_search_work")?;
                let t = s
                    .tables
                    .iter()
                    .find(|t| t.name == index.definition.source_table())
                    .ok_or(Error::SchemaMismatch)?;
                let pk = crate::primary_key::application_columns(t)[0].name.as_str();
                let mut count = 0;
                if index.rebuild {
                    if !index.started {
                        c.execute(
                            &format!("DELETE FROM {}", quote(&index.definition.physical_name())),
                            [],
                        )?;
                        index.started = true;
                    }
                    let columns = index
                        .definition
                        .columns()
                        .iter()
                        .map(|c| quote(c))
                        .collect::<Vec<_>>()
                        .join(",");
                    let sql = format!(
                        "SELECT {},{} FROM main.{} {} ORDER BY {} LIMIT ?",
                        quote(pk),
                        columns,
                        quote(&t.name),
                        if index.last.is_some() {
                            format!("WHERE {} > ?", quote(pk))
                        } else {
                            String::new()
                        },
                        quote(pk)
                    );
                    let mut q = c.prepare(&sql)?;
                    let mut cursor = if let Some(last) = index.last {
                        q.query(rusqlite::params![last, remaining as i64])?
                    } else {
                        q.query([remaining as i64])?
                    };
                    while let Some(row) = cursor.next()? {
                        let key = row.get(0)?;
                        let values = (1..=index.definition.columns().len())
                            .map(|i| Value::read(row.get_ref(i)?))
                            .collect::<rusqlite::Result<Vec<_>>>()?;
                        insert(c, &index.definition, key, &values)?;
                        index.last = Some(key);
                        count += 1;
                    }
                    if count < remaining {
                        index.rebuild = false;
                    }
                } else {
                    let keys = index
                        .keys
                        .iter()
                        .take(remaining)
                        .copied()
                        .collect::<Vec<_>>();
                    for key in &keys {
                        c.execute(
                            &format!(
                                "DELETE FROM {} WHERE rowid=?",
                                quote(&index.definition.physical_name())
                            ),
                            [key],
                        )?;
                        let columns = index
                            .definition
                            .columns()
                            .iter()
                            .map(|c| quote(c))
                            .collect::<Vec<_>>()
                            .join(",");
                        let mut q = c.prepare(&format!(
                            "SELECT {columns} FROM main.{} WHERE {}=?",
                            quote(&t.name),
                            quote(pk)
                        ))?;
                        let mut cursor = q.query([key])?;
                        if let Some(row) = cursor.next()? {
                            let values = (0..index.definition.columns().len())
                                .map(|i| Value::read(row.get_ref(i)?))
                                .collect::<rusqlite::Result<Vec<_>>>()?;
                            insert(c, &index.definition, *key, &values)?;
                        }
                    }
                    count = keys.len();
                    completed_keys = keys;
                }
                c.execute_batch("RELEASE murmur_search_work")?;
                Ok(count)
            })();
            match result {
                Ok(count) => {
                    for key in completed_keys {
                        index.keys.remove(&key);
                    }
                    remaining = remaining.saturating_sub(count.max(1));
                    index.error = None;
                    if !index.rebuild && index.keys.is_empty() {
                        index.ready = true;
                    }
                    if index.ready {
                        published.push(index.definition.name().to_owned());
                    }
                }
                Err(error) => {
                    let _ = c.execute_batch(
                        "ROLLBACK TO murmur_search_work; RELEASE murmur_search_work",
                    );
                    (index.rebuild, index.started, index.last) = saved;
                    index.ready = false;
                    index.error = Some(error.to_string());
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        self.publish_guard();
        self.publications.extend(published.iter().cloned());
        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(published)
        }
    }
    pub fn refresh(&mut self, c: &Connection, s: &Schema) -> Result<Vec<String>> {
        let mut published = BTreeSet::new();
        while self.pending() {
            published.extend(self.tick(c, s)?);
        }
        Ok(published.into_iter().collect())
    }
}
/// Register the vec0 module on ONE connection. Shadows live in main
/// schema and are read from every app connection, so vec0 must exist on
/// each of them; the embedding crate calls this per connection (app
/// `register()` + worker open). No-op without the `vector` feature;
/// vector *indexes* without the feature stay rejected by
/// [`validate_options`].
pub fn ensure_vec_module(c: &Connection) -> Result<()> {
    #[cfg(feature = "vector")]
    {
        // sqlite-vec is compiled with SQLITE_CORE. Call its actual C ABI on this
        // connection, avoiding process-wide auto-extension registration.
        type Init = unsafe extern "C" fn(
            *mut rusqlite::ffi::sqlite3,
            *mut *mut std::ffi::c_char,
            *const rusqlite::ffi::sqlite3_api_routines,
        ) -> std::ffi::c_int;
        let init: Init =
            unsafe { std::mem::transmute(sqlite_vec::sqlite3_vec_init as *const ()) };
        let mut message = std::ptr::null_mut();
        let rc = unsafe { init(c.handle(), &mut message, std::ptr::null()) };
        let error = if message.is_null() {
            None
        } else {
            Some(
                unsafe { std::ffi::CStr::from_ptr(message) }
                    .to_string_lossy()
                    .into_owned(),
            )
        };
        unsafe {
            rusqlite::ffi::sqlite3_free(message.cast());
        }
        if rc != rusqlite::ffi::SQLITE_OK {
            return Err(Error::Invalid(
                error.unwrap_or_else(|| "sqlite-vec initialization failed".into()),
            ));
        }
        Ok(())
    }
    #[cfg(not(feature = "vector"))]
    {
        let _ = c;
        Ok(())
    }
}

fn main_tables(c: &Connection) -> Result<BTreeSet<String>> {
    let mut q = c.prepare("SELECT name FROM sqlite_schema WHERE type IN ('table','index')")?;
    Ok(q.query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .map(|s| s.to_ascii_lowercase())
        .collect())
}

/// Tables owned by one index definition: the physical table plus its
/// module shadows, intersected with what actually exists. FTS5 shadows
/// are a fixed family; sqlite-vec adds numbered chunk families
/// (`_vector_chunksNN`, `_metadatachunksNN`, `_metadatatextNN`) matched
/// exactly so unrelated `murmur_*` tables are never swept into a drop.
fn discover_owned(c: &Connection, definition: &SearchIndex) -> Result<BTreeSet<String>> {
    let tables = main_tables(c)?;
    let physical = definition.physical_name().to_ascii_lowercase();
    let mut owned = BTreeSet::new();
    owned.insert(physical.clone());
    let suffixes: &[&str] = match definition {
        SearchIndex::Fts5(_) => &["_data", "_idx", "_content", "_docsize", "_config"],
        SearchIndex::Vector(_) => &["_info", "_chunks", "_rowids", "_auxiliary"],
    };
    for suffix in suffixes {
        let name = format!("{physical}{suffix}");
        if tables.contains(&name) {
            owned.insert(name);
        }
    }
    if matches!(definition, SearchIndex::Vector(_)) {
        for name in &tables {
            if let Some(rest) = name.strip_prefix(physical.as_str()) {
                for family in ["_vector_chunks", "_metadatachunks", "_metadatatext"] {
                    if let Some(digits) = rest.strip_prefix(family) {
                        if digits.len() == 2 && digits.bytes().all(|b| b.is_ascii_digit()) {
                            owned.insert(name.clone());
                        }
                    }
                }
            }
        }
    }
    Ok(owned)
}

fn is_virtual_table_sql(sql: &str) -> bool {
    sql.trim_start().to_ascii_uppercase().starts_with("CREATE VIRTUAL TABLE")
}
fn insert(c: &Connection, d: &SearchIndex, key: i64, values: &[Value]) -> Result<()> {
    if let SearchIndex::Vector(v) = d {
        if matches!(values[0], Value::Null) {
            return Ok(());
        }
        let Value::Blob(bytes) = &values[0] else {
            return Err(Error::Invalid("embedding must be a float32 BLOB".into()));
        };
        if bytes.len() != v.dimensions * 4
            || bytes
                .chunks_exact(4)
                .any(|chunk| !f32::from_le_bytes(chunk.try_into().unwrap()).is_finite())
        {
            return Err(Error::Invalid(format!(
                "invalid embedding for {} row {key}",
                v.name
            )));
        }
    }
    let mut params = vec![Value::Integer(key)];
    params.extend_from_slice(values);
    c.execute(
        &format!(
            "INSERT INTO {}(rowid,{}) VALUES({})",
            quote(&d.physical_name()),
            d.columns()
                .iter()
                .map(|c| quote(c))
                .collect::<Vec<_>>()
                .join(","),
            vec!["?"; params.len()].join(",")
        ),
        rusqlite::params_from_iter(&params),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn physical_fts() -> SearchIndex {
        Fts5Index::new("docs_fts", "docs", vec!["title".into()]).into()
    }

    #[test]
    fn physical_names_prefix_and_idempotence() {
        let explicit: SearchIndex =
            Fts5Index::new("docs_fts", "docs", vec!["title".into()]).into();
        assert_eq!(explicit.physical_name(), "murmur_fts_docs_fts");
        let derived: SearchIndex = Fts5Index::new("murmur_fts_abc", "docs", vec!["t".into()]).into();
        assert_eq!(derived.physical_name(), "murmur_fts_abc");
        let vec_explicit: SearchIndex =
            VectorIndex::new("docs_vec", "docs", "embedding", 2).into();
        assert_eq!(vec_explicit.physical_name(), "murmur_vec_docs_vec");
        // Declared names never leak into SQL: DDL targets main schema.
        assert!(explicit.create_sql().starts_with("CREATE VIRTUAL TABLE \"murmur_fts_docs_fts\""));
        assert!(!explicit.create_sql().contains("temp."));
    }

    #[test]
    fn reopen_adopts_persistent_shadows() -> Result<()> {
        // First open installs and populates; a fresh Manager over the
        // same database adopts the persistent tables without recreating.
        let c = Connection::open_in_memory()?;
        c.execute_batch("CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT)")?;
        c.execute("INSERT INTO docs VALUES(1,'hello world')", [])?;
        let schema = crate::schema::inspect(
            &c,
            &Schema::default(),
            &crate::Migration::new("CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT)"),
            &|_| false,
        )?;
        let definitions = vec![physical_fts()];
        Manager::register(&c, &definitions)?;
        let mut first = Manager::new(&SearchOptions {
            search_indexes: definitions.clone(),
            ..SearchOptions::default()
        });
        first.sync_schema(&c, &schema, false)?;
        first.observe("docs", &[1]);
        first.refresh(&c, &schema)?;
        let before: String = c.query_row(
            "SELECT sql FROM sqlite_schema WHERE name='murmur_fts_docs_fts'",
            [],
            |r| r.get(0),
        )?;
        drop(first);
        // Second open: same definitions, same DDL -> adopt, contents kept.
        let mut second = Manager::new(&SearchOptions {
            search_indexes: definitions,
            ..SearchOptions::default()
        });
        second.sync_schema(&c, &schema, false)?;
        let after: String = c.query_row(
            "SELECT sql FROM sqlite_schema WHERE name='murmur_fts_docs_fts'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(before, after);
        let count: i64 = c.query_row(
            "SELECT count(*) FROM murmur_fts_docs_fts WHERE murmur_fts_docs_fts MATCH 'hello'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(count, 1);
        assert!(second.status().iter().all(|s| s.name == "murmur_fts_docs_fts"));
        Ok(())
    }

    #[test]
    fn changed_ddl_recreates_and_user_tables_survive() -> Result<()> {
        let c = Connection::open_in_memory()?;
        c.execute_batch(
            "CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT, body TEXT)",
        )?;
        let schema = crate::schema::inspect(
            &c,
            &Schema::default(),
            &crate::Migration::new(
                "CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT, body TEXT)",
            ),
            &|_| false,
        )?;
        // Install over [title], then re-sync with [title, body]: same
        // declared name, different DDL -> drop + recreate.
        let one: Vec<SearchIndex> =
            vec![Fts5Index::new("docs_fts", "docs", vec!["title".into()]).into()];
        let mut manager = Manager::new(&SearchOptions {
            search_indexes: one,
            ..SearchOptions::default()
        });
        manager.sync_schema(&c, &schema, false)?;
        let two: Vec<SearchIndex> = vec![Fts5Index::new(
            "docs_fts",
            "docs",
            vec!["title".into(), "body".into()],
        )
        .into()];
        let mut changed = Manager::new(&SearchOptions {
            search_indexes: two,
            ..SearchOptions::default()
        });
        changed.sync_schema(&c, &schema, false)?;
        let sql: String = c.query_row(
            "SELECT sql FROM sqlite_schema WHERE name='murmur_fts_docs_fts'",
            [],
            |r| r.get(0),
        )?;
        assert!(sql.contains("\"body\""), "{sql}");
        // A same-named ORDINARY table is a user collision: sync fails and
        // the table is left untouched.
        c.execute_batch("CREATE TABLE murmur_fts_nope(id INTEGER PRIMARY KEY)")?;
        let clash: Vec<SearchIndex> =
            vec![Fts5Index::new("nope", "docs", vec!["title".into()]).into()];
        let mut collision = Manager::new(&SearchOptions {
            search_indexes: clash,
            ..SearchOptions::default()
        });
        let err = collision.sync_schema(&c, &schema, false).unwrap_err().to_string();
        assert!(err.contains("collides"), "{err}");
        let kept: i64 = c.query_row(
            "SELECT count(*) FROM sqlite_schema WHERE name='murmur_fts_nope'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(kept, 1);
        Ok(())
    }

    #[test]
    fn observe_tick_refresh_lifecycle() -> Result<()> {
        // End-to-end over an in-memory database: install one FTS index,
        // feed changed keys via observe(), and query after refresh.
        let c = Connection::open_in_memory()?;
        c.execute_batch("CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT)")?;
        let schema = crate::schema::inspect(
            &c,
            &Schema::default(),
            &crate::Migration::new("CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT)"),
            &|_| false,
        )?;
        assert_eq!(schema.tables.len(), 1);
        let definitions: Vec<SearchIndex> =
            vec![Fts5Index::new("docs_fts", "docs", vec!["title".into()]).into()];
        Manager::register(&c, &definitions)?;
        let mut manager = Manager::new(&SearchOptions {
            search_indexes: definitions,
            ..SearchOptions::default()
        });
        manager.sync_schema(&c, &schema, false)?;
        c.execute("INSERT INTO docs VALUES(1,'hello world')", [])?;
        manager.observe("docs", &[1]);
        assert!(manager.pending());
        manager.refresh(&c, &schema)?;
        assert!(!manager.pending());
        let count: i64 = c.query_row(
            "SELECT count(*) FROM murmur_fts_docs_fts WHERE murmur_fts_docs_fts MATCH 'hello'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(count, 1);
        assert!(manager.status().iter().all(|s| s.ready));
        // A second row flows through tick() incrementally.
        c.execute("INSERT INTO docs VALUES(2,'hello again')", [])?;
        manager.observe("docs", &[2]);
        while manager.pending() {
            manager.tick(&c, &schema)?;
        }
        let count: i64 = c.query_row(
            "SELECT count(*) FROM murmur_fts_docs_fts WHERE murmur_fts_docs_fts MATCH 'hello'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(count, 2);
        Ok(())
    }

    #[cfg(feature = "vector")]
    #[test]
    fn vector_knn_round_trip() -> Result<()> {
        // Mirrors archive tests/column_vectors.rs query shape:
        // BLOB float32 embeddings indexed and ranked by distance.
        let c = Connection::open_in_memory()?;
        c.execute_batch("CREATE TABLE docs(id INTEGER PRIMARY KEY, embedding BLOB)")?;
        let schema = crate::schema::inspect(
            &c,
            &Schema::default(),
            &crate::Migration::new("CREATE TABLE docs(id INTEGER PRIMARY KEY, embedding BLOB)"),
            &|_| false,
        )?;
        let mut index = VectorIndex::new("docs_vec", "docs", "embedding", 2);
        index.distance = DistanceMetric::L2;
        let definitions: Vec<SearchIndex> = vec![index.into()];
        Manager::register(&c, &definitions)?;
        let mut manager = Manager::new(&SearchOptions {
            search_indexes: definitions,
            ..SearchOptions::default()
        });
        manager.sync_schema(&c, &schema, false)?;
        let blob = |x: f32, y: f32| {
            let mut bytes = x.to_le_bytes().to_vec();
            bytes.extend_from_slice(&y.to_le_bytes());
            bytes
        };
        c.execute("INSERT INTO docs VALUES(1, ?1)", [&blob(1.0, 0.0)])?;
        c.execute("INSERT INTO docs VALUES(2, ?1)", [&blob(0.0, 1.0)])?;
        manager.observe("docs", &[1, 2]);
        manager.refresh(&c, &schema)?;
        let nearest: i64 = c.query_row(
            "SELECT rowid FROM murmur_vec_docs_vec WHERE embedding MATCH '[1,0]' AND k=1",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(nearest, 1);
        Ok(())
    }

    #[cfg(feature = "vector")]
    #[test]
    fn vector_module_registration_is_connection_local() -> Result<()> {
        let c = Connection::open_in_memory()?;
        Manager::register(
            &c,
            &[VectorIndex::new("vectors", "docs", "embedding", 2).into()],
        )?;
        assert!(
            c.query_row("SELECT vec_version()", [], |r| r.get::<_, String>(0))?
                .starts_with('v')
        );
        let other = Connection::open_in_memory()?;
        assert!(
            other
                .query_row("SELECT vec_version()", [], |r| r.get::<_, String>(0))
                .is_err()
        );
        Ok(())
    }
}
