//! Local schema policy: table sync is opt-in, keyed by stable table ID.
//! Missing entries mean local-only. Settings are persisted atomically with
//! the schema in Spool by the host; these helpers only interpret them.
use crate::model::*;

pub const SETTINGS_KEY: &[u8] = b"00/schema/table-flags-v2";
pub const LEGACY_SETTINGS_KEY: &[u8] = b"00/schema/replication";

pub fn enabled(schema: &Schema, table: &str) -> bool {
    schema.table_replication.get(table).copied().unwrap_or(false)
}

pub fn filter(state: &mut ReplicaState, schema: &Schema) {
    state.rows.retain(|row| enabled(schema, &row.table));
    state.tombstones.retain(|tomb| enabled(schema, &tomb.table));
}

/// Copy of `schema` with locally-disabled tables (and their indexes) removed,
/// for replication payloads and schema sync. Local settings stay untouched.
pub fn filter_schema(schema: &Schema) -> Schema {
    let mut out = schema.clone();
    out.tables.retain(|t| enabled(schema, &t.id));
    let present: std::collections::BTreeSet<&str> =
        out.tables.iter().map(|t| t.name.as_str()).collect();
    out.indexes.retain(|sql| match crate::schema::index_table(sql) {
        Some(table) => present.iter().any(|t| t.eq_ignore_ascii_case(&table)),
        // Unrecognized shape: keep and let SQLite validate on adopt.
        None => true,
    });
    out
}

#[cfg(test)]
mod filter_schema_tests {
    use super::*;
    use std::collections::BTreeMap;

    fn table(id: &str, name: &str) -> Table {
        Table {
            application_key: None,
            id: id.into(),
            name: name.into(),
            sql: format!("CREATE TABLE {name}(id INTEGER PRIMARY KEY)"),
            columns: vec![Column { vector: None, fts: false,
                policy: MergePolicy::Lww,
                id: format!("{id}/id"),
                name: "id".into(),
                primary_key: 1,
                declared_type: "INTEGER".into(),
                not_null: true,
            }],
        }
    }

    #[test]
    fn drops_disabled_tables_and_their_indexes() {
        let schema = Schema {
            table_replication: BTreeMap::from([("t-local".to_string(), false), ("t-items".to_string(), true)]),
            tables: vec![table("t-items", "items"), table("t-local", "scratch")],
            indexes: vec![
                "CREATE INDEX items_id ON items(id)".to_string(),
                "CREATE INDEX scratch_id ON scratch(id)".to_string(),
                // Unrecognized shape is kept for SQLite to validate.
                "CREATE INDEX".to_string(),
            ],
            history: [9; 32],
        };
        let filtered = filter_schema(&schema);
        assert_eq!(filtered.tables.len(), 1);
        assert_eq!(filtered.tables[0].id, "t-items");
        assert_eq!(filtered.indexes.len(), 2);
        assert!(filtered.indexes[0].contains("items"));
        // Settings map itself is untouched.
        assert_eq!(
            filtered.table_replication.get("t-local"),
            Some(&false)
        );
        assert_eq!(filtered.history, [9; 32]);
    }

    #[test]
    fn filters_rows_and_tombstones_by_stable_id_with_explicit_opt_in() {
        let schema=Schema{tables:vec![table("off","local"),table("on","shared")],table_replication:BTreeMap::from([("off".into(),false),("on".into(),true)]),..Schema::default()};
        let mut state=ReplicaState{database_id:[1;16],schema:schema.clone(),rows:vec![],tombstones:vec![],clock:Version::default()};
        for id in ["off","on"] {state.rows.push(RowState{table:id.into(),key:vec![Value::Integer(1)],generation:Version::default(),cells:vec![]});state.tombstones.push(Tombstone{table:id.into(),key:vec![1],generation:Version::default(),version:Version::default()});}
        filter(&mut state,&schema); assert_eq!(state.rows.len(),1); assert_eq!(state.rows[0].table,"on"); assert_eq!(state.tombstones.len(),1); assert_eq!(state.tombstones[0].table,"on");
    }
}
