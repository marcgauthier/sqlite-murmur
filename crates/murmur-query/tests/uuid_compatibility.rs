use murmur_query::crdt_compat::{Value, uuid};
#[test]
fn managed_identifiers_match_original_provider() {
    let cases = [
        ("Records", "BINARY", Value::Integer(42)),
        ("records", "BINARY", Value::Real(42.0)),
        ("records", "NOCASE", Value::Text("MiXeD".into())),
        ("records", "RTRIM", Value::Text("tail   ".into())),
        ("records", "BINARY", Value::Text("é\0世界".into())),
        ("records", "BINARY", Value::Blob(vec![0, 255, 128])),
    ];
    let managed: Vec<String> = cases
        .into_iter()
        .map(|(table, coll, value)| uuid(table, &[coll.into()], &[value]).unwrap())
        .collect();
    assert_eq!(
        managed.join("\n") + "\n",
        include_str!("../../common/fixtures/managed-uuids.txt")
    );
}
