use murmur_policy::{
    Error,
    carrier::{self, Payload, Row, Target, encode_payload, reduce_causal, reduce_extremum},
    causal::State,
    model::{Value, decode, encode},
    policies::{Causal, Counter, Policy, Set, Tag},
};

const BUDGET: usize = 1 << 20;

fn target() -> Target {
    Target {
        table: "items".into(),
        column: "value".into(),
        pk: vec![0xde, 0xad],
        incarnation: 3,
        live: true,
    }
}

fn baseline(causal: Causal, value: Value) -> State<u64> {
    State {
        version: 1,
        domain: [1; 16],
        branch: 0,
        scope: 3,
        baseline: value,
        basis: 1,
        table: "items".into(),
        column: "value".into(),
        key: vec![Value::Integer(7)],
        generation: 3,
        causal,
    }
}

fn counter_base() -> State<u64> {
    baseline(Causal::Counter(Counter::default()), Value::Integer(10))
}

fn set_base() -> State<u64> {
    baseline(
        Causal::Set(Set::default()),
        Value::Blob(encode(&Vec::<Value>::new()).unwrap()),
    )
}

fn row(actor: u8, payload: Payload) -> Row {
    let tag = match &payload {
        Payload::SetAdd { tag, .. } | Payload::SetRemove { tag } => encode(tag).unwrap(),
        _ => vec![],
    };
    Row {
        table: "items".into(),
        column: "value".into(),
        pk: target().pk,
        incarnation: 3,
        actor_database: vec![1; 16],
        actor_node: vec![actor; 16],
        kind: payload.kind(),
        tag,
        payload: encode_payload(&payload).unwrap(),
    }
}

fn tag(actor: u8, operation: u32) -> Tag {
    Tag {
        database: [1; 16],
        node: [actor; 16],
        transaction: [7; 16],
        operation,
    }
}

fn counter(actor: u8, positive: u64, negative: u64) -> Row {
    row(actor, Payload::Counter { positive, negative })
}

#[test]
fn actor_components_max_join_across_order_and_replay() {
    let base = counter_base();
    let rows = vec![counter(2, 4, 1), counter(2, 6, 1), counter(3, 2, 0)];
    let forward = reduce_causal(&target(), Policy::Counter, &base, &rows, BUDGET).unwrap();
    let reverse = reduce_causal(
        &target(),
        Policy::Counter,
        &base,
        &rows.iter().rev().cloned().collect::<Vec<_>>(),
        BUDGET,
    )
    .unwrap();
    assert_eq!(forward.projection, Some(Value::Integer(17)));
    assert_eq!(forward.state.causal, reverse.state.causal);
    let replay = reduce_causal(&target(), Policy::Counter, &forward.state, &rows, BUDGET).unwrap();
    assert_eq!(forward.state.causal, replay.state.causal);
    assert_eq!(replay.projection, forward.projection);
    assert_eq!(base.causal, Causal::Counter(Counter::default()));
}

#[test]
fn remove_before_add_and_concurrent_add_preserve_observed_remove_semantics() {
    let item = Value::Text("same value".into());
    let rows = vec![
        row(9, Payload::SetRemove { tag: tag(2, 0) }),
        row(
            2,
            Payload::SetAdd {
                tag: tag(2, 0),
                value: item.clone(),
            },
        ),
        row(
            3,
            Payload::SetAdd {
                tag: tag(3, 0),
                value: item.clone(),
            },
        ),
    ];
    let reduced = reduce_causal(&target(), Policy::Set, &set_base(), &rows, BUDGET).unwrap();
    let Some(Value::Blob(bytes)) = &reduced.projection else {
        panic!("set projection type")
    };
    assert_eq!(decode::<Vec<Value>>(bytes).unwrap(), vec![item]);
    let replay = reduce_causal(&target(), Policy::Set, &reduced.state, &rows, BUDGET).unwrap();
    assert_eq!(replay.state.causal, reduced.state.causal);
}

#[test]
fn immutable_add_tag_cannot_be_reused_for_a_different_value() {
    let a = row(
        2,
        Payload::SetAdd {
            tag: tag(2, 0),
            value: Value::Integer(1),
        },
    );
    let b = row(
        2,
        Payload::SetAdd {
            tag: tag(2, 0),
            value: Value::Integer(2),
        },
    );
    let initial = reduce_causal(
        &target(),
        Policy::Set,
        &set_base(),
        std::slice::from_ref(&a),
        BUDGET,
    )
    .unwrap();
    assert!(matches!(
        reduce_causal(&target(), Policy::Set, &set_base(), &[a, b.clone()], BUDGET),
        Err(Error::Corrupt(_))
    ));
    assert!(matches!(
        reduce_causal(&target(), Policy::Set, &initial.state, &[b], BUDGET),
        Err(Error::Corrupt(_))
    ));
}

#[test]
fn extrema_include_all_candidates_independently_of_arrival_order() {
    let rows = vec![
        row(2, Payload::Min(Value::Integer(-2))),
        row(2, Payload::Min(Value::Integer(4))),
    ];
    let forward =
        reduce_extremum(&target(), Policy::Min, &Value::Integer(10), &rows, BUDGET).unwrap();
    let reverse = reduce_extremum(
        &target(),
        Policy::Min,
        &Value::Integer(10),
        &rows.into_iter().rev().collect::<Vec<_>>(),
        BUDGET,
    )
    .unwrap();
    assert_eq!(forward.projection, Some(Value::Integer(-2)));
    assert_eq!(reverse.projection, forward.projection);
    let rows = [
        row(2, Payload::Max(Value::Integer(-20))),
        row(3, Payload::Max(Value::Integer(5))),
    ];
    assert_eq!(
        reduce_extremum(&target(), Policy::Max, &Value::Integer(-10), &rows, BUDGET)
            .unwrap()
            .projection,
        Some(Value::Integer(5))
    );
}

#[test]
fn native_lifecycle_and_partial_payloads_are_deferred_without_parity_rules() {
    let mut older = counter(2, 100, 0);
    older.incarnation = 1;
    let mut newer = counter(2, 200, 0);
    newer.incarnation = 5;
    let mut partial = counter(2, 300, 0);
    partial.payload.clear();
    let rows = vec![older, newer, partial, counter(2, 4, 0)];
    let reduction =
        reduce_causal(&target(), Policy::Counter, &counter_base(), &rows, BUDGET).unwrap();
    assert_eq!(reduction.projection, Some(Value::Integer(14)));
    assert_eq!(reduction.deferred_rows, 3);
    let mut inactive = target();
    inactive.live = false;
    let dead = reduce_causal(&inactive, Policy::Counter, &counter_base(), &rows, BUDGET).unwrap();
    assert_eq!(dead.projection, None);
    assert_eq!(dead.deferred_rows, 4);
    assert_eq!(dead.state.causal, counter_base().causal);
    // This adapter trusts explicit host lifecycle, not any odd/even inference.
    let mut native = target();
    native.incarnation = 2;
    let mut base = counter_base();
    base.generation = 2;
    let mut current = counter(2, 4, 0);
    current.incarnation = 2;
    assert_eq!(
        reduce_causal(&native, Policy::Counter, &base, &[current], BUDGET)
            .unwrap()
            .projection,
        Some(Value::Integer(14))
    );
}

#[test]
fn invalid_identity_kind_tag_and_encoding_are_rejected() {
    let valid = counter(2, 4, 0);
    let mut cases = vec![];
    let mut bad = valid.clone();
    bad.actor_node.pop();
    cases.push(bad);
    let mut bad = valid.clone();
    bad.actor_database.fill(0);
    cases.push(bad);
    let mut bad = valid.clone();
    bad.pk.push(0);
    cases.push(bad);
    let mut bad = valid.clone();
    bad.table = "other".into();
    cases.push(bad);
    let mut bad = valid.clone();
    bad.column = "other".into();
    cases.push(bad);
    let mut bad = valid.clone();
    bad.incarnation = -1;
    cases.push(bad);
    let mut bad = valid.clone();
    bad.tag.push(0);
    cases.push(bad);
    let mut bad = valid.clone();
    bad.kind = 99;
    cases.push(bad);
    let mut bad = valid.clone();
    bad.payload[0] = 2;
    cases.push(bad);
    let mut bad = valid.clone();
    bad.payload.pop();
    cases.push(bad);
    let mut bad = valid.clone();
    bad.payload.push(0);
    cases.push(bad);
    for bad in cases {
        assert!(
            reduce_causal(&target(), Policy::Counter, &counter_base(), &[bad], BUDGET).is_err()
        );
    }
    let mismatch = row(
        3,
        Payload::SetAdd {
            tag: tag(2, 0),
            value: Value::Integer(1),
        },
    );
    assert!(reduce_causal(&target(), Policy::Set, &set_base(), &[mismatch], BUDGET).is_err());
    let mut mismatch = row(2, Payload::Min(Value::Integer(1)));
    mismatch.payload = valid.payload;
    assert!(
        reduce_extremum(
            &target(),
            Policy::Min,
            &Value::Integer(0),
            &[mismatch],
            BUDGET
        )
        .is_err()
    );
}

#[test]
fn byte_budget_overflow_or_late_malformed_input_cannot_mutate_the_baseline() {
    let base = counter_base();
    let valid = counter(2, 4, 0);
    let size = valid.table.len()
        + valid.column.len()
        + valid.pk.len()
        + 32
        + 16
        + valid.payload.len()
        + valid.tag.len();
    assert!(matches!(
        reduce_causal(
            &target(),
            Policy::Counter,
            &base,
            std::slice::from_ref(&valid),
            size - 1
        ),
        Err(Error::Limit(_))
    ));
    assert!(
        reduce_causal(
            &target(),
            Policy::Counter,
            &base,
            std::slice::from_ref(&valid),
            size
        )
        .is_ok()
    );
    let mut malformed = valid.clone();
    malformed.payload = vec![255];
    assert!(
        reduce_causal(
            &target(),
            Policy::Counter,
            &base,
            &[valid, malformed],
            BUDGET
        )
        .is_err()
    );
    let large = counter(2, u64::MAX, 0);
    assert!(matches!(
        reduce_causal(&target(), Policy::Counter, &base, &[large], BUDGET),
        Err(Error::Limit(_))
    ));
    assert_eq!(base.causal, Causal::Counter(Counter::default()));
}

#[test]
fn policy_dispatch_baseline_identity_and_nan_are_checked() {
    assert!(reduce_causal(&target(), Policy::Lww, &counter_base(), &[], BUDGET).is_err());
    assert!(reduce_causal(&target(), Policy::Set, &counter_base(), &[], BUDGET).is_err());
    assert!(reduce_extremum(&target(), Policy::Counter, &Value::Null, &[], BUDGET).is_err());
    let mut base = counter_base();
    base.generation = 1;
    assert!(reduce_causal(&target(), Policy::Counter, &base, &[], BUDGET).is_err());
    let nan = row(2, Payload::Min(Value::Real(f64::NAN)));
    assert!(reduce_extremum(&target(), Policy::Min, &Value::Integer(1), &[nan], BUDGET).is_err());
    assert!(reduce_extremum(&target(), Policy::Max, &Value::Real(f64::NAN), &[], BUDGET).is_err());
}

#[test]
fn version_one_payload_encoding_is_pinned() {
    assert_eq!(carrier::PAYLOAD_VERSION, 1);
    assert_eq!(
        encode_payload(&Payload::Counter {
            positive: 2,
            negative: 1
        })
        .unwrap(),
        vec![1, 0, 2, 1]
    );
    assert_eq!(
        encode_payload(&Payload::Min(Value::Integer(-1))).unwrap(),
        vec![1, 3, 1, 1]
    );
}

#[test]
fn decode_payload_round_trips_and_rejects_bad_version() {
    for payload in [
        Payload::Counter {
            positive: 7,
            negative: 3,
        },
        Payload::Min(Value::Text("m".into())),
    ] {
        let bytes = encode_payload(&payload).unwrap();
        assert_eq!(carrier::decode_payload(&bytes).unwrap(), payload);
    }
    assert!(carrier::decode_payload(&[2, 0, 0, 0]).is_err());
    assert!(carrier::decode_payload(&[]).is_err());
}
