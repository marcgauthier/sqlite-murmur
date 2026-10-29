use murmur_policy::{
    Error,
    causal::{State, join_state, projection, rebase_orphan},
    compare::compare,
    model::{Value, decode, encode},
    policies::{Causal, Counter, Set, Tag},
};
use std::cmp::Ordering;

// Numeric fixture tokens stand for native events already ordered by a host;
// production code must supply accepted CRR metadata, not allocate a new clock.
fn state(causal: Causal, baseline: Value) -> State<u64> {
    State {
        version: 1,
        domain: [1; 16],
        branch: 0,
        scope: 1,
        baseline,
        basis: 1,
        table: "items".into(),
        column: "value".into(),
        key: vec![Value::Integer(1)],
        generation: 1,
        causal,
    }
}

fn tag(node: u8, operation: u32) -> Tag {
    Tag {
        database: [1; 16],
        node: [node; 16],
        transaction: [7; 16],
        operation,
    }
}

fn set_state(set: Set) -> State<u64> {
    state(
        Causal::Set(set),
        Value::Blob(encode(&Vec::<Value>::new()).unwrap()),
    )
}

fn values(state: &State<u64>) -> Vec<Value> {
    let Value::Blob(bytes) = projection(state).unwrap() else {
        panic!("set projection type")
    };
    decode(&bytes).unwrap()
}

#[test]
fn actor_counter_join_converges_and_replay_is_idempotent() {
    // The type comes directly from murmur-tx: there is only one causal kernel.
    let mut a = murmur_tx::policies::Counter::default();
    a.add([1; 16], [2; 16], 7).unwrap();
    a.add([1; 16], [2; 16], -2).unwrap();
    let mut b = Counter::default();
    b.add([1; 16], [3; 16], 4).unwrap();
    let mut left = state(Causal::Counter(a), Value::Integer(10));
    let mut right = state(Causal::Counter(b), Value::Integer(10));
    right.version = 2;
    let initial_left = left.clone();
    join_state(&mut left, &right).unwrap();
    join_state(&mut right, &initial_left).unwrap();
    assert_eq!(projection(&left).unwrap(), Value::Integer(19));
    assert_eq!(left.causal, right.causal);
    assert_eq!(left.version, 2);
    let previous = left.causal.clone();
    join_state(&mut left, &right).unwrap();
    assert_eq!(left.causal, previous);
    assert_eq!(projection(&left).unwrap(), Value::Integer(19));
}

#[test]
fn observed_remove_preserves_a_concurrent_add_of_the_same_value() {
    let item = Value::Text("item".into());
    let encoded = encode(&item).unwrap();
    let mut shared = Set::default();
    shared.add(encoded.clone(), tag(2, 0));
    let mut removed = shared.clone();
    removed.remove(&encoded);
    let mut concurrent = shared;
    concurrent.add(encoded, tag(3, 0));
    let mut a = set_state(removed);
    let mut b = set_state(concurrent);
    let original = a.clone();
    join_state(&mut a, &b).unwrap();
    join_state(&mut b, &original).unwrap();
    assert_eq!(a.causal, b.causal);
    assert_eq!(values(&a), vec![item]);
}

#[test]
fn newer_baseline_replaces_seed_tags_and_keeps_committed_operations() {
    let old_seed = Value::Text("old seed".into());
    let new_seed = Value::Text("new seed".into());
    let committed = Value::Text("committed".into());
    let mut old = Set::default();
    old.add(encode(&old_seed).unwrap(), tag(0, 0));
    old.add(encode(&committed).unwrap(), tag(2, 1));
    let mut new = Set::default();
    new.add(encode(&new_seed).unwrap(), tag(0, 2));
    let mut a = set_state(old);
    a.baseline = Value::Blob(encode(&vec![old_seed]).unwrap());
    let mut b = set_state(new);
    b.baseline = Value::Blob(encode(&vec![new_seed.clone()]).unwrap());
    b.basis = 2;
    let original = a.clone();
    join_state(&mut a, &b).unwrap();
    // Old baseline arriving after the new one must not restore its seed tags.
    join_state(&mut b, &original).unwrap();
    assert_eq!(a.baseline, b.baseline);
    assert_eq!(a.causal, b.causal);
    let projected = values(&a);
    assert_eq!(projected.len(), 2);
    assert!(projected.contains(&committed));
    assert!(projected.contains(&new_seed));
}

#[test]
fn orphan_rebase_respects_domain_branch_and_basis_fences() {
    let mut counter = Counter::default();
    counter.add([1; 16], [2; 16], 3).unwrap();
    let orphan = state(Causal::Counter(counter), Value::Integer(99));
    let mut winner = state(Causal::Counter(Counter::default()), Value::Integer(5));
    winner.basis = 2;
    for field in ["domain", "branch", "basis"] {
        let mut incompatible = orphan.clone();
        match field {
            "domain" => incompatible.domain = [9; 16],
            "branch" => incompatible.branch = 9,
            _ => incompatible.basis = 3,
        }
        assert!(!rebase_orphan(&mut winner, &incompatible).unwrap());
        assert_eq!(projection(&winner).unwrap(), Value::Integer(5));
    }
    assert!(rebase_orphan(&mut winner, &orphan).unwrap());
    assert_eq!(projection(&winner).unwrap(), Value::Integer(8));
    assert!(rebase_orphan(&mut winner, &orphan).unwrap());
    assert_eq!(projection(&winner).unwrap(), Value::Integer(8));
}

#[test]
fn invalid_baselines_policy_mismatch_and_overflow_are_errors() {
    let mut a = state(Causal::Counter(Counter::default()), Value::Integer(1));
    let different = state(Causal::Counter(Counter::default()), Value::Integer(2));
    assert!(matches!(
        join_state(&mut a, &different),
        Err(Error::Corrupt(_))
    ));
    let invalid = state(Causal::Counter(Counter::default()), Value::Null);
    assert!(matches!(projection(&invalid), Err(Error::Corrupt(_))));
    let mut too_large = Counter::default();
    too_large.add([1; 16], [2; 16], 1).unwrap();
    let too_large = state(Causal::Counter(too_large), Value::Integer(i64::MAX));
    assert!(matches!(projection(&too_large), Err(Error::Limit(_))));
    let mut a = set_state(Set::default());
    let b = state(Causal::Counter(Counter::default()), a.baseline.clone());
    assert!(matches!(join_state(&mut a, &b), Err(Error::Corrupt(_))));
}

#[test]
fn extrema_comparison_preserves_sqlite_numeric_boundaries() {
    assert_eq!(
        compare(
            &Value::Integer(9_007_199_254_740_993),
            &Value::Real(9_007_199_254_740_992.0)
        )
        .unwrap(),
        Ordering::Greater
    );
    assert_eq!(
        compare(
            &Value::Integer(i64::MAX),
            &Value::Real(9_223_372_036_854_775_808.0)
        )
        .unwrap(),
        Ordering::Less
    );
    assert_eq!(
        compare(
            &Value::Integer(i64::MIN),
            &Value::Real(-9_223_372_036_854_775_808.0)
        )
        .unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        compare(&Value::Integer(1), &Value::Real(1.5)).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        compare(&Value::Integer(-1), &Value::Real(-1.5)).unwrap(),
        Ordering::Greater
    );
    assert_eq!(
        compare(&Value::Real(1.5), &Value::Integer(1)).unwrap(),
        Ordering::Greater
    );
    assert!(matches!(
        compare(&Value::Real(f64::NAN), &Value::Integer(1)),
        Err(Error::Invalid(_))
    ));
    assert_eq!(
        compare(&Value::Null, &Value::Integer(0)).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        compare(&Value::Integer(0), &Value::Text("0".into())).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        compare(&Value::Text("0".into()), &Value::Blob(vec![0])).unwrap(),
        Ordering::Less
    );
}
