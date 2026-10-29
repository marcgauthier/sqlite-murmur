//! Proposed slice-4 API regressions; coordinator approval gates API freeze.
use murmur_policy::{
    Error,
    model::Value,
    ownership::{self, Event, EventKind, NativeStamp, Target, TargetKind},
};
use std::cmp::Ordering;

const MAX: usize = 1 << 20;

fn target() -> Target {
    Target::new("t".into(), vec![1], TargetKind::Column("v".into())).unwrap()
}
fn grant(value: i64) -> Event {
    Event {
        target: target(),
        kind: EventKind::Grant {
            value: Some(Value::Integer(value)),
        },
        prior_grant: None,
        prior_release: None,
    }
}
fn stamp(event: &Event, cl: i64, version: i64, site: u8, tie: bool) -> NativeStamp {
    NativeStamp::new(
        event.target.clone(),
        cl,
        version,
        ownership::encode_event(event, MAX).unwrap(),
        Some([site; 16]),
        tie,
        MAX,
    )
    .unwrap()
}

#[test]
fn grant_release_roundtrip_uses_prior_identity_without_a_self_stamp() {
    let first = grant(7);
    let reference = ownership::event_ref(&first, [2; 16], 1, 1, MAX).unwrap();
    let release = Event {
        target: target(),
        kind: EventKind::Release,
        prior_grant: Some(reference.clone()),
        prior_release: None,
    };
    let bytes = ownership::encode_event(&release, MAX).unwrap();
    assert_eq!(ownership::decode_event(&bytes, MAX).unwrap(), release);
    let reference2 = ownership::event_ref(&release, [2; 16], 1, 2, MAX).unwrap();
    let renewed = Event {
        target: target(),
        kind: EventKind::Grant {
            value: Some(Value::Integer(8)),
        },
        prior_grant: None,
        prior_release: Some(reference2),
    };
    assert_eq!(
        ownership::decode_event(&ownership::encode_event(&renewed, MAX).unwrap(), MAX).unwrap(),
        renewed
    );
    assert_eq!(
        stamp(&first, 1, 1, 2, false).reference([2; 16]).unwrap(),
        reference
    );
}

#[test]
fn row_kind_reserved_column_and_canonical_target_keys_are_distinct() {
    assert!(Target::new("t".into(), vec![1], TargetKind::Column("__RoW__".into())).is_err());
    let row = Target::new("T".into(), vec![1], TargetKind::Row).unwrap();
    let column = Target::new("T".into(), vec![1], TargetKind::Column("V".into())).unwrap();
    assert_eq!(column, target());
    assert_eq!(
        ownership::target_key(&row).unwrap(),
        b"09/bridge/owner-crr/v1/b66643830df485c06aae21f2e8cfbd1c59a36af042f504a68c580979f6174737"
    );
    assert_eq!(
        ownership::target_key(&column).unwrap(),
        b"09/bridge/owner-crr/v1/b4d29c84f23eb5c654d878b4d3b2fdd318ab3871dbbfc3033ea938d02bbf83f5"
    );
    let event = Event {
        target: row,
        kind: EventKind::Grant { value: None },
        prior_grant: None,
        prior_release: None,
    };
    assert!(ownership::encode_event(&event, MAX).is_ok());
    assert!(
        ownership::verify_target_key(&ownership::target_key(&column).unwrap(), &event.target)
            .is_err()
    );
}

#[test]
fn record_verifier_checks_envelope_version_canonical_bytes_and_target_binding() {
    let event = grant(1);
    let key = ownership::target_key(&event.target).unwrap();
    let bytes = ownership::encode_record(&event, MAX).unwrap();
    assert_eq!(ownership::decode_record(&key, &bytes, MAX).unwrap(), event);
    let mut future = bytes.clone();
    future[0] = 2;
    assert!(ownership::decode_record(&key, &future, MAX).is_err());
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(ownership::decode_record(&key, &trailing, MAX).is_err());
    for length in 0..bytes.len() {
        assert!(ownership::decode_record(&key, &bytes[..length], MAX).is_err());
    }
    let mut bad_key = key.clone();
    *bad_key.last_mut().unwrap() ^= 1;
    assert!(matches!(
        ownership::decode_record(&bad_key, &bytes, MAX),
        Err(Error::Corrupt(_))
    ));
    assert!(ownership::decode_record(b"09/bridge/owner/legacy", &bytes, MAX).is_err());
    assert!(matches!(
        ownership::decode_event(&bytes, bytes.len() - 1),
        Err(Error::Limit(_))
    ));
    assert!(ownership::decode_event(&bytes, 0).is_err());
    assert!(ownership::encode_event(&event, 0).is_err());
}

#[test]
fn malformed_event_shapes_references_and_identifiers_fail_closed() {
    let first = grant(1);
    let mut bad = first.clone();
    bad.kind = EventKind::Release;
    assert!(ownership::encode_event(&bad, MAX).is_err());
    let mut bad = first.clone();
    bad.kind = EventKind::Grant { value: None };
    assert!(ownership::encode_event(&bad, MAX).is_err());
    let mut bad = first.clone();
    bad.target.kind = TargetKind::Row;
    assert!(ownership::encode_event(&bad, MAX).is_err());
    let mut bad = first.clone();
    bad.target.table = "UPPER".into();
    assert!(ownership::encode_event(&bad, MAX).is_err());
    let mut bad = first.clone();
    bad.kind = EventKind::Grant {
        value: Some(Value::Real(f64::NAN)),
    };
    assert!(ownership::encode_event(&bad, MAX).is_err());
    let mut reference = ownership::event_ref(&first, [2; 16], 1, 1, MAX).unwrap();
    reference.target.table = "other".into();
    let mut bad = first.clone();
    bad.prior_grant = Some(reference);
    assert!(ownership::encode_event(&bad, MAX).is_err());
    assert!(ownership::event_ref(&first, [0; 16], 1, 1, MAX).is_err());
    assert!(ownership::event_ref(&first, [2; 16], -1, 1, MAX).is_err());
    assert!(ownership::event_ref(&first, [2; 16], 1, 0, MAX).is_err());
    for (table, pk) in [("", vec![1]), ("a\0b", vec![1]), ("t", vec![])] {
        assert!(Target::new(table.into(), pk, TargetKind::Row).is_err());
    }
}

#[test]
fn native_order_uses_lifecycle_version_blob_then_optional_site_and_rejects_cross_scope() {
    let low = grant(1);
    let high = grant(2);
    assert_eq!(
        stamp(&low, 1, 1, 1, false)
            .compare_same_target(&stamp(&high, 1, 1, 9, false))
            .unwrap(),
        Ordering::Less
    );
    assert_eq!(
        stamp(&low, 1, 2, 1, false)
            .compare_same_target(&stamp(&high, 1, 1, 9, false))
            .unwrap(),
        Ordering::Greater
    );
    assert_eq!(
        stamp(&low, 3, 1, 1, false)
            .compare_same_target(&stamp(&high, 1, 100, 9, false))
            .unwrap(),
        Ordering::Greater
    );
    assert_eq!(
        stamp(&low, 1, 1, 1, false)
            .compare_same_target(&stamp(&low, 1, 1, 9, false))
            .unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        stamp(&low, 1, 1, 1, true)
            .compare_same_target(&stamp(&low, 1, 1, 9, true))
            .unwrap(),
        Ordering::Less
    );
    let mut other = low.clone();
    other.target.pk = vec![2];
    assert!(
        stamp(&low, 1, 1, 1, true)
            .compare_same_target(&stamp(&other, 1, 1, 1, true))
            .is_err()
    );
    assert!(
        stamp(&low, 1, 1, 1, true)
            .compare_same_target(&stamp(&low, 1, 1, 1, false))
            .is_err()
    );
    assert!(
        NativeStamp::new(
            target(),
            1,
            1,
            ownership::encode_event(&low, MAX).unwrap(),
            None,
            true,
            MAX
        )
        .is_err()
    );
    assert!(
        NativeStamp::new(
            target(),
            -1,
            1,
            ownership::encode_event(&low, MAX).unwrap(),
            None,
            false,
            MAX
        )
        .is_err()
    );
    assert!(
        NativeStamp::new(
            target(),
            1,
            0,
            ownership::encode_event(&low, MAX).unwrap(),
            None,
            false,
            MAX
        )
        .is_err()
    );
}

#[test]
fn append_only_identity_preserves_replayed_loser_releases_and_reincarnated_events() {
    let first = grant(7);
    let first_ref = ownership::event_ref(&first, [2; 16], 1, 1, MAX).unwrap();
    let release = Event {
        target: target(),
        kind: EventKind::Release,
        prior_grant: Some(first_ref.clone()),
        prior_release: None,
    };
    let release_ref = ownership::event_ref(&release, [2; 16], 1, 2, MAX).unwrap();
    // A later native LWW winner does not identify or replace this earlier
    // release: distinct immutable event keys are the host storage contract.
    let later = grant(9);
    let later_ref = ownership::event_ref(&later, [2; 16], 1, 3, MAX).unwrap();
    assert_ne!(release_ref, later_ref);
    assert_eq!(
        ownership::event_ref(&release, [2; 16], 1, 2, MAX).unwrap(),
        release_ref
    );
    assert_ne!(
        first_ref,
        ownership::event_ref(&first, [2; 16], 3, 1, MAX).unwrap()
    );
    assert_ne!(
        first_ref,
        ownership::event_ref(&first, [3; 16], 1, 1, MAX).unwrap()
    );
    for event in [&release, &later, &first, &release] {
        let bytes = ownership::encode_event(event, MAX).unwrap();
        assert_eq!(&ownership::decode_event(&bytes, MAX).unwrap(), event);
    }
}

#[test]
fn native_blob_order_is_not_policy_numeric_order_and_does_not_allocate_tokens() {
    // Native CRR's carrier value is a BLOB: compare the actual accepted
    // envelope bytes, not the numeric Value nested inside that BLOB.
    let a = grant(64);
    let b = grant(100);
    let a_bytes = ownership::encode_event(&a, MAX).unwrap();
    let b_bytes = ownership::encode_event(&b, MAX).unwrap();
    assert_eq!(
        stamp(&a, 1, 1, 1, false)
            .compare_same_target(&stamp(&b, 1, 1, 1, false))
            .unwrap(),
        a_bytes.cmp(&b_bytes)
    );
    let native = stamp(&a, 2, 3, 4, false);
    assert_eq!(native.target(), &target());
    assert_eq!(native.row_cl(), 2);
    assert_eq!(native.col_version(), 3);
    assert_eq!(native.accepted_blob(), a_bytes);
    assert_eq!(native.site_id(), None);
    assert!(!native.merge_equal_values());
}
