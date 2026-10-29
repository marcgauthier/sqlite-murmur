use murmur_policy::{
    Error,
    high_low::{self, ImportDecision, LowSource, Owner, Projection, ROW, Target},
    model::Value,
};
use murmur_tx::bridge::{Header, Origin, Role};
use std::collections::BTreeSet;

fn target(column: &str) -> Target {
    Target {
        table: "items".into(),
        pk: vec![0xde, 0xad],
        column: column.into(),
    }
}

fn owned() -> Owner<u64> {
    high_low::promote_value(target("value"), None, 10, Value::Integer(1), 0).unwrap()
}

fn header() -> Header {
    Header {
        origin: None,
        source_database: [1; 16],
        source_node: [2; 16],
        stream: "stream".into(),
        sequence: 1,
        bundle: [3; 16],
        recipient: [4; 32],
        ephemeral: [5; 32],
        nonce: [6; 24],
        digest: [7; 32],
    }
}

#[test]
fn release_fence_defeats_delayed_promotion_and_reownership_starts_new_branch() {
    let first = owned();
    let updated =
        high_low::promote_value(target("value"), Some(&first), 20, Value::Integer(2), 0).unwrap();
    assert_eq!(updated.epoch, 10);
    assert_eq!(updated.value.as_ref().unwrap().native_version, 20);
    let released = high_low::release(&updated, 30).unwrap();
    assert!(!released.owned);
    assert_eq!(released.epoch, 30);
    assert_eq!(released.release_epoch, 30);
    // The delayed cell can have a larger cell version, but its older owner
    // epoch must never undo the release: this is the archived control rule.
    let delayed =
        high_low::promote_value(target("value"), Some(&first), 100, Value::Integer(99), 0).unwrap();
    assert_eq!(high_low::join_owner(&released, &delayed).unwrap(), released);
    assert_eq!(high_low::join_owner(&delayed, &released).unwrap(), released);
    let reclaimed =
        high_low::promote_value(target("value"), Some(&released), 40, Value::Integer(3), 0)
            .unwrap();
    assert_eq!(reclaimed.epoch, 40);
    assert_eq!(reclaimed.release_epoch, 30);
    assert_eq!(
        high_low::join_owner(&reclaimed, &released).unwrap(),
        reclaimed
    );
    assert_eq!(
        high_low::join_owner(&reclaimed, &reclaimed).unwrap(),
        reclaimed
    );
}

#[test]
fn same_owner_epoch_uses_cell_version_and_foreign_identity_is_rejected() {
    let first = owned();
    let updated =
        high_low::promote_value(target("value"), Some(&first), 20, Value::Integer(2), 0).unwrap();
    assert_eq!(high_low::join_owner(&first, &updated).unwrap(), updated);
    assert_eq!(high_low::join_owner(&updated, &first).unwrap(), updated);
    let mut foreign = updated.clone();
    foreign.target.pk.push(0);
    assert!(matches!(
        high_low::join_owner(&updated, &foreign),
        Err(Error::Corrupt(_))
    ));
    let mut invalid = first.clone();
    invalid.release_epoch = 11;
    assert!(high_low::join_owner(&first, &invalid).is_err());
}

#[test]
fn stale_release_or_promotion_fails_without_changing_the_owner() {
    let first = owned();
    assert!(high_low::release(&first, 10).is_err());
    let updated =
        high_low::promote_value(target("value"), Some(&first), 20, Value::Integer(2), 0).unwrap();
    assert!(high_low::release(&updated, 15).is_err());
    assert!(
        high_low::promote_value(target("value"), Some(&updated), 19, Value::Integer(3), 0).is_err()
    );
    let released = high_low::release(&updated, 30).unwrap();
    assert!(high_low::release(&released, 40).is_err());
    assert!(
        high_low::promote_value(target("value"), Some(&released), 30, Value::Integer(3), 0)
            .is_err()
    );
    assert!(high_low::promote_value(target("value"), None, 10, Value::Real(f64::NAN), 0).is_err());
    assert_eq!(first, owned());
}

#[test]
fn row_and_column_authority_select_projection_without_merging_rows() {
    let column = owned();
    let row = high_low::own_deletion(target(ROW), 15, 0).unwrap();
    assert_eq!(
        high_low::select_projection(&target("value"), Some(&row), Some(&column)).unwrap(),
        Projection::SuppressRow
    );
    assert!(!high_low::permits_low_delete(&target(ROW), std::slice::from_ref(&row)).unwrap());
    let row = high_low::release(&row, 16).unwrap();
    assert!(matches!(
        high_low::select_projection(&target("value"), Some(&row), Some(&column)).unwrap(),
        Projection::High(_)
    ));
    assert!(!high_low::permits_low_delete(&target(ROW), &[row.clone(), column.clone()]).unwrap());
    let column = high_low::release(&column, 20).unwrap();
    assert_eq!(
        high_low::select_projection(&target("value"), Some(&row), Some(&column)).unwrap(),
        Projection::Low
    );
    assert!(high_low::permits_low_delete(&target(ROW), &[row, column]).unwrap());
    let mut foreign = owned();
    foreign.target.table = "other".into();
    assert!(high_low::select_projection(&target("value"), None, Some(&foreign)).is_err());
    assert!(high_low::permits_low_delete(&target(ROW), &[foreign]).is_err());
}

#[test]
fn scope_selection_preserves_high_authority_low_provenance_and_release_fallback() {
    let owner = owned();
    let sources = vec![
        LowSource {
            target: target("value"),
            database: [2; 16],
            native_version: 35,
            incarnation: 1,
        },
        LowSource {
            target: target("value"),
            database: [3; 16],
            native_version: 35,
            incarnation: 2,
        },
    ];
    let high =
        high_low::select_scope(&target("value"), [1; 16], 3, 2, 0, Some(&owner), &sources).unwrap();
    assert_eq!(
        (high.domain, high.branch, high.incarnation, high.basis),
        ([1; 16], 0, 3, 10)
    );
    let released = high_low::release(&owner, 30).unwrap();
    let low = high_low::select_scope(
        &target("value"),
        [1; 16],
        3,
        2,
        0,
        Some(&released),
        &sources,
    )
    .unwrap();
    assert_eq!(
        (low.domain, low.branch, low.incarnation, low.basis),
        ([3; 16], 0, 2, 35)
    );
    let reversed = high_low::select_scope(
        &target("value"),
        [1; 16],
        3,
        2,
        0,
        Some(&released),
        &sources.into_iter().rev().collect::<Vec<_>>(),
    )
    .unwrap();
    assert_eq!(low, reversed);
    let fallback =
        high_low::select_scope(&target("value"), [1; 16], 3, 2, 0, Some(&released), &[]).unwrap();
    assert_eq!(
        (
            fallback.domain,
            fallback.branch,
            fallback.incarnation,
            fallback.basis
        ),
        ([1; 16], 30, 3, 2)
    );
    let mut bad = LowSource {
        target: target("foreign"),
        database: [2; 16],
        native_version: 40,
        incarnation: 1,
    };
    assert!(
        high_low::select_scope(&target("value"), [1; 16], 3, 2, 0, None, &[bad.clone()]).is_err()
    );
    bad.target = target("value");
    assert!(high_low::select_scope(&target("value"), [1; 16], 3, 2, 0, None, &[bad]).is_ok());
}

#[test]
fn trust_role_payload_identity_receipt_replay_and_conflicts_are_checked() {
    let header = header();
    let trusted = BTreeSet::from([[1; 16]]);
    let fresh =
        high_low::check_import(Role::HighReceiver, &trusted, &header, Some([1; 16]), None).unwrap();
    assert_eq!(
        fresh,
        ImportDecision::Fresh {
            key: high_low::receipt_key(&header),
            digest: [7; 32]
        }
    );
    assert_eq!(
        high_low::check_import(Role::HighReceiver, &trusted, &header, None, Some(&[7; 32]))
            .unwrap(),
        ImportDecision::Replay
    );
    assert!(matches!(
        high_low::check_import(Role::HighReceiver, &trusted, &header, None, Some(&[8; 32])),
        Err(Error::Corrupt(_))
    ));
    assert!(
        high_low::check_import(Role::HighReceiver, &trusted, &header, None, Some(&[7; 31]))
            .is_err()
    );
    assert!(matches!(
        high_low::check_import(Role::LowExporter, &trusted, &header, None, None),
        Err(Error::Invalid(_))
    ));
    assert!(
        high_low::check_import(
            Role::HighReceiver,
            &BTreeSet::new(),
            &header,
            None,
            Some(&[7; 32])
        )
        .is_err()
    );
    assert!(matches!(
        high_low::check_import(Role::HighReceiver, &trusted, &header, Some([9; 16]), None),
        Err(Error::SchemaMismatch)
    ));
}

#[test]
fn forwarded_origin_receipt_is_stable_across_bundle_rewrapping() {
    let mut original = header();
    original.origin = Some(Origin {
        node: [9; 16],
        sequence: 42,
        transaction: [8; 16],
        digest: [7; 32],
    });
    let expected = concat!(
        "09/bridge/receipt/",
        "cc8cd41cef907c4d216069122c4b89936211361f9050a717a1e37ad1862e952f/",
        "06232b08dabb5a4fdb466598179349c845b0dc3436324794e270257b4e0fdbce",
        "-00000000000000000042"
    );
    assert_eq!(high_low::receipt_key(&original), expected.as_bytes());
    let mut rewrapped = original.clone();
    rewrapped.bundle = [99; 16];
    rewrapped.source_node = [11; 16];
    rewrapped.sequence = 100;
    rewrapped.stream = "another".into();
    assert_eq!(
        high_low::receipt_key(&original),
        high_low::receipt_key(&rewrapped)
    );
    let key = high_low::receipt_key(&header());
    assert_eq!(
        key,
        concat!(
            "09/bridge/receipt/",
            "cc8cd41cef907c4d216069122c4b89936211361f9050a717a1e37ad1862e952f/",
            "6399f6d863b08f4652b342c2e1350b4c01e291332d9d84d84b575bf7272897d2"
        )
        .as_bytes()
    );
    let trusted = BTreeSet::from([[1; 16]]);
    rewrapped.digest = [8; 32];
    assert!(matches!(
        high_low::check_import(
            Role::HighReceiver,
            &trusted,
            &rewrapped,
            None,
            Some(&[7; 32])
        ),
        Err(Error::Corrupt(_))
    ));
}

#[test]
fn native_event_tokens_need_not_be_copy_or_a_logical_clock() {
    let first = high_low::promote_value(
        target("value"),
        None,
        "accepted-1".to_string(),
        Value::Integer(1),
        String::new(),
    )
    .unwrap();
    let released = high_low::release(&first, "accepted-2".to_string()).unwrap();
    assert_eq!(high_low::join_owner(&released, &first).unwrap(), released);
}
