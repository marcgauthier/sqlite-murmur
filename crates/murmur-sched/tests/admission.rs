use murmur_sched::{Error, SchedulerConfig, WriterClass, WriterScheduler};
use std::time::Duration;

#[test]
fn timed_out_waiter_does_not_block_later_grants() {
    let scheduler = WriterScheduler::new(SchedulerConfig::default());
    let held = scheduler.admit(WriterClass::Local).unwrap();
    assert!(matches!(
        scheduler.admit_timeout(WriterClass::Remote, Duration::from_millis(1)),
        Err(Error::Busy)
    ));
    let snapshot = scheduler.snapshot();
    assert_eq!(snapshot.remote.cancels, 1);
    assert_eq!(snapshot.remote.waiters, 0);
    assert_eq!(snapshot.remote.acquisitions, 0);
    drop(held);
    let granted = scheduler
        .admit_timeout(WriterClass::Remote, Duration::ZERO)
        .unwrap();
    assert_eq!(granted.class(), WriterClass::Remote);
    drop(granted);
    assert!(!scheduler.active());
}

#[test]
fn released_ticket_cannot_release_another_writer() {
    let scheduler = WriterScheduler::new(SchedulerConfig::default());
    let mut first = scheduler.admit(WriterClass::Local).unwrap();
    first.release();
    let second = scheduler.admit(WriterClass::Remote).unwrap();
    first.release();
    drop(first);
    assert!(scheduler.active());
    assert!(matches!(
        scheduler.admit_timeout(WriterClass::Maintenance, Duration::ZERO),
        Err(Error::Busy)
    ));
    drop(second);
    assert!(!scheduler.active());
    assert_eq!(scheduler.snapshot().maintenance.cancels, 1);
}

#[test]
fn close_preserves_active_work_and_rejects_new_admission() {
    let scheduler = WriterScheduler::new(SchedulerConfig::default());
    let held = scheduler.admit(WriterClass::Local).unwrap();
    scheduler.close();
    scheduler.close();
    assert!(scheduler.active());
    for class in [
        WriterClass::Local,
        WriterClass::Remote,
        WriterClass::Maintenance,
    ] {
        assert!(matches!(scheduler.admit(class), Err(Error::Closed)));
        assert!(matches!(
            scheduler.admit_timeout(class, Duration::ZERO),
            Err(Error::Closed)
        ));
    }
    drop(held);
    assert!(!scheduler.active());
    assert_eq!(scheduler.snapshot().local.acquisitions, 1);
}

#[test]
fn configuration_rejects_invalid_weights_debt_and_reserve() {
    let valid = SchedulerConfig::default();
    valid.validate().unwrap();
    for (local_share, remote_share) in [(0, 100), (100, 0), (1, 1), (99, 2)] {
        let invalid = SchedulerConfig {
            local_share,
            remote_share,
            ..valid.clone()
        };
        assert!(matches!(invalid.validate(), Err(Error::Invalid(_))));
    }
    assert!(matches!(
        SchedulerConfig {
            max_debt: Duration::ZERO,
            ..valid.clone()
        }
        .validate(),
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        SchedulerConfig {
            maintenance_reserve_every: 0,
            ..valid
        }
        .validate(),
        Err(Error::Invalid(_))
    ));
}
