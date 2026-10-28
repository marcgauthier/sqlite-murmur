//! Outbox crash-recovery: a row committed but never drained must replay on
//! the next `murmur_open`, whether the writer exited dirty or was SIGKILLed.

#[path = "common.rs"]
#[allow(dead_code)]
mod common;

use common::*;
use murmur_crdt::model::Value;
use std::process::{Child, Command, Stdio};

fn crash_host_bin() -> String {
    let status = Command::new(env!("CARGO"))
        .args(["build", "--offline", "-p", "crash-host"])
        .status()
        .unwrap();
    assert!(status.success(), "crash-host build failed");
    let exe = std::env::current_exe().unwrap();
    let bin = exe
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join(format!("crash-host{}", std::env::consts::EXE_SUFFIX));
    assert!(bin.exists(), "missing {}", bin.display());
    bin.to_str().unwrap().to_string()
}

fn verify_recovered(fx: &Fixture) {
    let conn = open_conn(fx);
    // Recovery drain ran inside murmur_open: nothing left, user row intact.
    assert_eq!(conn.query_int("SELECT murmur_drain()").unwrap(), 0);
    assert_eq!(
        conn.query_text("SELECT v FROM t WHERE id = 1").unwrap(),
        "crashed"
    );
    assert_eq!(
        conn.query_int("SELECT COUNT(*) FROM _murmur_outbox").unwrap(),
        0
    );
    let tables = conn.query_text("SELECT murmur_sync_tables()").unwrap();
    assert!(tables.contains("\"name\":\"t\""), "{tables}");
    let id = tables
        .split("\"id\":\"")
        .nth(1)
        .and_then(|s| s.split('"').next())
        .unwrap()
        .to_string();
    conn.exec("SELECT murmur_close()").unwrap();
    drop(conn);

    let store = open_spool(fx);
    let meta = read_meta(&store, &id, &[int_value(1)]);
    let v = meta
        .cells
        .iter()
        .find(|c| c.column == format!("{id}/v"))
        .unwrap();
    assert_eq!(v.value, Value::Text("crashed".into()));
    store.close().unwrap();
}

#[test]
fn exit_crash_recovers() {
    let fx = setup();
    let status = Command::new(crash_host_bin())
        .args([&fx.db, &cdylib(), &fx.spool, "exit"])
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(42));
    verify_recovered(&fx);
}

#[test]
#[cfg(unix)]
fn sigkill_crash_recovers() {
    let fx = setup();
    let mut child: Child = Command::new(crash_host_bin())
        .args([&fx.db, &cdylib(), &fx.spool, "sleep"])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    // Wait for READY: the row is committed, the crash window is open.
    let mut ready = String::new();
    std::io::BufRead::read_line(
        &mut std::io::BufReader::new(child.stdout.take().unwrap()),
        &mut ready,
    )
    .unwrap();
    assert_eq!(ready.trim(), "READY");
    let status = Command::new("kill")
        .args(["-9", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let _ = child.wait();
    verify_recovered(&fx);
}
