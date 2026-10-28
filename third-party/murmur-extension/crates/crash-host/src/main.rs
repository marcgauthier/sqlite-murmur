//! Crash simulator for outbox-recovery tests.
//!
//! Usage: `crash-host <db> <ext.so> <spool> <exit|sleep>`
//!
//! Commits one collected row through the extension, then either exits without
//! cleanup (`exit`, no destructors run) or prints READY and sleeps so the
//! parent test can SIGKILL it (`sleep`). Never drains: recovery must replay
//! the outbox row on the next `murmur_open`.

use host_shim::Conn;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 5 {
        eprintln!("usage: crash-host <db> <ext.so> <spool> <exit|sleep>");
        std::process::exit(2);
    }
    run(&args[1], &args[2], &args[3], &args[4]);
}

fn run(db_path: &str, ext_path: &str, spool_path: &str, mode: &str) {
    let conn = Conn::open(db_path).unwrap();
    conn.load_extension(ext_path, None).unwrap();
    conn.exec(&format!(
        "SELECT murmur_open('{}')",
        spool_path.replace('\'', "''")
    ))
    .unwrap();
    conn.exec("CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.query_text("SELECT murmur_enable_sync('t')").unwrap();
    conn.exec("BEGIN").unwrap();
    conn.exec("INSERT INTO t VALUES (1, 'crashed')").unwrap();
    let seq = conn.query_int_nullable("SELECT murmur_collect()").unwrap();
    assert_eq!(seq, Some(1), "first outbox row must be seq 1, got {seq:?}");
    conn.exec("COMMIT").unwrap();
    match mode {
        "exit" => std::process::exit(42),
        "sleep" => {
            println!("READY");
            loop {
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
        }
        other => {
            eprintln!("unknown mode: {other}");
            std::process::exit(2);
        }
    }
}
