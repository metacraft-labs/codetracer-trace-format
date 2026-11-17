//! `CtfsWriter::close` reports a failure of the last write that reaches the
//! operating system: the flush of the writer's buffer.
//!
//! The file-backed store buffers writes (`BufWriter`). A write whose flush
//! fails stays in the buffer, and dropping a `BufWriter` flushes it once more
//! and discards the result. So the bytes `close` writes — the member's last,
//! partial block and the root entries that publish every member's size —
//! reach the file only through flushes whose errors `close` returns; a full
//! disk (`ENOSPC`) or a file-size limit (`EFBIG`) that shows up there must
//! fail the close rather than leave a truncated container behind a success.
//!
//! HOW THE FAILURE IS PRODUCED. Nothing is mocked. The test re-runs its own
//! binary as a child under `bash -c 'trap "" XFSZ; ulimit -f 4; exec ...'`:
//! the ignored `SIGXFSZ` is inherited across `exec`, so a `write(2)` past
//! 4096 bytes fails with `EFBIG` on the real file system instead of killing
//! the process. The container is laid out in 1024-byte blocks and is 5120
//! bytes long; its first four blocks are written before `close`, and the
//! fifth — the member's partial last block, which `close` writes — is the
//! one that crosses the limit. The parent checks the file holds exactly the
//! 4096 bytes below the limit, which shows the writes before `close` landed.
//!
//! Dropping the error of those writes and the final `flush()?` in
//! `close_inner` (leaving the `BufWriter`'s drop to flush) makes this fail:
//! the child's `close` then returns `Ok` over a 4096-byte file.

#![cfg(target_os = "linux")]

use std::path::Path;
use std::process::Command;

use codetracer_ctfs::CtfsWriter;

const CHILD_ENV: &str = "CTFS_CLOSE_FLUSH_CHILD_PATH";
const LIMIT_BYTES: u64 = 4096;
const BLOCK_SIZE: u32 = 1024;

/// What the child does under the limit: write a 5120-byte container and close
/// it. Exit 0 when the writer reported a failure, 1 when it reported success,
/// 2 when the case's own setup broke.
fn child(path: &Path) -> ! {
    let mut w = match CtfsWriter::create(path, BLOCK_SIZE, 31) {
        Ok(w) => w,
        Err(e) => {
            eprintln!("child: create failed: {e}");
            std::process::exit(2);
        }
    };
    let payload: Vec<u8> = (0..3000u32).map(|i| (i * 31 + 7) as u8).collect();
    let written = w.add_file("a.dat").and_then(|h| w.write(h, &payload));
    if let Err(e) = written {
        eprintln!("child: a write before close reported: {e}");
        std::process::exit(2);
    }
    match w.close() {
        Err(e) => {
            eprintln!("child: close reported: {e}");
            std::process::exit(0);
        }
        Ok(()) => {
            eprintln!("child: close reported success");
            std::process::exit(1);
        }
    }
}

#[test]
fn close_reports_a_failed_final_flush() {
    if let Some(path) = std::env::var_os(CHILD_ENV) {
        child(Path::new(&path));
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("flush_failure.ct");

    // Without the limit the same container is 5120 bytes: the limit falls
    // inside it, past what the writer had to write before close.
    let probe = dir.path().join("probe.ct");
    let mut w = CtfsWriter::create(&probe, BLOCK_SIZE, 31).unwrap();
    let h = w.add_file("a.dat").unwrap();
    w.write(h, &(0..3000u32).map(|i| (i * 31 + 7) as u8).collect::<Vec<u8>>()).unwrap();
    w.close().unwrap();
    let full = std::fs::metadata(&probe).unwrap().len();
    assert!(full > LIMIT_BYTES, "the container ({full} bytes) must cross the {LIMIT_BYTES}-byte limit");

    let exe = std::env::current_exe().unwrap();
    let out = Command::new("bash")
        .arg("-c")
        .arg(format!("trap '' XFSZ; ulimit -f {}; exec \"$0\" \"$@\"", LIMIT_BYTES / 1024))
        .arg(&exe)
        .args(["--exact", "close_reports_a_failed_final_flush", "--test-threads=1", "--nocapture"])
        .env(CHILD_ENV, &path)
        .output()
        .expect("run the child under a file-size limit");
    let stderr = String::from_utf8_lossy(&out.stderr);
    let code = out.status.code();
    assert_ne!(code, Some(2), "the child's setup failed:\n{stderr}");
    assert_eq!(
        code,
        Some(0),
        "close returned success although its final flush failed with EFBIG; \
         the container on disk is truncated:\n{stderr}"
    );
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        LIMIT_BYTES,
        "the bytes below the limit reached the file:\n{stderr}"
    );
}
