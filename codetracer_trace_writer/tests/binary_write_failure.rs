//! The `Binary` (CBOR + seekable Zstandard) writer reports a write the file
//! system refuses at `finish_writing_trace_events`, instead of panicking in
//! the record call that hit it. Record calls return nothing, so the first
//! failure is held by the writer and the finishing call — the one that
//! returns a `Result` — fails with it.
//!
//! No mocks. The refused write is a real `ulimit -f` file-size limit on a
//! real file, in a child process that is this test binary re-executed. With
//! `SIGXFSZ` ignored the kernel refuses the write with `EFBIG` instead of
//! killing the process. On Windows, a real exclusive byte-range lock above
//! the header refuses a later event write through the child's separate handle.

use std::path::{Path, PathBuf};
use std::process::Command;

use codetracer_trace_types::{EventLogKind, Line, TypeKind, ValueRecord};
use codetracer_trace_writer::trace_writer::TraceWriter;
use codetracer_trace_writer::{TraceEventsFileFormat, create_trace_writer};

const CHILD_ENV: &str = "CT_BINARY_WRITE_FAILURE_CHILD";

/// Exit code the child uses when finishing reported an error.
const FINISH_FAILED: i32 = 3;

/// Enough poorly-compressible event content that the encoder writes
/// compressed blocks to the file while recording, well past the limit.
fn record(w: &mut dyn TraceWriter) {
    let a = Path::new("/src/a.py");
    TraceWriter::start(w, a, Line(1));
    let int = TraceWriter::ensure_type_id(w, TypeKind::Int, "int");
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    for i in 0..20_000i64 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        TraceWriter::register_step(w, a, Line(i % 500 + 1));
        TraceWriter::register_variable_with_full_value(w, "v", ValueRecord::Int { i: x as i64, type_id: int });
        TraceWriter::register_special_event(w, EventLogKind::Write, "stdout", &format!("{x:016x}{:016x}\n", x.rotate_left(29)));
    }
}

/// The child half: record into `$CHILD_ENV`, finish, and report the result
/// through the exit code.
#[test]
fn child_records_and_finishes() {
    let Ok(path) = std::env::var(CHILD_ENV) else {
        return; // Only meaningful when re-executed by the test below.
    };
    let mut w = create_trace_writer("refused", &[], TraceEventsFileFormat::Binary);
    TraceWriter::begin_writing_trace_events(&mut *w, Path::new(&path)).expect("begin");
    eprintln!("BEGIN: Ok");
    record(&mut *w);
    let r = TraceWriter::finish_writing_trace_events(&mut *w);
    eprintln!("FINISH: {r:?}");
    std::process::exit(if r.is_ok() { 0 } else { FINISH_FAILED });
}

fn run_child(path: &Path, pre: &str) -> (std::process::ExitStatus, String) {
    let exe = std::env::current_exe().unwrap();
    #[cfg(not(windows))]
    let out = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "{pre} exec \"$0\" --exact child_records_and_finishes --nocapture --test-threads=1"
        ))
        .arg(exe)
        .env(CHILD_ENV, path)
        .output()
        .expect("spawn child");
    #[cfg(windows)]
    let out = {
        assert!(pre.is_empty(), "Windows uses a native byte-range refusal");
        Command::new(exe)
            .args(["--exact", "child_records_and_finishes", "--nocapture", "--test-threads=1"])
            .env(CHILD_ENV, path)
            .output()
            .expect("spawn child")
    };
    (out.status, String::from_utf8_lossy(&out.stderr).into_owned())
}

// Windows byte-range locks are enforced against other handles, including
// writes beyond EOF. Keep the header writable and refuse the same later event
// write the POSIX file-size limit exercises. No mock writer or elevated volume
// configuration is involved.
// https://learn.microsoft.com/en-us/windows/win32/fileio/locking-and-unlocking-byte-ranges-in-files
#[cfg(windows)]
mod windows_refusal {
    use std::ffi::c_void;
    use std::fs::File;
    use std::io;
    use std::os::windows::io::AsRawHandle;
    use std::path::Path;

    #[repr(C)]
    struct Overlapped {
        internal: usize,
        internal_high: usize,
        offset: u32,
        offset_high: u32,
        event: *mut c_void,
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn LockFileEx(file: *mut c_void, flags: u32, reserved: u32, length_low: u32, length_high: u32, overlapped: *mut Overlapped) -> i32;
        fn UnlockFileEx(file: *mut c_void, reserved: u32, length_low: u32, length_high: u32, overlapped: *mut Overlapped) -> i32;
    }

    pub struct LockedRange {
        file: File,
        overlapped: Overlapped,
        locked: bool,
    }

    impl LockedRange {
        pub fn new(path: &Path) -> io::Result<Self> {
            let mut lock = Self {
                file: File::create(path)?,
                overlapped: Overlapped {
                    internal: 0,
                    internal_high: 0,
                    offset: 64 * 1024,
                    offset_high: 0,
                    event: std::ptr::null_mut(),
                },
                locked: false,
            };
            // Synchronous handle, exclusive + fail immediately. All pointers
            // remain valid for this call; no asynchronous operation is issued.
            let ok = unsafe { LockFileEx(lock.file.as_raw_handle(), 0x2 | 0x1, 0, u32::MAX, 0, &mut lock.overlapped) };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
            lock.locked = true;
            Ok(lock)
        }

        pub fn unlock(&mut self) -> io::Result<()> {
            if self.locked {
                // Same live handle, range and synchronous structure as above.
                let ok = unsafe { UnlockFileEx(self.file.as_raw_handle(), 0, u32::MAX, 0, &mut self.overlapped) };
                if ok == 0 {
                    return Err(io::Error::last_os_error());
                }
                self.locked = false;
            }
            Ok(())
        }
    }

    impl Drop for LockedRange {
        fn drop(&mut self) {
            // Also release on assertion failure; closing File releases the
            // owned lock if explicit unlocking itself fails during unwinding.
            let _ = self.unlock();
        }
    }
}

/// Without a limit the same recording finishes, so the failure below is the
/// limit's doing and not the recording's.
#[test]
fn the_recording_finishes_without_a_limit() {
    let dir = tempfile::tempdir().unwrap();
    let path: PathBuf = dir.path().join("trace.bin");
    let (status, stderr) = run_child(&path, "");
    assert!(status.success(), "{status:?}\n{stderr}");
    assert!(
        std::fs::metadata(&path).unwrap().len() > 64 * 1024,
        "the recording is larger than the limit used below"
    );
}

/// A write refused while recording fails the recording at finish, naming the
/// refused event write and the I/O error — it neither panics in the record
/// call nor succeeds.
#[test]
fn a_write_refused_while_recording_fails_the_finish() {
    let dir = tempfile::tempdir().unwrap();
    let path: PathBuf = dir.path().join("trace.bin");
    // 16 blocks of 512 bytes (or 1024, depending on the shell): the header
    // and the first compressed blocks fit, the recording does not.
    #[cfg(not(windows))]
    let (status, stderr) = run_child(&path, "trap '' XFSZ; ulimit -f 16;");
    #[cfg(windows)]
    let (status, stderr) = {
        let mut lock = windows_refusal::LockedRange::new(&path).expect("lock future event bytes");
        let result = run_child(&path, "");
        lock.unlock().expect("unlock owned event range");
        result
    };
    assert!(stderr.contains("BEGIN: Ok"), "the header succeeds before the refused event: {stderr}");
    assert_eq!(
        status.code(),
        Some(FINISH_FAILED),
        "the recording must fail at finish, not succeed or panic: {status:?}\n{stderr}"
    );
    assert!(!stderr.contains("panicked"), "no record call panics: {stderr}");
    assert!(stderr.contains("FINISH: Err"), "{stderr}");
    assert!(
        stderr.contains("writing an event"),
        "the failure is the event write that hit the limit: {stderr}"
    );
    #[cfg(not(windows))]
    assert!(
        stderr.to_lowercase().contains("file too large") || stderr.contains("EFBIG"),
        "the I/O error is named: {stderr}"
    );
    #[cfg(windows)]
    assert!(stderr.contains("os error 33"), "the native byte-range lock violation is named: {stderr}");
}
