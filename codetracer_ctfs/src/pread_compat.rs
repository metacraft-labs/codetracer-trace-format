//! Cross-platform positional read/write helpers.
//!
//! On Unix these delegate to `FileExt::read_at` / `FileExt::write_at`, on
//! Windows to `FileExt::seek_read` / `FileExt::seek_write` — atomic positional
//! I/O that does not move the file cursor.
//!
//! # A short positional read is never a full one
//!
//! Every one of those primitives is allowed to transfer **fewer** bytes than
//! the buffer holds, and they report how many. `pread` used to be
//!
//! ```text
//! file.read_at(buf, offset)?;
//! Ok(buf.len())
//! ```
//!
//! — a single non-looping call whose short count was discarded and replaced by
//! the buffer length. Callers pass buffers created by `vec![0u8; n]`, so a
//! short read left the untouched tail as zeros and the caller was told the
//! whole buffer was real. That is content fabrication, and it is wrong for
//! **any** input, not only for the truncated CTFS containers where M59 measured
//! it: a partial positional read at any offset silently yielded zeros. The
//! measured case was a container cut on a block boundary, where
//! `ConcurrentCtfsReader::read_file` returned 12 388 bytes, success, and 100
//! trailing zeros for a stream whose last data block was entirely past EOF.
//!
//! So the module now offers two shapes and no way to confuse them:
//!
//! - `pread` / `pwrite` loop until the buffer is exhausted and return the
//!   **true** byte count. It is short only at end of file (`pread`) or when the
//!   underlying device stops accepting bytes (`pwrite`).
//! - `pread_exact` / `pwrite_all` are for the callers — currently all of them —
//!   that require the whole buffer, and turn a short transfer into an error
//!   that says which offset came up short.

use std::fs::File;
use std::io;

#[cfg(unix)]
use std::os::unix::fs::FileExt;

#[cfg(windows)]
use std::os::windows::fs::FileExt;

/// One positional read attempt. May transfer fewer bytes than `buf` holds.
#[cfg(unix)]
fn read_once(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    file.read_at(buf, offset)
}

#[cfg(windows)]
fn read_once(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    file.seek_read(buf, offset)
}

// Fallback for platforms without native positional I/O (e.g. wasm32).
// Uses seek + read, which is not atomic, but allows compilation.
#[cfg(not(any(unix, windows)))]
fn read_once(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = file;
    f.seek(SeekFrom::Start(offset))?;
    f.read(buf)
}

/// One positional write attempt. May transfer fewer bytes than `buf` holds.
#[cfg(unix)]
fn write_once(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
    file.write_at(buf, offset)
}

#[cfg(windows)]
fn write_once(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
    file.seek_write(buf, offset)
}

#[cfg(not(any(unix, windows)))]
fn write_once(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
    use std::io::{Seek, SeekFrom, Write};
    let mut f = file;
    f.seek(SeekFrom::Start(offset))?;
    f.write(buf)
}

/// Read from `file` at the given byte `offset` without changing the file cursor.
///
/// Loops until `buf` is full or the file ends, and returns the number of bytes
/// **actually** read. A return value below `buf.len()` means the file supplied
/// no more; the remainder of `buf` is untouched and must not be treated as
/// content.
pub(crate) fn pread(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    let mut done = 0usize;
    while done < buf.len() {
        match read_once(file, &mut buf[done..], offset + done as u64) {
            Ok(0) => break,
            Ok(n) => done += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(done)
}

/// Read exactly `buf.len()` bytes at `offset`, or fail.
///
/// This is what every caller in this crate wants: the buffer is a block, a
/// header or a pointer, and a partially filled one is not a smaller version of
/// it, it is garbage. The error names the offset and both counts so a truncated
/// file is diagnosable rather than showing up as wrong content later.
pub(crate) fn pread_exact(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    let want = buf.len();
    let got = pread(file, buf, offset)?;
    if got != want {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("positional read at offset {offset} wanted {want} bytes but the file supplied {got}"),
        ));
    }
    Ok(())
}

/// Write to `file` at the given byte `offset` without changing the file cursor.
///
/// Loops until `buf` is exhausted or the file stops accepting bytes, and
/// returns the number of bytes **actually** written.
pub(crate) fn pwrite(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
    let mut done = 0usize;
    while done < buf.len() {
        match write_once(file, &buf[done..], offset + done as u64) {
            Ok(0) => break,
            Ok(n) => done += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(done)
}

/// Write all of `buf` at `offset`, or fail.
///
/// A short write inside a CTFS container leaves a half-written block or file
/// entry, which is exactly the state readers cannot tell from valid data, so
/// it has to be an error at the point it happens.
pub(crate) fn pwrite_all(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    let want = buf.len();
    let got = pwrite(file, buf, offset)?;
    if got != want {
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            format!("positional write at offset {offset} wanted {want} bytes but only {got} were accepted"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// The defect, isolated from CTFS entirely: a positional read that runs off
    /// the end of the file must report what it got, and must not present the
    /// untouched (zeroed) remainder of the buffer as content.
    ///
    /// Red before M59: `pread` returned `Ok(buf.len())` for every call that did
    /// not error, so this asserted 100 == 40 and the zero check below passed
    /// vacuously against fabricated bytes.
    #[test]
    fn a_short_positional_read_reports_the_count_it_actually_got() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(&[0xABu8; 100]).unwrap();
        tmp.flush().unwrap();
        let file = File::open(tmp.path()).unwrap();

        // 40 bytes remain from offset 60; ask for 100.
        let mut buf = vec![0u8; 100];
        let n = pread(&file, &mut buf, 60).unwrap();
        assert_eq!(
            n, 40,
            "a positional read past the end of a 100-byte file reported {n} bytes for a 100-byte buffer"
        );
        assert!(buf[..40].iter().all(|b| *b == 0xAB), "the bytes that were read came back wrong");
        assert!(buf[40..].iter().all(|b| *b == 0), "the tail of the buffer should be untouched");
    }

    /// A read fully past the end is zero bytes, not a buffer of zeros.
    #[test]
    fn a_positional_read_entirely_past_the_end_reads_nothing() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(&[0xCDu8; 100]).unwrap();
        tmp.flush().unwrap();
        let file = File::open(tmp.path()).unwrap();

        let mut buf = vec![0u8; 64];
        assert_eq!(pread(&file, &mut buf, 4096).unwrap(), 0);
    }

    /// `pread_exact` is the shape the readers use, and it must refuse rather
    /// than hand back a partially filled buffer.
    #[test]
    fn pread_exact_refuses_a_short_read_and_says_so() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(&[7u8; 100]).unwrap();
        tmp.flush().unwrap();
        let file = File::open(tmp.path()).unwrap();

        let mut buf = vec![0u8; 100];
        let err = pread_exact(&file, &mut buf, 60).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        let msg = err.to_string();
        assert!(
            msg.contains("wanted 100") && msg.contains("supplied 40"),
            "unhelpful short-read message: {msg}"
        );

        // …and succeeds when the bytes are all there.
        let mut ok = vec![0u8; 40];
        pread_exact(&file, &mut ok, 60).unwrap();
        assert!(ok.iter().all(|b| *b == 7));
    }

    /// A full read is still a full read: the loop must not change the ordinary
    /// case, or every caller above it is now reading the wrong bytes.
    #[test]
    fn a_full_positional_read_is_unchanged() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        let content: Vec<u8> = (0..=255u8).cycle().take(9000).collect();
        tmp.write_all(&content).unwrap();
        tmp.flush().unwrap();
        let file = File::open(tmp.path()).unwrap();

        let mut buf = vec![0u8; 4096];
        assert_eq!(pread(&file, &mut buf, 4096).unwrap(), 4096);
        assert_eq!(buf, content[4096..8192]);
    }

    /// The write half of the same defect. `pwrite` reported `buf.len()` on a
    /// short write too; `pwrite_all` is what the writers use.
    #[test]
    fn a_positional_write_round_trips_through_the_exact_helpers() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let file = std::fs::OpenOptions::new().read(true).write(true).open(tmp.path()).unwrap();

        let payload: Vec<u8> = (0..200u8).collect();
        pwrite_all(&file, &payload, 4096).unwrap();
        assert_eq!(file.metadata().unwrap().len(), 4096 + 200);

        let mut back = vec![0u8; 200];
        pread_exact(&file, &mut back, 4096).unwrap();
        assert_eq!(back, payload);
    }
}
