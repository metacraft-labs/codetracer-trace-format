//! Cross-platform positional read/write helpers.
//!
//! On Unix, these delegate to `FileExt::read_at` / `FileExt::write_at`.
//! On Windows, they delegate to `FileExt::seek_read` / `FileExt::seek_write`,
//! which have identical semantics (atomic positional I/O without moving the
//! file cursor).

use std::fs::File;
use std::io;

#[cfg(unix)]
use std::os::unix::fs::FileExt;

#[cfg(windows)]
use std::os::windows::fs::FileExt;

/// Read from `file` at the given byte `offset` without changing the file cursor.
pub fn pread(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    #[cfg(unix)]
    {
        file.read_at(buf, offset)?;
        Ok(buf.len())
    }
    #[cfg(windows)]
    {
        file.seek_read(buf, offset)
    }
    // Fallback for platforms without native positional I/O (e.g. wasm32).
    // Uses seek + read which is not atomic, but allows compilation.
    #[cfg(not(any(unix, windows)))]
    {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = file;
        f.seek(SeekFrom::Start(offset))?;
        f.read(buf)
    }
}

/// Write to `file` at the given byte `offset` without changing the file cursor.
pub fn pwrite(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
    #[cfg(unix)]
    {
        file.write_at(buf, offset)?;
        Ok(buf.len())
    }
    #[cfg(windows)]
    {
        file.seek_write(buf, offset)
    }
    // Fallback for platforms without native positional I/O (e.g. wasm32).
    #[cfg(not(any(unix, windows)))]
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = file;
        f.seek(SeekFrom::Start(offset))?;
        f.write(buf)
    }
}
