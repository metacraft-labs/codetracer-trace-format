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
}
