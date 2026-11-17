//! Real Windows byte-range refusal shared by the writer integration tests.
//! No mocks: the separate recording handle encounters ERROR_LOCK_VIOLATION.
//! https://learn.microsoft.com/en-us/windows/win32/fileio/locking-and-unlocking-byte-ranges-in-files

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
    pub fn new(path: &Path, first_refused_byte: u32) -> io::Result<Self> {
        let mut lock = Self {
            file: File::create(path)?,
            overlapped: Overlapped {
                internal: 0,
                internal_high: 0,
                offset: first_refused_byte,
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
