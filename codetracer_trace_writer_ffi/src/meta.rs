//! `meta.dat` encoded to a buffer and read back, buffers handed to the caller,
//! and internal files added to a closed container.

use std::os::raw::c_char;

use codetracer_trace_writer::meta_dat::{MetaDat, decode_meta_dat, encode_meta_dat};

use crate::{alloc_buffer, bytes, cstr_bytes, free_buffer, guarded, path_from_bytes, set_error};

/// Whether `id` is a canonical UUIDv7 in its 36-character lowercase text form.
pub(crate) fn validate_recording_id(id: &str) -> Result<(), String> {
    let s = id.as_bytes();
    if s.len() != 36 {
        return Err(format!("recording_id: expected 36 chars, got {}", s.len()));
    }
    for (i, &c) in s.iter().enumerate() {
        let hyphen = matches!(i, 8 | 13 | 18 | 23);
        if hyphen && c != b'-' {
            return Err(format!("recording_id: expected '-' at position {i}"));
        }
        if !hyphen && !matches!(c, b'0'..=b'9' | b'a'..=b'f') {
            return Err(format!("recording_id: non-lowercase-hex character at position {i}"));
        }
    }
    if s[14] != b'7' {
        return Err("recording_id: expected version nibble '7' at position 14 (not a UUIDv7)".to_string());
    }
    if !matches!(s[19], b'8' | b'9' | b'a' | b'b') {
        return Err("recording_id: expected variant nibble in {8,9,a,b} at position 19".to_string());
    }
    Ok(())
}

/// Encode a version 6 `meta.dat` into a buffer the caller releases with
/// [`ct_free_buffer`]. A NULL or empty `recording_id` mints one.
///
/// # Safety
/// Every `(pointer, length)` pair is readable or NULL/0; `args` and
/// `arg_lens` hold `args_count` entries; `out_buf` and `out_len` are writable.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn ct_write_meta_dat_to_buffer(
    program: *const u8,
    program_len: usize,
    workdir: *const u8,
    workdir_len: usize,
    args: *const *const u8,
    arg_lens: *const usize,
    args_count: usize,
    recorder_id: *const u8,
    recorder_id_len: usize,
    recording_id: *const u8,
    recording_id_len: usize,
    out_buf: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    guarded("ct_write_meta_dat_to_buffer", None, 1, || {
        if out_buf.is_null() || out_len.is_null() {
            set_error("NULL output pointers");
            return 1;
        }
        let text = |p: *const u8, n: usize| String::from_utf8_lossy(unsafe { bytes(p, n) }).into_owned();
        let mut arg_list = Vec::with_capacity(args_count);
        for i in 0..args_count {
            let (p, n) = if args.is_null() || arg_lens.is_null() {
                (std::ptr::null(), 0)
            } else {
                unsafe { (*args.add(i), *arg_lens.add(i)) }
            };
            arg_list.push(text(p, n));
        }
        let id = if recording_id.is_null() || recording_id_len == 0 {
            codetracer_trace_types::TraceMetadata::new("", vec![], Default::default()).recording_id
        } else {
            let id = text(recording_id, recording_id_len);
            if let Err(e) = validate_recording_id(&id) {
                set_error(&format!("recording_id not a canonical UUIDv7: {e}"));
                return 1;
            }
            id
        };
        let encoded = encode_meta_dat(
            &id,
            &text(program, program_len),
            &arg_list,
            &text(workdir, workdir_len),
            &text(recorder_id, recorder_id_len),
            0,
        );
        let buf = alloc_buffer(&encoded);
        if buf.is_null() {
            set_error("allocation failed");
            return 1;
        }
        unsafe {
            *out_buf = buf;
            *out_len = encoded.len();
        }
        0
    })
}

/// Release a buffer this library handed out. NULL is a no-op.
///
/// # Safety
/// `buf` is NULL or a buffer this library returned and not yet released.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_free_buffer(buf: *mut u8) {
    guarded("ct_free_buffer", None, (), || unsafe { free_buffer(buf) })
}

// ---------------------------------------------------------------------------
// meta.dat reader handle
// ---------------------------------------------------------------------------

pub struct MetaDatReader(MetaDat);

/// Decode a `meta.dat`. NULL on failure.
///
/// # Safety
/// `(data, len)` is readable or NULL/0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_read_meta_dat(data: *const u8, len: usize) -> *mut MetaDatReader {
    guarded("ct_read_meta_dat", None, std::ptr::null_mut(), || {
        if data.is_null() || len == 0 {
            set_error("NULL or empty data");
            return std::ptr::null_mut();
        }
        match decode_meta_dat(unsafe { bytes(data, len) }) {
            Ok(m) => Box::into_raw(Box::new(MetaDatReader(m))),
            Err(e) => {
                set_error(&e);
                std::ptr::null_mut()
            }
        }
    })
}

unsafe fn field(h: *mut MetaDatReader, out_len: *mut usize, get: impl FnOnce(&MetaDat) -> &[u8]) -> *const u8 {
    if h.is_null() || out_len.is_null() {
        return std::ptr::null();
    }
    let value = get(&unsafe { &*h }.0);
    unsafe { *out_len = value.len() };
    if value.is_empty() { std::ptr::null() } else { value.as_ptr() }
}

/// # Safety
/// `h` is NULL or a live `meta.dat` reader; `out_len` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_meta_dat_program(h: *mut MetaDatReader, out_len: *mut usize) -> *const u8 {
    guarded("ct_meta_dat_program", None, std::ptr::null(), || unsafe {
        field(h, out_len, |m| m.program.as_bytes())
    })
}

/// # Safety
/// `h` is NULL or a live `meta.dat` reader; `out_len` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_meta_dat_workdir(h: *mut MetaDatReader, out_len: *mut usize) -> *const u8 {
    guarded("ct_meta_dat_workdir", None, std::ptr::null(), || unsafe {
        field(h, out_len, |m| m.workdir.as_bytes())
    })
}

/// # Safety
/// `h` is NULL or a live `meta.dat` reader; `out_len` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_meta_dat_recorder_id(h: *mut MetaDatReader, out_len: *mut usize) -> *const u8 {
    guarded("ct_meta_dat_recorder_id", None, std::ptr::null(), || unsafe {
        field(h, out_len, |m| m.recorder_id.as_bytes())
    })
}

/// # Safety
/// `h` is NULL or a live `meta.dat` reader; `out_len` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_meta_dat_recording_id(h: *mut MetaDatReader, out_len: *mut usize) -> *const u8 {
    guarded("ct_meta_dat_recording_id", None, std::ptr::null(), || unsafe {
        field(h, out_len, |m| m.recording_id.as_bytes())
    })
}

/// # Safety
/// `h` is NULL or a live `meta.dat` reader.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_meta_dat_args_count(h: *mut MetaDatReader) -> usize {
    guarded("ct_meta_dat_args_count", None, 0, || {
        if h.is_null() { 0 } else { unsafe { &*h }.0.args.len() }
    })
}

/// # Safety
/// `h` is NULL or a live `meta.dat` reader; `out_len` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_meta_dat_arg(h: *mut MetaDatReader, idx: usize, out_len: *mut usize) -> *const u8 {
    guarded("ct_meta_dat_arg", None, std::ptr::null(), || {
        if h.is_null() || out_len.is_null() || idx >= unsafe { &*h }.0.args.len() {
            return std::ptr::null();
        }
        unsafe { field(h, out_len, |m| m.args[idx].as_bytes()) }
    })
}

/// 1 when the `meta.dat` carries a filter-provenance block, else 0.
///
/// # Safety
/// `h` is NULL or a live `meta.dat` reader.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_meta_dat_has_filter_provenance(h: *mut MetaDatReader) -> i32 {
    guarded("ct_meta_dat_has_filter_provenance", None, 0, || {
        if h.is_null() {
            return 0;
        }
        i32::from(unsafe { &*h }.0.blocks.filter_provenance.is_some())
    })
}

/// # Safety
/// `h` is NULL or a live `meta.dat` reader.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_meta_dat_filter_provenance_count(h: *mut MetaDatReader) -> usize {
    guarded("ct_meta_dat_filter_provenance_count", None, 0, || {
        if h.is_null() {
            return 0;
        }
        unsafe { &*h }.0.blocks.filter_provenance.as_ref().map_or(0, Vec::len)
    })
}

/// # Safety
/// `h` is NULL or a live `meta.dat` reader; `out_len` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_meta_dat_filter_provenance_path(h: *mut MetaDatReader, idx: usize, out_len: *mut usize) -> *const u8 {
    guarded("ct_meta_dat_filter_provenance_path", None, std::ptr::null(), || {
        let entries = if h.is_null() {
            None
        } else {
            unsafe { &*h }.0.blocks.filter_provenance.as_ref()
        };
        let Some(entry) = entries.and_then(|e| e.get(idx)) else {
            if !out_len.is_null() {
                unsafe { *out_len = 0 };
            }
            return std::ptr::null();
        };
        if out_len.is_null() {
            return std::ptr::null();
        }
        unsafe { *out_len = entry.path.len() };
        if entry.path.is_empty() { std::ptr::null() } else { entry.path.as_ptr() }
    })
}

/// Copy the 32-byte digest of filter-provenance entry `idx` to `out_buf`. 0 on
/// success.
///
/// # Safety
/// `h` is NULL or a live `meta.dat` reader; `out_buf` is NULL or 32 writable
/// bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_meta_dat_filter_provenance_sha256(h: *mut MetaDatReader, idx: usize, out_buf: *mut u8) -> i32 {
    guarded("ct_meta_dat_filter_provenance_sha256", None, 1, || {
        let entries = if h.is_null() {
            None
        } else {
            unsafe { &*h }.0.blocks.filter_provenance.as_ref()
        };
        let Some(entry) = entries.and_then(|e| e.get(idx)) else {
            return 1;
        };
        if out_buf.is_null() {
            return 1;
        }
        unsafe { std::ptr::copy_nonoverlapping(entry.sha256.as_ptr(), out_buf, 32) };
        0
    })
}

/// # Safety
/// `h` is NULL or a live `meta.dat` reader, not used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_meta_dat_free(h: *mut MetaDatReader) {
    guarded("ct_meta_dat_free", None, (), || {
        if !h.is_null() {
            drop(unsafe { Box::from_raw(h) });
        }
    })
}

// ---------------------------------------------------------------------------
// Internal files added to a closed container
// ---------------------------------------------------------------------------

/// Write a new, empty container at `path`; `block_size` 0 is 4096.
///
/// # Safety
/// `path` is NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_container_create(path: *const c_char, block_size: u32) -> i32 {
    guarded("ct_container_create", None, 1, || {
        if path.is_null() {
            set_error("ct_container_create: path is NULL");
            return 1;
        }
        let bs = if block_size == 0 { 4096 } else { block_size };
        if bs % 8 != 0
            || (bs as usize)
                < codetracer_ctfs::header::HEADER_SIZE + codetracer_ctfs::header::EXTENDED_HEADER_SIZE + codetracer_ctfs::file_entry::FILE_ENTRY_SIZE
        {
            set_error(&format!("ct_container_create: unusable block size {bs}"));
            return 1;
        }
        let p = path_from_bytes(unsafe { cstr_bytes(path) });
        // The default root-directory size of every container writer.
        let max_entries = 31;
        match codetracer_ctfs::CtfsWriter::create(&p, bs, max_entries).and_then(|w| w.close()) {
            Ok(()) => 0,
            Err(e) => {
                set_error(&format!("ct_container_create: {e}"));
                1
            }
        }
    })
}

/// Append `count` internal files to the closed container at `path`, as one
/// batch: a name already present, or repeated, refuses the whole batch.
///
/// # Safety
/// `path` is NULL or NUL-terminated; `names`, `contents` and `lengths` hold
/// `count` entries (or are NULL when `count` is 0); `contents[i]` holds
/// `lengths[i]` readable bytes or is NULL when that is 0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_container_append_files(
    path: *const c_char,
    names: *const *const c_char,
    contents: *const *const u8,
    lengths: *const usize,
    count: usize,
) -> i32 {
    guarded("ct_container_append_files", None, 1, || {
        if path.is_null() {
            set_error("ct_container_append_files: path is NULL");
            return 1;
        }
        if count == 0 {
            return 0;
        }
        if names.is_null() || lengths.is_null() {
            set_error("ct_container_append_files: names/lengths array is NULL");
            return 1;
        }
        let mut files = Vec::with_capacity(count);
        for i in 0..count {
            let name = unsafe { *names.add(i) };
            if name.is_null() {
                set_error(&format!("ct_container_append_files: names[{i}] is NULL"));
                return 1;
            }
            let n = unsafe { *lengths.add(i) };
            let data: &[u8] = if n == 0 {
                &[]
            } else {
                let p = if contents.is_null() {
                    std::ptr::null()
                } else {
                    unsafe { *contents.add(i) }
                };
                if p.is_null() {
                    set_error(&format!("ct_container_append_files: contents[{i}] is NULL but its length is {n}"));
                    return 1;
                }
                unsafe { std::slice::from_raw_parts(p, n) }
            };
            files.push((String::from_utf8_lossy(unsafe { cstr_bytes(name) }).into_owned(), data));
        }
        match append_files(&path_from_bytes(unsafe { cstr_bytes(path) }), &files) {
            Ok(()) => 0,
            Err(e) => {
                set_error(&e);
                1
            }
        }
    })
}

fn append_files(path: &std::path::Path, files: &[(String, &[u8])]) -> Result<(), String> {
    let at = |msg: String| format!("ctfs append: {}: {msg}", path.display());
    let mut encoded = Vec::with_capacity(files.len());
    for (name, _) in files {
        let code = codetracer_ctfs::encode_member_name(name).map_err(|e| format!("ctfs append: \"{name}\" is not a CTFS internal filename: {e}"))?;
        if encoded.contains(&code) {
            return Err(format!("ctfs append: {name} appears twice in one batch"));
        }
        encoded.push(code);
    }
    let mut writer = codetracer_ctfs::CtfsWriter::open_append(path).map_err(|e| at(e.to_string()))?;
    for (name, _) in files {
        if writer.find_file(name).is_some() {
            return Err(at(format!(
                "already contains an internal file named {name}; CTFS is append-only and this writer will not overwrite it"
            )));
        }
    }
    for (name, data) in files {
        let h = writer.add_file(name).map_err(|e| at(format!("cannot add {name}: {e}")))?;
        writer.write(h, data).map_err(|e| at(format!("cannot write {name}: {e}")))?;
    }
    writer.close().map_err(|e| at(e.to_string()))
}
