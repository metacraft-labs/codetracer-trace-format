//! Reading the members that annotate a recording, by container path: the span
//! stream and its type index, the line-hit index, the correlation index and
//! the marker labels. Each answer is a JSON document in a buffer the caller
//! releases with `ct_free_buffer`.

use std::os::raw::c_char;

use codetracer_ctfs::CtfsReader;
use codetracer_trace_reader::correlation_reader::{CorrelationMarker, marker_labels, open_correlation_index, open_line_hits};
use codetracer_trace_reader::span_stream_reader::{SpanStreamReader, read_span_type_namespace};
use codetracer_trace_writer::corrmark::{boundary_key, span_key};
use codetracer_trace_writer::span_stream::SpanRecord;

use crate::{alloc_buffer, bytes, cstr_bytes, guarded, path_from_bytes, set_error};

/// A JSON string literal: `"`, `\` and the three common control characters
/// escaped by name, the other ASCII controls as `\u00XX`, every other byte as
/// it is.
fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn span_json(s: &SpanRecord) -> String {
    let b = |v: bool| if v { "true" } else { "false" };
    let metadata: Vec<String> = s
        .metadata
        .iter()
        .map(|(k, v)| format!("[{},{}]", json_string(k), json_string(v)))
        .collect();
    format!(
        "{{\"span_id\":{},\"parent_span_id\":{},\"is_open\":{},\"is_external\":{},\"status\":{},\"start_wall_ns\":{},\
         \"end_wall_ns\":{},\"process_ord\":{},\"thread_id\":{},\"start_step\":{},\"end_step\":{},\"external_recording\":{},\
         \"external_path\":{},\"span_type\":{},\"label\":{},\"contiguous_on_one_thread\":{},\"shares_timeline\":{},\
         \"concurrent_with_siblings\":{},\"metadata\":[{}]}}",
        s.span_id,
        s.parent_span_id,
        b(s.is_open),
        b(s.is_external),
        s.status,
        s.start_wall_ns,
        s.end_wall_ns,
        s.process_ord,
        s.thread_id,
        s.start_step,
        s.end_step,
        json_string(&s.external_recording),
        json_string(&s.external_path),
        json_string(&s.span_type),
        json_string(&s.label),
        b(s.contiguous_on_one_thread),
        b(s.shares_timeline),
        b(s.concurrent_with_siblings),
        metadata.join(",")
    )
}

fn hex(data: &[u8]) -> String {
    data.iter().map(|b| format!("{b:02x}")).collect()
}

fn entry_json(key: u64, m: &CorrelationMarker) -> String {
    format!(
        "{{\"key\":{key},\"kind\":{},\"flags\":{},\"identity\":\"{}\",\"wall_time_unix_ns\":{},\"monotonic_time_ns\":{},\
         \"geid\":{},\"thread_id\":{}}}",
        m.kind,
        m.flags,
        hex(&m.identity),
        m.wall_time_unix_ns,
        m.monotonic_time_ns,
        m.geid,
        m.thread_id
    )
}

fn json_array(items: impl IntoIterator<Item = String>) -> String {
    format!("[{}]", items.into_iter().collect::<Vec<_>>().join(","))
}

/// The container at `path`, or the failure to read it, prefixed with `what`.
fn open(path: *const c_char, what: &str) -> Result<CtfsReader, String> {
    if path.is_null() {
        return Err(format!("{what}: NULL path"));
    }
    let p = path_from_bytes(unsafe { cstr_bytes(path) });
    let data = std::fs::read(&p).map_err(|_| format!("{what}: cannot read {}", p.display()))?;
    CtfsReader::from_bytes(data).map_err(|e| format!("{what}: {e}"))
}

// ---------------------------------------------------------------------------
// The span stream
// ---------------------------------------------------------------------------

/// A JSON document as a fresh buffer, its length in `*out_len`.
unsafe fn document(doc: String, what: &str, out_len: *mut usize) -> *mut u8 {
    let buf = alloc_buffer(doc.as_bytes());
    if buf.is_null() {
        set_error(&format!("{what}: out of memory"));
        return std::ptr::null_mut();
    }
    unsafe { *out_len = doc.len() };
    buf
}

/// The span records of the container at `path`, as a JSON array: settled
/// (last record wins, by span id) when `settled != 0`, else every record in
/// append order. NULL on failure, a container with no span stream included.
///
/// # Safety
/// `path` is NULL or NUL-terminated; `out_len` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_spans_json(path: *const c_char, settled: i32, out_len: *mut usize) -> *mut u8 {
    guarded("ct_spans_json", None, std::ptr::null_mut(), || {
        if out_len.is_null() {
            set_error("ct_spans_json: NULL outLen");
            return std::ptr::null_mut();
        }
        unsafe { *out_len = 0 };
        let spans = open(path, "ct_spans_json").and_then(|mut r| {
            let reader = SpanStreamReader::open(&mut r)
                .map_err(|e| format!("ct_spans_json: {e}"))?
                .ok_or("ct_spans_json: failed to read spans.dat: the container has no span stream")?;
            if settled != 0 {
                reader.settled_spans()
            } else {
                reader.read_all_span_records()
            }
            .map_err(|e| format!("ct_spans_json: {e}"))
        });
        match spans {
            Ok(spans) => unsafe { document(json_array(spans.iter().map(span_json)), "ct_spans_json", out_len) },
            Err(e) => {
                set_error(&e);
                std::ptr::null_mut()
            }
        }
    })
}

/// The span-type index of the container at `path`, as a JSON array of
/// `{"type_id","name","span_ids"}`. NULL on failure, a container with no
/// span-type index included.
///
/// # Safety
/// `path` is NULL or NUL-terminated; `out_len` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_span_types_json(path: *const c_char, out_len: *mut usize) -> *mut u8 {
    guarded("ct_span_types_json", None, std::ptr::null_mut(), || {
        if out_len.is_null() {
            set_error("ct_span_types_json: NULL outLen");
            return std::ptr::null_mut();
        }
        unsafe { *out_len = 0 };
        let entries = open(path, "ct_span_types_json").and_then(|mut r| {
            read_span_type_namespace(&mut r)
                .map_err(|e| format!("ct_span_types_json: {e}"))?
                .ok_or_else(|| "ct_span_types_json: failed to read spantype.ns: the container has no span-type index".to_string())
        });
        match entries {
            Ok(entries) => {
                let doc = json_array(entries.iter().map(|e| {
                    format!(
                        "{{\"type_id\":{},\"name\":{},\"span_ids\":{}}}",
                        e.type_id,
                        json_string(&e.name),
                        json_array(e.span_ids.iter().map(u64::to_string))
                    )
                }));
                unsafe { document(doc, "ct_span_types_json", out_len) }
            }
            Err(e) => {
                set_error(&e);
                std::ptr::null_mut()
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Line hits, the correlation index and the marker labels
// ---------------------------------------------------------------------------

/// Run a read whose answer is a document, `None` for a member the container
/// lacks: 0 with the document, 1 with nothing, -1 on failure.
fn read_side(what: &str, out_buf: *mut *mut u8, out_len: *mut usize, read: impl FnOnce() -> Result<Option<String>, String>) -> i32 {
    guarded(what, None, -1, || {
        if out_buf.is_null() || out_len.is_null() {
            set_error(&format!("{what}: NULL out_buf or out_len"));
            return -1;
        }
        unsafe {
            *out_buf = std::ptr::null_mut();
            *out_len = 0;
        }
        match read() {
            Ok(Some(doc)) => {
                let buf = alloc_buffer(doc.as_bytes());
                if buf.is_null() {
                    set_error(&format!("{what}: out of memory"));
                    return -1;
                }
                unsafe {
                    *out_buf = buf;
                    *out_len = doc.len();
                }
                0
            }
            Ok(None) => 1,
            Err(e) => {
                set_error(&e);
                -1
            }
        }
    })
}

/// `[{"position":P,"steps":[s,...]},...]` in position order; 1 when the
/// container keeps no line-hit index.
///
/// # Safety
/// `path` is NULL or NUL-terminated; `out_buf` and `out_len` are NULL or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_linehits_json(path: *const c_char, out_buf: *mut *mut u8, out_len: *mut usize) -> i32 {
    const WHAT: &str = "ct_linehits_json";
    read_side(WHAT, out_buf, out_len, || {
        let mut r = open(path, WHAT)?;
        let Some(hits) = open_line_hits(&mut r).map_err(|e| format!("{WHAT}: {e}"))? else {
            return Ok(None);
        };
        let mut items = Vec::new();
        for p in hits.positions() {
            let steps = hits.hits(p).map_err(|e| format!("{WHAT}: {e}"))?.unwrap_or_default();
            items.push(format!("{{\"position\":{p},\"steps\":{}}}", json_array(steps.iter().map(u64::to_string))));
        }
        Ok(Some(json_array(items)))
    })
}

/// Every correlation-index entry, in key order and bucket order; 1 when the
/// container is not indexed.
///
/// # Safety
/// `path` is NULL or NUL-terminated; `out_buf` and `out_len` are NULL or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_correlation_index_json(path: *const c_char, out_buf: *mut *mut u8, out_len: *mut usize) -> i32 {
    const WHAT: &str = "ct_correlation_index_json";
    read_side(WHAT, out_buf, out_len, || {
        let mut r = open(path, WHAT)?;
        let Some(idx) = open_correlation_index(&mut r).map_err(|e| format!("{WHAT}: {e}"))? else {
            return Ok(None);
        };
        let entries = idx.entries().map_err(|e| format!("{WHAT}: {e}"))?;
        Ok(Some(json_array(entries.iter().map(|(k, m)| entry_json(*k, m)))))
    })
}

/// The confirmed kind-0 entries for `(trace_id, span_id)`, wire bytes; 1
/// when the container is not indexed.
///
/// # Safety
/// `path` is NULL or NUL-terminated; both `(pointer, length)` pairs are
/// readable or NULL/0; `out_buf` and `out_len` are NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_correlation_lookup_span(
    path: *const c_char,
    trace_id: *const u8,
    trace_id_len: usize,
    span_id: *const u8,
    span_id_len: usize,
    out_buf: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    const WHAT: &str = "ct_correlation_lookup_span";
    read_side(WHAT, out_buf, out_len, || {
        if trace_id.is_null() || span_id.is_null() || trace_id_len != 16 || span_id_len != 8 {
            return Err(format!("{WHAT}: trace_id must be 16 bytes and span_id 8"));
        }
        let t: [u8; 16] = unsafe { bytes(trace_id, 16) }.try_into().expect("16 bytes");
        let s: [u8; 8] = unsafe { bytes(span_id, 8) }.try_into().expect("8 bytes");
        let mut r = open(path, WHAT)?;
        let Some(idx) = open_correlation_index(&mut r).map_err(|e| format!("{WHAT}: {e}"))? else {
            return Ok(None);
        };
        let hits = idx.lookup_span(&t, &s).map_err(|e| format!("{WHAT}: {e}"))?;
        let key = span_key(&t, &s);
        Ok(Some(json_array(hits.iter().map(|m| entry_json(key, m)))))
    })
}

/// The confirmed kind-1 entries for `(marker_id, key_value)`; 1 when the
/// container is not indexed.
///
/// # Safety
/// `path` is NULL or NUL-terminated; `(key_value, key_value_len)` is readable
/// or NULL/0; `out_buf` and `out_len` are NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_correlation_lookup_boundary(
    path: *const c_char,
    marker_id: u64,
    key_value: *const u8,
    key_value_len: usize,
    out_buf: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    const WHAT: &str = "ct_correlation_lookup_boundary";
    read_side(WHAT, out_buf, out_len, || {
        let mut r = open(path, WHAT)?;
        let Some(idx) = open_correlation_index(&mut r).map_err(|e| format!("{WHAT}: {e}"))? else {
            return Ok(None);
        };
        let key_value = unsafe { bytes(key_value, key_value_len) };
        let hits = idx.lookup_boundary(marker_id, key_value).map_err(|e| format!("{WHAT}: {e}"))?;
        let key = boundary_key(marker_id, key_value);
        Ok(Some(json_array(hits.iter().map(|m| entry_json(key, m)))))
    })
}

/// The marker labels in id order, each as the lowercase hex of its bytes; 1
/// when the container declares no marker.
///
/// # Safety
/// `path` is NULL or NUL-terminated; `out_buf` and `out_len` are NULL or
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_marker_labels_json(path: *const c_char, out_buf: *mut *mut u8, out_len: *mut usize) -> i32 {
    const WHAT: &str = "ct_marker_labels_json";
    read_side(WHAT, out_buf, out_len, || {
        let mut r = open(path, WHAT)?;
        let Some(labels) = marker_labels(&mut r).map_err(|e| format!("{WHAT}: {e}"))? else {
            return Ok(None);
        };
        Ok(Some(json_array(labels.iter().map(|l| format!("\"{}\"", hex(l))))))
    })
}
