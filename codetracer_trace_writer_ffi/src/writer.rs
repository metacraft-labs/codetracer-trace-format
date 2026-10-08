//! `trace_writer_*`: a recording written through a handle.
//!
//! `FFI_TRACE_FORMAT_BINARY` writes a split-stream CTFS container — the one
//! the Nim library writes, byte for byte — to `<dir>/<program stem>.ct`, `dir`
//! being the directory of the path `trace_writer_begin_events` is given, or in
//! memory after `trace_writer_begin_in_memory`. The other two formats write
//! the JSON and binary-v0 files of this repository's non-container writer;
//! the calls that need a container refuse on them.
//!
//! # The step being buffered
//!
//! `trace_writer_register_step` does not write its step at once. It holds it,
//! so that what the host registers next — values, assignments, drops, a
//! column — lands in that step's record, and writes it when the host moves on
//! (another step, a call, a return, a thread event, an exception, a reload, a
//! crossing, or the close). Values and value-stream events registered while no
//! step is held are held too, and join the next step written; at the close,
//! the last one.

use std::collections::HashMap;
use std::os::raw::c_char;
use std::path::{Path, PathBuf};

use codetracer_trace_types::{
    AssignCellRecord, AssignCompoundItemRecord, AssignmentRecord, BindVariableRecord, CallKey, CellValueRecord, CompoundValueRecord, EventLogKind,
    FullValueRecord, FunctionId, Line, PassBy, PathId, Place, RValue, ThreadId, TraceLowLevelEvent, TypeKind, ValueRecord, VariableCellRecord,
    VariableId,
};
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::ctfs_writer::{CtfsTraceWriter, INVALID_PATH_ID};
use codetracer_trace_writer::meta_dat::{AtomicMode, FilterProvenance, LayoutSnapshot, McrFields, MetaDatBlocks, ReplayLaunchFields, TickSource};
use codetracer_trace_writer::step_stream::SourceReloadChange;
use codetracer_trace_writer::trace_writer::TraceWriter;
use codetracer_trace_writer::{TraceEventsFileFormat, create_trace_writer};
use num_traits::FromPrimitive;

use crate::meta::validate_recording_id;
use crate::value_encoder::{int_value, raw_value};
use crate::{bytes, cstr_bytes, cstr_string, guarded, path_from_bytes, set_error, set_notice};

/// The failure return of the path-id calls.
pub const CT_TW_INVALID_PATH_ID: u64 = u64::MAX;

const FORMAT_JSON: i32 = 0;
const FORMAT_BINARY_V0: i32 = 1;
const FORMAT_BINARY: i32 = 2;

/// A value or value-stream event waiting for the step it belongs to.
enum Held {
    /// A variable's value, already encoded.
    Value(VariableId, Vec<u8>),
    /// A value-stream event, with the encoded payload it carries, if any.
    Event(TraceLowLevelEvent, Option<Vec<u8>>),
}

/// The writer behind a `trace_writer_t`.
pub struct TraceWriterHandle {
    /// The first failure of a call that could not report it; the close
    /// reports it.
    failure: Option<String>,
    format: i32,
    program: String,
    workdir: String,
    args: Vec<String>,
    recording_id: Option<String>,
    qualifier: String,
    compact_threshold: u64,
    meta_blocks: MetaDatBlocks,
    in_memory: bool,
    /// The container writer, once begun.
    ctfs: Option<CtfsTraceWriter>,
    /// The non-container writer, for the JSON and binary-v0 formats.
    legacy: Option<Box<dyn TraceWriter + Send>>,
    closed: bool,
    container: Vec<u8>,
    container_ready: bool,
    /// The step being buffered: path id, line, column delta.
    pending_step: Option<(PathId, i64, i64)>,
    held: Vec<Held>,
    call_args: Vec<(u64, Vec<u8>)>,
    /// Calls opened and not yet returned.
    open_calls: u64,
    function_ids: HashMap<String, usize>,
    type_ids: HashMap<(i32, String), usize>,
    /// One more than the highest type id handed out.
    type_count: usize,
}

impl TraceWriterHandle {
    pub(crate) fn latch(&mut self, msg: String) {
        self.failure.get_or_insert(msg);
    }

    fn multi(&self) -> bool {
        self.format == FORMAT_BINARY
    }
}

/// Run an entry point on a handle: `body` gets the handle, or the entry point
/// returns `null_value` (with no error) on NULL. `latch` holds any failure the
/// body reports on the handle.
fn with_handle<R>(
    name: &str,
    handle: *mut TraceWriterHandle,
    latch: bool,
    null_value: R,
    fail: R,
    body: impl FnOnce(&mut TraceWriterHandle) -> R,
) -> R {
    guarded(name, if latch { Some(handle) } else { None }, fail, || {
        if handle.is_null() {
            return null_value;
        }
        // SAFETY: a non-NULL handle is one `trace_writer_new` returned and not
        // yet freed (the handle invariant).
        body(unsafe { &mut *handle })
    })
}

/// Like [`with_handle`], but a NULL handle is a failure reported as `msg`.
fn with_handle_or<R: Copy>(
    name: &str,
    handle: *mut TraceWriterHandle,
    latch: bool,
    fail: R,
    msg: &str,
    body: impl FnOnce(&mut TraceWriterHandle) -> R,
) -> R {
    guarded(name, if latch { Some(handle) } else { None }, fail, || {
        if handle.is_null() {
            set_error(msg);
            return fail;
        }
        body(unsafe { &mut *handle })
    })
}

/// Report the refusals `w` recorded since it held `before` of them.
fn report_refusals(w: &CtfsTraceWriter, before: usize) -> bool {
    match w.refusals().get(before) {
        Some(r) => {
            set_error(r);
            false
        }
        None => true,
    }
}

fn to_line(line: i64) -> Line {
    Line(line)
}

// ---------------------------------------------------------------------------
// Writing what is held
// ---------------------------------------------------------------------------

fn emit_held(w: &mut CtfsTraceWriter, held: Vec<Held>) {
    for item in held {
        match item {
            Held::Value(variable_id, cbor) => w.register_value_event_cbor(
                TraceLowLevelEvent::Value(FullValueRecord {
                    variable_id,
                    value: codetracer_trace_types::NONE_VALUE,
                }),
                cbor,
            ),
            Held::Event(event, Some(cbor)) => w.register_value_event_cbor(event, cbor),
            Held::Event(event, None) => AbstractTraceWriter::add_event(w, event),
        }
    }
}

/// Write the buffered step and what was registered for it. A step the writer
/// refuses stays buffered, with what was held for it, and is offered again at
/// the next flush.
fn flush_pending_step(h: &mut TraceWriterHandle) -> Result<(), ()> {
    let Some((path_id, line, delta)) = h.pending_step else {
        return Ok(());
    };
    let Some(w) = h.ctfs.as_mut() else {
        return Ok(());
    };
    if delta != 0 && !w.column_aware_steps_enabled() {
        set_error(
            "registerStepWithColumn(columnDelta != 0) called on a writer that has not opted into column-aware mode \
             (call enableColumnAwareSteps first)",
        );
        return Err(());
    }
    let before = w.refusals().len();
    let count = w.exec_record_count();
    let column = (delta != 0).then(|| Line(delta + 1));
    w.register_step_at(path_id, to_line(line), column);
    if w.exec_record_count() == count {
        report_refusals(w, before);
        if w.refusals().len() == before {
            set_error("the buffered step could not be written");
        }
        return Err(());
    }
    h.pending_step = None;
    let held = std::mem::take(&mut h.held);
    emit_held(w, held);
    Ok(())
}

/// What is still held at the close joins the last step written; with none,
/// there is nowhere to put it.
fn flush_trailing(h: &mut TraceWriterHandle) -> Result<(), ()> {
    if h.held.is_empty() {
        return Ok(());
    }
    let Some(w) = h.ctfs.as_mut() else {
        return Ok(());
    };
    if w.exec_record_count() == 0 {
        set_error(&format!(
            "close: {} value(s) or value-stream event(s) are staged, but no step was ever recorded, so there is no \
             position to attach them to. Register a step before closing, or do not stage them.",
            h.held.len()
        ));
        return Err(());
    }
    let held = std::mem::take(&mut h.held);
    emit_held(w, held);
    Ok(())
}

/// The id of `path` for a step: its newest version, interned now if it is new.
fn path_id_for_step(w: &mut CtfsTraceWriter, path: &Path) -> Option<PathId> {
    let before = w.refusals().len();
    let id = AbstractTraceWriter::ensure_path_id(w, path);
    if id == INVALID_PATH_ID {
        report_refusals(w, before);
        return None;
    }
    Some(id)
}

/// Finish the container: the buffered step, what is held, the writer.
fn close_container(h: &mut TraceWriterHandle, who: &str) -> i32 {
    let mut rc = 0;
    if flush_pending_step(h).is_err() {
        rc = 1;
    }
    if rc == 0 && flush_trailing(h).is_err() {
        rc = 1;
    }
    if rc != 0 {
        return rc;
    }
    let Some(w) = h.ctfs.as_mut() else {
        return 0;
    };
    h.closed = true;
    let result = TraceWriter::finish_writing_trace_events(w);
    if h.in_memory
        && let Some(bytes) = w.take_container_bytes()
    {
        h.container = bytes;
        h.container_ready = true;
    }
    if let Err(e) = result {
        set_error(&format!("{who}: {e}"));
        return 1;
    }
    0
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/// Create a writer. `format` 2 (`FFI_TRACE_FORMAT_BINARY`) is the split-stream
/// container; 0 and 1 the non-container JSON and binary-v0 files.
///
/// # Safety
/// `program` is NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_new(program: *const c_char, format: i32) -> *mut TraceWriterHandle {
    guarded("trace_writer_new", None, std::ptr::null_mut(), || {
        let program = unsafe { cstr_string(program) };
        let legacy = match format {
            FORMAT_BINARY => None,
            FORMAT_JSON => Some(create_trace_writer(&program, &[], TraceEventsFileFormat::Json)),
            FORMAT_BINARY_V0 => Some(create_trace_writer(&program, &[], TraceEventsFileFormat::BinaryV0)),
            other => {
                set_error(&format!("trace_writer_new: unknown format {other}"));
                return std::ptr::null_mut();
            }
        };
        Box::into_raw(Box::new(TraceWriterHandle {
            failure: None,
            format,
            program,
            workdir: String::new(),
            args: Vec::new(),
            recording_id: None,
            qualifier: String::new(),
            compact_threshold: 0,
            meta_blocks: MetaDatBlocks::default(),
            in_memory: false,
            ctfs: None,
            legacy,
            closed: false,
            container: Vec::new(),
            container_ready: false,
            pending_step: None,
            held: Vec::new(),
            call_args: Vec::new(),
            open_calls: 0,
            function_ids: HashMap::new(),
            type_ids: HashMap::new(),
            type_count: 0,
        }))
    })
}

/// Free a writer, finishing its container first when it was not closed.
///
/// # Safety
/// `handle` is NULL or a writer not yet freed, not used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_free(handle: *mut TraceWriterHandle) {
    guarded("trace_writer_free", None, (), || {
        if handle.is_null() {
            return;
        }
        let mut h = unsafe { Box::from_raw(handle) };
        if h.multi() && h.ctfs.is_some() && !h.closed {
            if flush_pending_step(&mut h).is_err() {
                set_error("trace_writer_free: failed to flush the pending step");
            }
            if flush_trailing(&mut h).is_err() {
                set_error("trace_writer_free: failed to record the trailing values");
            }
            h.held.clear();
            h.pending_step = None;
            if let Some(w) = h.ctfs.as_mut()
                && let Err(e) = TraceWriter::finish_writing_trace_events(w)
            {
                set_error(&format!("trace_writer_free: close failed: {e}"));
            }
        }
    })
}

/// Close the writer: write the buffered step and what is held, and finish
/// the container. 0 on success; non-zero when the close failed or an earlier
/// call's failure was held.
///
/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_close(handle: *mut TraceWriterHandle) -> i32 {
    with_handle_or("trace_writer_close", handle, false, 1, "NULL handle", |h| {
        let rc = if h.multi() {
            if h.ctfs.is_none() || h.closed {
                0
            } else {
                close_container(h, "trace_writer_close")
            }
        } else {
            0
        };
        if rc == 0
            && let Some(failure) = &h.failure
        {
            set_error(&format!(
                "trace_writer_close: the container was finalized, but it is incomplete because an earlier call failed: {failure}"
            ));
            return 1;
        }
        rc
    })
}

/// # Safety
/// `handle` is NULL or a live writer; `path` is NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_begin_metadata(handle: *mut TraceWriterHandle, path: *const c_char) -> i32 {
    with_handle_or("trace_writer_begin_metadata", handle, false, 1, "NULL handle", |h| {
        match h.legacy.as_mut() {
            Some(w) => legacy_result(TraceWriter::begin_writing_trace_metadata(
                &mut **w,
                &path_from_bytes(unsafe { cstr_bytes(path) }),
            )),
            None => 0,
        }
    })
}

/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_finish_metadata(handle: *mut TraceWriterHandle) -> i32 {
    with_handle_or("trace_writer_finish_metadata", handle, false, 1, "NULL handle", |h| {
        match h.legacy.as_mut() {
            Some(w) => legacy_result(TraceWriter::finish_writing_trace_metadata(&mut **w)),
            None => 0,
        }
    })
}

/// # Safety
/// `handle` is NULL or a live writer; `path` is NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_begin_paths(handle: *mut TraceWriterHandle, path: *const c_char) -> i32 {
    with_handle_or("trace_writer_begin_paths", handle, false, 1, "NULL handle", |h| match h.legacy.as_mut() {
        Some(w) => legacy_result(TraceWriter::begin_writing_trace_paths(
            &mut **w,
            &path_from_bytes(unsafe { cstr_bytes(path) }),
        )),
        None => 0,
    })
}

/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_finish_paths(handle: *mut TraceWriterHandle) -> i32 {
    with_handle_or("trace_writer_finish_paths", handle, false, 1, "NULL handle", |h| {
        match h.legacy.as_mut() {
            Some(w) => legacy_result(TraceWriter::finish_writing_trace_paths(&mut **w)),
            None => 0,
        }
    })
}

fn legacy_result(r: Result<(), Box<dyn std::error::Error>>) -> i32 {
    match r {
        Ok(()) => 0,
        Err(e) => {
            set_error(&e.to_string());
            1
        }
    }
}

/// A container writer for the handle's program, identity and settings.
fn new_container_writer(h: &TraceWriterHandle, in_memory: bool) -> CtfsTraceWriter {
    let mut w = if in_memory {
        CtfsTraceWriter::new_in_memory(&h.program, &h.args)
    } else {
        CtfsTraceWriter::new(&h.program, &h.args)
    };
    if let Some(id) = &h.recording_id {
        w.set_recording_id(id.clone());
    }
    AbstractTraceWriter::set_workdir(&mut w, Path::new(&h.workdir));
    w.set_compact_threshold(h.compact_threshold);
    w
}

/// Open the container at `<dir of path>/<program stem>.ct`. A writer already
/// open on a file is left as it is; one open in memory is refused.
///
/// # Safety
/// `handle` is NULL or a live writer; `path` is NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_begin_events(handle: *mut TraceWriterHandle, path: *const c_char) -> i32 {
    with_handle_or("trace_writer_begin_events", handle, false, 1, "NULL handle", |h| {
        if h.in_memory {
            set_error("this writer is already open in memory; call trace_writer_begin_in_memory or trace_writer_begin_events, not both");
            return 1;
        }
        let events_path = path_from_bytes(unsafe { cstr_bytes(path) });
        if let Some(w) = h.legacy.as_mut() {
            return legacy_result(TraceWriter::begin_writing_trace_events(&mut **w, &events_path));
        }
        if h.ctfs.is_some() {
            return 0;
        }
        let dir = events_path.parent().map(Path::to_path_buf).unwrap_or_default();
        let stem = Path::new(&h.program).file_stem().map(|s| s.to_os_string()).unwrap_or_default();
        let mut name = stem;
        name.push(".ct");
        let ct_path: PathBuf = dir.join(name);
        let mut w = new_container_writer(h, false);
        if let Err(e) = TraceWriter::begin_writing_trace_events(&mut w, &ct_path) {
            set_error(&e.to_string());
            return 1;
        }
        h.ctfs = Some(w);
        0
    })
}

/// Open the container in memory; after the close its bytes are read with
/// `trace_writer_container_ptr` / `_len`.
///
/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_begin_in_memory(handle: *mut TraceWriterHandle) -> i32 {
    with_handle_or("trace_writer_begin_in_memory", handle, false, 1, "NULL handle", |h| {
        if !h.multi() {
            set_error("trace_writer_begin_in_memory: only the binary (split-stream) format writes a container in memory");
            return 1;
        }
        if h.ctfs.is_some() {
            if !h.in_memory {
                set_error("this writer is already open on a file; in-memory mode must be chosen before the first begin");
                return 1;
            }
            return 0;
        }
        let mut w = new_container_writer(h, true);
        if let Err(e) = TraceWriter::begin_writing_trace_events(&mut w, Path::new("")) {
            set_error(&e.to_string());
            return 1;
        }
        h.ctfs = Some(w);
        h.in_memory = true;
        0
    })
}

/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_finish_events(handle: *mut TraceWriterHandle) -> i32 {
    with_handle_or("trace_writer_finish_events", handle, false, 1, "NULL handle", |h| {
        match h.legacy.as_mut() {
            Some(w) => legacy_result(TraceWriter::finish_writing_trace_events(&mut **w)),
            None => 0,
        }
    })
}

/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_container_len(handle: *mut TraceWriterHandle) -> usize {
    with_handle_or("trace_writer_container_len", handle, false, 0, "NULL handle", |h| h.container.len())
}

/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_container_ready(handle: *mut TraceWriterHandle) -> i32 {
    with_handle_or("trace_writer_container_ready", handle, false, 0, "NULL handle", |h| {
        i32::from(h.container_ready)
    })
}

/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_container_ptr(handle: *mut TraceWriterHandle) -> *mut u8 {
    guarded("trace_writer_container_ptr", None, std::ptr::null_mut(), || {
        if handle.is_null() {
            set_error("NULL handle");
            return std::ptr::null_mut();
        }
        let h = unsafe { &mut *handle };
        if h.container.is_empty() {
            std::ptr::null_mut()
        } else {
            h.container.as_mut_ptr()
        }
    })
}

/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_set_compact_threshold(handle: *mut TraceWriterHandle, raw_bytes: u64) -> i32 {
    with_handle_or("trace_writer_set_compact_threshold", handle, false, 1, "NULL handle", |h| {
        if !h.multi() {
            set_error("the container profile is chosen only in CTFS multi-stream mode");
            return 1;
        }
        h.compact_threshold = raw_bytes;
        if let Some(w) = h.ctfs.as_mut() {
            w.set_compact_threshold(raw_bytes);
        }
        0
    })
}

// ---------------------------------------------------------------------------
// Identity, metadata and capabilities
// ---------------------------------------------------------------------------

/// # Safety
/// `handle` is NULL or a live writer; `recording_id` is NULL or
/// NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_set_recording_id(handle: *mut TraceWriterHandle, recording_id: *const c_char) -> i32 {
    with_handle_or("trace_writer_set_recording_id", handle, false, 1, "NULL handle", |h| {
        if h.ctfs.is_some() {
            set_error("the recording id must be set before the writer is begun; this writer is already open");
            return 1;
        }
        let id = unsafe { cstr_string(recording_id) };
        if id.is_empty() {
            set_error("recording_id is empty; pass a canonical UUIDv7, or do not call this at all to have one minted");
            return 1;
        }
        if let Err(e) = validate_recording_id(&id) {
            set_error(&format!("recording_id is not a canonical UUIDv7: {e}"));
            return 1;
        }
        h.recording_id = Some(id);
        0
    })
}

/// # Safety
/// `handle` is NULL or a live writer; `workdir` is NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_set_workdir(handle: *mut TraceWriterHandle, workdir: *const c_char) {
    with_handle("trace_writer_set_workdir", handle, true, (), (), |h| {
        let wd = unsafe { cstr_string(workdir) };
        if let Some(w) = h.legacy.as_mut() {
            TraceWriter::set_workdir(&mut **w, Path::new(&wd));
        }
        if let Some(w) = h.ctfs.as_mut() {
            if w.recording_started() {
                set_error("trace_writer_set_workdir: setWorkdir after the first record: meta.dat is already written");
                return;
            }
            AbstractTraceWriter::set_workdir(w, Path::new(&wd));
        }
        h.workdir = wd;
    })
}

/// Record the program's arguments in `meta.dat`.
///
/// # Safety
/// `handle` is NULL or a live writer; `args` and `arg_lens` hold `args_count`
/// entries, each `(args[i], arg_lens[i])` readable or NULL/0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_set_args(handle: *mut TraceWriterHandle, args: *const *const u8, arg_lens: *const usize, args_count: usize) {
    with_handle("trace_writer_set_args", handle, true, (), (), |h| {
        let mut list = vec![String::new(); args_count];
        if !args.is_null() && !arg_lens.is_null() {
            for (i, item) in list.iter_mut().enumerate() {
                *item = String::from_utf8_lossy(unsafe { bytes(*args.add(i), *arg_lens.add(i)) }).into_owned();
            }
        }
        if let Some(w) = h.ctfs.as_mut() {
            if w.recording_started() {
                set_error("trace_writer_set_args: setArgs after the first record: meta.dat is already written");
                return;
            }
            AbstractTraceWriter::get_mut_data(w).args = list.clone();
        }
        h.args = list;
    })
}

/// Stamp a producer namespace on every interned string. Only the empty
/// qualifier — bare payloads, as a standalone recording has — is written by
/// this library; a writer open with another is refused.
///
/// # Safety
/// `handle` is NULL or a live writer; `qualifier` is NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_set_interning_qualifier(handle: *mut TraceWriterHandle, qualifier: *const c_char) {
    with_handle("trace_writer_set_interning_qualifier", handle, true, (), (), |h| {
        h.qualifier = unsafe { cstr_string(qualifier) };
        if h.multi() && h.ctfs.is_some() && !h.qualifier.is_empty() {
            set_error(
                "trace_writer_set_interning_qualifier: a qualified interning namespace is written only by a writer that shares \
                 a container with the multi-core recorder; a standalone container's interned strings are bare",
            );
        }
    })
}

fn meta_block_writer(h: &mut TraceWriterHandle, what: &str) -> Option<()> {
    if !h.multi() {
        set_error(&format!("{what} only supported in CTFS multi-stream mode"));
        return None;
    }
    if h.ctfs.is_none() {
        set_error("writer not ready (call begin_events first)");
        return None;
    }
    Some(())
}

fn apply_meta_blocks(h: &mut TraceWriterHandle, update: impl FnOnce(&mut MetaDatBlocks)) -> i32 {
    let mut blocks = h.meta_blocks.clone();
    update(&mut blocks);
    let w = h.ctfs.as_mut().expect("checked by meta_block_writer");
    match w.set_meta_blocks(blocks.clone()) {
        Ok(()) => {
            h.meta_blocks = blocks;
            0
        }
        Err(e) => {
            set_error(&e);
            1
        }
    }
}

/// # Safety
/// `handle` is NULL or a live writer; every string is NULL or
/// NUL-terminated; `hook_strategies` holds `hook_strategies_count` strings.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn trace_writer_set_mcr_fields(
    handle: *mut TraceWriterHandle,
    tick_source: i32,
    total_threads: u32,
    atomic_mode: i32,
    total_events: u64,
    total_checkpoints: u32,
    start_time_unix_us: u64,
    platform: *const c_char,
    tick_granularity: *const c_char,
    tick_source_str: *const c_char,
    atomic_mode_str: *const c_char,
    start_time_str: *const c_char,
    hook_profile: *const c_char,
    hook_strategies: *const *const c_char,
    hook_strategies_count: usize,
) -> i32 {
    with_handle_or("trace_writer_set_mcr_fields", handle, false, 1, "NULL handle", |h| {
        if meta_block_writer(h, "MCR fields").is_none() {
            return 1;
        }
        let tick = match tick_source {
            0 => Some(TickSource::Rdtsc),
            1 => Some(TickSource::Monotonic),
            2 => Some(TickSource::PerfCounter),
            _ => None,
        };
        let Some(tick) = tick else {
            set_error(&format!("tick_source {tick_source} is not a TickSource ordinal"));
            return 1;
        };
        let atomic = match atomic_mode {
            0 => Some(AtomicMode::Relaxed),
            1 => Some(AtomicMode::SeqCst),
            _ => None,
        };
        let Some(atomic) = atomic else {
            set_error(&format!("atomic_mode {atomic_mode} is not an AtomicMode ordinal"));
            return 1;
        };
        if hook_strategies.is_null() && hook_strategies_count > 0 {
            set_error(&format!("hook_strategies is NULL with a count of {hook_strategies_count}"));
            return 1;
        }
        let strategies = (0..hook_strategies_count)
            .map(|i| unsafe { cstr_string(*hook_strategies.add(i)) })
            .collect();
        let fields = McrFields {
            tick_source: tick,
            total_threads,
            atomic_mode: atomic,
            total_events,
            total_checkpoints,
            start_time_unix_us,
            platform: unsafe { cstr_string(platform) },
            tick_granularity: unsafe { cstr_string(tick_granularity) },
            tick_source_str: unsafe { cstr_string(tick_source_str) },
            atomic_mode_str: unsafe { cstr_string(atomic_mode_str) },
            start_time_str: unsafe { cstr_string(start_time_str) },
            hook_profile: unsafe { cstr_string(hook_profile) },
            hook_strategies: strategies,
        };
        apply_meta_blocks(h, |b| b.mcr = Some(fields))
    })
}

/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_set_replay_launch_fields(handle: *mut TraceWriterHandle, aslr_disabled: i32) -> i32 {
    with_handle_or("trace_writer_set_replay_launch_fields", handle, false, 1, "NULL handle", |h| {
        if meta_block_writer(h, "replay-launch fields").is_none() {
            return 1;
        }
        apply_meta_blocks(h, |b| {
            b.replay_launch = Some(ReplayLaunchFields {
                aslr_disabled: aslr_disabled != 0,
            })
        })
    })
}

/// # Safety
/// `handle` is NULL or a live writer; `(fingerprint, fingerprint_len)` is
/// readable or NULL/0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_set_layout_snapshot(
    handle: *mut TraceWriterHandle,
    layout_hash: u64,
    fingerprint: *const u8,
    fingerprint_len: usize,
) -> i32 {
    with_handle_or("trace_writer_set_layout_snapshot", handle, false, 1, "NULL handle", |h| {
        if meta_block_writer(h, "layout snapshot").is_none() {
            return 1;
        }
        if fingerprint.is_null() && fingerprint_len > 0 {
            set_error(&format!("fingerprint is NULL with a length of {fingerprint_len}"));
            return 1;
        }
        let fp = unsafe { bytes(fingerprint, fingerprint_len) }.to_vec();
        apply_meta_blocks(h, |b| {
            b.layout_snapshot = Some(LayoutSnapshot {
                layout_hash,
                layout_fingerprint: fp,
            })
        })
    })
}

/// Record one trace-filter file in the filter-provenance block.
///
/// # Safety
/// `handle` is NULL or a live writer; both `(pointer, length)` pairs are
/// readable or NULL/0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_add_filter_provenance(
    handle: *mut TraceWriterHandle,
    path: *const u8,
    path_len: usize,
    sha256: *const u8,
    sha256_len: usize,
) -> i32 {
    with_handle_or("trace_writer_add_filter_provenance", handle, false, 1, "NULL handle", |h| {
        if !h.multi() {
            set_error("filter provenance only supported in CTFS multi-stream mode");
            return 1;
        }
        if h.ctfs.is_none() {
            set_error("writer not ready (call begin_events first)");
            return 1;
        }
        if sha256_len != 32 {
            set_error("sha256_bytes must be exactly 32 bytes");
            return 1;
        }
        let mut digest = [0u8; 32];
        if !sha256.is_null() {
            digest.copy_from_slice(unsafe { bytes(sha256, 32) });
        }
        let entry = FilterProvenance {
            path: String::from_utf8_lossy(unsafe { bytes(path, path_len) }).into_owned(),
            sha256: digest,
        };
        apply_meta_blocks(h, |b| b.filter_provenance.get_or_insert_with(Vec::new).push(entry))
    })
}

/// Record that the recorder implements trace filters and its chain is empty.
///
/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_record_empty_filter_provenance(handle: *mut TraceWriterHandle) -> i32 {
    with_handle_or("trace_writer_record_empty_filter_provenance", handle, false, 1, "NULL handle", |h| {
        if !h.multi() {
            set_error("filter provenance only supported in CTFS multi-stream mode");
            return 1;
        }
        if h.ctfs.is_none() {
            set_error("writer not ready (call begin_events first)");
            return 1;
        }
        apply_meta_blocks(h, |b| {
            b.filter_provenance.get_or_insert_with(Vec::new);
        })
    })
}

/// `meta.dat` is written by the container's first record; this succeeds on a
/// begun container writer.
///
/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_write_meta_dat(handle: *mut TraceWriterHandle, _recorder_id: *const u8, _recorder_id_len: usize) -> i32 {
    with_handle_or("ct_write_meta_dat", handle, false, 1, "NULL handle", |h| {
        if h.ctfs.is_none() {
            set_error("writer not ready (call begin_events first)");
            return 1;
        }
        0
    })
}

/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_declare_source_reload(handle: *mut TraceWriterHandle) -> i32 {
    with_handle_or("trace_writer_declare_source_reload", handle, false, 1, "NULL handle", |h| {
        if !h.multi() {
            set_error("trace_writer_declare_source_reload: source reloads are recorded by the multi-stream writer only");
            return 1;
        }
        let Some(w) = h.ctfs.as_mut() else {
            set_error("trace_writer_declare_source_reload: writer not ready (call trace_writer_begin_events first)");
            return 1;
        };
        match w.declare_source_reload() {
            Ok(()) => 0,
            Err(e) => {
                set_error(&e);
                1
            }
        }
    })
}

fn column_capability(name: &str, handle: *mut TraceWriterHandle, enable: impl FnOnce(&mut CtfsTraceWriter)) {
    with_handle(name, handle, true, (), (), |h| {
        let Some(w) = h.ctfs.as_mut() else {
            return;
        };
        let dropped = w.dropped_column_awareness();
        enable(w);
        if w.dropped_column_awareness() && !dropped {
            set_error(&format!(
                "{name}: the trace has recorded already, and meta.dat, which declares columns, is written"
            ));
        }
    })
}

/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_enable_column_aware_steps(handle: *mut TraceWriterHandle) {
    column_capability(
        "trace_writer_enable_column_aware_steps",
        handle,
        CtfsTraceWriter::enable_column_aware_steps,
    )
}

/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_enable_column_breakpoints_support(handle: *mut TraceWriterHandle) {
    column_capability(
        "trace_writer_enable_column_breakpoints_support",
        handle,
        CtfsTraceWriter::enable_column_breakpoints_support,
    )
}

/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_enable_column_motions_support(handle: *mut TraceWriterHandle) {
    column_capability(
        "trace_writer_enable_column_motions_support",
        handle,
        CtfsTraceWriter::enable_column_motions_support,
    )
}

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

/// The container writer of a multi-stream handle that has begun, or the
/// failure to report: `prefix` names the entry point.
fn ready<'a>(h: &'a mut TraceWriterHandle, prefix: &str, legacy_reason: &str) -> Result<&'a mut CtfsTraceWriter, ()> {
    if !h.multi() {
        set_error(&format!("{prefix}: the legacy single-stream backend {legacy_reason}"));
        return Err(());
    }
    match h.ctfs.as_mut() {
        Some(w) => Ok(w),
        None => {
            set_error(&format!("{prefix}: writer not ready (call trace_writer_begin_events first)"));
            Err(())
        }
    }
}

/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_enable_line_count_table(handle: *mut TraceWriterHandle) -> i32 {
    with_handle_or(
        "trace_writer_enable_line_count_table",
        handle,
        false,
        1,
        "trace_writer_enable_line_count_table: NULL handle",
        |h| {
            let Ok(w) = ready(h, "trace_writer_enable_line_count_table", "has no paths.dat to record line counts in") else {
                return 1;
            };
            match w.enable_line_count_table() {
                Ok(()) => 0,
                Err(e) => {
                    set_error(&e);
                    1
                }
            }
        },
    )
}

/// # Safety
/// `handle` is NULL or a live writer; `path` is NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_path_with_line_count(handle: *mut TraceWriterHandle, path: *const c_char, line_count: u64) -> i32 {
    with_handle_or(
        "trace_writer_register_path_with_line_count",
        handle,
        false,
        1,
        "trace_writer_register_path_with_line_count: NULL handle",
        |h| {
            let Ok(w) = ready(
                h,
                "trace_writer_register_path_with_line_count",
                "has no paths.dat to record line counts in",
            ) else {
                return 1;
            };
            match w.register_path_with_line_count(&path_from_bytes(unsafe { cstr_bytes(path) }), line_count) {
                Ok(_) => 0,
                Err(e) => {
                    set_error(&e);
                    1
                }
            }
        },
    )
}

/// # Safety
/// `handle` is NULL or a live writer; `path` is NULL or NUL-terminated; when
/// `line_count > 0`, `line_lengths` is NULL or holds that many entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_path_with_line_lengths(
    handle: *mut TraceWriterHandle,
    path: *const c_char,
    line_count: i32,
    line_lengths: *const u32,
) -> i32 {
    with_handle_or(
        "trace_writer_register_path_with_line_lengths",
        handle,
        true,
        1,
        "trace_writer_register_path_with_line_lengths: NULL handle",
        |h| {
            let p = path_from_bytes(unsafe { cstr_bytes(path) });
            if let Some(w) = h.legacy.as_mut() {
                TraceWriter::register_path(&mut **w, &p);
                return 0;
            }
            let Some(w) = h.ctfs.as_mut() else {
                set_error("trace_writer_register_path_with_line_lengths: writer not ready (call trace_writer_begin_events first)");
                return 1;
            };
            let lengths: &[u32] = if line_count > 0 && !line_lengths.is_null() {
                unsafe { std::slice::from_raw_parts(line_lengths, line_count as usize) }
            } else {
                &[]
            };
            match TraceWriter::register_path_with_line_lengths(w, &p, lengths) {
                Ok(_) => 0,
                Err(e) => {
                    set_error(&e.to_string());
                    1
                }
            }
        },
    )
}

/// # Safety
/// `handle` is NULL or a live writer; `path` is NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_path_version(handle: *mut TraceWriterHandle, path: *const c_char, line_count: u64) -> u64 {
    crate::trace_writer_clear_last_error();
    with_handle_or(
        "trace_writer_register_path_version",
        handle,
        false,
        CT_TW_INVALID_PATH_ID,
        "trace_writer_register_path_version: NULL handle",
        |h| {
            let Ok(w) = ready(h, "trace_writer_register_path_version", "has no paths.dat to version") else {
                return CT_TW_INVALID_PATH_ID;
            };
            match w.register_path_version(&path_from_bytes(unsafe { cstr_bytes(path) }), line_count) {
                Ok(id) => id.0 as u64,
                Err(e) => {
                    set_error(&e);
                    CT_TW_INVALID_PATH_ID
                }
            }
        },
    )
}

/// # Safety
/// `handle` is NULL or a live writer; `path` is NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_current_path_id(handle: *mut TraceWriterHandle, path: *const c_char) -> u64 {
    crate::trace_writer_clear_last_error();
    with_handle_or(
        "trace_writer_current_path_id",
        handle,
        false,
        CT_TW_INVALID_PATH_ID,
        "trace_writer_current_path_id: NULL handle",
        |h| {
            let Ok(w) = ready(h, "trace_writer_current_path_id", "has no paths.dat ids to answer with") else {
                return CT_TW_INVALID_PATH_ID;
            };
            let p = path_from_bytes(unsafe { cstr_bytes(path) });
            match w.current_path_id(&p) {
                Some(id) => id.0 as u64,
                None => {
                    set_error(&format!(
                        "trace_writer_current_path_id: no path {} has been registered on this writer",
                        p.display()
                    ));
                    CT_TW_INVALID_PATH_ID
                }
            }
        },
    )
}

/// # Safety
/// `handle` is NULL or a live writer; `path` is NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_path(handle: *mut TraceWriterHandle, path: *const c_char) -> u64 {
    crate::trace_writer_clear_last_error();
    with_handle_or(
        "trace_writer_register_path",
        handle,
        false,
        CT_TW_INVALID_PATH_ID,
        "trace_writer_register_path: NULL handle",
        |h| {
            let Ok(w) = ready(h, "trace_writer_register_path", "has no paths.dat to intern into") else {
                return CT_TW_INVALID_PATH_ID;
            };
            let p = path_from_bytes(unsafe { cstr_bytes(path) });
            if let Some(id) = w.current_path_id(&p) {
                return id.0 as u64;
            }
            if w.line_count_table_enabled() {
                set_error(&format!(
                    "trace_writer_register_path: {} has no recorded line count; under the line-count table every path is \
                     registered with trace_writer_register_path_with_line_count",
                    p.display()
                ));
                return CT_TW_INVALID_PATH_ID;
            }
            match path_id_for_step(w, &p) {
                Some(id) => id.0 as u64,
                None => CT_TW_INVALID_PATH_ID,
            }
        },
    )
}

/// # Safety
/// `handle` is NULL or a live writer; `name` is NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_variable_name(handle: *mut TraceWriterHandle, name: *const c_char) -> u64 {
    crate::trace_writer_clear_last_error();
    with_handle_or(
        "trace_writer_register_variable_name",
        handle,
        false,
        u64::MAX,
        "trace_writer_register_variable_name: NULL handle",
        |h| {
            let Ok(w) = ready(h, "trace_writer_register_variable_name", "has no varnames.dat to intern into") else {
                return u64::MAX;
            };
            AbstractTraceWriter::ensure_variable_id(w, &unsafe { cstr_string(name) }).0 as u64
        },
    )
}

/// One file's transition across a reload, as the header lays it out.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CtTwSourceReloadChange {
    pub old_path_id: u64,
    pub new_path_id: u64,
    pub generation: u64,
}

/// # Safety
/// `handle` is NULL or a live writer; `changed` holds `changed_count`
/// entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_source_reload(
    handle: *mut TraceWriterHandle,
    changed: *const CtTwSourceReloadChange,
    changed_count: usize,
    in_flight_frames: u64,
) -> u64 {
    crate::trace_writer_clear_last_error();
    with_handle_or(
        "trace_writer_register_source_reload",
        handle,
        false,
        0,
        "trace_writer_register_source_reload: NULL handle",
        |h| {
            if ready(h, "trace_writer_register_source_reload", "has no execution stream to annotate").is_err() {
                return 0;
            }
            if changed.is_null() || changed_count == 0 {
                set_error(
                    "trace_writer_register_source_reload: no changed files. A marker that records a reload without \
                     recording what it changed cannot be told apart from one whose files were lost",
                );
                return 0;
            }
            let changes: Vec<SourceReloadChange> = unsafe { std::slice::from_raw_parts(changed, changed_count) }
                .iter()
                .map(|c| SourceReloadChange {
                    old_path_id: c.old_path_id,
                    new_path_id: c.new_path_id,
                    generation: c.generation,
                })
                .collect();
            if flush_pending_step(h).is_err() {
                return 0;
            }
            let w = h.ctfs.as_mut().expect("checked by ready");
            match w.register_source_reload(&changes, in_flight_frames) {
                Ok(ordinal) => ordinal,
                Err(e) => {
                    set_error(&e);
                    0
                }
            }
        },
    )
}

/// Buffer an alternate source view of the registered path `path_id`; its
/// 0-based index, or -1 on failure.
///
/// # Safety
/// `handle` is NULL or a live writer; each `(pointer, length)` pair is
/// readable or NULL/0.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn trace_writer_register_source_view(
    handle: *mut TraceWriterHandle,
    path_id: u64,
    view_kind: u8,
    view_name: *const c_char,
    view_name_len: usize,
    content: *const u8,
    content_len: usize,
    sourcemap: *const u8,
    sourcemap_len: usize,
) -> i64 {
    with_handle_or(
        "trace_writer_register_source_view",
        handle,
        false,
        -1,
        "trace_writer_register_source_view: NULL handle",
        |h| {
            if !h.multi() {
                set_error("trace_writer_register_source_view: only the multi-stream backend supports alternate source views");
                return -1;
            }
            let Some(w) = h.ctfs.as_mut() else {
                set_error("trace_writer_register_source_view: writer not ready (call trace_writer_begin_events first)");
                return -1;
            };
            let name = unsafe { bytes(view_name as *const u8, view_name_len) };
            match w.register_source_view(path_id, view_kind, name, unsafe { bytes(content, content_len) }, unsafe {
                bytes(sourcemap, sourcemap_len)
            }) {
                Ok(i) => i as i64,
                Err(e) => {
                    set_error(&e);
                    -1
                }
            }
        },
    )
}

/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_source_reload_count(handle: *mut TraceWriterHandle) -> u64 {
    with_handle("trace_writer_source_reload_count", handle, false, 0, 0, |h| {
        h.ctfs.as_ref().map_or(0, CtfsTraceWriter::source_reload_count)
    })
}

/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_next_step_index(handle: *mut TraceWriterHandle) -> u64 {
    with_handle("trace_writer_next_step_index", handle, false, 0, 0, |h| match h.ctfs.as_ref() {
        Some(w) => w.exec_record_count() + u64::from(h.pending_step.is_some()),
        None => 0,
    })
}

// ---------------------------------------------------------------------------
// Steps, functions, types, calls
// ---------------------------------------------------------------------------

/// The `<toplevel>` function, its call, and the entry step.
///
/// # Safety
/// `handle` is NULL or a live writer; `path` is NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_start(handle: *mut TraceWriterHandle, path: *const c_char, line: i64) {
    with_handle("trace_writer_start", handle, true, (), (), |h| {
        let p = path_from_bytes(unsafe { cstr_bytes(path) });
        if let Some(w) = h.legacy.as_mut() {
            TraceWriter::start(&mut **w, &p, Line(line));
            return;
        }
        let Some(w) = h.ctfs.as_mut() else {
            return;
        };
        let Some(path_id) = path_id_for_step(w, &p) else {
            return;
        };
        let function_id = AbstractTraceWriter::ensure_function_id(w, "<toplevel>", &p, Line(line.max(1)));
        w.register_call_cbor(function_id, vec![]);
        h.open_calls += 1;
        h.pending_step = Some((path_id, line, 0));
    })
}

/// # Safety
/// `handle` is NULL or a live writer; `path` is NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_step(handle: *mut TraceWriterHandle, path: *const c_char, line: i64) {
    with_handle("trace_writer_register_step", handle, true, (), (), |h| {
        let p = path_from_bytes(unsafe { cstr_bytes(path) });
        if let Some(w) = h.legacy.as_mut() {
            TraceWriter::register_step(&mut **w, &p, Line(line));
            return;
        }
        if h.ctfs.is_none() {
            return;
        }
        let _ = flush_pending_step(h);
        let w = h.ctfs.as_mut().expect("checked above");
        let Some(path_id) = path_id_for_step(w, &p) else {
            return;
        };
        h.pending_step = Some((path_id, line, 0));
    })
}

/// A step at `(path, line)`; a column is not carried by this entry point,
/// and asking for one is reported.
///
/// # Safety
/// `handle` is NULL or a live writer; `path` is NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_assignment_with_column(handle: *mut TraceWriterHandle, path: *const c_char, line: i64, column: i64, has_column: i32) {
    with_handle("ct_assignment_with_column", handle, true, (), (), |h| {
        let p = path_from_bytes(unsafe { cstr_bytes(path) });
        if let Some(w) = h.legacy.as_mut() {
            TraceWriter::register_step_with_column(&mut **w, &p, Line(line), (has_column != 0).then_some(Line(column)));
            return;
        }
        if h.ctfs.is_none() {
            return;
        }
        let _ = flush_pending_step(h);
        let w = h.ctfs.as_mut().expect("checked above");
        let Some(path_id) = path_id_for_step(w, &p) else {
            return;
        };
        if has_column != 0 {
            set_error("ct_assignment_with_column: multi-stream Step does not yet carry column; falling back to no-column");
        }
        h.pending_step = Some((path_id, line, 0));
    })
}

/// Move the buffered step's column by `column_delta`; with no step buffered,
/// a column step of its own.
///
/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_delta_column(handle: *mut TraceWriterHandle, column_delta: i64) {
    with_handle("trace_writer_register_delta_column", handle, true, (), (), |h| {
        if !h.multi() {
            set_error("trace_writer_register_delta_column: only the multi-stream backend supports column-aware events");
            return;
        }
        if h.ctfs.is_none() {
            return;
        }
        if let Some((_, _, delta)) = h.pending_step.as_mut() {
            *delta += column_delta;
            return;
        }
        let w = h.ctfs.as_mut().expect("checked above");
        if !w.column_aware_steps_enabled() {
            set_error(
                "register_column_step called on a writer that has not opted into column-aware mode \
                 (call enable_column_aware_steps before begin_writing_trace_events)",
            );
            return;
        }
        let held = std::mem::take(&mut h.held);
        emit_held(w, held);
        if let Err(e) = w.register_column_step(column_delta) {
            set_error(&e);
        }
    })
}

/// # Safety
/// `handle` is NULL or a live writer; `name` and `path` are NULL or
/// NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_ensure_function_id(
    handle: *mut TraceWriterHandle,
    name: *const c_char,
    path: *const c_char,
    line: i64,
) -> usize {
    with_handle("trace_writer_ensure_function_id", handle, true, usize::MAX, usize::MAX, |h| {
        let n = unsafe { cstr_string(name) };
        if let Some(&id) = h.function_ids.get(&n) {
            return id;
        }
        let p = path_from_bytes(unsafe { cstr_bytes(path) });
        let id = if let Some(w) = h.legacy.as_mut() {
            TraceWriter::ensure_function_id(&mut **w, &n, &p, Line(line)).0
        } else {
            let Some(w) = h.ctfs.as_mut() else {
                set_error("trace_writer_ensure_function_id: writer is not ready");
                return usize::MAX;
            };
            AbstractTraceWriter::ensure_function_id(w, &n, &p, Line(line.max(1))).0
        };
        h.function_ids.insert(n, id);
        id
    })
}

/// # Safety
/// `handle` is NULL or a live writer; `lang_type` is NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_ensure_type_id(handle: *mut TraceWriterHandle, kind: i32, lang_type: *const c_char) -> usize {
    with_handle("trace_writer_ensure_type_id", handle, true, usize::MAX, usize::MAX, |h| {
        ensure_type_id(h, kind, unsafe { cstr_string(lang_type) })
    })
}

fn ensure_type_id(h: &mut TraceWriterHandle, kind: i32, lang_type: String) -> usize {
    let key = (kind, lang_type);
    if let Some(&id) = h.type_ids.get(&key) {
        return id;
    }
    let Some(tk) = u8::try_from(kind).ok().and_then(TypeKind::from_u8) else {
        set_error(&format!("trace_writer_ensure_type_id: {kind} is not a TypeKind ordinal"));
        return usize::MAX;
    };
    let id = if let Some(w) = h.legacy.as_mut() {
        TraceWriter::ensure_type_id(&mut **w, tk, &key.1).0
    } else {
        let Some(w) = h.ctfs.as_mut() else {
            set_error("trace_writer_ensure_type_id: writer is not ready");
            return usize::MAX;
        };
        AbstractTraceWriter::ensure_type_id(w, tk, &key.1).0
    };
    h.type_ids.insert(key, id);
    h.type_count = h.type_count.max(id + 1);
    id
}

/// A dangling type id is refused, naming it.
fn dangling(h: &TraceWriterHandle, type_id: usize, what: &str) -> bool {
    if type_id < h.type_count {
        return false;
    }
    set_error(&format!(
        "{what}: type id {type_id} was never registered. Call trace_writer_ensure_type_id first; {} type(s) are registered \
         on this writer.",
        h.type_count
    ));
    true
}

/// Stage one argument of the next call: its name and encoded value.
///
/// # Safety
/// `handle` is NULL or a live writer; `name` is NULL or NUL-terminated;
/// `(cbor_data, cbor_len)` is readable or NULL/0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_call_arg(handle: *mut TraceWriterHandle, name: *const c_char, cbor_data: *const u8, cbor_len: usize) {
    with_handle("trace_writer_register_call_arg", handle, true, (), (), |h| {
        let Some(w) = h.ctfs.as_mut() else {
            return;
        };
        let id = AbstractTraceWriter::ensure_variable_id(w, &unsafe { cstr_string(name) });
        h.call_args.push((id.0 as u64, unsafe { bytes(cbor_data, cbor_len) }.to_vec()));
    })
}

/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_call(handle: *mut TraceWriterHandle, function_id: usize) {
    with_handle("trace_writer_register_call", handle, true, (), (), |h| {
        if let Some(w) = h.legacy.as_mut() {
            TraceWriter::register_call(&mut **w, FunctionId(function_id), vec![]);
            return;
        }
        if h.ctfs.is_none() {
            set_error("trace_writer_register_call: writer is not ready");
            return;
        }
        let _ = flush_pending_step(h);
        let args = std::mem::take(&mut h.call_args);
        h.ctfs.as_mut().expect("checked above").register_call_cbor(FunctionId(function_id), args);
        h.open_calls += 1;
    })
}

/// A return with an encoded value (empty: no value), or a notice when no
/// call is open.
fn register_return_bytes(h: &mut TraceWriterHandle, value: Vec<u8>) {
    if h.ctfs.is_none() {
        set_error("register_return: writer is not ready");
        return;
    }
    let _ = flush_pending_step(h);
    if h.open_calls == 0 {
        set_notice("register_return: call stack underflow: a return with no call open");
        return;
    }
    h.open_calls -= 1;
    h.ctfs.as_mut().expect("checked above").register_return_cbor_value(value);
}

/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_return(handle: *mut TraceWriterHandle) {
    with_handle("trace_writer_register_return", handle, true, (), (), |h| {
        if let Some(w) = h.legacy.as_mut() {
            TraceWriter::register_return(&mut **w, codetracer_trace_types::NONE_VALUE);
            return;
        }
        register_return_bytes(h, vec![]);
    })
}

/// # Safety
/// `handle` is NULL or a live writer; `type_name` is NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_return_int(handle: *mut TraceWriterHandle, value: i64, type_kind: i32, type_name: *const c_char) {
    with_handle("trace_writer_register_return_int", handle, true, (), (), |h| {
        let type_id = ensure_type_id(h, type_kind, unsafe { cstr_string(type_name) });
        return_int(h, value, type_id);
    })
}

fn return_int(h: &mut TraceWriterHandle, value: i64, type_id: usize) {
    if let Some(w) = h.legacy.as_mut() {
        TraceWriter::register_return(
            &mut **w,
            ValueRecord::Int {
                i: value,
                type_id: codetracer_trace_types::TypeId(type_id),
            },
        );
        return;
    }
    register_return_bytes(h, int_value(value, type_id as u64));
}

/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_return_int_by_type_id(handle: *mut TraceWriterHandle, value: i64, type_id: usize) {
    with_handle("trace_writer_register_return_int_by_type_id", handle, true, (), (), |h| {
        if dangling(h, type_id, "trace_writer_register_return_int_by_type_id") {
            return;
        }
        return_int(h, value, type_id);
    })
}

/// # Safety
/// `handle` is NULL or a live writer; `value_repr` and `type_name` are NULL
/// or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_return_raw(
    handle: *mut TraceWriterHandle,
    value_repr: *const c_char,
    type_kind: i32,
    type_name: *const c_char,
) {
    with_handle("trace_writer_register_return_raw", handle, true, (), (), |h| {
        let type_id = ensure_type_id(h, type_kind, unsafe { cstr_string(type_name) });
        let repr = unsafe { cstr_bytes(value_repr) };
        if let Some(w) = h.legacy.as_mut() {
            TraceWriter::register_return(
                &mut **w,
                ValueRecord::Raw {
                    r: String::from_utf8_lossy(repr).into_owned(),
                    type_id: codetracer_trace_types::TypeId(type_id),
                },
            );
            return;
        }
        register_return_bytes(h, raw_value(repr, type_id as u64));
    })
}

/// A return whose value is already encoded; an empty value is no value.
///
/// # Safety
/// `handle` is NULL or a live writer; `(cbor_data, cbor_len)` is readable or
/// NULL/0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_return_cbor(handle: *mut TraceWriterHandle, cbor_data: *const u8, cbor_len: usize) {
    with_handle("trace_writer_register_return_cbor", handle, true, (), (), |h| {
        let data = unsafe { bytes(cbor_data, cbor_len) }.to_vec();
        if let Some(w) = h.legacy.as_mut() {
            TraceWriter::register_return_cbor(&mut **w, &data);
            return;
        }
        register_return_bytes(h, data);
    })
}

// ---------------------------------------------------------------------------
// Values and the value stream
// ---------------------------------------------------------------------------

fn hold_value(h: &mut TraceWriterHandle, name: &str, cbor: Vec<u8>) {
    let Some(w) = h.ctfs.as_mut() else {
        return;
    };
    let id = AbstractTraceWriter::ensure_variable_id(w, name);
    h.held.push(Held::Value(id, cbor));
}

/// # Safety
/// `handle` is NULL or a live writer; the strings are NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_variable_int(
    handle: *mut TraceWriterHandle,
    name: *const c_char,
    value: i64,
    type_kind: i32,
    type_name: *const c_char,
) {
    with_handle("trace_writer_register_variable_int", handle, true, (), (), |h| {
        let type_id = ensure_type_id(h, type_kind, unsafe { cstr_string(type_name) });
        variable_int(h, &unsafe { cstr_string(name) }, value, type_id);
    })
}

fn variable_int(h: &mut TraceWriterHandle, name: &str, value: i64, type_id: usize) {
    if let Some(w) = h.legacy.as_mut() {
        TraceWriter::register_variable_with_full_value(
            &mut **w,
            name,
            ValueRecord::Int {
                i: value,
                type_id: codetracer_trace_types::TypeId(type_id),
            },
        );
        return;
    }
    hold_value(h, name, int_value(value, type_id as u64));
}

/// # Safety
/// `handle` is NULL or a live writer; `name` is NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_variable_int_by_type_id(
    handle: *mut TraceWriterHandle,
    name: *const c_char,
    value: i64,
    type_id: usize,
) {
    with_handle("trace_writer_register_variable_int_by_type_id", handle, true, (), (), |h| {
        if dangling(h, type_id, "trace_writer_register_variable_int_by_type_id") {
            return;
        }
        variable_int(h, &unsafe { cstr_string(name) }, value, type_id);
    })
}

fn variable_raw(h: &mut TraceWriterHandle, name: &str, repr: &[u8], type_id: usize) {
    if let Some(w) = h.legacy.as_mut() {
        TraceWriter::register_variable_with_full_value(
            &mut **w,
            name,
            ValueRecord::Raw {
                r: String::from_utf8_lossy(repr).into_owned(),
                type_id: codetracer_trace_types::TypeId(type_id),
            },
        );
        return;
    }
    hold_value(h, name, raw_value(repr, type_id as u64));
}

/// # Safety
/// `handle` is NULL or a live writer; the strings are NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_variable_raw(
    handle: *mut TraceWriterHandle,
    name: *const c_char,
    value_repr: *const c_char,
    type_kind: i32,
    type_name: *const c_char,
) {
    with_handle("trace_writer_register_variable_raw", handle, true, (), (), |h| {
        let type_id = ensure_type_id(h, type_kind, unsafe { cstr_string(type_name) });
        variable_raw(h, &unsafe { cstr_string(name) }, unsafe { cstr_bytes(value_repr) }, type_id);
    })
}

/// # Safety
/// `handle` is NULL or a live writer; the strings are NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_variable_raw_by_type_id(
    handle: *mut TraceWriterHandle,
    name: *const c_char,
    value_repr: *const c_char,
    type_id: usize,
) {
    with_handle("trace_writer_register_variable_raw_by_type_id", handle, true, (), (), |h| {
        if dangling(h, type_id, "trace_writer_register_variable_raw_by_type_id") {
            return;
        }
        variable_raw(h, &unsafe { cstr_string(name) }, unsafe { cstr_bytes(value_repr) }, type_id);
    })
}

/// A variable whose value is already encoded, stored verbatim.
///
/// # Safety
/// `handle` is NULL or a live writer; `name` is NULL or NUL-terminated;
/// `(cbor_data, cbor_len)` is readable or NULL/0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_variable_cbor(
    handle: *mut TraceWriterHandle,
    name: *const c_char,
    cbor_data: *const u8,
    cbor_len: usize,
) {
    with_handle("trace_writer_register_variable_cbor", handle, true, (), (), |h| {
        let name = unsafe { cstr_string(name) };
        let data = unsafe { bytes(cbor_data, cbor_len) }.to_vec();
        if let Some(w) = h.legacy.as_mut() {
            TraceWriter::register_variable_cbor(&mut **w, &name, &data);
            return;
        }
        hold_value(h, &name, data);
    })
}

/// The checks every value-stream entry point makes: a container writer that
/// has begun.
fn value_stream_writer<'a>(h: &'a mut TraceWriterHandle, entry: &str) -> Option<&'a mut CtfsTraceWriter> {
    if !h.multi() {
        set_error(&format!(
            "{entry}: the single-stream writer does not record the place model; open the writer in the binary (multi-stream) format"
        ));
        return None;
    }
    match h.ctfs.as_mut() {
        Some(w) => Some(w),
        None => {
            set_error(&format!("{entry}: writer is not ready"));
            None
        }
    }
}

/// Hold `event` (with its encoded payload) for the step it belongs to.
fn hold_event(h: &mut TraceWriterHandle, entry: &str, make: impl FnOnce(&mut CtfsTraceWriter) -> (TraceLowLevelEvent, Option<Vec<u8>>)) -> i32 {
    let Some(w) = value_stream_writer(h, entry) else {
        return 1;
    };
    let (event, payload) = make(w);
    h.held.push(Held::Event(event, payload));
    0
}

/// # Safety
/// `handle` is NULL or a live writer; `target_name` is NULL or
/// NUL-terminated; `(rvalue_cbor, rvalue_cbor_len)` is readable or NULL/0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_assignment(
    handle: *mut TraceWriterHandle,
    target_name: *const c_char,
    pass_by: u8,
    rvalue_cbor: *const u8,
    rvalue_cbor_len: usize,
) -> i32 {
    with_handle("trace_writer_register_assignment", handle, true, 1, 1, |h| {
        let name = unsafe { cstr_string(target_name) };
        let rvalue = unsafe { bytes(rvalue_cbor, rvalue_cbor_len) }.to_vec();
        if pass_by > 1 {
            set_error(&format!(
                "trace_writer_register_assignment: pass_by {pass_by} is not a PassBy ordinal (0 or 1)"
            ));
            return 1;
        }
        hold_event(h, "trace_writer_register_assignment", |w| {
            let to = AbstractTraceWriter::ensure_variable_id(w, &name);
            let pass_by = if pass_by == 0 { PassBy::Value } else { PassBy::Reference };
            (
                TraceLowLevelEvent::Assignment(AssignmentRecord {
                    to,
                    pass_by,
                    from: RValue::Literal,
                }),
                Some(rvalue),
            )
        })
    })
}

/// # Safety
/// `handle` is NULL or a live writer; `names` holds `count` strings, each NULL
/// or NUL-terminated (`names` may be NULL when `count` is 0).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_drop_variables(handle: *mut TraceWriterHandle, names: *const *const c_char, count: usize) -> i32 {
    with_handle("trace_writer_register_drop_variables", handle, true, 1, 1, |h| {
        if names.is_null() && count > 0 {
            set_error(&format!("trace_writer_register_drop_variables: names is NULL but count is {count}"));
            return 1;
        }
        let list: Vec<String> = (0..count).map(|i| unsafe { cstr_string(*names.add(i)) }).collect();
        hold_event(h, "trace_writer_register_drop_variables", |w| {
            let ids = list.iter().map(|n| AbstractTraceWriter::ensure_variable_id(w, n)).collect();
            (TraceLowLevelEvent::DropVariables(ids), None)
        })
    })
}

/// # Safety
/// `handle` is NULL or a live writer; `name` is NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_drop_variable(handle: *mut TraceWriterHandle, name: *const c_char) -> i32 {
    with_handle("trace_writer_register_drop_variable", handle, true, 1, 1, |h| {
        let name = unsafe { cstr_string(name) };
        hold_event(h, "trace_writer_register_drop_variable", |w| {
            (TraceLowLevelEvent::DropVariable(AbstractTraceWriter::ensure_variable_id(w, &name)), None)
        })
    })
}

/// # Safety
/// `handle` is NULL or a live writer; `variable_name` is NULL or
/// NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_bind_variable(handle: *mut TraceWriterHandle, variable_name: *const c_char, place: i64) -> i32 {
    with_handle_or(
        "trace_writer_bind_variable",
        handle,
        true,
        1,
        "trace_writer_bind_variable: NULL handle",
        |h| {
            let name = unsafe { cstr_string(variable_name) };
            hold_event(h, "trace_writer_bind_variable", |w| {
                let variable_id = AbstractTraceWriter::ensure_variable_id(w, &name);
                (
                    TraceLowLevelEvent::BindVariable(BindVariableRecord {
                        variable_id,
                        place: Place(place),
                    }),
                    None,
                )
            })
        },
    )
}

/// # Safety
/// `handle` is NULL or a live writer; `variable_name` is NULL or
/// NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_variable_cell(handle: *mut TraceWriterHandle, variable_name: *const c_char, place: i64) -> i32 {
    with_handle_or(
        "trace_writer_register_variable_cell",
        handle,
        true,
        1,
        "trace_writer_register_variable_cell: NULL handle",
        |h| {
            let name = unsafe { cstr_string(variable_name) };
            hold_event(h, "trace_writer_register_variable_cell", |w| {
                let variable_id = AbstractTraceWriter::ensure_variable_id(w, &name);
                (
                    TraceLowLevelEvent::VariableCell(VariableCellRecord {
                        variable_id,
                        place: Place(place),
                    }),
                    None,
                )
            })
        },
    )
}

/// # Safety
/// `handle` is NULL or a live writer; `(value_cbor, value_cbor_len)` is
/// readable or NULL/0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_cell_value(
    handle: *mut TraceWriterHandle,
    place: i64,
    value_cbor: *const u8,
    value_cbor_len: usize,
) -> i32 {
    with_handle_or(
        "trace_writer_register_cell_value",
        handle,
        true,
        1,
        "trace_writer_register_cell_value: NULL handle",
        |h| {
            let cbor = unsafe { bytes(value_cbor, value_cbor_len) }.to_vec();
            hold_event(h, "trace_writer_register_cell_value", |_| {
                (
                    TraceLowLevelEvent::CellValue(CellValueRecord {
                        place: Place(place),
                        value: codetracer_trace_types::NONE_VALUE,
                    }),
                    Some(cbor),
                )
            })
        },
    )
}

/// # Safety
/// `handle` is NULL or a live writer; `(value_cbor, value_cbor_len)` is
/// readable or NULL/0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_compound_value(
    handle: *mut TraceWriterHandle,
    place: i64,
    value_cbor: *const u8,
    value_cbor_len: usize,
) -> i32 {
    with_handle_or(
        "trace_writer_register_compound_value",
        handle,
        true,
        1,
        "trace_writer_register_compound_value: NULL handle",
        |h| {
            let cbor = unsafe { bytes(value_cbor, value_cbor_len) }.to_vec();
            hold_event(h, "trace_writer_register_compound_value", |_| {
                (
                    TraceLowLevelEvent::CompoundValue(CompoundValueRecord {
                        place: Place(place),
                        value: codetracer_trace_types::NONE_VALUE,
                    }),
                    Some(cbor),
                )
            })
        },
    )
}

/// # Safety
/// `handle` is NULL or a live writer; `(new_value_cbor, new_value_cbor_len)`
/// is readable or NULL/0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_assign_cell(
    handle: *mut TraceWriterHandle,
    place: i64,
    new_value_cbor: *const u8,
    new_value_cbor_len: usize,
) -> i32 {
    with_handle_or(
        "trace_writer_assign_cell",
        handle,
        true,
        1,
        "trace_writer_assign_cell: NULL handle",
        |h| {
            let cbor = unsafe { bytes(new_value_cbor, new_value_cbor_len) }.to_vec();
            hold_event(h, "trace_writer_assign_cell", |_| {
                (
                    TraceLowLevelEvent::AssignCell(AssignCellRecord {
                        place: Place(place),
                        new_value: codetracer_trace_types::NONE_VALUE,
                    }),
                    Some(cbor),
                )
            })
        },
    )
}

/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_assign_compound_item(handle: *mut TraceWriterHandle, place: i64, index: u64, item_place: i64) -> i32 {
    with_handle_or(
        "trace_writer_assign_compound_item",
        handle,
        true,
        1,
        "trace_writer_assign_compound_item: NULL handle",
        |h| {
            hold_event(h, "trace_writer_assign_compound_item", |_| {
                (
                    TraceLowLevelEvent::AssignCompoundItem(AssignCompoundItemRecord {
                        place: Place(place),
                        index: index as usize,
                        item_place: Place(item_place),
                    }),
                    None,
                )
            })
        },
    )
}

/// `BindVariable` for a host with no use for the failure value.
///
/// # Safety
/// `handle` is NULL or a live writer; `variable_name` is NULL or
/// NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_bind_variable(handle: *mut TraceWriterHandle, variable_name: *const c_char, place: i64) {
    with_handle("ct_bind_variable", handle, true, (), (), |h| {
        let name = unsafe { cstr_string(variable_name) };
        if let Some(w) = h.legacy.as_mut() {
            TraceWriter::bind_variable(&mut **w, &name, Place(place));
            return;
        }
        if h.ctfs.is_none() {
            set_error("ct_bind_variable: writer is not ready");
            return;
        }
        let _ = hold_event(h, "ct_bind_variable", |w| {
            let variable_id = AbstractTraceWriter::ensure_variable_id(w, &name);
            (
                TraceLowLevelEvent::BindVariable(BindVariableRecord {
                    variable_id,
                    place: Place(place),
                }),
                None,
            )
        });
    })
}

/// An assignment whose right-hand side is given by kind and operands.
///
/// # Safety
/// `handle` is NULL or a live writer; the strings are NULL or NUL-terminated;
/// `compound_ids` holds `compound_len` entries or is NULL.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn ct_assignment(
    handle: *mut TraceWriterHandle,
    target_name: *const c_char,
    pass_by: i32,
    rvalue_kind: i32,
    simple_variable_id: usize,
    compound_ids: *const usize,
    compound_len: usize,
    field_name: *const c_char,
    index: i64,
    call_key: i64,
) {
    with_handle("ct_assignment", handle, true, (), (), |h| {
        let rvalue = match rvalue_kind {
            0 => RValue::Simple(VariableId(simple_variable_id)),
            1 => RValue::Compound(if compound_ids.is_null() || compound_len == 0 {
                vec![]
            } else {
                unsafe { std::slice::from_raw_parts(compound_ids, compound_len) }
                    .iter()
                    .copied()
                    .map(VariableId)
                    .collect()
            }),
            2 => RValue::Literal,
            3 => RValue::FieldAccess {
                receiver: VariableId(simple_variable_id),
                field: unsafe { cstr_string(field_name) },
            },
            4 => RValue::IndexAccess {
                receiver: VariableId(simple_variable_id),
                index,
            },
            5 => RValue::FunctionReturn { call_key: CallKey(call_key) },
            other => {
                set_error(&format!("ct_assignment: {other} is not an RValue kind"));
                return;
            }
        };
        let pass = if pass_by == 0 { PassBy::Value } else { PassBy::Reference };
        let name = unsafe { cstr_string(target_name) };
        if let Some(w) = h.legacy.as_mut() {
            TraceWriter::assign(&mut **w, &name, rvalue, pass);
            return;
        }
        if h.ctfs.is_none() {
            set_error("ct_assignment: writer is not ready");
            return;
        }
        let _ = hold_event(h, "ct_assignment", |w| {
            let to = AbstractTraceWriter::ensure_variable_id(w, &name);
            (
                TraceLowLevelEvent::Assignment(AssignmentRecord {
                    to,
                    pass_by: pass,
                    from: rvalue,
                }),
                None,
            )
        });
    })
}

// ---------------------------------------------------------------------------
// I/O events, threads, exceptions
// ---------------------------------------------------------------------------

/// `kind` is the event's `EventLogKind` ordinal (0-13); another value is
/// refused.
///
/// # Safety
/// `handle` is NULL or a live writer; `metadata` and `content` are NULL or
/// NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_special_event(
    handle: *mut TraceWriterHandle,
    kind: i32,
    metadata: *const c_char,
    content: *const c_char,
) {
    with_handle("trace_writer_register_special_event", handle, true, (), (), |h| {
        let ordinal = u8::try_from(kind).ok().filter(|k| *k <= 13);
        if let Some(w) = h.legacy.as_mut() {
            let Some(kind) = ordinal.and_then(EventLogKind::from_u8) else {
                set_error(&format!("trace_writer_register_special_event: {kind} is not an EventLogKind ordinal"));
                return;
            };
            TraceWriter::register_special_event(&mut **w, kind, &unsafe { cstr_string(metadata) }, &unsafe { cstr_string(content) });
            return;
        }
        let pending = h.pending_step.is_some();
        let Some(w) = h.ctfs.as_mut() else {
            return;
        };
        let Some(ordinal) = ordinal else {
            set_error(&format!("trace_writer_register_special_event: {kind} is not an EventLogKind ordinal"));
            return;
        };
        let count = w.exec_record_count();
        let step = if pending { count } else { count.saturating_sub(1) };
        if let Err(e) = w.register_io_event_at(ordinal, unsafe { cstr_bytes(metadata) }, unsafe { cstr_bytes(content) }, step) {
            set_error(&e);
        }
    })
}

fn thread_event(name: &str, handle: *mut TraceWriterHandle, event: TraceLowLevelEvent) {
    with_handle(name, handle, true, (), (), |h| {
        if let Some(w) = h.legacy.as_mut() {
            TraceWriter::add_event(&mut **w, event);
            return;
        }
        if h.ctfs.is_none() {
            return;
        }
        let _ = flush_pending_step(h);
        AbstractTraceWriter::add_event(h.ctfs.as_mut().expect("checked above"), event);
    })
}

/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_thread_start(handle: *mut TraceWriterHandle, thread_id: u64) {
    thread_event(
        "trace_writer_register_thread_start",
        handle,
        TraceLowLevelEvent::ThreadStart(ThreadId(thread_id)),
    )
}

/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_thread_exit(handle: *mut TraceWriterHandle, thread_id: u64) {
    thread_event(
        "trace_writer_register_thread_exit",
        handle,
        TraceLowLevelEvent::ThreadExit(ThreadId(thread_id)),
    )
}

/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_thread_switch(handle: *mut TraceWriterHandle, thread_id: u64) {
    thread_event(
        "trace_writer_register_thread_switch",
        handle,
        TraceLowLevelEvent::ThreadSwitch(ThreadId(thread_id)),
    )
}

/// # Safety
/// `handle` is NULL or a live writer; `(message, message_len)` is readable or
/// NULL/0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_raise(handle: *mut TraceWriterHandle, exception_type_id: u64, message: *const u8, message_len: usize) {
    with_handle("trace_writer_register_raise", handle, true, (), (), |h| {
        if message.is_null() && message_len > 0 {
            set_error(&format!("trace_writer_register_raise: NULL message with length {message_len}"));
            return;
        }
        if !h.multi() {
            set_error("trace_writer_register_raise: the legacy writer records no Raise events; use the split-stream writer");
            return;
        }
        if h.ctfs.is_none() {
            return;
        }
        let msg = unsafe { bytes(message, message_len) }.to_vec();
        let _ = flush_pending_step(h);
        if let Err(e) = h.ctfs.as_mut().expect("checked above").register_raise(exception_type_id, &msg) {
            set_error(&e);
        }
    })
}

/// # Safety
/// `handle` is NULL or a live writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn trace_writer_register_catch(handle: *mut TraceWriterHandle, exception_type_id: u64) {
    with_handle("trace_writer_register_catch", handle, true, (), (), |h| {
        if !h.multi() {
            set_error("trace_writer_register_catch: the legacy writer records no Catch events; use the split-stream writer");
            return;
        }
        if h.ctfs.is_none() {
            return;
        }
        let _ = flush_pending_step(h);
        if let Err(e) = h.ctfs.as_mut().expect("checked above").register_catch(exception_type_id) {
            set_error(&e);
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The JSON writer still writes its three files through the handle.
    #[test]
    fn the_json_format_writes_its_files() {
        let dir = tempfile::tempdir().unwrap();
        let c = |name: &str| std::ffi::CString::new(dir.path().join(name).to_str().unwrap()).unwrap();
        let program = std::ffi::CString::new("test_program").unwrap();
        let handle = unsafe { trace_writer_new(program.as_ptr(), FORMAT_JSON) };
        assert!(!handle.is_null());
        assert_eq!(unsafe { trace_writer_begin_metadata(handle, c("trace_metadata.json").as_ptr()) }, 0);
        assert_eq!(unsafe { trace_writer_begin_events(handle, c("trace.json").as_ptr()) }, 0);
        assert_eq!(unsafe { trace_writer_begin_paths(handle, c("trace_paths.json").as_ptr()) }, 0);
        let source = std::ffi::CString::new("/test/main.rs").unwrap();
        unsafe { trace_writer_start(handle, source.as_ptr(), 1) };
        unsafe { trace_writer_register_step(handle, source.as_ptr(), 2) };
        let var = std::ffi::CString::new("x").unwrap();
        let ty = std::ffi::CString::new("i32").unwrap();
        unsafe { trace_writer_register_variable_int(handle, var.as_ptr(), 42, 7, ty.as_ptr()) };
        assert_eq!(unsafe { trace_writer_finish_events(handle) }, 0);
        assert_eq!(unsafe { trace_writer_finish_metadata(handle) }, 0);
        assert_eq!(unsafe { trace_writer_finish_paths(handle) }, 0);
        assert_eq!(unsafe { trace_writer_close(handle) }, 0);
        unsafe { trace_writer_free(handle) };
        let trace: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(dir.path().join("trace.json")).unwrap()).unwrap();
        assert!(trace.as_array().is_some_and(|a| !a.is_empty()));
        assert!(dir.path().join("trace_metadata.json").exists());
        assert!(dir.path().join("trace_paths.json").exists());
    }
}
