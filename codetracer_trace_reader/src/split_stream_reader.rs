//! Reconstruct a linear `Vec<TraceLowLevelEvent>` from a split-stream CTFS
//! container — one that carries no `events.log`.
//!
//! # Why this exists
//!
//! `read_trace_from_ctfs` reads `events.log` and nothing else. That stream is
//! not in the spec: `internal-files.md` defines `events.dat`, and the event
//! disposition table records the combined log as *moved to events.dat*. So a
//! conformant container has no `events.log`, and until this module existed the
//! Rust reader could not read one — the per-stream readers beside this file all
//! yield stream RECORDS, and nothing assembled them.
//!
//! # What it is not
//!
//! It is not a port of Nim's `NewTraceReader`. That reader is RANDOM ACCESS —
//! `step(i)`, `value(i)`, `ioEvent(i)` — and is the right shape for a debugger
//! seeking around a recording. This produces a SEQUENCE, because that is what
//! `load_trace_events` returns and what every consumer of this crate expects.
//! The decoding of each individual stream is shared with that reader (the
//! wire formats are the same, and `nim_*_crossread` tests pin that); only the
//! assembly is new.
//!
//! # The assembly order, and why it is this one
//!
//! The streams are parallel-indexed rather than interleaved, so the original
//! event order is not stored anywhere and has to be RECONSTRUCTED. The order
//! below is the one that satisfies the format's own stated invariants — from
//! `types.rs`: *`Path` should always be generated before usage*, *`Type` before
//! a `Value` referencing it*, *`Function` before a `CallRecord` referencing
//! it*:
//!
//! 1. Every interning table, in id order: `Path`, `VariableName`, `Type`,
//!    `Function`. All four precede anything that can reference them.
//! 2. Then one pass over the step stream. At step `i`, in this order:
//!    a. `Call` for every call whose `first_step_id` is `i`, by `call_key` —
//!       a call is entered before the step it lands on.
//!    b. the step record itself (`Step`, or `ThreadSwitch`/`ThreadStart`/
//!       `ThreadExit` for the non-step tags the stream carries).
//!    c. the value stream's events for step `i`, in stream order.
//!    d. `Event` for every I/O record whose `step_id` is `i` — I/O is attributed
//!       to the most recently emitted step, which is how the writer records it.
//!    e. `Return` for every call whose `last_step_id` is `i`, deepest first, so
//!       a nested call closes before its parent.
//!
//! # What `TraceLowLevelEvent` cannot spell
//!
//! The streams carry four things the event enum has no variant or field for:
//! a column-aware step's COLUMN, a call's raised EXCEPTION and CHILDREN, the
//! `Raise`/`Catch` exec records, and SOURCE RELOAD markers (with the path
//! versions they mint). Adding variants to that published enum would break
//! every exhaustive match downstream, so [`read_trace_with_details`] /
//! [`read_window_with_details`] return them beside the events in a
//! [`SplitStreamDetails`], each item tied to the event sequence by
//! `event_index` and to the execution stream by `step_index`. The plain
//! [`read_trace_from_split_streams`] / [`read_window`] skip that work.
//!
//! # Refusal
//!
//! Every failure here names what was missing and why. This campaign's most
//! expensive recurring defect is a reader that returns nothing and reports
//! success — three instances so far, all of them silent empties. A container
//! without a step stream is REFUSED rather than read as a recording with no
//! steps, because those two are different claims and only one of them is
//! usually true.

use codetracer_ctfs::CtfsReader;
use codetracer_trace_types::*;
use codetracer_trace_writer::call_stream::CallStreamRecord;
use codetracer_trace_writer::line_position::LinePositionSpace;
use codetracer_trace_writer::step_stream::StepStreamRecord;
use codetracer_trace_writer::value_stream::ValueStreamEvent;

use crate::call_stream_reader::CallStreamReader;
use crate::interning_tables_reader::InterningTablesReader;
use crate::io_event_stream_reader::IoEventStreamReader;
use crate::step_stream_reader::StepStreamReader;
use crate::value_stream_reader::ValueStreamReader;

/// Decode a CBOR payload that the writer stored as an opaque blob.
///
/// An empty payload is `None` rather than an error: the writer stores "no
/// args" and "no value" as zero bytes, and that is a legitimate state.
fn decode_cbor<T: serde::de::DeserializeOwned>(bytes: &[u8], what: &str) -> Result<Option<T>, String> {
    if bytes.is_empty() {
        return Ok(None);
    }
    cbor4ii::serde::from_slice::<T>(bytes)
        .map(Some)
        .map_err(|e| format!("split-stream reader: {what} is not decodable CBOR: {e}"))
}

/// True when the container carries the split streams this module reads.
///
/// The test is `steps.dat`, not the ABSENCE of `events.log`. Nim's reader
/// infers v4 from that absence (`codetracer_trace_reader.nim`), which works
/// only while the legacy stream still exists to be absent — once it is gone
/// from every writer the inference has nothing to test. A positive test for the
/// stream this reader actually needs does not have that problem.
pub fn has_split_streams(reader: &CtfsReader) -> bool {
    reader.list_files().iter().any(|f| f == "steps.dat")
}

/// Reconstruct the whole event sequence from a container's split streams.
pub fn read_trace_from_split_streams(reader: &mut CtfsReader) -> Result<Vec<TraceLowLevelEvent>, String> {
    read_window(reader, 0, u64::MAX)
}

/// The events of a split-stream container, together with what the streams
/// carry that no `TraceLowLevelEvent` can.
#[derive(Debug, Clone)]
pub struct SplitStreamTrace {
    pub events: Vec<TraceLowLevelEvent>,
    pub details: SplitStreamDetails,
}

/// What the split streams carry beyond `TraceLowLevelEvent`, for the events
/// read alongside it.
///
/// `event_index` is an index into those events. A record that has no event of
/// its own (`Raise`, `Catch`, a reload marker) sits immediately BEFORE
/// `events[event_index]` — it equals `events.len()` when nothing follows it.
/// `step_index` is the record's index in the execution stream (`steps.dat`),
/// the index `calls.dat` step ranges and `events.dat` are expressed in.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SplitStreamDetails {
    /// The column of every `Step` event of a column-aware trace, in event
    /// order. Empty for a line-only trace, whose positions have no column.
    pub step_columns: Vec<StepColumn>,
    /// Every call entered in the read, in entry order.
    pub calls: Vec<CallDetail>,
    /// Every `Raise` and `Catch` exec record in the read, in stream order.
    pub exception_events: Vec<ExceptionEvent>,
    /// Every source reload marker in the read, in stream order.
    pub source_reloads: Vec<SourceReloadMarker>,
    /// Every path's version, indexed by path id — see
    /// [`InterningTablesReader::path_versions`].
    pub path_versions: Vec<crate::interning_tables_reader::PathVersion>,
}

/// The column a `Step` event was recorded at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StepColumn {
    /// Index of the `Step` event.
    pub event_index: usize,
    pub step_index: u64,
    /// 1-based, as the line is.
    pub column: u64,
}

/// A call's `calls.dat` record, beyond what its `Call`/`Return` events carry.
#[derive(Debug, Clone, PartialEq)]
pub struct CallDetail {
    pub call_key: u64,
    /// `None` for a root call.
    pub parent_key: Option<u64>,
    pub depth: u64,
    /// The calls this one made, by `call_key`, in entry order.
    pub children: Vec<u64>,
    /// The exception the call ended by raising; `None` when it returned.
    pub raised_exception: Option<ValueRecord>,
    pub first_step_id: u64,
    pub last_step_id: u64,
    /// Index of this call's `Call` event.
    pub call_event_index: usize,
    /// Index of its `Return` event; `None` when the return falls after the
    /// read (a window that ends inside the call).
    pub return_event_index: Option<usize>,
}

/// A `Raise` or `Catch` exec record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExceptionEvent {
    pub event_index: usize,
    pub step_index: u64,
    pub kind: ExceptionEventKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExceptionEventKind {
    /// An exception was raised, before unwinding.
    Raise { exception_type_id: u64, message: Vec<u8> },
    /// An exception was caught by a handler.
    Catch { exception_type_id: u64 },
}

/// A source reload marker (`trace-events.md` §"Source Reload Marker (Tag
/// 0x08)"). The `step_index` is what ties the reload to the steps on either
/// side of it; Nim's `sourceReloads` reports the same records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceReloadMarker {
    pub event_index: usize,
    pub step_index: u64,
    pub reload_ordinal: u64,
    pub changed: Vec<codetracer_trace_writer::step_stream::SourceReloadChange>,
    pub in_flight_frames: u64,
}

/// [`read_trace_from_split_streams`], with the [`SplitStreamDetails`].
pub fn read_trace_with_details(reader: &mut CtfsReader) -> Result<SplitStreamTrace, String> {
    read_window_with_details(reader, 0, u64::MAX)
}

/// [`read_window`], with the [`SplitStreamDetails`] of the window. Path
/// versions are reported for every path, as the window's `Path` events are.
pub fn read_window_with_details(reader: &mut CtfsReader, start_step: u64, max_steps: u64) -> Result<SplitStreamTrace, String> {
    let mut details = SplitStreamDetails::default();
    let events = assemble(reader, start_step, max_steps, Some(&mut details))?;
    Ok(SplitStreamTrace { events, details })
}

/// Reconstruct the events for a WINDOW of steps, `[start_step, start_step + max_steps)`.
///
/// **The unit is STEPS, and it has to be.** The combined `events.log` had a
/// global event ordinal, and `seek_events_in_ctfs` seeked on it. A split-stream
/// container has no such number: the events are spread across four streams that
/// are indexed by step, by call key and by record, and the single ordering they
/// share is the one this module reconstructs. So a caller asking to seek is
/// answered in the unit the container actually indexes.
///
/// The interning tables are emitted for every window, not only the first,
/// because a window is meant to be independently usable and the format's own
/// rule is that a `Path`, `Type` or `Function` precedes anything referencing it.
pub fn read_window(reader: &mut CtfsReader, start_step: u64, max_steps: u64) -> Result<Vec<TraceLowLevelEvent>, String> {
    assemble(reader, start_step, max_steps, None)
}

/// The assembly behind every read. `details` is filled when given; when it is
/// `None` nothing beyond the events is decoded or kept.
fn assemble(
    reader: &mut CtfsReader,
    start_step: u64,
    max_steps: u64,
    mut details: Option<&mut SplitStreamDetails>,
) -> Result<Vec<TraceLowLevelEvent>, String> {
    crate::retired_streams::refuse_retired_members(reader)?;
    let mut out: Vec<TraceLowLevelEvent> = Vec::new();

    // ---- 1. the interning tables ------------------------------------------
    //
    // REQUIRED, and refused by name when absent. Every other stream references
    // these by id; without them a step's path and a call's function cannot be
    // resolved at all, and emitting steps that point at nothing would be the
    // silent-empty defect in a different costume.
    let tables = InterningTablesReader::open(reader)
        .map_err(|e| format!("split-stream reader: interning tables would not open: {e}"))?
        .ok_or_else(|| {
            "split-stream reader: this container has no interning tables (paths.dat/funcs.dat/\
             types.dat/varnames.dat), so no step, call or value in it can be resolved"
                .to_string()
        })?;

    for path_id in 0..tables.path_count() {
        let p = tables
            .path_str(path_id as u64)
            .map_err(|e| format!("split-stream reader: path {path_id} is unreadable: {e}"))?;
        out.push(TraceLowLevelEvent::Path(std::path::PathBuf::from(p)));
    }
    if let Some(d) = details.as_deref_mut() {
        d.path_versions = tables
            .path_versions()
            .map_err(|e| format!("split-stream reader: path versions are unreadable: {e}"))?;
    }
    for name_id in 0..tables.varname_count() {
        let n = tables
            .varname_str(name_id as u64)
            .map_err(|e| format!("split-stream reader: variable name {name_id} is unreadable: {e}"))?;
        // Both variants are emitted because both exist in the format and
        // consumers differ in which they match on: `VariableName` is the
        // current spelling and `Variable` is kept for backward compatibility,
        // as `types.rs` says at the declaration.
        out.push(TraceLowLevelEvent::VariableName(n));
    }
    for type_id in 0..tables.type_count() {
        let t = tables
            .type_record(type_id as u64)
            .map_err(|e| format!("split-stream reader: type {type_id} is unreadable: {e}"))?;
        out.push(TraceLowLevelEvent::Type(TypeRecord {
            kind: t.type_kind().unwrap_or(TypeKind::Raw),
            lang_type: String::from_utf8_lossy(&t.lang_type).into_owned(),
            specific_info: TypeSpecificInfo::None,
        }));
    }

    // The position space is built from the per-file line counts the container
    // records, and it is what turns a step's `global_line_index` back into a
    // (path, line) pair. It must be built from the SAME table the writer
    // interned against, which is why it comes from `tables` rather than from a
    // count of anything observed in the streams.
    // A container that records REAL per-file line counts gives the exact space.
    // One that does not — every line-only trace today — gets the uniform space
    // the writer itself addressed against, which is what makes `resolve` the
    // exact inverse of the `global_index` that produced these addresses. Taking
    // the real counts when they are absent would build a SHORTER space and
    // resolve every address in the wrong file, silently.
    let line_counts = tables.line_counts();
    let space = if line_counts.is_empty() {
        LinePositionSpace::uniform(tables.path_count())
    } else {
        LinePositionSpace::from_line_counts(line_counts)
    };

    // STEP addresses are a different space in a column-aware trace: each
    // `global_position_index` names a `(line, column)` pair, a file with a
    // per-line table is `sum(line_lengths)` addresses wide and one with the
    // conventional table (`line_count = 0`) 100000 × 1024 (`trace-events.md`
    // §"Source Location Addressing", `internal-files.md` §"`paths.dat` Layout
    // A"). Resolving those through the line space above read a
    // column as a line and placed every later file at the wrong base.
    // `funcs.dat` addresses stay line addresses in both modes, so `space`
    // above is still the one functions resolve through.
    let mut step_space = if tables.is_column_aware() {
        let mut ps = codetracer_trace_writer::column_aware::PositionSpace::new(true);
        for path_id in 0..tables.path_count() {
            let table = tables
                .path_file_table(path_id as u64)
                .map_err(|e| format!("split-stream reader: path {path_id}'s line table is unreadable: {e}"))?
                .ok_or_else(|| format!("split-stream reader: path {path_id} has no Layout A table"))?;
            ps.push_file_table(&table);
        }
        Some(ps)
    } else {
        None
    };

    for function_id in 0..tables.func_count() {
        let f = tables
            .func(function_id as u64)
            .map_err(|e| format!("split-stream reader: function {function_id} is unreadable: {e}"))?;
        let (path_id, line) = f
            .path_id_and_line(&space)
            .map_err(|e| format!("split-stream reader: function {function_id} has an undecodable position: {e:?}"))?;
        out.push(TraceLowLevelEvent::Function(FunctionRecord {
            path_id: PathId(path_id),
            line: Line(line),
            name: String::from_utf8_lossy(&f.name).into_owned(),
        }));
    }

    // ---- 2. the streams ----------------------------------------------------
    let mut steps = StepStreamReader::open(reader)
        .map_err(|e| format!("split-stream reader: the step stream would not open: {e}"))?
        .ok_or_else(|| {
            "split-stream reader: this container has no steps.dat, so it is not a split-stream \
             recording this reader can assemble"
                .to_string()
        })?;
    let mut values = ValueStreamReader::open(reader).map_err(|e| format!("split-stream reader: the value stream would not open: {e}"))?;
    let mut calls = CallStreamReader::open(reader).map_err(|e| format!("split-stream reader: the call stream would not open: {e}"))?;
    let mut io = IoEventStreamReader::open(reader).map_err(|e| format!("split-stream reader: the I/O event stream would not open: {e}"))?;

    // Calls are keyed by `call_key` and carry the step range they cover, so
    // they are bucketed by entry step and by exit step up front rather than
    // rescanned per step.
    let call_count = calls.as_ref().map(|c| c.count()).unwrap_or(0);
    let mut all_calls: Vec<CallStreamRecord> = Vec::with_capacity(call_count as usize);
    if let Some(ref mut c) = calls {
        for key in 0..call_count {
            all_calls.push(c.read(key).map_err(|e| format!("split-stream reader: call {key} is unreadable: {e}"))?);
        }
    }
    let step_count = steps.count();
    let mut entering: Vec<Vec<usize>> = vec![Vec::new(); step_count as usize + 1];
    let mut leaving: Vec<Vec<usize>> = vec![Vec::new(); step_count as usize + 1];
    for (i, c) in all_calls.iter().enumerate() {
        if c.first_step_id <= step_count {
            entering[c.first_step_id.min(step_count) as usize].push(i);
        }
        if c.last_step_id <= step_count {
            leaving[c.last_step_id.min(step_count) as usize].push(i);
        }
    }
    // A nested call must close before its parent, so exits at the same step go
    // deepest first. Entries stay in `call_key` order, which is entry order.
    for bucket in leaving.iter_mut() {
        bucket.sort_by_key(|&i| std::cmp::Reverse(all_calls[i].depth));
    }

    let io_count = io.as_ref().map(|r| r.count()).unwrap_or(0);
    let mut io_by_step: std::collections::HashMap<u64, Vec<(EventLogKind, String, String)>> = std::collections::HashMap::new();
    if let Some(ref mut r) = io {
        for idx in 0..io_count {
            let rec = r
                .read(idx)
                .map_err(|e| format!("split-stream reader: I/O event {idx} is unreadable: {e}"))?;
            io_by_step.entry(rec.step_id).or_default().push((
                codetracer_trace_writer::event_stream::event_log_kind(rec.kind).map_err(|e| format!("split-stream reader: I/O event {idx}: {e}"))?,
                String::from_utf8_lossy(&rec.metadata).into_owned(),
                String::from_utf8_lossy(&rec.content).into_owned(),
            ));
        }
    }

    // Which `details.calls` entry each call record became, so its `Return`
    // can be tied back to it. Only kept when details are wanted.
    let mut call_detail_of: Vec<usize> = if details.is_some() {
        vec![usize::MAX; all_calls.len()]
    } else {
        Vec::new()
    };

    // ---- 3. one pass over the steps ---------------------------------------
    let window_end = start_step.saturating_add(max_steps).min(step_count);
    for i in start_step.min(step_count)..window_end {
        for &ci in &entering[i as usize] {
            let c = &all_calls[ci];
            // One `calls.dat` entry per argument, each carrying its own
            // interned name (`trace-events.md` §"Call Stream (`calls.dat`)").
            // The name is the reason the entries are separate: a
            // `FullValueRecord` is a (variable, value) pair and there is
            // nowhere else to recover the variable from.
            let mut args: Vec<FullValueRecord> = Vec::with_capacity(c.args.len());
            for arg in &c.args {
                if let Some(value) = decode_cbor::<ValueRecord>(&arg.value, "a call argument")? {
                    args.push(FullValueRecord {
                        variable_id: VariableId(arg.varname_id as usize),
                        value,
                    });
                }
            }
            if let Some(d) = details.as_deref_mut() {
                call_detail_of[ci] = d.calls.len();
                d.calls.push(CallDetail {
                    call_key: c.call_key,
                    parent_key: u64::try_from(c.parent_key).ok(),
                    depth: c.depth,
                    children: c.children.clone(),
                    raised_exception: decode_cbor::<ValueRecord>(&c.raised_exception, "a call's raised exception")?,
                    first_step_id: c.first_step_id,
                    last_step_id: c.last_step_id,
                    call_event_index: out.len(),
                    return_event_index: None,
                });
            }
            out.push(TraceLowLevelEvent::Call(CallRecord {
                function_id: FunctionId(c.function_id as usize),
                args,
            }));
        }

        let rec = steps.read(i).map_err(|e| format!("split-stream reader: step {i} is unreadable: {e}"))?;
        match rec {
            StepStreamRecord::Step { global_line_index }
            | StepStreamRecord::DeltaColumn {
                global_position_index: global_line_index,
                ..
            } => {
                let (path_id, line) = match step_space.as_mut() {
                    Some(ps) => {
                        let (file, line, column) = ps.resolve(global_line_index).ok_or_else(|| {
                            format!("split-stream reader: step {i} has position {global_line_index}, outside the column-aware space")
                        })?;
                        if let (Some(d), Some(column)) = (details.as_deref_mut(), column) {
                            d.step_columns.push(StepColumn {
                                event_index: out.len(),
                                step_index: i,
                                column,
                            });
                        }
                        (file as usize, line as i64)
                    }
                    None => space
                        .resolve(global_line_index)
                        .map_err(|e| format!("split-stream reader: step {i} has an undecodable position {global_line_index}: {e:?}"))?,
                };
                out.push(TraceLowLevelEvent::Step(StepRecord {
                    path_id: PathId(path_id),
                    line: Line(line),
                }));
            }
            StepStreamRecord::ThreadSwitch { thread_id } => {
                out.push(TraceLowLevelEvent::ThreadSwitch(ThreadId(thread_id)));
            }
            StepStreamRecord::ThreadStart { thread_id } => {
                out.push(TraceLowLevelEvent::ThreadStart(ThreadId(thread_id)));
            }
            StepStreamRecord::ThreadExit { thread_id } => {
                out.push(TraceLowLevelEvent::ThreadExit(ThreadId(thread_id)));
            }
            // `Raise`, `Catch` and reload markers have no `TraceLowLevelEvent`
            // spelling, so they are reported in the details rather than mapped
            // onto something they are not. Each is still an exec record: its
            // (empty) value record and anything attributed to its index are
            // emitted below.
            StepStreamRecord::Raise { exception_type_id, message } => {
                if let Some(d) = details.as_deref_mut() {
                    d.exception_events.push(ExceptionEvent {
                        event_index: out.len(),
                        step_index: i,
                        kind: ExceptionEventKind::Raise { exception_type_id, message },
                    });
                }
            }
            StepStreamRecord::Catch { exception_type_id } => {
                if let Some(d) = details.as_deref_mut() {
                    d.exception_events.push(ExceptionEvent {
                        event_index: out.len(),
                        step_index: i,
                        kind: ExceptionEventKind::Catch { exception_type_id },
                    });
                }
            }
            StepStreamRecord::SourceReload {
                reload_ordinal,
                changed,
                in_flight_frames,
            } => {
                if let Some(d) = details.as_deref_mut() {
                    d.source_reloads.push(SourceReloadMarker {
                        event_index: out.len(),
                        step_index: i,
                        reload_ordinal,
                        changed,
                        in_flight_frames,
                    });
                }
            }
        }

        if let Some(ref mut v) = values
            && i < v.count()
        {
            let entry = v
                .read(i)
                .map_err(|e| format!("split-stream reader: values for step {i} are unreadable: {e}"))?;
            for ev in entry.events {
                push_value_event(&mut out, ev)?;
            }
        }

        if let Some(events) = io_by_step.get(&i) {
            for (kind, metadata, content) in events {
                out.push(TraceLowLevelEvent::Event(RecordEvent {
                    kind: *kind,
                    metadata: metadata.clone(),
                    content: content.clone(),
                }));
            }
        }

        for &ci in &leaving[i as usize] {
            let c = &all_calls[ci];
            let return_value = decode_return_value(&c.return_value)?;
            if let Some(d) = details.as_deref_mut()
                && let Some(detail) = d.calls.get_mut(call_detail_of[ci])
            {
                detail.return_event_index = Some(out.len());
            }
            out.push(TraceLowLevelEvent::Return(ReturnRecord { return_value }));
        }
    }

    Ok(out)
}

/// The writer stores a void return as a single marker byte rather than as CBOR,
/// so that case is recognised before anything tries to decode it.
fn decode_return_value(bytes: &[u8]) -> Result<ValueRecord, String> {
    use codetracer_trace_writer::call_stream::VOID_RETURN_MARKER;
    if bytes.is_empty() || bytes == [VOID_RETURN_MARKER] {
        return Ok(ValueRecord::None { type_id: TypeId(0) });
    }
    Ok(decode_cbor::<ValueRecord>(bytes, "a call's return value")?.unwrap_or(ValueRecord::None { type_id: TypeId(0) }))
}

fn push_value_event(out: &mut Vec<TraceLowLevelEvent>, ev: ValueStreamEvent) -> Result<(), String> {
    match ev {
        ValueStreamEvent::StepValues { values } => {
            for (name_id, blob) in values {
                if let Some(value) = decode_cbor::<ValueRecord>(&blob, "a step value")? {
                    out.push(TraceLowLevelEvent::Value(FullValueRecord {
                        variable_id: VariableId(name_id as usize),
                        value,
                    }));
                }
            }
        }
        ValueStreamEvent::BindVariable { variable_id, place } => {
            out.push(TraceLowLevelEvent::BindVariable(BindVariableRecord {
                variable_id: VariableId(variable_id as usize),
                place: Place(place),
            }));
        }
        ValueStreamEvent::DropVariable { variable_id } => {
            out.push(TraceLowLevelEvent::DropVariable(VariableId(variable_id as usize)));
        }
        ValueStreamEvent::DropVariables { variable_ids } => {
            out.push(TraceLowLevelEvent::DropVariables(
                variable_ids.into_iter().map(|v| VariableId(v as usize)).collect(),
            ));
        }
        ValueStreamEvent::CellValue { place, value } => {
            if let Some(value) = decode_cbor::<ValueRecord>(&value, "a cell value")? {
                out.push(TraceLowLevelEvent::CellValue(CellValueRecord { place: Place(place), value }));
            }
        }
        ValueStreamEvent::CompoundValue { place, value } => {
            if let Some(value) = decode_cbor::<ValueRecord>(&value, "a compound value")? {
                out.push(TraceLowLevelEvent::CompoundValue(CompoundValueRecord { place: Place(place), value }));
            }
        }
        ValueStreamEvent::AssignCell { place, new_value } => {
            if let Some(new_value) = decode_cbor::<ValueRecord>(&new_value, "an assigned cell value")? {
                out.push(TraceLowLevelEvent::AssignCell(AssignCellRecord {
                    place: Place(place),
                    new_value,
                }));
            }
        }
        ValueStreamEvent::AssignCompoundItem { place, index, item_place } => {
            out.push(TraceLowLevelEvent::AssignCompoundItem(AssignCompoundItemRecord {
                place: Place(place),
                index: index as usize,
                item_place: Place(item_place),
            }));
        }
        ValueStreamEvent::VariableCell { variable_id, place } => {
            out.push(TraceLowLevelEvent::VariableCell(VariableCellRecord {
                variable_id: VariableId(variable_id as usize),
                place: Place(place),
            }));
        }
        ValueStreamEvent::Assignment { to, pass_by, from } => {
            if let Some(from) = decode_cbor::<RValue>(&from, "an assignment source")? {
                out.push(TraceLowLevelEvent::Assignment(AssignmentRecord {
                    to: VariableId(to as usize),
                    pass_by: pass_by_from_ordinal(pass_by),
                    from,
                }));
            }
        }
        // A self-delimiting event this binary has no `TraceLowLevelEvent`
        // spelling for (tag >= 10, length-prefixed). Skipping it is the point
        // of the self-delimiting design — a reader older than a tag stays
        // usable instead of refusing the whole recording — but skipping it
        // QUIETLY would make a trace that carries records this reader cannot
        // show indistinguishable from one that carries none. So it is counted
        // and named once per tag, matching what the Nim reader reports for the
        // same container.
        ValueStreamEvent::Unknown { tag, payload } => {
            warn_unknown_value_tag_once(tag, payload.len());
        }
    }
    Ok(())
}

/// Name an unhandled value-stream tag on stderr, once per distinct tag per
/// process.
///
/// Once per tag rather than once per occurrence: an unknown tag typically
/// appears on a large fraction of the steps in a recording, and a per-record
/// warning would bury the rest of the output while telling the reader nothing
/// the first line did not.
fn warn_unknown_value_tag_once(tag: u8, payload_len: usize) {
    use std::sync::{Mutex, OnceLock};
    static SEEN: OnceLock<Mutex<std::collections::HashSet<u8>>> = OnceLock::new();
    let seen = SEEN.get_or_init(|| Mutex::new(std::collections::HashSet::new()));
    // A poisoned lock here means another thread panicked mid-warning; the
    // tally is advisory, so recover the set rather than propagate the panic
    // into a trace read that is otherwise fine.
    let mut seen = seen.lock().unwrap_or_else(|e| e.into_inner());
    if seen.insert(tag) {
        eprintln!(
            "WARNING: values.dat carries a tag-{tag} event ({payload_len} byte payload) that this \
             reader has no representation for; events of this tag are being SKIPPED. The \
             container was written by a newer writer — rebuild this binary from \
             codetracer-trace-format to see them."
        );
    }
}

fn pass_by_from_ordinal(pass_by: u8) -> PassBy {
    match pass_by {
        0 => PassBy::Value,
        _ => PassBy::Reference,
    }
}
