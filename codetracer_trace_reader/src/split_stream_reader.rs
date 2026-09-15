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
                event_log_kind_from_ordinal(rec.kind),
                String::from_utf8_lossy(&rec.metadata).into_owned(),
                String::from_utf8_lossy(&rec.content).into_owned(),
            ));
        }
    }

    // ---- 3. one pass over the steps ---------------------------------------
    let window_end = start_step.saturating_add(max_steps).min(step_count);
    for i in start_step.min(step_count)..window_end {
        for &ci in &entering[i as usize] {
            let c = &all_calls[ci];
            let args: Vec<FullValueRecord> = decode_cbor(&c.args, "a call's args")?.unwrap_or_default();
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
                let (path_id, line) = space
                    .resolve(global_line_index)
                    .map_err(|e| format!("split-stream reader: step {i} has an undecodable position {global_line_index}: {e:?}"))?;
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
            // `Raise` and `Catch` have no `TraceLowLevelEvent` spelling, so they
            // are carried in the step stream and dropped here rather than
            // mapped onto something they are not.
            StepStreamRecord::Raise { .. } | StepStreamRecord::Catch { .. } => {}
        }

        if let Some(ref mut v) = values {
            if i < v.count() {
                let entry = v
                    .read(i)
                    .map_err(|e| format!("split-stream reader: values for step {i} are unreadable: {e}"))?;
                for ev in entry.events {
                    push_value_event(&mut out, ev)?;
                }
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

fn event_log_kind_from_ordinal(kind: u8) -> EventLogKind {
    // The ordinals are `EventLogKind`'s declaration order, and they are spelled
    // out rather than guessed: the first draft of this list omitted the six
    // unused middle variants (`ReadDir` … `Open`), which shifted `Error` and
    // `TraceLogEvent` down by six and turned every recorded error into a
    // `Write`. The test that caught it compares the kind it wrote against the
    // kind it read back, which is the only reason a silent relabelling of one
    // enum member onto another was visible at all.
    match kind {
        0 => EventLogKind::Write,
        1 => EventLogKind::WriteFile,
        2 => EventLogKind::WriteOther,
        3 => EventLogKind::Read,
        4 => EventLogKind::ReadFile,
        5 => EventLogKind::ReadOther,
        6 => EventLogKind::ReadDir,
        7 => EventLogKind::OpenDir,
        8 => EventLogKind::CloseDir,
        9 => EventLogKind::Socket,
        10 => EventLogKind::Open,
        11 => EventLogKind::Error,
        12 => EventLogKind::TraceLogEvent,
        13 => EventLogKind::EvmEvent,
        _ => EventLogKind::Write,
    }
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
    }
    Ok(())
}

fn pass_by_from_ordinal(pass_by: u8) -> PassBy {
    match pass_by {
        0 => PassBy::Value,
        _ => PassBy::Reference,
    }
}
