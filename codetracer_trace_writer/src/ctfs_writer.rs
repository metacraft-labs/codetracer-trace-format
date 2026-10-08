use std::path::Path;

use codetracer_ctfs::CtfsWriter;

use crate::{
    abstract_trace_writer::{AbstractTraceWriter, AbstractTraceWriterData},
    call_stream::{CallStreamBuilder, DEFAULT_CALLS_CHUNK_SIZE},
    chunk_sink::ChunkSink,
    column_aware::{
        CONVENTIONAL_LINE_LENGTH, DEFAULT_LINES_PER_FILE, EXEC_COMPRESSION_LEVEL, ExecStreamEncoder, PositionSpace, StepEncoder,
        column_table_at_first_mention,
    },
    event_stream::{DEFAULT_EVENTS_CHUNK_SIZE, IoEventStreamBuilder},
    interning_tables::InterningTablesBuilder,
    meta_dat::{
        FLAG_EXT_HAS_SOURCE_RELOAD, FLAG_HAS_CALL_STREAM, FLAG_HAS_COLUMN_AWARE_STEPS, FLAG_HAS_INTERNING_TABLES, FLAG_HAS_IO_EVENT_STREAM,
        FLAG_HAS_LINE_COUNT_TABLE, FLAG_HAS_STEP_STREAM, FLAG_HAS_VALUE_STREAM, FLAG_SUPPORTS_COLUMN_BREAKPOINTS, FLAG_SUPPORTS_COLUMN_MOTIONS,
        MetaDatBlocks, encode_meta_dat_with_blocks,
    },
    step_stream::{DEFAULT_STEPS_CHUNK_SIZE, SourceReloadChange, StepStreamBuilder},
    trace_writer::TraceWriter,
    value_stream::{DEFAULT_VALUES_CHUNK_SIZE, ValueStreamBuilder},
};
use codetracer_trace_types::TraceLowLevelEvent;

/// Default Zstd level for the dedicated call stream, matching the unified
/// stream and seekable-zstd.md §Configuration.
const DEFAULT_CALLS_ZSTD_LEVEL: i32 = 3;

/// Where a [`CtfsTraceWriter`] lays its container out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CtfsOutput {
    /// A `.ct` file on disk, at the path handed to
    /// `begin_writing_trace_events` (with the extension replaced). The
    /// default, and the only behaviour that existed before in-memory output.
    File,
    /// A `Vec<u8>` held by the writer, collected after
    /// `finish_writing_trace_events` with
    /// [`take_container_bytes`](CtfsTraceWriter::take_container_bytes).
    /// The only mode available on `wasm32-unknown-unknown`, which has no
    /// filesystem.
    Memory,
}

/// A trace writer that outputs a single `.ct` CTFS container file.
///
/// The container holds the split streams the production Nim writer
/// produces -- `calls.dat`/`.idx`, `steps.dat`/`.idx`, `values.dat`/`.idx`,
/// `events.dat`/`.idx`, the `paths`/`funcs`/`types`/`varnames` `.dat`+`.off`
/// interning tables, `meta.dat` and, for a line-only trace, `step-map.ns`.
pub struct CtfsTraceWriter {
    base: AbstractTraceWriterData,
    ctfs_writer: Option<CtfsWriter>,

    /// File or memory. See [`CtfsOutput`].
    output: CtfsOutput,
    /// The finished container, when `output` is [`CtfsOutput::Memory`].
    container_bytes: Option<Vec<u8>>,
    /// The `.ct` file being written, when `output` is [`CtfsOutput::File`].
    ct_path: Option<std::path::PathBuf>,
    /// The raw-byte threshold under which the finished container is
    /// converted to the compact profile; `0` writes the full profile always.
    /// See [`with_compact_threshold`](CtfsTraceWriter::with_compact_threshold).
    compact_threshold: u64,
    /// Overrides the `recording_id` that would otherwise be minted
    /// when `meta.dat` is written. See
    /// [`set_recording_id`](CtfsTraceWriter::set_recording_id).
    recording_id: Option<String>,
    // --- M17a: dedicated call stream ---
    /// Builds the call records from the observed event sequence (present only
    /// while a trace is being written).
    call_stream_builder: Option<CallStreamBuilder>,
    /// Records-per-chunk for `calls.dat`.
    calls_chunk_size: usize,

    // --- M23a / M23e-4: dedicated execution (step) stream (default-on) ---
    /// Builds the compact step records from the observed event sequence (present
    /// while a trace is being written).
    step_stream_builder: Option<StepStreamBuilder>,
    /// The `step-map.ns` index, built alongside the line-only step stream (a
    /// column-aware trace carries none).
    step_map_builder: Option<crate::step_map::StepMapBuilder>,
    /// Records-per-chunk for `steps.dat`.
    steps_chunk_size: usize,

    // --- M23b / M23e-4: dedicated parallel value stream (default-on) ---
    /// Builds the per-step value records from the observed event sequence
    /// (present while a trace is being written).
    value_stream_builder: Option<ValueStreamBuilder>,
    /// Records-per-chunk for `values.dat`.
    values_chunk_size: usize,

    // --- M23c / M23e-4: dedicated I/O event stream (default-on) ---
    /// Builds the I/O event records from the observed event sequence (present
    /// while a trace is being written).
    io_event_stream_builder: Option<IoEventStreamBuilder>,
    /// Records-per-chunk for `events.dat`.
    events_chunk_size: usize,

    // --- M23d / M23e-4: binary varint interning tables (default-on) ---
    /// Builds the interning-table records from the observed event sequence
    /// (present while a trace is being written).
    interning_tables_builder: Option<InterningTablesBuilder>,

    // --- Column-aware step mode (parity with the Nim writer) ---
    //
    // When on, the writer produces a `steps.dat` whose `global_position_index`
    // addresses `(line, column)` pairs, a `paths.dat` in spec Layout A, and
    // sets `meta.dat` bit 4. All three change together — see
    // `crate::meta_dat::FLAG_HAS_COLUMN_AWARE_STEPS` — because the flag is what
    // tells a reader which parse to use.
    //
    // The mode is TRACE-GLOBAL and must be selected before
    // `begin_writing_trace_events`. A request that arrives after the trace has
    // started is refused rather than half-applied, and
    // `dropped_column_awareness()` reports it.
    /// True once a caller asked for column-aware output.
    column_aware_requested: bool,
    /// True once column-aware output is actually in effect for the trace being
    /// written. Diverges from `column_aware_requested` exactly when the request
    /// arrived too late.
    column_aware_active: bool,
    /// Set when a column-aware request could not be honoured. Read through
    /// [`CtfsTraceWriter::dropped_column_awareness`].
    column_awareness_dropped: bool,
    /// Capability bit 6 — the recorder's columns are breakpoint-sharp.
    column_breakpoints_requested: bool,
    /// The flag-gated `meta.dat` blocks: MCR, replay-launch, layout snapshot,
    /// trace-filter provenance.
    meta_blocks: MetaDatBlocks,
    /// Capability bit 7 — the recorder supports per-column motions.
    column_motions_requested: bool,
    /// The `(line, column)` address space, built from the per-path
    /// `line_lengths` tables. Only consulted in column-aware mode.
    position_space: PositionSpace,
    /// Path ids for which a column was offered and dropped because the file
    /// has no per-line table, and so no column axis to place one on.
    ///
    /// The writer's diagnostic channel for that refusal — a channel rather
    /// than an error because the step itself is kept. One entry per path, not
    /// per step: a recorder whose source paths do not resolve on the recording
    /// machine offers a column on every step of every file.
    columns_dropped_for_paths: std::collections::BTreeSet<u64>,
    /// Nim's delta-vs-absolute policy and running cursor.
    step_encoder: StepEncoder,
    /// The column-aware `steps.dat` encoder. `Some` only while a column-aware
    /// trace is being written; the line-only path keeps using
    /// `step_stream_builder` so its bytes do not move.
    exec_encoder: Option<ExecStreamEncoder>,
    /// Per-line table waiting to be attached to the next `Path` event. Set by
    /// [`CtfsTraceWriter::register_path_with_line_lengths`] immediately before
    /// the path is registered, and consumed when that `Path` event arrives, so
    /// the table lands on the right interning id without a second lookup.
    pending_line_lengths: Option<Vec<u32>>,
    /// Column offset to fold into the next `Step` event, in the same
    /// consume-once way as `pending_line_lengths`. Set by
    /// [`AbstractTraceWriter::register_step_with_column`].
    pending_column_delta: i64,
    /// `(path id, line)` of the last step written in column-aware mode: the
    /// line a column-only step moves along.
    last_step_location: Option<(u64, u64)>,

    // --- Per-file line counts, path versions, source reloads ----------------
    //
    // Parity with the Nim writer's `enableLineCountTable`,
    // `registerPathVersion` and `registerSourceReload`
    // (`internal-files.md` §"`paths.dat` line-count table" and §"`paths.dat`
    // path versions"; `trace-events.md` §"Source Reload Marker (Tag 0x08)").
    /// Whether every `paths.dat` record carries its file's line count
    /// (`meta.dat` bit 14).
    line_count_table: bool,
    /// The line count the next `Path` event is registered with. Set by the
    /// registration entry points and consumed by that event.
    pending_line_count: Option<u64>,
    /// The recorded line count of each path id, when `line_count_table`.
    path_line_counts: Vec<u64>,
    /// How many `SourceReload` markers have been written; the next one's
    /// ordinal is this plus one.
    source_reloads: u64,
    /// Whether the recorder declared, before the trace opened, that source
    /// reloads may occur (`meta.dat` `flags_ext` bit 0).
    source_reloads_declared: bool,
    /// Operations this writer refused because honouring them would have
    /// written a location or record the container cannot represent. See
    /// [`CtfsTraceWriter::refusals`].
    refusals: Vec<String>,
    /// A refusal that makes the container unfinishable (a function whose
    /// declaration path has no recorded size); `finish_writing_trace_events`
    /// fails with it.
    fatal_refusal: Option<String>,
    /// Functions registered but not yet written, in id order: `(name, path,
    /// line)`. Written at `finish_writing_trace_events`, when every path the
    /// recorder registers is registered — see
    /// [`AbstractTraceWriter::register_function`] on this type.
    /// The id is the declaration file's when it was already registered at the
    /// function's registration: the version current then.
    pending_functions: Vec<(
        String,
        std::path::PathBuf,
        codetracer_trace_types::Line,
        Option<codetracer_trace_types::PathId>,
    )>,

    // --- Durability (`ctfs-container.md` §6) ---------------------------------
    //
    // The container is published as it is recorded: `meta.dat` by the first
    // record, then every sealed chunk of every stream, with the interning
    // records it may refer to, before the append that sealed it returns. A
    // recording whose process dies leaves a container readable up to the last
    // chunk each stream sealed.
    /// The members, created when `meta.dat` is committed.
    members: Option<Members>,
    /// Whether `meta.dat` is written. From then on every field and flag in it
    /// is fixed, and a call that would change one is refused.
    meta_committed: bool,
    /// `calls.dat`, `values.dat` and `events.dat`, written as their records
    /// become final.
    calls_sink: Option<ChunkSink>,
    values_sink: Option<ChunkSink>,
    events_sink: Option<ChunkSink>,
    /// Interning records published so far, per table, and each table's
    /// `.dat` length.
    interning_published: [(usize, u64); 4],
    /// Interning tables appended to since their entries were last published.
    interning_dirty: [bool; 4],
    /// Re-entrancy guard: writing a function record goes through `add_event`.
    writing_functions: bool,
    /// The first failure to encode or write the container. Recording calls
    /// cannot return it, so it is held and returned by
    /// `finish_writing_trace_events`; nothing is written after it.
    write_error: Option<String>,
    /// The span stream, created by the first span.
    spans: Option<SpanStream>,
    /// The last span id minted for a crossing; 0 before the first.
    last_crossing_id: u64,
    /// Open crossings, innermost last: span id, span type, start step.
    open_crossings: Vec<(u64, String, u64)>,

    // --- Line hits and correlation markers ----------------------------------
    /// Whether the recording keeps a `linehits.tc` index. See
    /// [`CtfsTraceWriter::enable_line_hits`].
    line_hits_requested: bool,
    /// The correlation-marker label table, `markers.dat` / `markers.off`,
    /// created by the first [`CtfsTraceWriter::ensure_marker_id`].
    marker_labels: Option<MarkerLabels>,
    /// The correlation markers declared, each with its index key, written to
    /// `corrmark.ns` at close. None declared: no `corrmark.ns`.
    correlation_markers: Vec<(u64, crate::corrmark::CorrelationMarker)>,
    /// Interned type ids by `(kind, lang_type)`: `types.dat` records a type's
    /// kind as well as its name, so two kinds sharing a name are two types.
    type_ids: std::collections::HashMap<(u8, String), codetracer_trace_types::TypeId>,
    /// Alternate source views, encoded, written at finish as
    /// `srcviews.dat` / `srcviews.off`.
    source_views: Vec<Vec<u8>>,
}

/// The span stream of a container being written.
struct SpanStream {
    builder: crate::span_stream::SpanStreamBuilder,
    dat: codetracer_ctfs::FileHandle,
    idx: codetracer_ctfs::FileHandle,
}

/// The correlation-marker label table being written.
struct MarkerLabels {
    ids: std::collections::HashMap<Vec<u8>, u64>,
    dat: codetracer_ctfs::FileHandle,
    off: codetracer_ctfs::FileHandle,
    dat_len: u64,
}

/// The members of a container being written, by role.
struct Members {
    calls: (codetracer_ctfs::FileHandle, codetracer_ctfs::FileHandle),
    steps: (codetracer_ctfs::FileHandle, codetracer_ctfs::FileHandle),
    values: (codetracer_ctfs::FileHandle, codetracer_ctfs::FileHandle),
    events: (codetracer_ctfs::FileHandle, codetracer_ctfs::FileHandle),
    /// `paths`, `funcs`, `types`, `varnames`: each a `.dat` and its `.off`.
    interning: [(codetracer_ctfs::FileHandle, codetracer_ctfs::FileHandle); 4],
}

/// Whether `event` is a record — something `meta.dat` must be committed
/// before (`ctfs-container.md` §6, "Durability", rule 1) — rather than an
/// interning registration.
fn is_record(event: &TraceLowLevelEvent) -> bool {
    !matches!(
        event,
        TraceLowLevelEvent::Path(_)
            | TraceLowLevelEvent::VariableName(_)
            | TraceLowLevelEvent::Variable(_)
            | TraceLowLevelEvent::Type(_)
            | TraceLowLevelEvent::Function(_)
    )
}

/// JSON string escaping for a `MarkerPayload` field: `"` and `\` escaped,
/// newline, carriage return and tab by their short escapes, every other byte
/// below 0x20 as `\u00XX` (lowercase hex), everything else as is.
fn marker_json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u00{:02x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// The id handed back for a path the writer refused to register. Never a real
/// `paths.dat` index.
pub const INVALID_PATH_ID: codetracer_trace_types::PathId = codetracer_trace_types::PathId(usize::MAX);

/// THE refusal of a table offered for a path interned with a different one
/// (`internal-files.md` §"`paths.dat` Layout A"). The Nim writer states it in
/// the same words.
pub fn late_column_table_diagnostic(path: &Path, recorded_lines: usize, offered_lines: usize) -> String {
    format!(
        "paths.dat: {} was interned with a {recorded_lines}-line table, and a later registration offering a \
         different {offered_lines}-line table is refused; a file's table is fixed when the file is first interned, \
         so register it before the file's first step, function or call",
        path.display()
    )
}

/// THE refusal of a step past the last line of a file with the conventional
/// table (`internal-files.md` §"`paths.dat` Layout A"). The Nim writer states
/// it in the same words.
pub fn conventional_line_diagnostic(path: &Path, line: i64) -> String {
    format!(
        "step at line {line} of {}, which has the conventional table of 100000 lines; its position would fall \
         inside the next file's range",
        path.display()
    )
}

impl CtfsTraceWriter {
    /// Create a new CTFS trace writer.
    pub fn new(program: &str, args: &[String]) -> Self {
        CtfsTraceWriter {
            base: AbstractTraceWriterData::new(program, args),
            ctfs_writer: None,
            output: CtfsOutput::File,
            container_bytes: None,
            ct_path: None,
            compact_threshold: 0,
            recording_id: None,
            call_stream_builder: None,
            calls_chunk_size: DEFAULT_CALLS_CHUNK_SIZE,
            step_stream_builder: None,
            step_map_builder: None,
            steps_chunk_size: DEFAULT_STEPS_CHUNK_SIZE,
            value_stream_builder: None,
            values_chunk_size: DEFAULT_VALUES_CHUNK_SIZE,
            io_event_stream_builder: None,
            events_chunk_size: DEFAULT_EVENTS_CHUNK_SIZE,
            interning_tables_builder: None,
            // Column-aware mode is OFF by default. Turning it on changes
            // `steps.dat` addressing, `paths.dat` record shape and a meta.dat
            // bit that column-unaware readers are required to reject, so it is
            // a deliberate opt-in per trace and never a default.
            column_aware_requested: false,
            column_aware_active: false,
            column_awareness_dropped: false,
            column_breakpoints_requested: false,
            column_motions_requested: false,
            meta_blocks: MetaDatBlocks::default(),
            position_space: PositionSpace::new(false),
            columns_dropped_for_paths: std::collections::BTreeSet::new(),
            step_encoder: StepEncoder::new(),
            exec_encoder: None,
            pending_line_lengths: None,
            pending_column_delta: 0,
            last_step_location: None,
            line_count_table: false,
            pending_line_count: None,
            path_line_counts: Vec::new(),
            source_reloads: 0,
            source_reloads_declared: false,
            refusals: Vec::new(),
            fatal_refusal: None,
            pending_functions: Vec::new(),
            members: None,
            meta_committed: false,
            calls_sink: None,
            values_sink: None,
            events_sink: None,
            interning_published: [(0, 0); 4],
            interning_dirty: [false; 4],
            writing_functions: false,
            write_error: None,
            spans: None,
            last_crossing_id: 0,
            open_crossings: Vec::new(),
            line_hits_requested: false,
            marker_labels: None,
            correlation_markers: Vec::new(),
            type_ids: std::collections::HashMap::new(),
            source_views: Vec::new(),
        }
    }

    // --- Per-file line counts, path versions, source reloads ----------------

    /// Record every file's line count in `paths.dat` (`meta.dat` bit 14) and
    /// size each file's slot in the position space from it. Mirrors the Nim
    /// writer's `enableLineCountTable`.
    ///
    /// After this every path is registered through
    /// [`Self::register_path_with_line_count`] (or
    /// [`Self::register_path_version`]); a step or function naming a path with
    /// no recorded count is refused. Must be called before the first path is
    /// registered, and is refused on a column-aware writer, whose Layout A
    /// records already carry `line_count`.
    pub fn enable_line_count_table(&mut self) -> Result<(), String> {
        if self.meta_committed {
            return Err("enable_line_count_table: the trace has recorded already, and meta.dat, which declares \
                        the table, was written by its first record"
                .to_string());
        }
        if self.column_aware_requested {
            return Err(
                "enable_line_count_table: this writer is column-aware, whose paths.dat records already carry the \
                        file's line_count and whose files are sized in addressable columns rather than lines"
                    .to_string(),
            );
        }
        if !self.base.path_list.is_empty() {
            return Err(format!(
                "enable_line_count_table: {} path(s) are already interned under the bare paths.dat layout and cannot \
                 grow a line count; enable the table before the first path is registered",
                self.base.path_list.len()
            ));
        }
        self.line_count_table = true;
        if let Some(builder) = self.interning_tables_builder.as_mut() {
            builder.set_line_count_table(true);
        }
        Ok(())
    }

    /// Whether this writer records per-file line counts.
    pub fn line_count_table_enabled(&self) -> bool {
        self.line_count_table
    }

    /// Register `path` with the number of lines its file has. Mirrors the Nim
    /// writer's `registerPath(path, lineCount = …)`.
    ///
    /// Without the line-count table this registers the path alone. With it,
    /// a zero count is refused, and a path already registered resolves to its
    /// newest version without writing a record.
    pub fn register_path_with_line_count(&mut self, path: &Path, line_count: u64) -> Result<codetracer_trace_types::PathId, String> {
        if let Some(id) = self.base.paths.get(path) {
            return Ok(*id);
        }
        if !self.line_count_table {
            return Ok(AbstractTraceWriter::ensure_path_id(self, path));
        }
        if line_count == 0 {
            return Err(format!(
                "register_path_with_line_count: line_count 0 for {}. A file sized 0 shares its base with the next \
                 file; a recorder that cannot count the lines records the ceiling it lays the file out with",
                path.display()
            ));
        }
        Ok(self.append_path_record(path, line_count))
    }

    /// Register a NEW VERSION of `path` with its own line count, and make it
    /// the version a bare `path` resolves to from now on. Mirrors the Nim
    /// writer's `registerPathVersion`
    /// (`internal-files.md` §"`paths.dat` path versions").
    pub fn register_path_version(&mut self, path: &Path, line_count: u64) -> Result<codetracer_trace_types::PathId, String> {
        if self.column_aware_requested {
            return Err(
                "register_path_version: this writer is column-aware, whose paths.dat records size a file in \
                        addressable columns; versioned paths are defined for the line-only line-count-table layout"
                    .to_string(),
            );
        }
        if !self.line_count_table {
            return Err(format!(
                "register_path_version: this writer has no line-count table, so a second record for {} would carry no \
                 size and both versions would be laid out at the conventional stride. Call enable_line_count_table \
                 before the first path is registered",
                path.display()
            ));
        }
        if line_count == 0 {
            return Err(format!("register_path_version: line_count 0 for {}", path.display()));
        }
        Ok(self.append_path_record(path, line_count))
    }

    /// The id a bare step on `path` is attributed to right now: its newest
    /// version. `None` for a path this writer has not registered.
    pub fn current_path_id(&self, path: &Path) -> Option<codetracer_trace_types::PathId> {
        self.base.paths.get(path).copied()
    }

    /// Append the `paths.dat` record of `path`, whose id the caller has
    /// already assigned.
    fn emit_path_record(&mut self, path: &Path) {
        self.base.path_list.push(path.to_path_buf());
        AbstractTraceWriter::add_event(self, TraceLowLevelEvent::Path(path.to_path_buf()));
    }

    /// Append a `paths.dat` record for `path` with `line_count`, and point the
    /// path's name at it.
    fn append_path_record(&mut self, path: &Path, line_count: u64) -> codetracer_trace_types::PathId {
        let id = codetracer_trace_types::PathId(self.base.path_list.len());
        self.base.paths.insert(path.to_path_buf(), id);
        self.pending_line_count = Some(line_count);
        self.emit_path_record(path);
        self.pending_line_count = None;
        id
    }

    /// Write a `SourceReload` marker (tag 0x08) at the current point of the
    /// execution stream and return its 1-based `reload_ordinal`. Mirrors the
    /// Nim writer's `registerSourceReload`
    /// (`trace-events.md` §"Source Reload Marker (Tag 0x08)").
    ///
    /// Refused, by name, when `changed` is empty, when an id is not a
    /// registered path, when a change's two ids are equal, or when a
    /// generation is below 2.
    pub fn register_source_reload(&mut self, changed: &[SourceReloadChange], in_flight_frames: u64) -> Result<u64, String> {
        if self.ctfs_writer.is_none() {
            return Err("register_source_reload called before begin_writing_trace_events".to_string());
        }
        if !self.source_reloads_declared {
            return Err(
                "register_source_reload: this trace did not declare source reloads before its first record \
                        (call declare_source_reload). meta.dat is written by the first record and does not \
                        admit a SourceReload record unless it declares one may occur"
                    .to_string(),
            );
        }
        if changed.is_empty() {
            return Err(
                "register_source_reload: no changed files. A marker that records a reload without recording what \
                        it changed cannot be told apart from one whose files were lost"
                    .to_string(),
            );
        }
        let path_count = self.base.path_list.len() as u64;
        for (i, ch) in changed.iter().enumerate() {
            if ch.old_path_id >= path_count {
                return Err(format!(
                    "register_source_reload: changed[{i}].old_path_id {} is not a registered path ({path_count} registered)",
                    ch.old_path_id
                ));
            }
            if ch.new_path_id >= path_count {
                return Err(format!(
                    "register_source_reload: changed[{i}].new_path_id {} is not a registered path ({path_count} registered)",
                    ch.new_path_id
                ));
            }
            if ch.old_path_id == ch.new_path_id {
                return Err(format!(
                    "register_source_reload: changed[{i}] reports old_path_id == new_path_id == {}. A reload that minted no \
                     new path index cannot attribute its post-reload steps to the version that ran them",
                    ch.old_path_id
                ));
            }
            if ch.generation < 2 {
                return Err(format!(
                    "register_source_reload: changed[{i}].generation is {}. Generation 1 is the content the process \
                     started with, so a reload's generation is 2 or more",
                    ch.generation
                ));
            }
        }
        let ordinal = self.source_reloads + 1;
        self.commit_meta();
        // The line-only stream is built by `StepStreamBuilder`, which counts
        // the exec records `step-map.ns` indexes; the column-aware one is
        // written straight into the encoder.
        if let Some(builder) = self.step_stream_builder.as_mut() {
            builder.push_source_reload(ordinal, changed.to_vec(), in_flight_frames);
        } else if let Some(encoder) = self.exec_encoder.as_mut() {
            encoder.write_event(crate::column_aware::StepEvent::SourceReload {
                reload_ordinal: ordinal,
                changed: changed.to_vec(),
                in_flight_frames,
            })?;
            self.step_encoder.note_non_step_event();
        }
        self.note_non_step_exec_record();
        self.source_reloads = ordinal;
        self.after_record();
        Ok(ordinal)
    }

    /// Write a `Raise` record (tag 0x02) at the current point of the execution
    /// stream: an exception of type `exception_type_id` was raised, before any
    /// unwinding. Mirrors the Nim writer's `registerRaise`
    /// (`trace-events.md` §"Execution Stream").
    ///
    /// The record is an exec record but not a step: it owns an empty value
    /// record and advances the index `calls.dat` and `events.dat` are
    /// expressed in, and it does not move the running position.
    pub fn register_raise(&mut self, exception_type_id: u64, message: &[u8]) -> Result<(), String> {
        self.write_exception_record(crate::column_aware::StepEvent::Raise {
            exception_type_id,
            message: message.to_vec(),
        })
    }

    /// Write a `Catch` record (tag 0x03) at the current point of the execution
    /// stream: an exception of type `exception_type_id` was caught. Mirrors the
    /// Nim writer's `registerCatch`; accounted like [`Self::register_raise`].
    pub fn register_catch(&mut self, exception_type_id: u64) -> Result<(), String> {
        self.write_exception_record(crate::column_aware::StepEvent::Catch { exception_type_id })
    }

    fn write_exception_record(&mut self, event: crate::column_aware::StepEvent) -> Result<(), String> {
        if self.ctfs_writer.is_none() {
            return Err("register_raise/register_catch called before begin_writing_trace_events".to_string());
        }
        self.commit_meta();
        // The line-only stream is built by `StepStreamBuilder`; the
        // column-aware one is written straight into the encoder.
        if let Some(builder) = self.step_stream_builder.as_mut() {
            match event {
                crate::column_aware::StepEvent::Raise { exception_type_id, message } => builder.push_raise(exception_type_id, message),
                crate::column_aware::StepEvent::Catch { exception_type_id } => builder.push_catch(exception_type_id),
                _ => unreachable!("only Raise and Catch are written here"),
            }
        } else if let Some(encoder) = self.exec_encoder.as_mut() {
            encoder.write_event(event)?;
            self.step_encoder.note_non_step_event();
        }
        self.note_non_step_exec_record();
        self.after_record();
        Ok(())
    }

    /// Exit the innermost call by `exception`: its call record carries the
    /// exception, and no return value. Mirrors the Nim writer's
    /// `registerReturn(exception = ...)` (`trace-events.md` §"Call Stream
    /// Records"). Refused when no call is open.
    pub fn register_return_exception(&mut self, exception: &codetracer_trace_types::ValueRecord) -> Result<(), String> {
        let Some(builder) = self.call_stream_builder.as_mut() else {
            return Err("register_return_exception called before begin_writing_trace_events".to_string());
        };
        if builder.open_calls() == 0 {
            return Err("register_return_exception: call stack underflow: return without matching call".to_string());
        }
        let cbor = cbor4ii::serde::to_vec(Vec::new(), exception).map_err(|e| format!("encoding an exception: {e}"))?;
        builder.stage_exception(cbor);
        AbstractTraceWriter::register_return(
            self,
            codetracer_trace_types::ValueRecord::None {
                type_id: codetracer_trace_types::TypeId(0),
            },
        );
        Ok(())
    }

    /// Append `span` to the span stream (`spans.dat`, `spans.idx`), which the
    /// first span creates. A span may be appended open and again settled
    /// under the same id; readers keep the last record. Refused for a record
    /// the stream cannot carry (`span_stream::encode_span_record`).
    pub fn register_span(&mut self, span: &crate::span_stream::SpanRecord) -> Result<(), String> {
        if self.ctfs_writer.is_none() {
            return Err("register_span called before begin_writing_trace_events".to_string());
        }
        if let Some(e) = &self.write_error {
            return Err(format!("the trace could not be written: {e}"));
        }
        self.commit_meta();
        if self.spans.is_none() {
            let builder = crate::span_stream::SpanStreamBuilder::default();
            let header = builder.index_header();
            let writer = self.ctfs_writer.as_mut().expect("checked above");
            let created = (|| -> Result<SpanStream, codetracer_ctfs::CtfsError> {
                let dat = writer.add_file(crate::span_stream::SPANS_DATA_FILE_NAME)?;
                let idx = writer.add_file(crate::span_stream::SPANS_INDEX_FILE_NAME)?;
                writer.write(idx, &header)?;
                writer.sync_entry(idx)?;
                Ok(SpanStream { builder, dat, idx })
            })()
            .map_err(|e| format!("creating the span stream: {e}"))?;
            self.spans = Some(created);
        }
        let spans = self.spans.as_mut().expect("created above");
        if let Some(chunk) = spans.builder.push(span)? {
            self.write_span_chunk(chunk)?;
        }
        Ok(())
    }

    /// Seal the buffered spans as a chunk, possibly a short one, and publish
    /// it, so a reader of the growing container sees them. A no-op when no
    /// span is buffered or none was ever registered.
    pub fn flush_spans(&mut self) -> Result<(), String> {
        let Some(spans) = self.spans.as_mut() else {
            return Ok(());
        };
        match spans.builder.seal()? {
            Some(chunk) => self.write_span_chunk(chunk),
            None => Ok(()),
        }
    }

    /// The span records registered, sealed or not; an open record and its
    /// settled one count as two.
    pub fn span_count(&self) -> u64 {
        self.spans.as_ref().map_or(0, |s| s.builder.count())
    }

    /// The chunk's bytes, then its index entry, each published after its data
    /// (`ctfs-container.md` §6, "Durability").
    fn write_span_chunk(&mut self, chunk: crate::span_stream::SealedSpanChunk) -> Result<(), String> {
        let (Some(spans), Some(w)) = (self.spans.as_ref(), self.ctfs_writer.as_mut()) else {
            return Err("the span stream is not open".to_string());
        };
        let (dat, idx) = (spans.dat, spans.idx);
        let r = (|| -> Result<(), codetracer_ctfs::CtfsError> {
            w.write(dat, &chunk.data)?;
            w.write(idx, &chunk.index_entry)?;
            w.write_pending(dat)?;
            w.write_pending(idx)?;
            w.publish_entry(dat)?;
            w.publish_entry(idx)?;
            w.flush()
        })()
        .map_err(|e| format!("writing a span chunk: {e}"));
        if let Err(e) = &r {
            self.latch(Err::<(), _>(e.clone()));
        }
        r
    }

    /// Open a native-to-VM crossing of type `span_type`: a span the writer
    /// mints the id of (1, 2, ... in this recording), starting at the next
    /// exec record. Its open record is published at once, so a reader of the
    /// growing container sees the crossing in flight. Returns the id, to pass
    /// to [`Self::end_crossing`].
    pub fn begin_crossing(&mut self, span_type: &str) -> Result<u64, String> {
        if self.ctfs_writer.is_none() {
            return Err("begin_crossing called before begin_writing_trace_events".to_string());
        }
        self.commit_meta();
        let span_id = self.last_crossing_id + 1;
        let start_step = self.exec_record_count();
        let open = crate::span_stream::SpanRecord {
            span_id,
            is_open: true,
            start_step,
            span_type: span_type.to_string(),
            contiguous_on_one_thread: true,
            shares_timeline: true,
            ..Default::default()
        };
        self.last_crossing_id = span_id;
        self.register_span(&open)?;
        self.flush_spans()?;
        self.open_crossings.push((span_id, span_type.to_string(), start_step));
        Ok(span_id)
    }

    /// Close the crossing `span_id`, which must be the innermost open one:
    /// its settled record ends at the last exec record (0 when there is
    /// none) and is published at once.
    pub fn end_crossing(&mut self, span_id: u64) -> Result<(), String> {
        if self.ctfs_writer.is_none() {
            return Err("end_crossing called before begin_writing_trace_events".to_string());
        }
        self.commit_meta();
        let Some((top, _, _)) = self.open_crossings.last() else {
            return Err(format!(
                "endCrossing: span_id {span_id} is not the innermost open crossing (no open crossings)"
            ));
        };
        if *top != span_id {
            return Err(format!("endCrossing: span_id {span_id} is not the innermost open crossing ({top})"));
        }
        let (_, span_type, start_step) = self.open_crossings.pop().expect("checked above");
        let settled = crate::span_stream::SpanRecord {
            span_id,
            status: crate::span_stream::SPAN_STATUS_OK,
            start_step,
            end_step: self.exec_record_count().saturating_sub(1),
            span_type,
            contiguous_on_one_thread: true,
            shares_timeline: true,
            ..Default::default()
        };
        self.register_span(&settled)?;
        self.flush_spans()
    }

    /// The number of exec records written: the id the next one takes.
    pub fn exec_record_count(&self) -> u64 {
        self.call_stream_builder.as_ref().map_or(0, |b| b.exec_records())
    }

    /// Keep a `linehits.tc` index: from this call on, every step records its
    /// position and exec-record index, and the index is written when the
    /// trace finishes. Steps recorded before the call are not in it.
    pub fn enable_line_hits(&mut self) {
        self.line_hits_requested = true;
        if let Some(encoder) = self.exec_encoder.as_mut() {
            encoder.enable_line_hits();
        }
    }

    /// The step a marker declared now belongs to: the last exec record, or 0
    /// before the first.
    fn enclosing_step(&self) -> u64 {
        match (&self.io_event_stream_builder, &self.exec_encoder) {
            (Some(builder), _) => builder.current_step().unwrap_or(0),
            (None, Some(encoder)) => encoder.total_events().saturating_sub(1),
            (None, None) => 0,
        }
    }

    /// Intern a correlation-marker label and return its id, creating
    /// `markers.dat` / `markers.off` on the first call. A recording that
    /// declares no marker has neither.
    pub fn ensure_marker_id(&mut self, label: &str) -> Result<u64, String> {
        if self.ctfs_writer.is_none() {
            return Err("ensure_marker_id called before begin_writing_trace_events".to_string());
        }
        if let Some(e) = &self.write_error {
            return Err(e.clone());
        }
        if self.marker_labels.is_none() {
            self.create_members();
            let w = self.ctfs_writer.as_mut().expect("checked above");
            let created = (|| -> Result<MarkerLabels, codetracer_ctfs::CtfsError> {
                let dat = w.add_file("markers.dat")?;
                let off = w.add_file("markers.off")?;
                w.write(off, &0u64.to_le_bytes())?;
                w.sync_entry(off)?;
                Ok(MarkerLabels {
                    ids: std::collections::HashMap::new(),
                    dat,
                    off,
                    dat_len: 0,
                })
            })()
            .map_err(|e| format!("creating markers.dat: {e}"))?;
            self.marker_labels = Some(created);
        }
        let labels = self.marker_labels.as_mut().expect("created above");
        if let Some(&id) = labels.ids.get(label.as_bytes()) {
            return Ok(id);
        }
        let id = labels.ids.len() as u64;
        let w = self.ctfs_writer.as_mut().expect("checked above");
        let result = (|| -> Result<(), codetracer_ctfs::CtfsError> {
            if !label.is_empty() {
                w.write(labels.dat, label.as_bytes())?;
                w.sync_entry(labels.dat)?;
            }
            w.write(labels.off, &(labels.dat_len + label.len() as u64).to_le_bytes())?;
            w.sync_entry(labels.off)
        })();
        result.map_err(|e| format!("writing markers.dat: {e}"))?;
        labels.dat_len += label.len() as u64;
        labels.ids.insert(label.as_bytes().to_vec(), id);
        Ok(id)
    }

    /// Declare a boundary crossing against an interned label id: an I/O
    /// event carrying the `MarkerPayload` document in its metadata, and a
    /// kind-1 `corrmark.ns` entry, both at `step_id` (default: the last exec
    /// record). `direction` `"recv"` or `"receive"` is the receiving side,
    /// anything else the sending side; an empty `key_text` is `"key"`; the
    /// show fields are written when either is given, an empty `show_text`
    /// being `"show"`; `description` when given.
    #[allow(clippy::too_many_arguments)]
    pub fn register_correlation_marker_by_id(
        &mut self,
        direction: &str,
        marker_id: u64,
        boundary_label: &str,
        key_value: &str,
        show_value: &str,
        description: &str,
        key_text: &str,
        show_text: &str,
        step_id: Option<u64>,
    ) -> Result<(), String> {
        if self.ctfs_writer.is_none() {
            return Err("register_correlation_marker called before begin_writing_trace_events".to_string());
        }
        self.commit_meta();
        let recv = direction == "recv" || direction == "receive";
        let mut payload = format!("{{\"marker_id\":{marker_id}");
        payload.push_str(&format!(",\"boundary_id\":\"{}\"", marker_json_escape(boundary_label)));
        payload.push_str(&format!(",\"direction\":\"{}\"", if recv { "recv" } else { "send" }));
        payload.push_str(&format!(
            ",\"key_text\":\"{}\"",
            marker_json_escape(if key_text.is_empty() { "key" } else { key_text })
        ));
        payload.push_str(&format!(",\"key_value\":\"{}\"", marker_json_escape(key_value)));
        if !show_value.is_empty() || !show_text.is_empty() {
            payload.push_str(&format!(
                ",\"show_text\":\"{}\"",
                marker_json_escape(if show_text.is_empty() { "show" } else { show_text })
            ));
            payload.push_str(&format!(",\"show_value\":\"{}\"", marker_json_escape(show_value)));
        }
        if !description.is_empty() {
            payload.push_str(&format!(",\"description\":\"{}\"", marker_json_escape(description)));
        }
        payload.push('}');
        let step = step_id.unwrap_or_else(|| self.enclosing_step());
        if let Some(builder) = self.io_event_stream_builder.as_mut() {
            builder.push_record(crate::event_stream::IoEventRecord {
                kind: 0,
                step_id: step,
                metadata: payload.into_bytes(),
                content: Vec::new(),
            });
        }
        self.correlation_markers.push((
            crate::corrmark::boundary_key(marker_id, key_value.as_bytes()),
            crate::corrmark::CorrelationMarker::boundary(marker_id, key_value.as_bytes(), recv, step, 0),
        ));
        self.after_record();
        self.write_error.clone().map_or(Ok(()), Err)
    }

    /// [`Self::register_correlation_marker_by_id`] with the label interned
    /// here.
    #[allow(clippy::too_many_arguments)]
    pub fn register_correlation_marker(
        &mut self,
        direction: &str,
        boundary_id: &str,
        key_value: &str,
        show_value: &str,
        description: &str,
        key_text: &str,
        show_text: &str,
        step_id: Option<u64>,
    ) -> Result<(), String> {
        let id = self.ensure_marker_id(boundary_id)?;
        self.register_correlation_marker_by_id(
            direction,
            id,
            boundary_id,
            key_value,
            show_value,
            description,
            key_text,
            show_text,
            step_id,
        )
    }

    /// Declare that the recording covers the distributed-trace span
    /// `(trace_id, span_id)`, given as wire bytes (16 and 8): a kind-0
    /// `corrmark.ns` entry at `step_id` (default: the last exec record). No
    /// I/O event is written.
    #[allow(clippy::too_many_arguments)]
    pub fn register_span_coverage(
        &mut self,
        trace_id: &[u8],
        span_id: &[u8],
        wall_time_unix_ns: u64,
        monotonic_time_ns: u64,
        thread_id: u64,
        is_exit: bool,
        step_id: Option<u64>,
    ) -> Result<(), String> {
        if self.ctfs_writer.is_none() {
            return Err("register_span_coverage called before begin_writing_trace_events".to_string());
        }
        self.commit_meta();
        let trace: &[u8; 16] = trace_id
            .try_into()
            .map_err(|_| format!("correlation trace_id must be 16 bytes (wire order), got {}", trace_id.len()))?;
        let span: &[u8; 8] = span_id
            .try_into()
            .map_err(|_| format!("correlation span_id must be 8 bytes (wire order), got {}", span_id.len()))?;
        let step = step_id.unwrap_or_else(|| self.enclosing_step());
        self.correlation_markers.push((
            crate::corrmark::span_key(trace, span),
            crate::corrmark::CorrelationMarker::span(trace, span, wall_time_unix_ns, monotonic_time_ns, step, thread_id, is_exit),
        ));
        Ok(())
    }

    /// [`Self::register_span_coverage`] with the ids as hex (32 and 16
    /// characters, either case).
    #[allow(clippy::too_many_arguments)]
    pub fn register_span_coverage_hex(
        &mut self,
        trace_id_hex: &str,
        span_id_hex: &str,
        wall_time_unix_ns: u64,
        monotonic_time_ns: u64,
        thread_id: u64,
        is_exit: bool,
        step_id: Option<u64>,
    ) -> Result<(), String> {
        let trace = crate::corrmark::decode_hex_id(trace_id_hex, 16)?;
        let span = crate::corrmark::decode_hex_id(span_id_hex, 8)?;
        self.register_span_coverage(&trace, &span, wall_time_unix_ns, monotonic_time_ns, thread_id, is_exit, step_id)
    }

    /// Declare that this recording may contain source reload markers
    /// (`meta.dat` `flags_ext` bit 0, `internal-files.md` §"Extended flags").
    ///
    /// A capability, declared like the line-count table before the trace's
    /// first record: `meta.dat` is written by that record and never
    /// rewritten, so it is refused after it. A trace that declares it and
    /// records no reload is well-formed.
    /// Record an alternate source view of the registered path `path_id`
    /// (`internal-files.md` §"Alternate Source Views") and return its index.
    /// The views are written at finish, as `srcviews.dat` / `srcviews.off`.
    pub fn register_source_view(&mut self, path_id: u64, view_kind: u8, view_name: &[u8], content: &[u8], sourcemap: &[u8]) -> Result<u64, String> {
        if self.ctfs_writer.is_none() {
            return Err("register_source_view called before begin_writing_trace_events".to_string());
        }
        let paths = self.base.path_list.len() as u64;
        if path_id >= paths {
            return Err(format!(
                "registerSourceView: path_id {path_id} is out of range (only {paths} path(s) registered)"
            ));
        }
        let mut rec = Vec::new();
        let varint = |v: u64, out: &mut Vec<u8>| {
            let mut v = v;
            loop {
                let b = (v & 0x7f) as u8;
                v >>= 7;
                if v == 0 {
                    out.push(b);
                    break;
                }
                out.push(b | 0x80);
            }
        };
        varint(path_id, &mut rec);
        rec.push(view_kind);
        for part in [view_name, content, sourcemap] {
            varint(part.len() as u64, &mut rec);
            rec.extend_from_slice(part);
        }
        self.source_views.push(rec);
        Ok(self.source_views.len() as u64 - 1)
    }

    /// Whether the first record has been written, and with it `meta.dat`:
    /// from then on every field and flag in it is fixed.
    pub fn recording_started(&self) -> bool {
        self.meta_committed
    }

    /// Record an I/O event attributed to the exec record `step_id`, with its
    /// metadata and content as bytes.
    ///
    /// [`register_special_event`](AbstractTraceWriter::register_special_event)
    /// attributes an event to the last exec record written. A caller that
    /// holds a step back so later values can join its record names the step
    /// the event belongs to here instead.
    pub fn register_io_event_at(&mut self, kind: u8, metadata: &[u8], content: &[u8], step_id: u64) -> Result<(), String> {
        crate::event_stream::event_log_kind(kind)?;
        if self.ctfs_writer.is_none() {
            return Err("register_io_event_at called before begin_writing_trace_events".to_string());
        }
        self.commit_meta();
        if let Some(builder) = self.io_event_stream_builder.as_mut() {
            builder.push_record(crate::event_stream::IoEventRecord {
                kind,
                step_id,
                metadata: metadata.to_vec(),
                content: content.to_vec(),
            });
        }
        self.after_record();
        Ok(())
    }

    /// Record a step at an already-interned path id, with the refusals
    /// [`register_step`](AbstractTraceWriter::register_step) applies to a step
    /// at a path. A `column` is folded into the step on a column-aware trace.
    pub fn register_step_at(
        &mut self,
        path_id: codetracer_trace_types::PathId,
        line: codetracer_trace_types::Line,
        column: Option<codetracer_trace_types::Line>,
    ) {
        let Some(path) = self.base.path_list.get(path_id.0).cloned() else {
            self.refusals
                .push(format!("step at path id {}, which is not a registered path", path_id.0));
            return;
        };
        if self.line_count_table
            && let Some(count) = self.path_line_counts.get(path_id.0)
            && line.0 > 0
            && line.0 as u64 > *count
        {
            self.refusals.push(format!(
                "step at line {} of {}, which this trace records as having {count} line(s); its address would fall \
                 inside the next file's range",
                line.0,
                path.display()
            ));
            return;
        }
        if self.column_aware_active {
            self.pending_column_delta = column.map(|c| c.0 - 1).unwrap_or(0);
        }
        AbstractTraceWriter::add_event(self, TraceLowLevelEvent::Step(codetracer_trace_types::StepRecord { path_id, line }));
        self.pending_column_delta = 0;
    }

    /// Record `event`, a value-stream event, storing `cbor` verbatim as its
    /// value payload in place of the event's own encoding of it.
    pub fn register_value_event_cbor(&mut self, event: TraceLowLevelEvent, cbor: Vec<u8>) {
        if let Some(builder) = self.value_stream_builder.as_mut() {
            builder.override_next_blob(cbor);
        }
        AbstractTraceWriter::add_event(self, event);
    }

    /// Record a call whose arguments are already encoded: `(varname id, CBOR
    /// value)` pairs, stored verbatim on the call record and not as step
    /// values.
    pub fn register_call_cbor(&mut self, function_id: codetracer_trace_types::FunctionId, args: Vec<(u64, Vec<u8>)>) {
        if let Some(builder) = self.call_stream_builder.as_mut() {
            builder.override_next_call_args(
                args.into_iter()
                    .map(|(varname_id, value)| crate::call_stream::CallArg { varname_id, value })
                    .collect(),
            );
        }
        AbstractTraceWriter::add_event(
            self,
            TraceLowLevelEvent::Call(codetracer_trace_types::CallRecord { function_id, args: vec![] }),
        );
    }

    /// Record a return whose value is already encoded, stored verbatim; an
    /// empty `cbor` is a return with no value.
    pub fn register_return_cbor_value(&mut self, cbor: Vec<u8>) {
        if let Some(builder) = self.call_stream_builder.as_mut() {
            builder.override_next_return(cbor);
        }
        AbstractTraceWriter::add_event(
            self,
            TraceLowLevelEvent::Return(codetracer_trace_types::ReturnRecord {
                return_value: codetracer_trace_types::NONE_VALUE,
            }),
        );
    }

    pub fn declare_source_reload(&mut self) -> Result<(), String> {
        if self.meta_committed {
            return Err("declare_source_reload: the trace has recorded already, and meta.dat, which carries the \
                        declaration, was written by its first record"
                .to_string());
        }
        self.source_reloads_declared = true;
        Ok(())
    }

    /// Set the flag-gated blocks `meta.dat` carries after `recorder_id`: MCR
    /// fields, replay-launch fields, a layout snapshot and the trace-filter
    /// provenance chain (`internal-files.md` §"Metadata (meta.dat)"). Each
    /// present block sets its flag bit. Refused once the trace has recorded:
    /// `meta.dat` is written by the first record and never rewritten.
    pub fn set_meta_blocks(&mut self, blocks: MetaDatBlocks) -> Result<(), String> {
        if self.meta_committed {
            return Err(
                "set_meta_blocks: the trace has recorded already, and meta.dat, which carries the blocks, \
                        was written by its first record"
                    .to_string(),
            );
        }
        self.meta_blocks = blocks;
        Ok(())
    }

    /// How many source reload markers this writer has written.
    pub fn source_reload_count(&self) -> u64 {
        self.source_reloads
    }

    /// Operations refused because honouring them would have written something
    /// the container cannot represent — a step at a path with no recorded
    /// line count, or past the end of its file. The step is not written, and
    /// `finish_writing_trace_events` fails naming the first refusal, as the
    /// Nim writer's close does.
    pub fn refusals(&self) -> &[String] {
        &self.refusals
    }

    /// Account for one exec record that is not a step in every stream indexed
    /// by exec record: an empty value record, and one more index for
    /// `calls.dat` and `events.dat`.
    fn note_non_step_exec_record(&mut self) {
        if let Some(builder) = self.value_stream_builder.as_mut() {
            builder.note_non_step_record();
        }
        if let Some(builder) = self.call_stream_builder.as_mut() {
            builder.note_exec_record();
        }
        if let Some(builder) = self.io_event_stream_builder.as_mut() {
            builder.note_exec_record();
        }
    }

    // --- Column-aware step mode ---------------------------------------------

    /// Opt this trace into column-aware step encoding.
    ///
    /// Must be called **before** `begin_writing_trace_events`: the mode decides
    /// `paths.dat`'s record shape and `steps.dat`'s addressing, and the spec
    /// forbids mixing column-aware and line-only records inside one trace. A
    /// call after the trace has begun is refused and recorded — see
    /// [`Self::dropped_column_awareness`] — rather than applied to the tail of
    /// the stream, which would produce a container no reader can decode.
    ///
    /// Mirrors the Nim writer's `enableColumnAwareSteps`.
    pub fn enable_column_aware_steps(&mut self) {
        self.column_aware_requested = true;
        if self.column_aware_active {
            return;
        }
        if self.ctfs_writer.is_some() {
            // Until the first record, and before any path is laid out, the
            // trace is still empty: the step stream and the position space
            // are rebuilt column-aware. After either, it is too late.
            if self.meta_committed || !self.base.path_list.is_empty() {
                self.column_awareness_dropped = true;
                return;
            }
            self.column_aware_active = true;
            self.position_space = PositionSpace::new(true);
            self.step_stream_builder = None;
            self.step_map_builder = None;
            if let Some(tables) = self.interning_tables_builder.as_mut() {
                tables.set_column_aware(true);
            }
            return;
        }
        self.column_aware_active = true;
    }

    /// Declare that this recorder's columns are sharp enough for the GUI to
    /// place per-column breakpoints (`meta.dat` bit 6).
    ///
    /// Implies [`Self::enable_column_aware_steps`], because a capability bit
    /// without column data on the wire is undefined per spec — the Nim writer's
    /// `enableColumnBreakpointsSupport` auto-enables it for the same reason.
    pub fn enable_column_breakpoints_support(&mut self) {
        self.column_breakpoints_requested = true;
        self.enable_column_aware_steps();
    }

    /// Declare that this recorder supports per-column step over / in / out
    /// (`meta.dat` bit 7). Implies [`Self::enable_column_aware_steps`].
    pub fn enable_column_motions_support(&mut self) {
        self.column_motions_requested = true;
        self.enable_column_aware_steps();
    }

    /// Whether this writer is producing a column-aware trace.
    pub fn column_aware_steps_enabled(&self) -> bool {
        self.column_aware_active
    }

    /// Whether a caller asked for column-aware output that this writer could
    /// not produce.
    ///
    /// A caller whose correctness depends on columns should assert this is
    /// `false` at close. It answers `true` in exactly one reachable situation:
    /// [`Self::enable_column_aware_steps`] was called after
    /// `begin_writing_trace_events`, when the mode can no longer be made
    /// trace-global. It answers `false` both when nobody asked and when the
    /// request was honoured, so the signal is only meaningful where columns
    /// were requested — asserting it unconditionally would pass on every
    /// ordinary recording for the wrong reason.
    pub fn dropped_column_awareness(&self) -> bool {
        self.column_awareness_dropped
    }

    /// Register a source path together with its per-line addressable column
    /// counts (spec `paths.dat` Layout A), returning its interning id.
    ///
    /// `line_lengths[i]` is the number of addressable columns on line `i + 1`.
    /// Implementations are free to use `actual_columns + 1` so the trailing
    /// "one past end of line" position gets its own address.
    ///
    /// Outside column-aware mode the table is accepted and ignored, exactly as
    /// the Nim writer ignores it, so a recorder can call this unconditionally
    /// without changing a line-only trace's bytes.
    ///
    /// In column-aware mode the file's table is decided when the path is first
    /// mentioned — here, or by a step, a function or an id request naming it —
    /// by [`column_table_at_first_mention`] (`internal-files.md` §"`paths.dat`
    /// Layout A"): a given table as given, except that one whose lines hold
    /// nothing gives its first line a position; an empty table or none, the
    /// conventional table. A recorder that can read a file's source registers
    /// its real table before the file's first mention.
    ///
    /// For a path already interned, a non-empty table that is not the recorded
    /// one (after the same normalisation) is refused, naming the path:
    /// positions already written depend on the file's size. The refusal is
    /// recorded in [`Self::refusals`], which fails the recording, and
    /// [`INVALID_PATH_ID`] is returned. The same table again, or none, returns
    /// the existing id.
    ///
    /// Mirrors the Nim writer's `registerPath(path, lineLengths)`.
    pub fn register_path_with_line_lengths(&mut self, path: &Path, line_lengths: &[u32]) -> codetracer_trace_types::PathId {
        match self.try_register_path_with_line_lengths(path, line_lengths) {
            Ok(id) => id,
            Err(refusal) => {
                self.refusals.push(refusal);
                INVALID_PATH_ID
            }
        }
    }

    fn try_register_path_with_line_lengths(&mut self, path: &Path, line_lengths: &[u32]) -> Result<codetracer_trace_types::PathId, String> {
        if let Some(id) = self.base.paths.get(path).copied() {
            if self.column_aware_active
                && !line_lengths.is_empty()
                && let Some(recorded) = self.position_space.line_lengths().get(id.0)
            {
                let offered = column_table_at_first_mention(line_lengths);
                if &offered != recorded {
                    // An empty held table is the conventional one.
                    let lines = |t: &[u32]| if t.is_empty() { DEFAULT_LINES_PER_FILE as usize } else { t.len() };
                    return Err(late_column_table_diagnostic(path, lines(recorded), lines(&offered)));
                }
            }
            return Ok(id);
        }
        self.pending_line_lengths = Some(if self.column_aware_active {
            column_table_at_first_mention(line_lengths)
        } else {
            line_lengths.to_vec()
        });
        let id = AbstractTraceWriter::ensure_path_id(self, path);
        // `ensure_path_id` emits the `Path` event, which consumes the pending
        // table. Clear it defensively so a path that somehow did not emit one
        // cannot leak its table onto the next path registered.
        self.pending_line_lengths = None;
        Ok(id)
    }

    /// Emit a column-only step: a `DeltaColumn` (tag 0x07) record that advances
    /// the cursor's column inside the current line.
    ///
    /// `column_delta` is signed and zigzag-encoded; magnitudes up to ±63 cost
    /// two bytes. A value record is opened alongside it so `values.dat` stays
    /// parallel-indexed to `steps.dat` — without that the two streams drift by
    /// one record per column move.
    ///
    /// Refused when the trace is not column-aware, when no trace is open, or
    /// when it would be the first step (the running cursor must be defined
    /// first). Mirrors the Nim writer's `registerColumnStep`.
    pub fn register_column_step(&mut self, column_delta: i64) -> Result<(), String> {
        if !self.column_aware_active {
            return Err("register_column_step called on a writer that has not opted into column-aware mode \
                        (call enable_column_aware_steps before begin_writing_trace_events)"
                .to_string());
        }
        if self.exec_encoder.is_none() {
            return Err("register_column_step called before begin_writing_trace_events".to_string());
        }
        // On a file with the conventional table a move past column
        // `CONVENTIONAL_LINE_LENGTH` stops at that column of the current line.
        let mut column_delta = column_delta;
        if let Some((path_id, line)) = self.last_step_location
            && self.position_space.is_conventional(path_id)
        {
            let line_base = self.position_space.position_of(path_id, line);
            let column = self.step_encoder.last_position() as i64 - line_base as i64 + 1;
            column_delta = (column + column_delta).min(i64::from(CONVENTIONAL_LINE_LENGTH)) - column;
        }
        let event = self.step_encoder.column_step(column_delta)?;
        self.commit_meta();
        self.exec_encoder.as_mut().expect("checked above").write_event(event)?;
        // A column step is an exec record: it owns a value record and advances
        // the index `calls.dat` and `events.dat` are expressed in.
        if let Some(builder) = self.value_stream_builder.as_mut() {
            builder.open_step_record();
        }
        if let Some(builder) = self.call_stream_builder.as_mut() {
            builder.note_exec_record();
        }
        if let Some(builder) = self.io_event_stream_builder.as_mut() {
            builder.note_exec_record();
        }
        self.after_record();
        Ok(())
    }

    /// The per-file `line_lengths` tables registered so far, in interning-id
    /// order — what a reader's `GlobalPositionDecoder::from_line_lengths`
    /// consumes to resolve this trace's positions.
    pub fn line_lengths(&self) -> &[Vec<u32>] {
        self.position_space.line_lengths()
    }

    /// Set the records-per-chunk for `calls.dat` (seek granularity). Smaller
    /// chunks give finer seeks at a slightly lower compression ratio.
    pub fn with_calls_chunk_size(mut self, chunk_size: usize) -> Self {
        self.calls_chunk_size = chunk_size.max(1);
        self
    }

    /// Set the records-per-chunk for `steps.dat` (seek granularity). Smaller
    /// chunks give finer seeks at a slightly lower compression ratio.
    pub fn with_steps_chunk_size(mut self, chunk_size: usize) -> Self {
        self.steps_chunk_size = chunk_size.max(1);
        self
    }

    /// Set the records-per-chunk for `values.dat` (seek granularity). Smaller
    /// chunks give finer seeks at a slightly lower compression ratio.
    pub fn with_values_chunk_size(mut self, chunk_size: usize) -> Self {
        self.values_chunk_size = chunk_size.max(1);
        self
    }

    /// Set the records-per-chunk for `events.dat` (the event-log page
    /// granularity). Smaller chunks give finer pages at a slightly lower
    /// compression ratio.
    pub fn with_events_chunk_size(mut self, chunk_size: usize) -> Self {
        self.events_chunk_size = chunk_size.max(1);
        self
    }

    /// Create a CTFS trace writer that builds the container **in memory**
    /// instead of on disk.
    ///
    /// This is the constructor to use from WebAssembly, where there is no
    /// filesystem — but nothing about it is wasm-specific, and on a host it
    /// produces the same container the file-backed writer would.
    ///
    /// Usage is otherwise identical to [`new`](Self::new). The `path` handed
    /// to `begin_writing_trace_events` is ignored (pass anything, e.g.
    /// `Path::new("trace")`); after `finish_writing_trace_events` the bytes
    /// come out of [`take_container_bytes`](Self::take_container_bytes):
    ///
    /// ```no_run
    /// use codetracer_trace_writer::{ctfs_writer::CtfsTraceWriter, trace_writer::TraceWriter};
    /// use std::path::Path;
    ///
    /// let mut writer = CtfsTraceWriter::new_in_memory("program", &[]);
    /// writer.begin_writing_trace_events(Path::new("trace"))?;
    /// // ... register steps/calls/values ...
    /// writer.finish_writing_trace_events()?;
    /// let ct_bytes: Vec<u8> = writer.take_container_bytes().expect("in-memory writer");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// `ct_bytes` is a complete `.ct` container — write it to a file, hand it
    /// to a `Blob`, upload it. On `wasm32-unknown-unknown` you will usually
    /// also want [`set_recording_id`](Self::set_recording_id), since the
    /// module cannot mint a real UUIDv7 without a clock or a CSPRNG.
    pub fn new_in_memory(program: &str, args: &[String]) -> Self {
        let mut writer = Self::new(program, args);
        writer.output = CtfsOutput::Memory;
        writer
    }

    /// Choose file-backed or in-memory output. Must be set before
    /// `begin_writing_trace_events`.
    pub fn with_output(mut self, output: CtfsOutput) -> Self {
        self.output = output;
        self
    }

    /// Where this writer lays the container out.
    pub fn output(&self) -> CtfsOutput {
        self.output
    }

    /// Choose the container profile at close (`ctfs-container.md` §1e, §1f).
    ///
    /// The writer always records the full profile, streamed as it goes. With
    /// a non-zero `raw_bytes`, finishing the trace then converts the finished
    /// container: when the members of a compact container of it -- every zstd
    /// frame inflated -- total fewer than `raw_bytes` bytes, the compact
    /// container replaces the full one (in the file, through a sibling
    /// temporary and a rename, or in the in-memory bytes). `0`, the default,
    /// writes the full profile always. The Nim writer's
    /// `trace_writer_set_compact_threshold` makes the same choice, and the two
    /// writers' compact containers are byte-identical.
    /// [`compact_profile::DEFAULT_RAW_BYTE_THRESHOLD`](crate::compact_profile::DEFAULT_RAW_BYTE_THRESHOLD)
    /// is §1e's recommended figure.
    pub fn with_compact_threshold(mut self, raw_bytes: u64) -> Self {
        self.compact_threshold = raw_bytes;
        self
    }

    /// [`with_compact_threshold`](Self::with_compact_threshold) on a writer
    /// already built. May be called at any time before the trace is finished.
    pub fn set_compact_threshold(&mut self, raw_bytes: u64) {
        self.compact_threshold = raw_bytes;
    }

    /// The raw-byte threshold the profile is chosen by; `0` when the full
    /// profile is always written.
    pub fn compact_threshold(&self) -> u64 {
        self.compact_threshold
    }

    /// Take the finished container bytes.
    ///
    /// Returns `Some` only for an in-memory writer whose
    /// `finish_writing_trace_events` has completed; `None` for a file-backed
    /// writer (whose bytes are on disk) or before the trace is finished. The
    /// bytes are moved out, so a second call returns `None`.
    pub fn take_container_bytes(&mut self) -> Option<Vec<u8>> {
        self.container_bytes.take()
    }

    /// Borrow the finished container bytes without consuming them.
    pub fn container_bytes(&self) -> Option<&[u8]> {
        self.container_bytes.as_deref()
    }

    /// Replace the finished full container with its compact one when the
    /// compact members total fewer than the threshold's raw bytes
    /// (`ctfs-container.md` §1e). A file is replaced through a sibling
    /// temporary and a rename, so it holds the full container or the compact
    /// one and never a partial write.
    fn choose_profile(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        use crate::compact_profile::select_profile;
        let choosing = |e: String| format!("choosing the container profile: {e}");
        match self.output {
            CtfsOutput::Memory => {
                if let Some(full) = self.container_bytes.take() {
                    let (_, chosen, _) = select_profile(full, self.compact_threshold).map_err(choosing)?;
                    self.container_bytes = Some(chosen);
                }
            }
            CtfsOutput::File => {
                let Some(ct_path) = self.ct_path.clone() else {
                    return Ok(());
                };
                let full = std::fs::read(&ct_path)?;
                let (profile, chosen, _) = select_profile(full, self.compact_threshold).map_err(choosing)?;
                if profile == codetracer_ctfs::compact::Profile::Compact {
                    let mut tmp = ct_path.clone().into_os_string();
                    tmp.push(".compact.tmp");
                    let tmp = std::path::PathBuf::from(tmp);
                    if let Err(e) = std::fs::write(&tmp, &chosen).and_then(|_| std::fs::rename(&tmp, &ct_path)) {
                        let _ = std::fs::remove_file(&tmp);
                        return Err(format!("replacing {} with its compact container: {e}", ct_path.display()).into());
                    }
                }
            }
        }
        Ok(())
    }

    /// Pin the `recording_id` stamped into `meta.json` and `meta.dat`.
    ///
    /// By default the writer mints a fresh UUIDv7 when it writes `meta.dat`. Set it explicitly when the identity is
    /// decided elsewhere — an import pinning a pre-existing id, a test that
    /// wants a reproducible container, or a browser host minting the id in
    /// JavaScript because `wasm32-unknown-unknown` has neither a wall clock
    /// nor an entropy source.
    ///
    /// A change is refused, failing the recording, once `meta.dat` has been
    /// written by the first record.
    pub fn set_recording_id(&mut self, recording_id: impl Into<String>) {
        let recording_id = recording_id.into();
        if self.meta_committed && self.recording_id.as_deref() != Some(recording_id.as_str()) {
            self.refusals
                .push("set_recording_id after the first record: meta.dat, which carries the recording id, is already written".to_string());
            return;
        }
        self.recording_id = Some(recording_id);
    }

    /// Hold the first failure to encode or write the container.
    fn latch<E: std::fmt::Display>(&mut self, result: Result<(), E>) {
        if let Err(e) = result {
            self.write_error.get_or_insert_with(|| e.to_string());
        }
    }

    /// The `meta.dat` this trace is committed with.
    fn meta_dat_bytes(&mut self) -> Vec<u8> {
        let recording_id = self
            .recording_id
            .get_or_insert_with(|| {
                codetracer_trace_types::TraceMetadata::new(self.base.program.clone(), self.base.args.clone(), self.base.workdir.clone()).recording_id
            })
            .clone();
        // Bits 8-12: the streams this writer creates when it commits; it never
        // creates a member lazily, so it sets no other stream-presence bit
        // (`internal-files.md` §"Stream-presence flags are a hint, not a gate").
        let mut flags = FLAG_HAS_CALL_STREAM | FLAG_HAS_STEP_STREAM | FLAG_HAS_VALUE_STREAM | FLAG_HAS_IO_EVENT_STREAM | FLAG_HAS_INTERNING_TABLES;
        // The column bits. Bit 4 says the wire format changed — `paths.dat`
        // is Layout A and `steps.dat` positions address (line, column) —
        // and a reader that does not know it is required by spec to refuse
        // the container rather than misdecode it. Bits 6 and 7 are
        // capability claims about the recorder, and are meaningless without
        // bit 4, so they are only ever set alongside it.
        if self.column_aware_active {
            flags |= FLAG_HAS_COLUMN_AWARE_STEPS;
            if self.column_breakpoints_requested {
                flags |= FLAG_SUPPORTS_COLUMN_BREAKPOINTS;
            }
            if self.column_motions_requested {
                flags |= FLAG_SUPPORTS_COLUMN_MOTIONS;
            }
        }
        if self.line_count_table {
            flags |= FLAG_HAS_LINE_COUNT_TABLE;
        }
        let ext_flags = if self.source_reloads_declared { FLAG_EXT_HAS_SOURCE_RELOAD } else { 0 };
        encode_meta_dat_with_blocks(
            &recording_id,
            &self.base.program,
            &self.base.args,
            &self.base.workdir.to_string_lossy(),
            "",
            flags,
            ext_flags,
            &self.meta_blocks,
        )
    }

    /// Write `meta.dat`, complete, and create the members the trace is
    /// recorded into. Done by the first record, or at finish for a trace with
    /// none (`ctfs-container.md` §6, "Durability", rule 1); every field and
    /// flag is fixed from then on.
    /// Create the members the trace is recorded into, once. Done when
    /// `meta.dat` is committed, or earlier by a member that must follow them
    /// in the container's member order (the marker label table).
    fn create_members(&mut self) {
        if self.members.is_some() || self.ctfs_writer.is_none() || self.write_error.is_some() {
            return;
        }
        let result = (|| -> Result<Members, codetracer_ctfs::CtfsError> {
            let w = self.ctfs_writer.as_mut().expect("checked above");
            let mut pair = |dat: &str, idx: &str| -> Result<_, codetracer_ctfs::CtfsError> { Ok((w.add_file(dat)?, w.add_file(idx)?)) };
            // Created in the Nim writer's order: the container's member order
            // is the compact container's (`ctfs-container.md` §1d, §1f), so
            // the two writers' compact containers agree only if it agrees.
            // Fields are evaluated in the order written.
            Ok(Members {
                interning: [
                    pair("paths.dat", "paths.off")?,
                    pair("funcs.dat", "funcs.off")?,
                    pair("types.dat", "types.off")?,
                    pair("varnames.dat", "varnames.off")?,
                ],
                steps: pair("steps.dat", "steps.idx")?,
                values: pair("values.dat", "values.idx")?,
                calls: pair("calls.dat", "calls.idx")?,
                events: pair("events.dat", "events.idx")?,
            })
        })();
        match result {
            Ok(members) => self.members = Some(members),
            Err(e) => self.latch(Err::<(), _>(format!("creating the trace's members: {e}"))),
        }
    }

    fn commit_meta(&mut self) {
        if self.meta_committed || self.ctfs_writer.is_none() {
            return;
        }
        self.meta_committed = true;
        if self.members.is_none() {
            return;
        }
        let meta = self.meta_dat_bytes();
        let w = self.ctfs_writer.as_mut().expect("checked above");
        let result = (|| -> Result<(), codetracer_ctfs::CtfsError> {
            let meta_handle = w.add_file("meta.dat")?;
            w.write(meta_handle, &meta)?;
            w.sync_entry(meta_handle)
        })();
        self.latch(result.map_err(|e| format!("writing meta.dat: {e}")));
    }

    /// Create the trace's members, in the order the container lists them,
    /// and write what each holds before any record: every offset table's
    /// record-0 offset `0`, in table order, then every stream index's header,
    /// in stream order (`ctfs-container.md` §6, "Block placement").
    fn open_members(&mut self) {
        self.create_members();
        let Some(members) = self.members.as_ref() else {
            return;
        };
        let offs: Vec<_> = members.interning.iter().map(|&(_, off)| off).collect();
        let w = self.ctfs_writer.as_mut().expect("members exist only while the container is open");
        let result = (|| -> Result<(), codetracer_ctfs::CtfsError> {
            for off in offs {
                w.write(off, &0u64.to_le_bytes())?;
            }
            Ok(())
        })();
        self.latch(result.map_err(|e| format!("opening the trace's members: {e}")));
        self.interning_dirty = [true; 4];
        self.publish();
    }

    /// After an interning registration: append its record, and after a path,
    /// the function records it made writable. Kept out of line, since
    /// `add_event` runs for every record and this for few.
    #[cold]
    #[inline(never)]
    fn after_interning(&mut self, path: bool) {
        self.append_interning_records();
        if path {
            self.write_ready_functions();
        }
    }

    /// Append every interning record registered since the last append, as
    /// the record is registered: its bytes to the table's `.dat`, then its
    /// end offset to the `.off` (`ctfs-container.md` §6, "Block placement").
    /// They are published with the next sealed chunk.
    fn append_interning_records(&mut self) {
        let (Some(members), Some(tables), Some(w)) = (self.members.as_ref(), self.interning_tables_builder.as_ref(), self.ctfs_writer.as_mut())
        else {
            return;
        };
        let counts = [tables.path_count(), tables.func_count(), tables.type_count(), tables.varname_count()];
        let mut result: Result<(), codetracer_ctfs::CtfsError> = Ok(());
        for (t, &(dat_h, off_h)) in members.interning.iter().enumerate() {
            let (published, mut dat_len) = self.interning_published[t];
            for id in published..counts[t] {
                let rec = match t {
                    0 => tables.path_record(id),
                    1 => tables.func_record(id),
                    2 => tables.type_record(id),
                    _ => tables.varname_record(id),
                };
                dat_len += rec.len() as u64;
                if result.is_ok() {
                    result = w.write(dat_h, &rec).and_then(|_| w.write(off_h, &dat_len.to_le_bytes())).map(|_| ());
                }
                self.interning_dirty[t] = true;
            }
            self.interning_published[t] = (counts[t], dat_len);
        }
        self.latch(result.map_err(|e| format!("writing an interning record: {e}")));
    }

    /// Move every record that has become final into its stream: steps as they
    /// are built, values once no later event can change them, calls once they
    /// and every call before them have returned, I/O events as they are
    /// built. Chunks seal as they fill.
    fn drain_streams(&mut self) {
        let mut result: Result<(), String> = Ok(());
        if let (Some(builder), Some(encoder)) = (self.step_stream_builder.as_mut(), self.exec_encoder.as_mut()) {
            for record in builder.drain() {
                if result.is_ok() {
                    result = crate::step_stream::write_record(encoder, &record);
                }
            }
        }
        if let (Some(builder), Some(sink)) = (self.value_stream_builder.as_mut(), self.values_sink.as_mut()) {
            for record in builder.take_final() {
                let mut bytes = Vec::new();
                record.encode(&mut bytes);
                if result.is_ok() {
                    result = sink.push(&bytes);
                }
            }
        }
        if let (Some(builder), Some(sink)) = (self.call_stream_builder.as_mut(), self.calls_sink.as_mut()) {
            for record in builder.take_complete() {
                let mut bytes = Vec::new();
                record.encode(&mut bytes);
                if result.is_ok() {
                    result = sink.push(&bytes);
                }
            }
        }
        if let (Some(builder), Some(sink)) = (self.io_event_stream_builder.as_mut(), self.events_sink.as_mut()) {
            for record in builder.take_records() {
                if result.is_ok() {
                    result = crate::event_stream::event_log_kind(record.kind).and_then(|_| {
                        let mut bytes = Vec::new();
                        record.encode(&mut bytes);
                        sink.push(&bytes)
                    });
                }
            }
        }
        self.latch(result);
    }

    /// Whether a stream sealed a chunk that is not yet in the container.
    fn sealed_unpublished(&self) -> bool {
        self.exec_encoder.as_ref().is_some_and(|e| e.has_sealed())
            || [&self.calls_sink, &self.values_sink, &self.events_sink]
                .iter()
                .any(|s| s.as_ref().is_some_and(ChunkSink::has_new))
    }

    /// After a record: drain what became final and, if a chunk sealed,
    /// publish it.
    fn after_record(&mut self) {
        self.drain_streams();
        if self.sealed_unpublished() {
            self.publish();
        }
    }

    /// Write the functions whose declaration path is registered, in id order,
    /// stopping at the first whose path is not: its record can only be written
    /// once its file is laid out, and `funcs.dat` is in id order.
    fn write_ready_functions(&mut self) {
        if self.members.is_none() || self.writing_functions {
            return;
        }
        self.writing_functions = true;
        while let Some((_, path, _, known)) = self.pending_functions.first() {
            let Some(path_id) = known.or_else(|| self.base.paths.get(path).copied()) else {
                break;
            };
            let (name, _, line, _) = self.pending_functions.remove(0);
            self.base.function_list.push((name.clone(), path_id, line));
            AbstractTraceWriter::add_event(
                self,
                TraceLowLevelEvent::Function(codetracer_trace_types::FunctionRecord { name, path_id, line }),
            );
        }
        self.writing_functions = false;
    }

    /// Publish everything sealed so far (`ctfs-container.md` §6,
    /// "Durability", rule 2): the sealed chunks' bytes and index entries,
    /// every interning record registered so far, then the root entries of
    /// every member that grew — data before the entry that publishes it.
    fn publish(&mut self) {
        self.publish_in_order(false);
    }

    /// [`Self::publish`]; `closing` puts the call stream's chunks first, as
    /// the writer seals them at close.
    fn publish_in_order(&mut self, closing: bool) {
        if self.write_error.is_some() || self.members.is_none() {
            return;
        }
        let members = self.members.as_ref().expect("checked above");
        let mut writes: Vec<(codetracer_ctfs::FileHandle, Vec<u8>)> = Vec::new();
        let mut stream = |handles: (codetracer_ctfs::FileHandle, codetracer_ctfs::FileHandle), (dat, idx): (Vec<u8>, Vec<u8>)| {
            writes.push((handles.0, dat));
            writes.push((handles.1, idx));
        };
        // While recording, one record seals the step stream before the value
        // stream; at close the call stream's last chunk seals first.
        let mut calls = self.calls_sink.as_mut().map(ChunkSink::take);
        if closing && let Some(c) = calls.take() {
            stream(members.calls, c);
        }
        if let Some(e) = self.exec_encoder.as_mut() {
            stream(members.steps, e.take_sealed());
        }
        if let Some(s) = self.values_sink.as_mut() {
            stream(members.values, s.take());
        }
        if let Some(c) = calls {
            stream(members.calls, c);
        }
        if let Some(s) = self.events_sink.as_mut() {
            stream(members.events, s.take());
        }
        writes.retain(|(_, bytes)| !bytes.is_empty());
        let w = self.ctfs_writer.as_mut().expect("members exist only while the container is open");
        let result = (|| -> Result<(), codetracer_ctfs::CtfsError> {
            for (h, bytes) in &writes {
                w.write(*h, bytes)?;
            }
            Ok(())
        })();
        self.latch(result.map_err(|e| format!("publishing a sealed chunk: {e}")));

        let members = self.members.as_ref().expect("checked above");
        let mut handles: Vec<codetracer_ctfs::FileHandle> = writes.iter().map(|(h, _)| *h).collect();
        for (t, &(dat_h, off_h)) in members.interning.iter().enumerate() {
            if std::mem::take(&mut self.interning_dirty[t]) {
                handles.push(dat_h);
                handles.push(off_h);
            }
        }
        let w = self.ctfs_writer.as_mut().expect("members exist only while the container is open");
        let result = (|| -> Result<(), codetracer_ctfs::CtfsError> {
            for h in &handles {
                w.write_pending(*h)?;
            }
            for h in &handles {
                w.publish_entry(*h)?;
            }
            w.flush()
        })();
        self.latch(result.map_err(|e| format!("publishing a sealed chunk: {e}")));
    }
}

impl AbstractTraceWriter for CtfsTraceWriter {
    fn get_data(&self) -> &AbstractTraceWriterData {
        &self.base
    }

    /// Intern a type by its kind and name: `types.dat` records both, so a
    /// name shared by two kinds is two types.
    fn ensure_raw_type_id(&mut self, typ: codetracer_trace_types::TypeRecord) -> codetracer_trace_types::TypeId {
        let key = (typ.kind as u8, typ.lang_type.clone());
        if let Some(id) = self.type_ids.get(&key) {
            return *id;
        }
        let id = codetracer_trace_types::TypeId(self.type_ids.len());
        self.type_ids.insert(key, id);
        self.base.types.entry(typ.lang_type.clone()).or_insert(id);
        AbstractTraceWriter::register_raw_type(self, typ);
        id
    }

    fn get_mut_data(&mut self) -> &mut AbstractTraceWriterData {
        &mut self.base
    }

    /// Set the working directory `meta.dat` records. A change is refused,
    /// failing the recording, once `meta.dat` has been written by the first
    /// record
    /// (`internal-files.md` §"Extended flags": every field is fixed then).
    fn set_workdir(&mut self, workdir: &Path) {
        if self.meta_committed && self.base.workdir != workdir {
            self.refusals.push(format!(
                "set_workdir({}) after the first record: meta.dat, which carries the workdir, is already written",
                workdir.display()
            ));
            return;
        }
        self.base.workdir = workdir.to_path_buf();
    }

    /// Intern `path`, resolving it to its newest version.
    ///
    /// The id of a new path is the number of `paths.dat` records written so
    /// far, not the number of distinct names: with path versions the two
    /// differ. Under the line-count table a path with no recorded count is
    /// refused (recorded in [`CtfsTraceWriter::refusals`]) and
    /// [`INVALID_PATH_ID`] is returned.
    fn ensure_path_id(&mut self, path: &std::path::Path) -> codetracer_trace_types::PathId {
        if let Some(id) = self.base.paths.get(path) {
            return *id;
        }
        if self.line_count_table && self.pending_line_count.is_none() {
            self.refusals.push(format!(
                "{} has no recorded line count; under the line-count table every path is registered with \
                 register_path_with_line_count",
                path.display()
            ));
            return INVALID_PATH_ID;
        }
        let id = codetracer_trace_types::PathId(self.base.path_list.len());
        self.base.paths.insert(path.to_path_buf(), id);
        self.emit_path_record(path);
        id
    }

    /// Register `path`: intern it, as every other mention of a path does. A
    /// path already registered keeps its id and gains no `paths.dat` record.
    fn register_path(&mut self, path: &std::path::Path) {
        AbstractTraceWriter::ensure_path_id(self, path);
    }

    /// Record a step, refusing one the container cannot represent: a path with
    /// no recorded line count, or a line past the file's recorded count, whose
    /// address would fall inside the NEXT file's range
    /// (`internal-files.md` §"`paths.dat` line-count table").
    fn register_step(&mut self, path: &std::path::Path, line: codetracer_trace_types::Line) {
        let path_id = AbstractTraceWriter::ensure_path_id(self, path);
        if path_id == INVALID_PATH_ID {
            return;
        }
        if self.line_count_table
            && let Some(count) = self.path_line_counts.get(path_id.0)
            && line.0 > 0
            && line.0 as u64 > *count
        {
            self.refusals.push(format!(
                "step at line {} of {}, which this trace records as having {count} line(s); its address would fall \
                 inside the next file's range",
                line.0,
                path.display()
            ));
            return;
        }
        AbstractTraceWriter::add_event(self, TraceLowLevelEvent::Step(codetracer_trace_types::StepRecord { path_id, line }));
    }

    /// Register a function. Its record is written once its declaration path
    /// is registered — at the next publication after that, or at finish — not
    /// now. A path registered already resolves now, to the version current
    /// now (`internal-files.md` §"`paths.dat` path versions"), so a version
    /// registered later does not move the function.
    ///
    /// A function's declaration path may be a file no step has visited yet.
    /// Interning it now would give it the next path id ahead of the files the
    /// recorder registers next, and would intern it with no size: a later
    /// `register_path_with_line_lengths` for it would find it interned and
    /// lose its per-line table, and under the line-count table a later
    /// `register_path_with_line_count` would come too late for the function
    /// to be accepted. At finish every such registration has happened; a path
    /// still unregistered then is interned there, and under the line-count
    /// table refused by name.
    /// Register a function. Its `funcs.dat` record is written as soon as it
    /// and every function registered before it have a registered declaration
    /// path: now, or when that path is registered.
    fn register_function(&mut self, name: &str, path: &std::path::Path, line: codetracer_trace_types::Line) {
        let known = self.base.paths.get(path).copied();
        self.pending_functions.push((name.to_string(), path.to_path_buf(), line, known));
        self.write_ready_functions();
    }

    /// Record a step at `(path, line, column)`.
    ///
    /// This overrides the trait's column-dropping shim. In column-aware mode
    /// the column is folded into the step's `global_position_index`, so the
    /// wire carries ONE record at `(line, column)` — matching the canonical Nim
    /// FFI, whose `trace_writer_register_delta_column` folds into the pending
    /// step for the same reason.
    ///
    /// Outside column-aware mode the column is still dropped, because there is
    /// nowhere in a line-only address space to put it; the difference from the
    /// old behaviour is that a caller can now detect that case up front through
    /// [`CtfsTraceWriter::column_aware_steps_enabled`] instead of discovering it
    /// in the decoded trace.
    fn register_step_with_column(
        &mut self,
        path: &std::path::Path,
        line: codetracer_trace_types::Line,
        column: Option<codetracer_trace_types::Line>,
    ) {
        if self.column_aware_active {
            // CTFS columns are 1-based, so column 1 is a zero delta.
            self.pending_column_delta = column.map(|c| c.0 - 1).unwrap_or(0);
        }
        AbstractTraceWriter::register_step(self, path, line);
        // Defensive: `register_step` always emits a `Step` event, which
        // consumes the delta. Clearing it anyway means a future refactor that
        // suppresses the event cannot leak a column onto an unrelated step.
        self.pending_column_delta = 0;
    }

    fn add_event(&mut self, event: TraceLowLevelEvent) {
        // A step past the last line of a file with the conventional table would
        // address the next file's range; it is refused, failing the recording,
        // as under the line-count table.
        if self.column_aware_active
            && let TraceLowLevelEvent::Step(step) = &event
            && self.position_space.is_conventional(step.path_id.0 as u64)
            && step.line.0 > DEFAULT_LINES_PER_FILE as i64
        {
            let path = self.base.path_list.get(step.path_id.0).cloned().unwrap_or_default();
            self.refusals.push(conventional_line_diagnostic(&path, step.line.0));
            self.pending_column_delta = 0;
            return;
        }
        if is_record(&event) {
            self.commit_meta();
        }
        // Column-aware mode intercepts the two events that carry source
        // positions BEFORE the line-only builders see them. `Path` grows the
        // position space; `Step` is encoded through Nim's delta policy into the
        // exec encoder instead of through `StepStreamBuilder`. Everything else
        // flows on unchanged, so `calls.dat`, `values.dat` and
        // `events.dat` are produced identically in both modes.
        if self.column_aware_active {
            match &event {
                TraceLowLevelEvent::Path(_) => {
                    // A path first mentioned with no table — by a step, a
                    // function or an id request — gets the conventional one.
                    // An empty table is the conventional one, held as its rule.
                    let lls = self.pending_line_lengths.take().unwrap_or_default();
                    let path_id = self.position_space.push_path(&lls) as usize;
                    if let Some(ref mut builder) = self.interning_tables_builder {
                        builder.set_path_line_lengths(path_id, &lls);
                    }
                }
                TraceLowLevelEvent::Step(step) => {
                    let position = self.position_space.position_of(step.path_id.0 as u64, step.line.0.max(0) as u64);
                    // A bare `StepRecord` carries no column, so the delta is 0
                    // and the step addresses column 1 of the line.
                    // `register_step_with_column` stages a non-zero delta here
                    // so the `(line, column)` pair becomes ONE record rather
                    // than a line step followed by a column step — that folding
                    // is what the canonical Nim FFI does, and the reason is
                    // behavioural rather than aesthetic: an intermediate
                    // column-1 step carries no variables, so a line-granular
                    // step-over lands on it and `variables_at` answers empty.
                    let mut column_delta = std::mem::replace(&mut self.pending_column_delta, 0);
                    // On a file with the conventional table a column above
                    // `CONVENTIONAL_LINE_LENGTH` is recorded at that column of
                    // its line, as a line 0 is recorded as line 1.
                    if self.position_space.is_conventional(step.path_id.0 as u64) {
                        column_delta = column_delta.min(i64::from(CONVENTIONAL_LINE_LENGTH) - 1);
                    }
                    self.last_step_location = Some((step.path_id.0 as u64, step.line.0.max(0) as u64));
                    // Every registered file has a column axis — its table or
                    // the conventional one — so this guards a path id the
                    // space does not know, whose column would name a later
                    // address rather than a column. The step is
                    // kept at its line and the column is dropped, which is
                    // what the spec requires of a column arriving as part of
                    // a step (`trace-events.md` §"A column needs a file with
                    // a column axis"); the line is a position the recorder
                    // did observe, and a missing step is much harder to
                    // notice than a missing column.
                    if column_delta != 0 && !self.position_space.has_column_axis(step.path_id.0 as u64) {
                        self.columns_dropped_for_paths.insert(step.path_id.0 as u64);
                        column_delta = 0;
                    }
                    let step_event = self.step_encoder.step_at(position, column_delta);
                    if let Some(encoder) = self.exec_encoder.as_mut() {
                        // A failure here is a zstd failure: held, and
                        // returned when the recording finishes.
                        let r = encoder.write_event(step_event);
                        self.latch(r);
                    }
                }
                TraceLowLevelEvent::ThreadSwitch(codetracer_trace_types::ThreadId(tid)) => {
                    // A thread switch is a record in the execution stream and
                    // occupies a step slot: the Nim writer writes an empty
                    // value record beside it and increments `stepCount`. Both
                    // matter — the value record keeps `values.dat` parallel,
                    // and the counter is what decides whether the NEXT step is
                    // forced absolute.
                    // The value record is opened by `ValueStreamBuilder::observe`
                    // below, as it is for the line-only stream.
                    if let Some(encoder) = self.exec_encoder.as_mut() {
                        let r = encoder.write_event(crate::column_aware::StepEvent::ThreadSwitch { thread_id: *tid });
                        self.latch(r);
                    }
                    self.step_encoder.note_non_step_event();
                }
                TraceLowLevelEvent::ThreadStart(codetracer_trace_types::ThreadId(tid)) => {
                    if let Some(encoder) = self.exec_encoder.as_mut() {
                        let r = encoder.write_event(crate::column_aware::StepEvent::ThreadStart { thread_id: *tid });
                        self.latch(r);
                    }
                    self.step_encoder.note_non_step_event();
                }
                TraceLowLevelEvent::ThreadExit(codetracer_trace_types::ThreadId(tid)) => {
                    if let Some(encoder) = self.exec_encoder.as_mut() {
                        let r = encoder.write_event(crate::column_aware::StepEvent::ThreadExit { thread_id: *tid });
                        self.latch(r);
                    }
                    self.step_encoder.note_non_step_event();
                }
                _ => {}
            }
        }
        // A line-count-table trace sizes every file from its recorded count.
        // The count rides on the registration that emits this `Path` event.
        if self.line_count_table
            && let TraceLowLevelEvent::Path(_) = &event
        {
            let count = self.pending_line_count.take().unwrap_or(crate::line_position::DEFAULT_LINES_PER_FILE);
            self.path_line_counts.push(count);
            if let Some(builder) = self.step_stream_builder.as_mut() {
                builder.set_next_path_line_count(count);
            }
            if let Some(builder) = self.interning_tables_builder.as_mut() {
                builder.set_next_path_line_count(count);
            }
        }
        // M17a: feed the dedicated call-stream builder from the SAME event
        // sequence as every other stream, so calls.dat stays consistent.
        if let Some(ref mut builder) = self.call_stream_builder {
            builder.observe(&event);
        }
        // M23a: feed the dedicated step-stream builder from the SAME event
        // sequence as every other stream, so steps.dat stays consistent.
        // Armed only in line-only mode; the column-aware path above owns
        // `steps.dat` instead.
        if let Some(ref mut builder) = self.step_stream_builder {
            // The step's id in `step-map.ns` is the exec-record index it is
            // about to take, so it is read before the builder appends it.
            if let (TraceLowLevelEvent::Step(step), Some(map)) = (&event, self.step_map_builder.as_mut()) {
                map.record_step(step.path_id.0 as u64, step.line.0, builder.len() as u64);
            }
            builder.observe(&event);
        }
        // M23b: feed the dedicated value-stream builder from the SAME event
        // sequence as every other stream, so values.dat stays consistent and
        // parallel-indexed to the step stream.
        if let Some(ref mut builder) = self.value_stream_builder {
            builder.observe(&event);
        }
        // M23c: feed the dedicated I/O event-stream builder from the SAME event
        // sequence as every other stream, so events.dat stays consistent.
        if let Some(ref mut builder) = self.io_event_stream_builder {
            builder.observe(&event);
        }
        // M23d: feed the interning-tables builder from the SAME
        // Path/Function/Type/VariableName events the streams intern against, so
        // the binary tables resolve exactly the ids the streams reference.
        if let Some(ref mut builder) = self.interning_tables_builder {
            builder.observe(&event);
        }
        match event {
            TraceLowLevelEvent::Path(_) => self.after_interning(true),
            TraceLowLevelEvent::Function(_) | TraceLowLevelEvent::Type(_) | TraceLowLevelEvent::VariableName(_) => self.after_interning(false),
            _ => {}
        }
        self.after_record();
    }

    fn append_events(&mut self, events: &mut Vec<TraceLowLevelEvent>) {
        for e in events {
            AbstractTraceWriter::add_event(self, e.clone());
        }
    }
}

impl TraceWriter for CtfsTraceWriter {
    // ---------------------------------------------------------------------
    // THE COLUMN-AWARE FAMILY IS HONOURED NOW, AND THESE OVERRIDES ARE WHAT
    // DELIVERS THAT TO A CALLER HOLDING THE TRAIT.
    //
    // They used to set a `column_aware_requested` flag and nothing else,
    // because this writer had no column-bearing step encoder. It has one.
    // Each override therefore FORWARDS to the inherent method of the same
    // name, which arms the position space, the step policy and the `meta.dat`
    // capability bits.
    //
    // DELETING THEM WOULD NOT BE A SIMPLIFICATION, IT WOULD BE A SILENT
    // NO-OP. `TraceWriter`'s defaults for this family are empty bodies, so a
    // caller that reaches the writer through the trait — `ct_writer_open` in
    // `aztec-avm-runtime/ct-writer` does exactly that — would get columns
    // accepted, ignored, and `dropped_column_awareness()` answering `false`
    // because nobody recorded that anybody asked. That is the campaign's
    // silent-wrong-answer shape, so the forwarding is deliberate and is
    // covered by `the_trait_column_family_reaches_the_real_implementation`.
    // ---------------------------------------------------------------------
    fn enable_column_aware_steps(&mut self) {
        CtfsTraceWriter::enable_column_aware_steps(self);
    }

    fn enable_column_breakpoints_support(&mut self) {
        CtfsTraceWriter::enable_column_breakpoints_support(self);
    }

    fn enable_column_motions_support(&mut self) {
        CtfsTraceWriter::enable_column_motions_support(self);
    }

    fn write_delta_column(&mut self, column_delta: i64) {
        // The trait surface accepts and ignores a column step on a writer that
        // is not column-aware; `dropped_column_awareness` reports a request
        // for columns that could not be honoured. A caller that needs the
        // refusal calls `register_column_step`.
        let _ignored = CtfsTraceWriter::register_column_step(self, column_delta);
    }

    fn register_path_with_line_lengths(
        &mut self,
        path: &Path,
        line_lengths: &[u32],
    ) -> Result<codetracer_trace_types::PathId, Box<dyn std::error::Error>> {
        match self.try_register_path_with_line_lengths(path, line_lengths) {
            Ok(id) => Ok(id),
            Err(refusal) => {
                // Also held, so the recording fails even if the caller drops
                // this error.
                self.refusals.push(refusal.clone());
                Err(refusal.into())
            }
        }
    }

    fn begin_writing_trace_events(&mut self, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        self.ct_path = None;
        let writer = match self.output {
            // Create .ct file at path (replace any existing extension)
            CtfsOutput::File => {
                let ct_path = path.with_extension("ct");
                let writer = CtfsWriter::create(&ct_path, 4096, 31)?;
                self.ct_path = Some(ct_path);
                writer
            }
            CtfsOutput::Memory => CtfsWriter::create_in_memory(4096, 31, codetracer_ctfs::CompressionMethod::None)?,
        };
        self.container_bytes = None;
        self.ctfs_writer = Some(writer);

        // Column-aware mode: arm the Nim-parity position space, step policy and
        // exec-stream encoder, and leave `StepStreamBuilder` disarmed so only
        // one of the two owns `steps.dat`.
        self.position_space = PositionSpace::new(self.column_aware_active);
        self.step_encoder = StepEncoder::new();
        // The one `steps.dat` chunk encoder. The column-aware path writes into
        // it directly; the line-only path through `StepStreamBuilder`.
        let mut exec_encoder = ExecStreamEncoder::new(self.steps_chunk_size, EXEC_COMPRESSION_LEVEL);
        if self.line_hits_requested {
            exec_encoder.enable_line_hits();
        }
        self.exec_encoder = Some(exec_encoder);
        self.calls_sink = Some(ChunkSink::new("calls.dat", self.calls_chunk_size, DEFAULT_CALLS_ZSTD_LEVEL));
        self.values_sink = Some(ChunkSink::new("values.dat", self.values_chunk_size, DEFAULT_CALLS_ZSTD_LEVEL));
        self.events_sink = Some(ChunkSink::new("events.dat", self.events_chunk_size, DEFAULT_CALLS_ZSTD_LEVEL));
        self.members = None;
        self.marker_labels = None;
        self.correlation_markers.clear();
        self.meta_committed = false;
        self.interning_published = [(0, 0); 4];
        self.interning_dirty = [false; 4];
        self.write_error = None;
        self.spans = None;
        self.last_crossing_id = 0;
        self.open_crossings.clear();
        self.pending_line_lengths = None;

        // Every stream is written: each event kind has exactly one stream to
        // live in (`trace-events.md` §"Event Variants by Stream"), so a
        // container missing one has lost that kind of event. There is no
        // switch to turn one off.
        self.call_stream_builder = Some(CallStreamBuilder::new());
        // The line-only step builder; a column-aware trace uses `exec_encoder`.
        self.step_stream_builder = if !self.column_aware_active {
            Some(StepStreamBuilder::new())
        } else {
            None
        };
        self.step_map_builder = if !self.column_aware_active {
            Some(crate::step_map::StepMapBuilder::new())
        } else {
            None
        };
        self.value_stream_builder = Some(ValueStreamBuilder::new());
        self.io_event_stream_builder = Some(IoEventStreamBuilder::new());
        // In column-aware mode `paths.dat` records are spec Layout A.
        let mut tables = InterningTablesBuilder::new();
        tables.set_column_aware(self.column_aware_active);
        tables.set_line_count_table(self.line_count_table);
        self.interning_tables_builder = Some(tables);
        self.open_members();

        Ok(())
    }

    fn finish_writing_trace_events(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // A trace with no record commits its meta.dat now.
        self.commit_meta();
        // The function records still waiting for their declaration path, in
        // id order: each path is registered now if no step did.
        for (name, path, line, known) in std::mem::take(&mut self.pending_functions) {
            let path_id = known.unwrap_or_else(|| AbstractTraceWriter::ensure_path_id(self, &path));
            if path_id == INVALID_PATH_ID {
                self.fatal_refusal.get_or_insert(format!(
                    "function {name} is declared at {}, which has no recorded line count under the line-count table",
                    path.display()
                ));
                continue;
            }
            self.base.function_list.push((name.clone(), path_id, line));
            AbstractTraceWriter::add_event(
                self,
                TraceLowLevelEvent::Function(codetracer_trace_types::FunctionRecord { name, path_id, line }),
            );
        }
        if let Some(err) = self.fatal_refusal.take() {
            return Err(err.into());
        }

        // Close every stream: what was still open becomes final, the trailing
        // partial chunks seal, and all of it is published.
        self.drain_streams();
        let mut closing: Result<(), String> = Ok(());
        if let Some(sink) = self.values_sink.as_mut() {
            for record in self.value_stream_builder.take().map(|b| b.finish()).unwrap_or_default() {
                let mut bytes = Vec::new();
                record.encode(&mut bytes);
                closing = closing.and_then(|_| sink.push(&bytes));
            }
            closing = closing.and_then(|_| sink.finish());
        }
        if let Some(sink) = self.calls_sink.as_mut() {
            for record in self.call_stream_builder.take().map(|b| b.finish()).unwrap_or_default() {
                let mut bytes = Vec::new();
                record.encode(&mut bytes);
                closing = closing.and_then(|_| sink.push(&bytes));
            }
            closing = closing.and_then(|_| sink.finish());
        }
        if let Some(sink) = self.events_sink.as_mut() {
            closing = closing.and_then(|_| sink.finish());
        }
        self.latch(closing);
        if let Some(encoder) = self.exec_encoder.as_mut() {
            let r = encoder.seal();
            self.latch(r);
        }
        self.publish_in_order(true);

        // The span stream's last chunk, and the span-type index of the whole
        // recording.
        if self.spans.is_some() {
            let r = self.flush_spans();
            self.latch(r);
            if let (Some(spans), Some(writer)) = (self.spans.as_ref(), self.ctfs_writer.as_mut())
                && self.write_error.is_none()
            {
                let image = spans.builder.span_type_namespace();
                let r = writer
                    .add_file(crate::span_stream::SPAN_TYPE_NAMESPACE_FILE_NAME)
                    .and_then(|h| writer.write(h, &image).map(|_| ()))
                    .map_err(|e| format!("writing spantype.ns: {e}"));
                self.latch(r);
            }
        }

        // Close-time members (`ctfs-container.md` §6, "Durability", rule 4).
        if !self.source_views.is_empty()
            && self.write_error.is_none()
            && let Some(writer) = self.ctfs_writer.as_mut()
        {
            let views = std::mem::take(&mut self.source_views);
            let r = (|| -> Result<(), codetracer_ctfs::CtfsError> {
                let dat = writer.add_file("srcviews.dat")?;
                let off = writer.add_file("srcviews.off")?;
                let mut offsets = 0u64.to_le_bytes().to_vec();
                let mut end = 0u64;
                for v in &views {
                    end += v.len() as u64;
                    offsets.extend_from_slice(&end.to_le_bytes());
                }
                writer.write(dat, &views.concat())?;
                writer.write(off, &offsets)?;
                Ok(())
            })();
            self.latch(r.map_err(|e| format!("writing srcviews.dat: {e}")));
        }
        if let (Some(map), Some(writer)) = (self.step_map_builder.take(), self.ctfs_writer.as_mut())
            && self.write_error.is_none()
        {
            let r = map.serialize().and_then(|bytes| {
                let h = writer.add_file(crate::step_map::STEP_MAP_FILE_NAME).map_err(|e| e.to_string())?;
                writer.write(h, &bytes).map_err(|e| e.to_string())?;
                Ok(())
            });
            self.latch(r);
        }

        // `linehits.tc`, when kept, then `corrmark.ns`, when any marker was
        // declared: an absent index says "not indexed", an empty one "indexed,
        // covers nothing", so none is written for a recording with no marker.
        let line_hits = self.exec_encoder.as_ref().and_then(|e| e.line_hits()).map(|h| h.serialize());
        let corrmark = (!self.correlation_markers.is_empty()).then(|| crate::corrmark::serialize_corrmark(&self.correlation_markers));
        for (name, image) in [
            (crate::linehits::LINEHITS_FILE_NAME, line_hits),
            (crate::corrmark::CORRMARK_FILE_NAME, corrmark),
        ] {
            let (Some(image), Some(writer)) = (image, self.ctfs_writer.as_mut()) else {
                continue;
            };
            if self.write_error.is_some() {
                break;
            }
            let r = image.and_then(|bytes| {
                let h = writer.add_file(name).map_err(|e| e.to_string())?;
                writer.write(h, &bytes).map_err(|e| e.to_string())?;
                Ok(())
            });
            self.latch(r.map_err(|e| format!("writing {name}: {e}")));
        }

        if let Some(err) = self.write_error.take() {
            self.ctfs_writer = None;
            return Err(format!("the trace could not be written: {err}").into());
        }

        // Close the CTFS container (takes ownership)
        if let Some(writer) = self.ctfs_writer.take() {
            match self.output {
                CtfsOutput::File => writer.close()?,
                CtfsOutput::Memory => self.container_bytes = Some(writer.finish_to_bytes()?),
            }
            if self.compact_threshold > 0 {
                self.choose_profile()?;
            }
        }

        // The container is finalized either way, so what was recorded can be
        // inspected; but a recording that refused an operation is missing
        // what that operation carried, and finishing it as a success would
        // hand the recorder an incomplete trace it believes is complete. The
        // Nim writer's `trace_writer_close` fails for the same reason.
        if let Some(first) = self.refusals.first() {
            return Err(format!(
                "the trace is incomplete: {} operation(s) were refused; the first: {first}",
                self.refusals.len()
            )
            .into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use codetracer_trace_types::*;

    /// Create a simple step event for testing.
    fn make_step_event(line: i64) -> TraceLowLevelEvent {
        TraceLowLevelEvent::Step(StepRecord {
            path_id: PathId(0),
            line: Line(line),
        })
    }

    #[test]
    fn steps_round_trip_through_the_split_streams() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trace");

        let mut writer = CtfsTraceWriter::new("test", &[]).with_steps_chunk_size(50);
        writer.begin_writing_trace_events(&path).unwrap();

        AbstractTraceWriter::add_event(&mut writer, TraceLowLevelEvent::Path(std::path::PathBuf::from("/test/file.rs")));

        let num_events = 200;
        for i in 0..num_events {
            AbstractTraceWriter::add_event(&mut writer, make_step_event(i + 1));
        }

        writer.finish_writing_trace_events().unwrap();

        // Read back and verify.
        let ct_path = path.with_extension("ct");
        let mut reader = codetracer_trace_reader::create_trace_reader(codetracer_trace_reader::TraceEventsFileFormat::Ctfs);
        let events = reader.load_trace_events(&ct_path).unwrap();

        let step_events: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                TraceLowLevelEvent::Step(s) => Some(s),
                _ => None,
            })
            .collect();

        assert_eq!(
            step_events.len(),
            num_events as usize,
            "Expected {} step events, got {}",
            num_events,
            step_events.len()
        );

        for (i, step) in step_events.iter().enumerate() {
            assert_eq!(step.line, Line(i as i64 + 1));
        }
    }
}
