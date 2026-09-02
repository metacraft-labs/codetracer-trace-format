//! Cross-**writer** differential: the Nim writer and the pure-Rust
//! `CtfsTraceWriter` fed the same fixture must produce the same stream bytes.
//!
//! # Why byte comparison, and why only some files
//!
//! "Both readers accept both outputs" and "the decoded fields look right" are
//! the two claims that have historically hidden encoder divergence, because a
//! reader that is lenient in the same place the writer is wrong reports
//! success. So the assertion here is byte equality of the produced streams.
//!
//! Whole-**container** byte equality is structurally unreachable, and the
//! reasons are enumerated rather than waved at, because each one is a place
//! where a narrower comparison could hide a real difference:
//!
//! 1. **The Rust writer emits four files the Nim writer never does** —
//!    `events.log`, `events.fmt`, `meta.json`, `paths.json`. They are the
//!    legacy unified-stream surface kept for old readers. Different file
//!    *sets* mean different CTFS directory blocks.
//! 2. **`recording_id` is a freshly minted UUIDv7 on both sides**, so
//!    `meta.dat` and `meta.json` differ in bytes on every run by construction.
//!    `meta.dat` is therefore compared on its *flags word*, not its body.
//! 3. **The Nim writer emits files of its own** the Rust writer does not
//!    (`step-map.ns`, the span/linehit tables, …).
//! 4. **CTFS block allocation** follows the file set, so even a shared file
//!    lands at a different container offset.
//!
//! None of those touch the contents of a stream. What IS compared, in full:
//!
//! * `steps.dat` — every encoded step record, including chunk framing and the
//!   Zstd frames themselves.
//! * `steps.idx` — the companion offset index.
//! * `paths.dat` / `paths.off` — the interning table, in spec Layout A.
//! * `values.dat` / `values.idx` — the parallel-indexed value stream.
//! * the `meta.dat` flags word, masked to the column bits.
//!
//! # The exclusion list is a measurement, not an omission
//!
//! An exclusion list is where a differential goes to die, so
//! [`every_file_in_the_container_is_either_compared_or_a_named_divergence`]
//! drives a fixture that populates *every* stream, enumerates the union of both
//! containers' file sets, and requires each entry to be one of:
//!
//! * byte-identical, or
//! * present in one writer only for a reason stated in `RUST_ONLY` / `NIM_ONLY`,
//!   or
//! * a divergence declared BY NAME in `KNOWN_DIVERGENCES`, with its measured
//!   cause.
//!
//! A file that is none of those fails the test. So a stream added to one writer
//! and not the other, or a divergence that appears later, cannot hide in the
//! gap between "compared" and "not mentioned" — which is where `values.dat`
//! spent this work's first draft. That draft excluded it on the stated grounds
//! that "the two writers' value-record encodings differ independently of
//! columns"; the encodings are byte-identical and the whole divergence was the
//! Zstd frame header, i.e. the unfixed half of the finding the test three
//! functions below exists to pin.
//!
//! # The negative controls
//!
//! A differential that cannot fail is worth nothing, so three of the tests
//! below deliberately break something and assert the comparison goes red:
//! a wrong column delta, a wrong `line_lengths` table, and the streaming-Zstd
//! framing the writer used to emit. Each names which comparison caught it.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use codetracer_ctfs::CtfsReader;
use codetracer_trace_types::Line;
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::trace_writer::TraceWriter;
use codetracer_trace_writer_nim::{NimTraceReaderHandle, NimTraceWriter, TraceEventsFileFormat};

/// The Nim runtime is not thread-safe — its global state lives behind a single
/// lock — so every test in this binary is serialised, as in the other
/// Nim-backed suites in this crate.
static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

/// Take the Nim serialisation lock, ignoring poisoning.
///
/// `lock().unwrap()` would be wrong here and it mattered: when one test in this
/// binary panics — which is the *expected* outcome under mutation testing —
/// the mutex is poisoned and every later test dies on the unwrap rather than on
/// its own assertion. The mutation matrix then reports "all nine tests went
/// red" for a defect only one of them can see, and "the check failed" is
/// mistaken for "the check saw what I broke". The lock exists to serialise the
/// Nim runtime, not to propagate failure; a poisoned `()` guards nothing.
fn nim_lock() -> std::sync::MutexGuard<'static, ()> {
    NIM_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

// ---------------------------------------------------------------------------
// The fixture
// ---------------------------------------------------------------------------

/// One operation of the shared fixture.
///
/// The fixture is a sequence of *operations*, not only of positions, because
/// three of the encoder behaviours under test are not reachable from a plain
/// list of steps: a standalone `DeltaColumn` record (tag 0x07) only happens
/// when no line step is pending, and reaching that state through the Nim FFI
/// requires an intervening event that flushes it.
#[derive(Debug, Clone, Copy)]
enum Op {
    /// A step at a 1-based `(line, column)` in `file`.
    Step { file: usize, line: u32, column: u32 },
    /// A thread switch. Occupies a step slot in both writers and flushes the
    /// Nim FFI's pending step, which is what makes the next `Column` op
    /// standalone.
    ThreadSwitch { thread_id: u64 },
    /// A column-only move from the current cursor. Emits tag 0x07 when it
    /// follows a flush, and folds into the pending step otherwise — the
    /// fixture only uses it in the former position.
    Column { delta: i64 },
}

/// The resolved position of a `Step` op, for the round-trip assertions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Pos {
    file: u64,
    line: u32,
    column: u32,
}

/// Three files with deliberately uneven line tables:
///
/// * file 0 — one line of 8 columns: the "everything on line 1" minified case,
///   and the origin of the whole address space.
/// * file 1 — three lines of 64 / 70 / 3: the long lines exist so the fixture
///   can produce position deltas of **exactly ±63, ±64 and ±65** and pin Nim's
///   delta window. With the original 5/12/3 table every delta in the trace was
///   under 7, and widening the window from 63 to 64 changed no byte — the
///   mutation matrix caught that and this is the repair.
/// * file 2 — one line of 4 columns: a file boundary late in the trace.
///
/// Total addressable positions: 8 + 137 + 4 = 149.
fn fixture_line_lengths() -> Vec<Vec<u32>> {
    vec![vec![8], vec![64, 70, 3], vec![4]]
}

fn fixture_paths() -> Vec<PathBuf> {
    vec![
        PathBuf::from("/fixture/minified.js"),
        PathBuf::from("/fixture/three_lines.rs"),
        PathBuf::from("/fixture/tail.py"),
    ]
}

/// The operation sequence.
///
/// File bases are 0 / 8 / 145, so the positions below are, in order:
/// 0, 3, 7, 8, 71, 8, 72, 8, 73, 8, then a thread switch, then a column move
/// to 11, then 78, 145, 148. The deltas that matter:
///
/// | from → to | delta | expected encoding |
/// |---|---|---|
/// | 8 → 71 | +63 | `DeltaStep` — the last value inside Nim's window |
/// | 71 → 8 | -63 | `DeltaStep` |
/// | 8 → 72 | +64 | **`AbsoluteStep`** — one past the window |
/// | 72 → 8 | -64 | `DeltaStep` — the last value inside on the negative side |
/// | 8 → 73 | +65 | `AbsoluteStep` |
/// | 73 → 8 | -65 | `AbsoluteStep` — one past on the negative side |
///
/// Those six rows are what make a `NIM_DELTA_MAX` or `NIM_DELTA_MIN` off by one
/// visible in the bytes.
fn fixture_ops() -> Vec<Op> {
    vec![
        // A thread switch BEFORE the first step. Both writers count it as a
        // step slot, and `step_count` is what decides "the first step is
        // absolute" — so the step after it encodes as a `DeltaStep` from
        // position 0, not as an `AbsoluteStep`. Without this op in the fixture,
        // making the counter stop counting changed no byte.
        Op::ThreadSwitch { thread_id: 1 },
        Op::Step { file: 0, line: 1, column: 1 }, // 0   — first step, absolute
        Op::Step { file: 0, line: 1, column: 4 }, // 3   — +3
        Op::Step { file: 0, line: 1, column: 8 }, // 7   — +4
        Op::Step { file: 1, line: 1, column: 1 }, // 8   — +1, crosses a file
        Op::Step {
            file: 1,
            line: 1,
            column: 64,
        }, // 71  — +63, window edge
        Op::Step { file: 1, line: 1, column: 1 }, // 8   — -63
        Op::Step { file: 1, line: 2, column: 1 }, // 72  — +64, one past
        Op::Step { file: 1, line: 1, column: 1 }, // 8   — -64, window edge
        Op::Step { file: 1, line: 2, column: 2 }, // 73  — +65
        Op::Step { file: 1, line: 1, column: 1 }, // 8   — -65, one past
        Op::ThreadSwitch { thread_id: 7 },        // flushes the pending step
        Op::Column { delta: 3 },                  // 11  — standalone tag 0x07
        Op::Step { file: 1, line: 2, column: 7 }, // 78
        Op::Step { file: 2, line: 1, column: 1 }, // 145 — crosses a file
        Op::Step { file: 2, line: 1, column: 4 }, // 148
    ]
}

/// The `(file, line, column)` each fixture *record* resolves to, in record
/// order — steps, thread switches and column moves alike. Thread switches carry
/// no position, so they appear as `None`.
///
/// Computed from the fixture rather than copied out of a writer, so a writer
/// that agrees with itself but not with the fixture still fails.
fn expected_positions(line_lengths: &[Vec<u32>], ops: &[Op]) -> Vec<Option<Pos>> {
    let mut bases = Vec::with_capacity(line_lengths.len());
    let mut running = 0u64;
    for lls in line_lengths {
        bases.push(running);
        running += lls.iter().map(|l| u64::from(*l)).sum::<u64>().max(1);
    }

    let mut out = Vec::with_capacity(ops.len());
    let mut cursor = 0i64;
    for op in ops {
        match op {
            Op::Step { file, line, column } => {
                let lls = &line_lengths[*file];
                let up_to = ((*line as usize).saturating_sub(1)).min(lls.len());
                let line_offset: u64 = lls[..up_to].iter().map(|l| u64::from(*l)).sum();
                cursor = (bases[*file] + line_offset) as i64 + i64::from(*column - 1);
                out.push(Some(Pos {
                    file: *file as u64,
                    line: *line,
                    column: *column,
                }));
            }
            Op::ThreadSwitch { .. } => out.push(None),
            Op::Column { delta } => {
                cursor += delta;
                // Resolve the moved cursor back to (file, line, column).
                out.push(Some(resolve(line_lengths, &bases, cursor as u64)));
            }
        }
    }
    out
}

/// The `global_position_index` of a resolved `(file, line, column)`.
/// Independent of the writer's arithmetic, so the fixture's own claims about
/// which deltas it produces can be checked.
fn position_of(line_lengths: &[Vec<u32>], p: &Pos) -> u64 {
    let mut base = 0u64;
    for lls in line_lengths.iter().take(p.file as usize) {
        base += lls.iter().map(|l| u64::from(*l)).sum::<u64>().max(1);
    }
    let lls = &line_lengths[p.file as usize];
    let up_to = ((p.line as usize).saturating_sub(1)).min(lls.len());
    base + lls[..up_to].iter().map(|l| u64::from(*l)).sum::<u64>() + u64::from(p.column - 1)
}

/// Inverse of the position layout — the same walk the reader's decoder does,
/// written independently here so the round-trip is not compared against the
/// encoder's own arithmetic.
fn resolve(line_lengths: &[Vec<u32>], bases: &[u64], position: u64) -> Pos {
    for (fid, lls) in line_lengths.iter().enumerate() {
        let size: u64 = lls.iter().map(|l| u64::from(*l)).sum();
        if position >= bases[fid] && position < bases[fid] + size {
            let mut q = position - bases[fid];
            for (li, len) in lls.iter().enumerate() {
                if q < u64::from(*len) {
                    return Pos {
                        file: fid as u64,
                        line: li as u32 + 1,
                        column: q as u32 + 1,
                    };
                }
                q -= u64::from(*len);
            }
        }
    }
    panic!("position {position} is outside the fixture's address space");
}

// ---------------------------------------------------------------------------
// Driving the two writers
// ---------------------------------------------------------------------------

/// Write the fixture through the Nim writer and return the `.ct` path.
///
/// `line_lengths` and `steps` are parameters rather than constants so the
/// negative controls can corrupt one side and re-run the same comparison.
fn write_with_nim(dir: &Path, program: &str, line_lengths: &[Vec<u32>], ops: &[Op]) -> PathBuf {
    let mut writer = NimTraceWriter::new(program, &[], TraceEventsFileFormat::Ctfs);
    writer.begin_writing_trace_events(&dir.join("trace.json")).expect("nim begin_events");
    writer
        .begin_writing_trace_metadata(&dir.join("trace_metadata.json"))
        .expect("nim begin_metadata");
    writer.begin_writing_trace_paths(&dir.join("trace_paths.json")).expect("nim begin_paths");

    // Trace-global, and before any event: the flag decides paths.dat's record
    // shape and steps.dat's addressing.
    writer.enable_column_aware_steps();

    let paths = fixture_paths();
    for (i, p) in paths.iter().enumerate() {
        writer.register_path_with_line_lengths(p, &line_lengths[i]).expect("nim register path");
    }

    for op in ops {
        match op {
            Op::Step { file, line, column } => {
                writer.register_step_with_column(&paths[*file], Line(i64::from(*line)), Some(Line(i64::from(*column))));
            }
            Op::ThreadSwitch { thread_id } => writer.register_thread_switch(*thread_id),
            // With no pending step (the thread switch above flushed it) the
            // FFI emits a stand-alone `registerColumnStep` — tag 0x07.
            Op::Column { delta } => writer.write_delta_column(*delta),
        }
    }

    writer.finish_writing_trace_events().expect("nim finish_events");
    writer.finish_writing_trace_metadata().expect("nim finish_metadata");
    writer.finish_writing_trace_paths().expect("nim finish_paths");
    writer.close().expect("nim close");
    drop(writer);

    dir.join(format!("{program}.ct"))
}

/// Write the same fixture through the pure-Rust `CtfsTraceWriter`.
fn write_with_rust(dir: &Path, program: &str, line_lengths: &[Vec<u32>], ops: &[Op]) -> PathBuf {
    let mut writer = CtfsTraceWriter::new(program, &[]);
    // Same ordering contract as the Nim side: the mode first, then paths, then
    // steps.
    writer.enable_column_aware_steps();
    let out = dir.join(program);
    TraceWriter::begin_writing_trace_events(&mut writer, &out).expect("rust begin_events");

    let paths = fixture_paths();
    for (i, p) in paths.iter().enumerate() {
        writer.register_path_with_line_lengths(p, &line_lengths[i]);
    }

    for op in ops {
        match op {
            Op::Step { file, line, column } => {
                AbstractTraceWriter::register_step_with_column(&mut writer, &paths[*file], Line(i64::from(*line)), Some(Line(i64::from(*column))));
            }
            Op::ThreadSwitch { thread_id } => {
                AbstractTraceWriter::add_event(
                    &mut writer,
                    codetracer_trace_types::TraceLowLevelEvent::ThreadSwitch(codetracer_trace_types::ThreadId(*thread_id)),
                );
            }
            Op::Column { delta } => writer.register_column_step(*delta).expect("rust column step"),
        }
    }

    TraceWriter::finish_writing_trace_events(&mut writer).expect("rust finish_events");
    assert!(
        !writer.dropped_column_awareness(),
        "the Rust writer dropped the column-aware request; the comparison below would be vacuous"
    );
    out.with_extension("ct")
}

// ---------------------------------------------------------------------------
// Extracting and comparing
// ---------------------------------------------------------------------------

/// Pull one internal file out of a `.ct` container.
fn read_internal(ct: &Path, name: &str) -> Vec<u8> {
    let mut reader = CtfsReader::open(ct).unwrap_or_else(|e| panic!("open {} : {e:?}", ct.display()));
    reader
        .read_file(name)
        .unwrap_or_else(|e| panic!("read {name} from {} : {e:?}", ct.display()))
}

fn meta_flags(ct: &Path) -> u16 {
    let meta = read_internal(ct, "meta.dat");
    codetracer_trace_writer::meta_dat::read_meta_dat_flags(&meta).expect("meta.dat header parses")
}

/// The files whose bytes must match exactly on the column-aware fixture. Named
/// here so a test that adds a stream to one writer and not the other fails on
/// the list rather than silently comparing a shrinking set.
///
/// Every entry is non-empty in BOTH containers on both fixtures below — the
/// streams that are empty unless a fixture populates them (`calls.dat`,
/// `events.dat`, `funcs.dat`, `types.dat`) are compared by
/// [`every_file_in_the_container_is_either_compared_or_a_named_divergence`]
/// instead, on a fixture that populates them, because comparing two empty files
/// is the degenerate pass this campaign has been burned by.
const COMPARED_FILES: [&str; 6] = ["steps.dat", "steps.idx", "paths.dat", "paths.off", "values.dat", "values.idx"];

/// Compare the two containers' streams. Returns the list of files that
/// differed, so callers can assert both "empty" (parity) and "contains X"
/// (a negative control caught the right thing).
fn diff_streams(nim_ct: &Path, rust_ct: &Path) -> Vec<String> {
    let mut differing = Vec::new();
    for name in COMPARED_FILES {
        let a = read_internal(nim_ct, name);
        let b = read_internal(rust_ct, name);
        // A comparison whose two sides could both be empty proves nothing.
        assert!(!a.is_empty(), "the Nim container's {name} is empty — nothing to compare");
        assert!(!b.is_empty(), "the Rust container's {name} is empty — nothing to compare");
        if a != b {
            differing.push(format!("{name} (nim {} bytes, rust {} bytes)", a.len(), b.len()));
        }
    }
    differing
}

// ---------------------------------------------------------------------------
// The differential
// ---------------------------------------------------------------------------

#[test]
fn the_two_writers_produce_identical_column_aware_streams() {
    let _guard = nim_lock();
    let dir = tempfile::tempdir().expect("tempdir");
    let lls = fixture_line_lengths();
    let ops = fixture_ops();

    let nim_ct = write_with_nim(dir.path(), "differential_nim", &lls, &ops);
    let rust_ct = write_with_rust(dir.path(), "differential_rust", &lls, &ops);

    // Non-degeneracy first: a fixture that produced no steps would make every
    // byte comparison below trivially true. And the fixture must actually
    // contain the shapes the comparison is supposed to be sensitive to — the
    // first version of this test had neither a standalone column step nor a
    // delta anywhere near the window edge, and two mutations of the encoder
    // passed it green.
    assert!(ops.len() >= 12, "the fixture must exercise more than a couple of operations");
    assert!(
        ops.iter().any(|o| matches!(o, Op::Column { .. })),
        "the fixture must contain a stand-alone column step, or tag 0x07 is never encoded"
    );
    assert!(
        ops.iter().any(|o| matches!(o, Op::ThreadSwitch { .. })),
        "the fixture must contain a thread switch, or the non-step record path is never encoded"
    );
    // The delta window is only pinned if some consecutive pair is exactly at
    // its edge. Derived from the fixture rather than asserted as a comment.
    let positions: Vec<i64> = expected_positions(&lls, &ops)
        .iter()
        .filter_map(|p| p.as_ref())
        .map(|p| position_of(&lls, p) as i64)
        .collect();
    let deltas: Vec<i64> = positions.windows(2).map(|w| w[1] - w[0]).collect();
    for edge in [63i64, 64, -64, -65] {
        assert!(
            deltas.contains(&edge),
            "the fixture must produce a position delta of {edge} so the delta window is pinned; deltas = {deltas:?}"
        );
    }

    let differing = diff_streams(&nim_ct, &rust_ct);
    assert!(
        differing.is_empty(),
        "the two writers disagree on: {differing:?}\n  nim:  {}\n  rust: {}",
        nim_ct.display(),
        rust_ct.display()
    );

    // The column bits must agree too. Compared as a masked word rather than as
    // whole `meta.dat` bytes, because `recording_id` differs by construction.
    let column_mask = codetracer_trace_writer::meta_dat::FLAG_HAS_COLUMN_AWARE_STEPS
        | codetracer_trace_writer::meta_dat::FLAG_SUPPORTS_COLUMN_BREAKPOINTS
        | codetracer_trace_writer::meta_dat::FLAG_SUPPORTS_COLUMN_MOTIONS;
    let nim_flags = meta_flags(&nim_ct);
    let rust_flags = meta_flags(&rust_ct);
    assert_eq!(
        nim_flags & column_mask,
        rust_flags & column_mask,
        "column flag words differ: nim {nim_flags:#06x} rust {rust_flags:#06x}"
    );
    assert_ne!(
        nim_flags & column_mask,
        0,
        "control: the mask must select a bit that is actually set, or the equality above is 0 == 0"
    );
}

/// Both writers chunk the execution stream at 4096 records, so a trace has to
/// exceed that to exercise chunk framing at all.
const CHUNK_CROSSING_STEPS: usize = 5000;

#[test]
fn the_two_writers_agree_across_a_chunk_boundary() {
    // The single-chunk fixture above cannot see chunk framing: with 16 records
    // and a 4096-record chunk there is exactly one chunk, and deleting the
    // chunk-boundary promotion entirely left every assertion green. This arm
    // crosses the boundary, so the promoted leading `AbsoluteStep` of chunk 1,
    // the second `steps.idx` offset, and the running cursor carried across the
    // boundary are all in the compared bytes.
    let _guard = nim_lock();
    let dir = tempfile::tempdir().expect("tempdir");
    let lls = fixture_line_lengths();

    // A walk inside file 1 that never leaves the delta window, so the encoder
    // would emit a `DeltaStep` at the chunk boundary if nothing promoted it.
    let mut ops = Vec::with_capacity(CHUNK_CROSSING_STEPS);
    for i in 0..CHUNK_CROSSING_STEPS {
        ops.push(Op::Step {
            file: 1,
            line: 1,
            column: (i % 60) as u32 + 1,
        });
    }

    let nim_ct = write_with_nim(dir.path(), "chunked_nim", &lls, &ops);
    let rust_ct = write_with_rust(dir.path(), "chunked_rust", &lls, &ops);

    // Non-degeneracy: the index must actually list more than one chunk, or the
    // whole point of this arm is missing.
    let idx = read_internal(&nim_ct, "steps.idx");
    let chunk_count = (idx.len() - 4) / 8;
    assert!(chunk_count >= 2, "the fixture must cross a chunk boundary; got {chunk_count} chunk(s)");

    let differing = diff_streams(&nim_ct, &rust_ct);
    assert!(differing.is_empty(), "the two writers disagree across a chunk boundary on: {differing:?}");

    // And the promoted record is really there: chunk 1 must open with an
    // AbsoluteStep (tag 0x00), not a DeltaStep.
    let dat = read_internal(&nim_ct, "steps.dat");
    let off1 = u64::from_le_bytes(idx[12..20].try_into().expect("second offset")) as usize;
    let chunk1 = zstd::decode_all(&dat[off1..]).expect("chunk 1 decompresses");
    assert_eq!(chunk1[0], 0x00, "chunk 1 must open with an AbsoluteStep so it decodes standalone");
}

// ---------------------------------------------------------------------------
// The exclusion list, as a measurement
// ---------------------------------------------------------------------------

/// Files only the Rust writer emits, with the reason. These are the legacy
/// unified-stream surface it keeps for old readers plus its JSON sidecars; the
/// Nim writer routes the same information through the split streams only.
const RUST_ONLY: [&str; 4] = ["events.log", "events.fmt", "meta.json", "paths.json"];

/// Files only the Nim writer emits. Empty today; the constant exists so a Nim
/// stream that appears later is a failure with a name rather than a silent
/// widening of the "not compared" set.
const NIM_ONLY: [&str; 0] = [];

/// Streams that still differ, each with its MEASURED cause. This is the honest
/// statement of what did not reach parity, and it is enforced in both
/// directions: a file here that stops differing fails the test (so a fix cannot
/// leave a stale exclusion behind), and a file that differs without being here
/// fails it too.
const KNOWN_DIVERGENCES: [(&str, &str); 5] = [
    (
        "meta.dat",
        "recording_id is a freshly minted UUIDv7 on both sides, and the Rust header carries \
         program/args fields the Nim one does not. Compared on its masked flags word instead.",
    ),
    (
        "funcs.dat",
        "the function interning table's record shape: Nim writes the bare name bytes, Rust writes \
         a length/id-prefixed record. The same class M24 flagged for paths.dat, unfixed for funcs.",
    ),
    ("funcs.off", "follows funcs.dat's record lengths."),
    (
        "types.dat",
        "the Rust writer does not auto-register a type name for an Int value; the Nim writer \
         registers `type_0`.",
    ),
    ("types.off", "follows types.dat."),
];

/// Drive a fixture that populates EVERY stream through both writers.
fn write_populated(dir: &Path, program: &str, nim: bool) -> PathBuf {
    let lls = fixture_line_lengths();
    let ps = fixture_paths();
    let value = |i: u32| codetracer_trace_types::ValueRecord::Int {
        i: i64::from(i),
        type_id: codetracer_trace_types::TypeId(0),
    };

    if nim {
        let mut w = NimTraceWriter::new(program, &[], TraceEventsFileFormat::Ctfs);
        w.begin_writing_trace_events(&dir.join("p_events.json")).expect("nim begin_events");
        w.begin_writing_trace_metadata(&dir.join("p_meta.json")).expect("nim begin_metadata");
        w.begin_writing_trace_paths(&dir.join("p_paths.json")).expect("nim begin_paths");
        w.enable_column_aware_steps();
        for (i, p) in ps.iter().enumerate() {
            w.register_path_with_line_lengths(p, &lls[i]).expect("nim register path");
        }
        w.register_function("f", &ps[1], Line(1));
        for i in 0..12u32 {
            w.register_step_with_column(&ps[1], Line(1), Some(Line(i64::from(i % 60 + 1))));
            w.register_variable_with_full_value("v", value(i));
            if i == 2 {
                w.register_call(codetracer_trace_types::FunctionId(0), vec![]);
            }
            if i == 4 {
                w.register_special_event(codetracer_trace_types::EventLogKind::Write, "", "hello");
            }
        }
        w.finish_writing_trace_events().expect("nim finish_events");
        w.finish_writing_trace_metadata().expect("nim finish_metadata");
        w.finish_writing_trace_paths().expect("nim finish_paths");
        w.close().expect("nim close");
        drop(w);
        dir.join(format!("{program}.ct"))
    } else {
        let mut w = CtfsTraceWriter::new(program, &[]);
        w.enable_column_aware_steps();
        let out = dir.join(program);
        TraceWriter::begin_writing_trace_events(&mut w, &out).expect("rust begin_events");
        for (i, p) in ps.iter().enumerate() {
            w.register_path_with_line_lengths(p, &lls[i]);
        }
        AbstractTraceWriter::register_function(&mut w, "f", &ps[1], Line(1));
        for i in 0..12u32 {
            AbstractTraceWriter::register_step_with_column(&mut w, &ps[1], Line(1), Some(Line(i64::from(i % 60 + 1))));
            AbstractTraceWriter::register_variable_with_full_value(&mut w, "v", value(i));
            if i == 2 {
                AbstractTraceWriter::register_call(&mut w, codetracer_trace_types::FunctionId(0), vec![]);
            }
            if i == 4 {
                AbstractTraceWriter::register_special_event(&mut w, codetracer_trace_types::EventLogKind::Write, "", "hello");
            }
        }
        TraceWriter::finish_writing_trace_events(&mut w).expect("rust finish_events");
        out.with_extension("ct")
    }
}

#[test]
fn every_file_in_the_container_is_either_compared_or_a_named_divergence() {
    // The exclusion list is where a differential goes to die. This test refuses
    // to let a file sit in the gap between "compared" and "not mentioned":
    // every entry of the union of the two file sets must be identical,
    // one-sided for a declared reason, or a declared divergence.
    let _guard = nim_lock();
    let dir = tempfile::tempdir().expect("tempdir");
    let nim_ct = write_populated(dir.path(), "census_nim", true);
    let rust_ct = write_populated(dir.path(), "census_rust", false);

    let mut nim_reader = CtfsReader::open(&nim_ct).expect("open nim");
    let mut rust_reader = CtfsReader::open(&rust_ct).expect("open rust");
    let mut names: Vec<String> = nim_reader.list_files();
    for n in rust_reader.list_files() {
        if !names.contains(&n) {
            names.push(n);
        }
    }
    names.sort();
    assert!(names.len() >= 15, "control: the census must see a whole container, got {names:?}");

    let mut identical: Vec<String> = Vec::new();
    let mut differing: Vec<String> = Vec::new();
    let mut unexplained: Vec<String> = Vec::new();

    for name in &names {
        let a = nim_reader.read_file(name).ok();
        let b = rust_reader.read_file(name).ok();
        match (a, b) {
            (Some(a), Some(b)) if a == b => identical.push(name.clone()),
            (Some(a), Some(b)) => {
                differing.push(name.clone());
                if !KNOWN_DIVERGENCES.iter().any(|(n, _)| n == name) {
                    unexplained.push(format!(
                        "{name} differs (nim {} B, rust {} B) and is not in KNOWN_DIVERGENCES",
                        a.len(),
                        b.len()
                    ));
                }
            }
            (None, Some(_)) => {
                if !RUST_ONLY.contains(&name.as_str()) {
                    unexplained.push(format!("{name} is Rust-only and is not in RUST_ONLY"));
                }
            }
            (Some(_), None) => {
                if !NIM_ONLY.contains(&name.as_str()) {
                    unexplained.push(format!("{name} is Nim-only and is not in NIM_ONLY"));
                }
            }
            (None, None) => unreachable!("a name came from one of the two listings"),
        }
    }

    assert!(
        unexplained.is_empty(),
        "every file must be identical, one-sided for a declared reason, or a declared divergence:\n  {}",
        unexplained.join("\n  ")
    );

    // The other direction: a declared divergence that has stopped diverging is
    // a stale exclusion, and stale exclusions are how a fixed defect keeps
    // being described as unfixable.
    for (name, why) in KNOWN_DIVERGENCES {
        assert!(
            differing.iter().any(|d| d == name),
            "{name} no longer differs — remove it from KNOWN_DIVERGENCES and let the census compare it. \
             Its declared reason was: {why}"
        );
    }

    // Non-degeneracy: the census is only worth something if it is actually
    // comparing populated streams. Every stream a recorder writes must be
    // non-empty in BOTH containers here, or a fixture change has quietly turned
    // one of these comparisons into 0 == 0.
    for stream in ["steps.dat", "values.dat", "calls.dat", "events.dat", "paths.dat"] {
        for (label, r) in [("nim", &mut nim_reader), ("rust", &mut rust_reader)] {
            let bytes = r.read_file(stream).unwrap_or_default();
            assert!(
                !bytes.is_empty(),
                "{label}: {stream} is empty — the census fixture no longer populates it"
            );
        }
    }

    // And the streams the column work is about really are in the identical set,
    // stated positively rather than inferred from the absence of a failure.
    for stream in [
        "steps.dat",
        "steps.idx",
        "paths.dat",
        "paths.off",
        "values.dat",
        "values.idx",
        "events.dat",
        // The call stream. Its `first_step_id` was off by one against the Nim
        // writer's documented "CTFS-M entry_step" convention; asserted
        // positively here so a regression is reported as "calls.dat stopped
        // matching" rather than as a new line in KNOWN_DIVERGENCES.
        "calls.dat",
        "calls.idx",
        "varnames.dat",
        "varnames.off",
    ] {
        assert!(
            identical.contains(&stream.to_string()),
            "{stream} must be byte-identical; identical set = {identical:?}"
        );
    }
}

#[test]
fn a_wrong_column_delta_fails_the_differential() {
    // The single most important control. If the comparison cannot see a
    // one-column error it is not measuring the column encoding at all.
    let _guard = nim_lock();
    let dir = tempfile::tempdir().expect("tempdir");
    let lls = fixture_line_lengths();
    let ops = fixture_ops();

    let nim_ct = write_with_nim(dir.path(), "control_col_nim", &lls, &ops);

    // Shift exactly one step by one column.
    let mut mutated = ops.clone();
    match &mut mutated[1] {
        Op::Step { column, .. } => *column += 1,
        other => panic!("fixture op 1 is expected to be a Step, got {other:?}"),
    }
    let rust_ct = write_with_rust(dir.path(), "control_col_rust", &lls, &mutated);

    let differing = diff_streams(&nim_ct, &rust_ct);
    assert!(
        differing.iter().any(|d| d.starts_with("steps.dat")),
        "a one-column shift must move steps.dat; differing = {differing:?}"
    );
    // And it must NOT move paths.dat — a control that reddens everything is
    // as uninformative as one that reddens nothing.
    assert!(
        !differing.iter().any(|d| d.starts_with("paths.dat")),
        "a column shift must not touch paths.dat; differing = {differing:?}"
    );
}

#[test]
fn a_wrong_line_lengths_table_fails_the_differential() {
    // The per-line table feeds two things at once: the Layout A record in
    // paths.dat, and the prefix sums every position in steps.dat is built
    // from. Corrupting it must move both, and a control that moved only one
    // would mean the writer had stopped deriving positions from the table.
    let _guard = nim_lock();
    let dir = tempfile::tempdir().expect("tempdir");
    let lls = fixture_line_lengths();
    let ops = fixture_ops();

    let nim_ct = write_with_nim(dir.path(), "control_lls_nim", &lls, &ops);

    let mut mutated = lls.clone();
    mutated[1][0] += 1; // file 1's first line gains one addressable column
    let rust_ct = write_with_rust(dir.path(), "control_lls_rust", &mutated, &ops);

    let differing = diff_streams(&nim_ct, &rust_ct);
    assert!(
        differing.iter().any(|d| d.starts_with("paths.dat")),
        "a line-length change must move the Layout A table; differing = {differing:?}"
    );
    assert!(
        differing.iter().any(|d| d.starts_with("steps.dat")),
        "a line-length change must move every position after it; differing = {differing:?}"
    );
}

#[test]
fn the_streaming_zstd_framing_fails_the_differential() {
    // The framing control, and the reason it is here: the writer used to
    // compress chunks with `zstd::encode_all` (the streaming API), which omits
    // the frame's pledged content size. That is not a cosmetic difference —
    // the canonical Nim reader calls ZSTD_getFrameContentSize and FAILS on
    // UNKNOWN — so this control pins the one-shot call rather than trusting a
    // comment.
    let _guard = nim_lock();
    let dir = tempfile::tempdir().expect("tempdir");
    let lls = fixture_line_lengths();
    let ops = fixture_ops();

    let nim_ct = write_with_nim(dir.path(), "control_zstd_nim", &lls, &ops);
    let nim_steps = read_internal(&nim_ct, "steps.dat");

    // Reproduce the chunk payload, then compress it both ways.
    let raw = zstd::decode_all(&nim_steps[..]).expect("the Nim chunk decompresses");
    assert!(!raw.is_empty(), "control: the decompressed chunk must not be empty");

    let one_shot = zstd::bulk::compress(&raw, 3).expect("one-shot compress");
    let streaming = zstd::encode_all(std::io::Cursor::new(&raw[..]), 3).expect("streaming compress");

    assert_eq!(one_shot, nim_steps, "the one-shot framing is what the Nim writer emits");
    assert_ne!(streaming, nim_steps, "the streaming framing is NOT what the Nim writer emits");
    assert!(
        zstd::zstd_safe::get_frame_content_size(&nim_steps).expect("valid frame").is_some(),
        "the Nim frame pledges its content size"
    );
    assert_eq!(
        zstd::zstd_safe::get_frame_content_size(&streaming).expect("valid frame"),
        None,
        "the streaming frame does not — which is why the Nim reader refuses it"
    );
}

// ---------------------------------------------------------------------------
// Round-trip through the readers
// ---------------------------------------------------------------------------

#[test]
fn both_containers_round_trip_through_the_nim_reader() {
    // Byte identity says the two writers agree; this says what they agree ON
    // is decodable, and that a Rust-written column-aware container is readable
    // by the reference reader — the property the pledged-content-size framing
    // above exists to preserve.
    let _guard = nim_lock();
    let dir = tempfile::tempdir().expect("tempdir");
    let lls = fixture_line_lengths();
    let ops = fixture_ops();

    let nim_ct = write_with_nim(dir.path(), "roundtrip_nim", &lls, &ops);
    let rust_ct = write_with_rust(dir.path(), "roundtrip_rust", &lls, &ops);
    // Keep the files alive past the tempdir guard for the reader handles.
    #[allow(deprecated)]
    let _kept = dir.into_path();

    // One record per operation: steps, the thread switch and the column move
    // all occupy a slot in the execution stream.
    let expected = expected_positions(&lls, &ops);
    let n = expected.len();

    for (label, ct) in [("nim", &nim_ct), ("rust", &rust_ct)] {
        let reader = NimTraceReaderHandle::open(ct.to_str().expect("utf-8 path")).unwrap_or_else(|e| panic!("{label}: reader open failed: {e}"));
        assert!(reader.has_column_aware_steps(), "{label}: the container must declare column-aware steps");
        assert_eq!(reader.step_count(), n as u64, "{label}: step count");
        assert_eq!(reader.path_count(), 3, "{label}: path count");

        // The per-line tables must come back exactly as registered — this is
        // the Layout A record decoded by the reference reader rather than by
        // our own encoder's inverse.
        for (fid, table) in lls.iter().enumerate() {
            for (li, len) in table.iter().enumerate() {
                assert_eq!(
                    reader.line_length(fid as u64, li as u32),
                    Some(*len),
                    "{label}: file {fid} line {li} length",
                );
            }
        }

        // Every position-bearing record resolves back to where it was written.
        // Decoded through the Nim column-aware bulk API.
        let mut files = vec![0u64; n];
        let mut lines = vec![0u64; n];
        let mut columns = vec![0u64; n];
        let got = reader
            .step_locations_with_columns(0, n as u64, &mut files, &mut lines, &mut columns)
            .unwrap_or_else(|e| panic!("{label}: step_locations_with_columns: {e}"));
        assert_eq!(got, n as u64, "{label}: the reader must resolve every record");
        let mut checked = 0usize;
        for (i, want) in expected.iter().enumerate() {
            let Some(want) = want else { continue }; // thread switch: no position
            assert_eq!(
                (files[i], lines[i] as u32, columns[i] as u32),
                (want.file, want.line, want.column),
                "{label}: record {i} decoded to the wrong position",
            );
            checked += 1;
        }
        assert!(
            checked >= 12,
            "{label}: only {checked} records carried a position — too few to be a round-trip"
        );
    }
}

#[test]
fn the_nim_reader_reads_every_stream_of_a_rust_container_not_only_its_steps() {
    // THE ASSERTION THAT WAS MISSING, and the defect it would have caught.
    //
    // The pledged-content-size fix was applied to `steps.dat` and to nothing
    // else, and every check in this file stayed green — because the round-trip
    // above asks the Nim reader for steps and positions and never for a value,
    // a call or an I/O event. Measured on the half-fixed writer, over a Rust
    // container the reference reader had opened successfully:
    //
    //     step_count  12    call_count 0    event_count 0
    //     values_json(0) = Err("cannot determine decompressed size for value chunk")
    //
    // `call_count = 0` for a container holding one call is the same
    // silently-empty answer the `steps.dat` finding was about. Four stream
    // families need the pledge, not one, so this test asks the reference reader
    // for one record out of each.
    let _guard = nim_lock();
    let dir = tempfile::tempdir().expect("tempdir");
    let nim_ct = write_populated(dir.path(), "reads_nim", true);
    let rust_ct = write_populated(dir.path(), "reads_rust", false);
    #[allow(deprecated)]
    let _kept = dir.into_path();

    for (label, ct) in [("nim", &nim_ct), ("rust", &rust_ct)] {
        let r = NimTraceReaderHandle::open(ct.to_str().expect("utf-8 path")).unwrap_or_else(|e| panic!("{label}: open: {e}"));

        // Counts first: a stream whose frames the reader cannot size reports
        // ZERO records rather than refusing, so these are the assertions that
        // catch the silent case.
        assert_eq!(r.step_count(), 12, "{label}: step_count");
        assert_eq!(
            r.call_count(),
            1,
            "{label}: call_count — 0 here means the reader could not size calls.dat"
        );
        assert_eq!(
            r.event_count(),
            1,
            "{label}: event_count — 0 here means the reader could not size events.dat"
        );

        // Then the records themselves, which catch the loud case.
        let values = r.values_json(0).unwrap_or_else(|e| panic!("{label}: values_json(0): {e}"));
        assert!(values.contains("varname_id"), "{label}: values_json(0) = {values}");
        let call = r.call_json(0).unwrap_or_else(|e| panic!("{label}: call_json(0): {e}"));
        assert!(call.contains("function_id"), "{label}: call_json(0) = {call}");
        let event = r.event_json(0).unwrap_or_else(|e| panic!("{label}: event_json(0): {e}"));
        assert!(event.contains("step_id"), "{label}: event_json(0) = {event}");
        let step = r.step_json(0).unwrap_or_else(|e| panic!("{label}: step_json(0): {e}"));
        assert!(step.contains("global_line_index"), "{label}: step_json(0) = {step}");
    }
}

#[test]
fn every_compressed_stream_a_rust_container_carries_pledges_its_content_size() {
    // The mechanism behind the test above, pinned directly so a regression is
    // reported as "this stream stopped pledging" rather than as a decode error
    // three layers away. `ZSTD_getFrameContentSize` returning UNKNOWN is what
    // the five Nim readers refuse on.
    let dir = tempfile::tempdir().expect("tempdir");
    let rust_ct = write_populated(dir.path(), "pledge_rust", false);

    let mut unpledged = Vec::new();
    let mut pledged = Vec::new();
    for stream in ["steps.dat", "values.dat", "calls.dat", "events.dat"] {
        let bytes = read_internal(&rust_ct, stream);
        assert!(!bytes.is_empty(), "{stream} is empty — the fixture no longer populates it");
        match zstd::zstd_safe::get_frame_content_size(&bytes) {
            Ok(Some(n)) => pledged.push(format!("{stream}={n}")),
            Ok(None) => unpledged.push(stream),
            Err(_) => panic!("{stream} does not begin with a Zstd frame"),
        }
    }
    assert!(
        unpledged.is_empty(),
        "these streams do not pledge their content size and the reference reader reads them as EMPTY: {unpledged:?}"
    );
    assert_eq!(pledged.len(), 4, "control: all four streams were actually inspected, got {pledged:?}");
}

#[test]
fn a_rust_written_column_aware_container_is_read_by_the_rust_reader() {
    // The pure-Rust half of the round-trip: the same container, decoded
    // without the Nim runtime, through the ported `GlobalPositionDecoder`.
    let _guard = nim_lock();
    let dir = tempfile::tempdir().expect("tempdir");
    let lls = fixture_line_lengths();
    let ops = fixture_ops();

    let rust_ct = write_with_rust(dir.path(), "rustreader_rust", &lls, &ops);

    let mut reader = codetracer_trace_reader::step_stream_reader::open_step_stream(&rust_ct)
        .expect("open_step_stream")
        .expect("the container declares a step stream");
    let expected = expected_positions(&lls, &ops);
    assert_eq!(reader.count(), expected.len() as u64);

    // The value stream is parallel-indexed to the execution stream: one record
    // per step slot, INCLUDING the column-only steps and the thread switches,
    // which have no `Step` event behind them. Without that the two streams
    // drift and `variables_at(step_id)` answers with a neighbour's values from
    // the first column move onwards.
    let values = codetracer_trace_reader::value_stream_reader::open_value_stream(&rust_ct)
        .expect("open_value_stream")
        .expect("the container declares a value stream");
    assert_eq!(
        values.count(),
        expected.len() as u64,
        "values.dat must carry one record per execution-stream record ({} steps)",
        expected.len()
    );
    assert!(expected.len() >= 12, "control: the count compared above is not a small number");

    let decoder = codetracer_trace_reader::global_position_decoder::GlobalPositionDecoder::from_line_lengths(lls.clone());
    let records = reader.read_all().expect("read every step record");
    assert_eq!(records.len(), expected.len());

    use codetracer_trace_writer::step_stream::StepStreamRecord;
    let mut checked = 0usize;
    let mut saw_delta_column = false;
    for (i, (rec, want)) in records.iter().zip(expected.iter()).enumerate() {
        let position = match (rec, want) {
            (StepStreamRecord::Step { global_line_index }, Some(_)) => *global_line_index,
            (StepStreamRecord::DeltaColumn { global_position_index, .. }, Some(_)) => {
                saw_delta_column = true;
                *global_position_index
            }
            // The thread switch: no position, and the record kind must match
            // the fixture's expectation rather than be skipped blindly.
            (StepStreamRecord::ThreadSwitch { .. }, None) => continue,
            (other, want) => panic!("record {i} decoded as {other:?} but the fixture expects {want:?}"),
        };
        let decoded = decoder
            .decode_global_position_index(position)
            .unwrap_or_else(|e| panic!("record {i} position {position}: {e}"));
        let want = want.expect("checked above");
        assert_eq!(
            (decoded.file, decoded.line, decoded.column),
            (want.file, want.line, want.column),
            "record {i} resolved to the wrong position",
        );
        checked += 1;
    }
    assert!(
        saw_delta_column,
        "the Rust reader never saw a DeltaColumn record — tag 0x07 is not being exercised"
    );
    assert!(checked >= 12, "only {checked} records carried a position — too few to be a round-trip");
}

// ---------------------------------------------------------------------------
// The line-only path must not have moved
// ---------------------------------------------------------------------------

#[test]
fn a_line_only_trace_still_uses_the_legacy_paths_dat_record() {
    // Column support is additive. A writer that never opts in must produce the
    // bare-path-bytes `paths.dat` it always did, with the column bits clear —
    // otherwise every existing container's reader breaks on a change that was
    // supposed to be opt-in.
    let dir = tempfile::tempdir().expect("tempdir");
    let mut writer = CtfsTraceWriter::new("line_only", &[]);
    let out = dir.path().join("line_only");
    TraceWriter::begin_writing_trace_events(&mut writer, &out).expect("begin");
    let p = Path::new("/fixture/three_lines.rs");
    // Offering a table that must be ignored is the point: it is the same call
    // a recorder would make unconditionally.
    writer.register_path_with_line_lengths(p, &[5, 12, 3]);
    AbstractTraceWriter::register_step(&mut writer, p, Line(1));
    AbstractTraceWriter::register_step(&mut writer, p, Line(2));
    TraceWriter::finish_writing_trace_events(&mut writer).expect("finish");

    let ct = out.with_extension("ct");
    let paths_dat = read_internal(&ct, "paths.dat");
    assert_eq!(
        paths_dat,
        b"/fixture/three_lines.rs".to_vec(),
        "a line-only paths.dat record is the bare path bytes, with no length prefix and no line table"
    );

    let flags = meta_flags(&ct);
    assert_eq!(
        flags & codetracer_trace_writer::meta_dat::FLAG_HAS_COLUMN_AWARE_STEPS,
        0,
        "a line-only trace must leave the column-aware bit clear"
    );
    assert_ne!(
        flags & codetracer_trace_writer::meta_dat::FLAG_HAS_STEP_STREAM,
        0,
        "control: the flags word is populated, so the assertion above is not reading a zeroed field"
    );
}

#[test]
fn a_late_column_request_is_refused_and_reported() {
    // `dropped_column_awareness` has to be able to answer BOTH ways or it is
    // not a signal. This is the reachable `true`: the mode is trace-global, so
    // a request that arrives after the trace opened cannot be honoured, and
    // silently half-applying it would produce a container no reader can parse.
    let dir = tempfile::tempdir().expect("tempdir");
    let mut writer = CtfsTraceWriter::new("late_request", &[]);
    let out = dir.path().join("late_request");
    TraceWriter::begin_writing_trace_events(&mut writer, &out).expect("begin");
    writer.enable_column_aware_steps();
    assert!(writer.dropped_column_awareness(), "a post-begin request must be reported as dropped");
    assert!(!writer.column_aware_steps_enabled(), "and must not half-apply");
    let err = writer.register_column_step(1).expect_err("a column step must be refused too");
    assert!(err.contains("column-aware"), "{err}");
    TraceWriter::finish_writing_trace_events(&mut writer).expect("finish");

    // The `false` side, on a writer that asked in time.
    let mut ok = CtfsTraceWriter::new("timely_request", &[]);
    ok.enable_column_aware_steps();
    assert!(!ok.dropped_column_awareness());
    assert!(ok.column_aware_steps_enabled());
}
