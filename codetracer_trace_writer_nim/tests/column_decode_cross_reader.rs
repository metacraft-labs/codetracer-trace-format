//! M4 — cross-reader consistency test.
//!
//! Drives the same column-aware fixture through both decoders:
//!   * Nim canonical decoder: `decodeGlobalPositionIndex` invoked
//!     under the hood by `ct_reader_step_locations_with_columns`
//!     (the M1 FFI shim).
//!   * Rust pure-port:
//!     `codetracer_trace_reader::global_position_decoder::GlobalPositionDecoder`
//!     fed the same per-file `line_lengths` tables.
//!
//! For every step in the trace we assert the two decoders produce the
//! exact same `(file, line, column)` triple.  This is the M4 plan's
//! Test 3 — "without it, parity is just a label.  If the Nim and
//! Rust readers disagree on a single byte of the fixture, M4 is not
//! done."
//!
//! Strategy: in column-aware mode the multi-stream writer maps
//! `(path_id, line, column)` to a deterministic
//! `global_position_index` (the byte-offset prefix-sum of per-file
//! `line_lengths`).  We can therefore compute every step's expected
//! GLI without round-tripping through any FFI — the Rust decoder is
//! fed the same GLI sequence the writer emitted, and we compare its
//! output to what the Nim FFI's column-aware bulk decoder returned.

use std::path::Path;
use std::sync::Mutex;

use codetracer_trace_reader::global_position_decoder::{
    DecodedPosition, GlobalPositionDecoder,
};
use codetracer_trace_types::Line;
use codetracer_trace_writer_nim::{NimTraceReaderHandle, NimTraceWriter, TraceEventsFileFormat};

/// The Nim runtime is **not** thread-safe — its global state lives behind
/// a single lock.  Multiple `cargo test` threads creating writers
/// concurrently corrupt the state and segfault.  Serialize every test in
/// this binary through this mutex.
static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

/// A single step event registered by the test, together with the
/// (file, line, column) it conceptually addresses.  We keep both the
/// writer-side ground truth and the expected decoded triple so we can
/// cross-validate every layer (writer encoding → Nim reader → Rust
/// reader).
#[derive(Debug, Clone, Copy)]
struct ExpectedStep {
    /// Path interning table id used to emit the step.
    file: u64,
    /// 1-indexed line at which the writer emitted the step.
    line: u32,
    /// 1-indexed column the resulting global position addresses.
    column: u32,
}

/// Three-file column-aware fixture exercised by every cross-reader
/// test in this binary.  The layout is hand-picked to surface the
/// edge cases the M4 plan called out:
///
///   * file 0 (one line of 8 columns) — covers the "everything on
///     line 1" minified-JS case and the first-file/first-line/first-
///     column origin.
///   * file 1 (three lines of [5, 12, 3] columns) — covers
///     line-transition resets, a long middle line, and a short tail.
///   * file 2 (one line of 4 columns) — covers a file boundary deep
///     into the trace and a small trailing file.
///
/// Total addressable positions: 8 + (5 + 12 + 3) + 4 = 32.
fn fixture_line_lengths() -> Vec<Vec<u32>> {
    vec![vec![8], vec![5, 12, 3], vec![4]]
}

/// Compute the per-file `file_base` cumulative table — identical to
/// what `GlobalPositionDecoder` builds internally and what the Nim
/// writer's `toGlobalLineIndex` consumes.  Lives in the test so the
/// assertions can spell out every GLI explicitly without trusting
/// either decoder.
fn file_bases(line_lengths: &[Vec<u32>]) -> Vec<u64> {
    let mut bases = Vec::with_capacity(line_lengths.len());
    let mut running: u64 = 0;
    for lls in line_lengths {
        bases.push(running);
        running += lls.iter().map(|x| u64::from(*x)).sum::<u64>();
    }
    bases
}

/// Compute the GLI a `register_step(path=file, line=line)` call lands
/// at in column-aware mode.  The writer's `toGlobalLineIndex` puts
/// the cursor at column 1 of the requested line, which is
/// `file_base[file] + sum(line_lengths[file][0..line-1])`.
fn gli_of_step(line_lengths: &[Vec<u32>], file: u64, line: u32) -> u64 {
    let bases = file_bases(line_lengths);
    let base = bases[file as usize];
    let lls = &line_lengths[file as usize];
    let mut offset: u64 = 0;
    let up_to = std::cmp::min((line as usize).saturating_sub(1), lls.len());
    for ll in &lls[..up_to] {
        offset += u64::from(*ll);
    }
    base + offset
}

/// Drive the fixture through the Nim writer, recording every step's
/// expected position alongside the GLI we conceptually emitted, then
/// open the result via the Nim reader and assert both decoders
/// agree.  Two paths through the test:
///
///   * "writer-known GLI sequence" — for every step we know the GLI
///     analytically.  We feed it into the Rust decoder and compare
///     to the Nim FFI's bulk column-aware decode.
///   * "exhaustive GLI sweep" — separately we sweep every GLI in
///     `[0, total_positions)` through the Rust decoder and assert
///     the result matches the same arithmetic the Nim reader uses.
///     This guards against the two decoders agreeing only on the
///     specific GLIs the writer produced.
#[test]
fn nim_and_rust_decoders_agree_on_every_step_in_fixture() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    let dir = tempfile::tempdir().expect("tempdir");
    let program = "ctfs_cross_reader_decode";
    let events_path = dir.path().join("trace.json");
    let metadata_path = dir.path().join("trace_metadata.json");
    let paths_path = dir.path().join("trace_paths.json");

    let mut writer = NimTraceWriter::new(program, &[], TraceEventsFileFormat::Ctfs);
    writer
        .begin_writing_trace_events(&events_path)
        .expect("begin_events");
    writer
        .begin_writing_trace_metadata(&metadata_path)
        .expect("begin_metadata");
    writer
        .begin_writing_trace_paths(&paths_path)
        .expect("begin_paths");

    // Opt the writer into column-aware mode BEFORE registering paths
    // — the spec requires the column flag to be trace-global and
    // flipped before the first event.
    writer.enable_column_aware_steps();

    let line_lengths = fixture_line_lengths();

    let path0 = Path::new("/tmp/ctfs_cross_reader_decode/a.py");
    let path1 = Path::new("/tmp/ctfs_cross_reader_decode/b.py");
    let path2 = Path::new("/tmp/ctfs_cross_reader_decode/c.py");
    writer
        .register_path_with_line_lengths(path0, &line_lengths[0])
        .expect("register path 0");
    writer
        .register_path_with_line_lengths(path1, &line_lengths[1])
        .expect("register path 1");
    writer
        .register_path_with_line_lengths(path2, &line_lengths[2])
        .expect("register path 2");

    // Emit a sequence of pending line steps annotated with column
    // deltas.  Current split-stream FFI semantics fold a delta into the
    // unflushed line step, so each `(register_step, write_delta_column)`
    // pair below becomes one absolute step at the requested
    // `(file, line, column)`.
    let mut expected: Vec<ExpectedStep> = Vec::new();

    // Step 0: start at (file 0, line 1), then annotate column 5.
    writer.start(path0, Line(1));
    writer.write_delta_column(4);
    expected.push(ExpectedStep { file: 0, line: 1, column: 5 });

    // Step 1: cross-file jump to (file 1, line 1), then annotate column 3.
    writer.register_step(path1, Line(1));
    writer.write_delta_column(2);
    expected.push(ExpectedStep { file: 1, line: 1, column: 3 });

    // Step 2: jump to (file 1, line 2), then annotate column 10.
    writer.register_step(path1, Line(2));
    writer.write_delta_column(9);
    expected.push(ExpectedStep { file: 1, line: 2, column: 10 });

    // Step 3: cross-file jump to (file 2, line 1), then annotate column 4.
    writer.register_step(path2, Line(1));
    writer.write_delta_column(3);
    expected.push(ExpectedStep { file: 2, line: 1, column: 4 });

    writer.finish_writing_trace_events().expect("finish_events");
    writer.finish_writing_trace_metadata().expect("finish_metadata");
    writer.finish_writing_trace_paths().expect("finish_paths");
    writer.close().expect("close");
    drop(writer);

    let ct_path = dir.path().join(format!("{program}.ct"));
    // Persist by leaking the tempdir — the reader needs the file to
    // outlive this scope.
    #[allow(deprecated)]
    let _dir_path = dir.into_path();
    assert!(
        ct_path.exists(),
        ".ct trace file was not created at {}",
        ct_path.display()
    );

    let reader = NimTraceReaderHandle::open(ct_path.to_str().unwrap()).expect("reader open");
    assert!(
        reader.has_column_aware_steps(),
        "fixture must read back as column-aware"
    );

    let nim_step_count = reader.step_count();
    assert_eq!(
        nim_step_count as usize,
        expected.len(),
        "writer + reader must agree on step count (writer emitted {} steps; reader sees {})",
        expected.len(),
        nim_step_count,
    );

    // ── Step 1: harvest line_lengths back via the existing FFI so
    // we exercise the round-trip rather than re-using the writer-side
    // fixture array.  This is the data the Rust decoder will be fed.
    let mut harvested_line_lengths: Vec<Vec<u32>> = Vec::with_capacity(reader.path_count() as usize);
    for file_id in 0..reader.path_count() {
        let mut lls: Vec<u32> = Vec::new();
        // The reader returns `None` once the line index runs past the
        // file's known table — that's our loop terminator.  No need
        // for an explicit length FFI: this is the same termination
        // strategy `db-backend` will use when migrating off the FFI
        // shim onto the new Rust decoder.
        let mut line_index_0_based: u32 = 0;
        while let Some(length) = reader.line_length(file_id, line_index_0_based) {
            lls.push(length);
            line_index_0_based = line_index_0_based.checked_add(1).expect("line index overflow");
        }
        harvested_line_lengths.push(lls);
    }
    assert_eq!(
        harvested_line_lengths, line_lengths,
        "writer-side line_lengths must round-trip through the reader's `line_length` FFI",
    );

    // ── Step 2: drain the Nim canonical decoder via the M1 FFI.
    let mut nim_path_ids = vec![0u64; nim_step_count as usize];
    let mut nim_lines = vec![0u64; nim_step_count as usize];
    let mut nim_columns = vec![0u64; nim_step_count as usize];
    let written = reader
        .step_locations_with_columns(
            0,
            nim_step_count,
            &mut nim_path_ids,
            &mut nim_lines,
            &mut nim_columns,
        )
        .expect("step_locations_with_columns");
    assert_eq!(
        written, nim_step_count,
        "step_locations_with_columns must drain every step in one call",
    );

    // ── Step 3: confirm the Nim canonical decoder returns exactly
    // the (file, line, column) triples the writer emitted.  This
    // pins the writer↔Nim-reader round-trip so any later disagreement
    // with the Rust decoder is unambiguously a Rust bug rather than
    // a writer/reader mismatch.
    for (i, exp) in expected.iter().enumerate() {
        assert_eq!(
            nim_path_ids[i] as u64, exp.file as u64,
            "step {i}: writer→Nim path_id mismatch"
        );
        assert_eq!(
            nim_lines[i] as u32, exp.line,
            "step {i}: writer→Nim line mismatch"
        );
        assert_eq!(
            nim_columns[i] as u32, exp.column,
            "step {i}: writer→Nim column mismatch"
        );
    }

    // ── Step 4: build the Rust decoder from the harvested
    // line_lengths and decode every conceptual GLI we know the
    // writer emitted.  This is the canonical "Rust port of
    // decodeGlobalPositionIndex agrees with the Nim source of truth"
    // assertion.
    let rust_decoder = GlobalPositionDecoder::from_line_lengths(harvested_line_lengths.clone());
    for (i, exp) in expected.iter().enumerate() {
        let gli = gli_of_step(&line_lengths, exp.file, exp.line) + u64::from(exp.column - 1);
        let pos = rust_decoder
            .decode_global_position_index(gli)
            .unwrap_or_else(|e| panic!("step {i}: Rust decoder rejected GLI {gli}: {e}"));
        assert_eq!(
            pos,
            DecodedPosition {
                file: exp.file,
                line: exp.line,
                column: exp.column,
            },
            "step {i}: Rust decoder disagrees with writer ground truth on GLI {gli}",
        );

        // And the Rust decoder MUST agree byte-for-byte with the Nim
        // decoder's answer (the actual cross-reader consistency
        // assertion the M4 plan demands).
        assert_eq!(
            u64::from(pos.file),
            nim_path_ids[i],
            "step {i}: Rust↔Nim file disagreement at GLI {gli}",
        );
        assert_eq!(
            u64::from(pos.line),
            nim_lines[i],
            "step {i}: Rust↔Nim line disagreement at GLI {gli}",
        );
        assert_eq!(
            u64::from(pos.column),
            nim_columns[i],
            "step {i}: Rust↔Nim column disagreement at GLI {gli}",
        );
    }

    // ── Step 5: exhaustive sweep — every addressable GLI in the
    // global position space must decode the same way under the Rust
    // port as it does under the analytic spec algorithm applied
    // directly to the fixture.  This pins the bit-for-bit behavioural
    // contract independent of whatever step sequence the writer
    // chose to emit.
    let total_positions: u64 = harvested_line_lengths
        .iter()
        .map(|lls| lls.iter().map(|x| u64::from(*x)).sum::<u64>())
        .sum();
    for gli in 0..total_positions {
        let rust = rust_decoder
            .decode_global_position_index(gli)
            .expect("every in-range GLI must decode");
        // Reference: spec-algorithm computed analytically right here
        // so the Rust decoder isn't grading its own homework.
        let mut expected_file = u64::MAX;
        let mut expected_line = 0u32;
        let mut expected_column = 0u32;
        let mut running: u64 = 0;
        'files: for (fid, lls) in harvested_line_lengths.iter().enumerate() {
            let file_size: u64 = lls.iter().map(|x| u64::from(*x)).sum();
            if gli < running + file_size {
                let mut q = gli - running;
                for (l_idx, &ll) in lls.iter().enumerate() {
                    if q < u64::from(ll) {
                        expected_file = fid as u64;
                        expected_line = (l_idx as u32) + 1;
                        expected_column = (q as u32) + 1;
                        break 'files;
                    }
                    q -= u64::from(ll);
                }
            }
            running += file_size;
        }
        assert_eq!(
            rust,
            DecodedPosition {
                file: expected_file,
                line: expected_line,
                column: expected_column,
            },
            "exhaustive sweep: Rust decoder disagrees with spec algorithm at GLI {gli}",
        );
    }

    // ── Step 6: out-of-range GLI rejection — the Rust decoder must
    // refuse `total_positions` and beyond, matching the Nim
    // decoder's "global_position_index N out of range" branch.
    rust_decoder
        .decode_global_position_index(total_positions)
        .expect_err("first past-end GLI must be rejected");
    rust_decoder
        .decode_global_position_index(total_positions + 1_000)
        .expect_err("far past-end GLI must be rejected");
}
