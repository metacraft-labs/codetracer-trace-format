//! M4 — pure-Rust port of `decodeGlobalPositionIndex` parity tests.
//!
//! These tests exercise the new
//! [`codetracer_trace_reader::global_position_decoder::GlobalPositionDecoder`]
//! against hand-picked line-length fixtures whose per-file global
//! position layout is fully known.  They are the Rust-side mirror of
//! the Nim reader's
//! `tests/test_column_aware_steps.nim::test_decode_global_position_index`
//! coverage and lock the algorithm spec
//! (`codetracer-trace-format-spec/trace-events.md` §"Decoding
//! `global_position_index`") to a Rust-native implementation.

use codetracer_trace_reader::global_position_decoder::{
    ColumnAwareStepRecord, DecodeError, DecodedPosition, GlobalPositionDecoder,
};

/// Three-file fixture used by every test in this module.
///
/// Layout (each row = one file's per-line addressable column counts):
///   file 0 — 2 lines: line 1 has 10 columns, line 2 has 20 columns
///   file 1 — 3 lines: line 1 has  5 columns, line 2 has 15 columns, line 3 has 25 columns
///   file 2 — 1 line : line 1 has  7 columns
///
/// Global position layout:
///   file 0 → [ 0, 30)   size = 10 + 20 = 30
///   file 1 → [30, 75)   size =  5 + 15 + 25 = 45
///   file 2 → [75, 82)   size =  7
fn three_file_fixture() -> Vec<Vec<u32>> {
    vec![vec![10, 20], vec![5, 15, 25], vec![7]]
}

#[test]
fn decode_global_position_index_first_step_file_zero_line_one_column_one() {
    let decoder = GlobalPositionDecoder::from_line_lengths(three_file_fixture());

    // GLI = 0 → file 0, line 1, column 1 — the start of the trace.
    let pos = decoder
        .decode_global_position_index(0)
        .expect("GLI 0 must decode on a fixture with at least one column");
    assert_eq!(
        pos,
        DecodedPosition { file: 0, line: 1, column: 1 }
    );
}

#[test]
fn decode_global_position_index_same_line_column_advances() {
    let decoder = GlobalPositionDecoder::from_line_lengths(three_file_fixture());

    // GLI = 5 → file 0, line 1, column 6  (offset 5 inside line 1 of file 0).
    let pos = decoder.decode_global_position_index(5).expect("GLI 5 valid");
    assert_eq!(
        pos,
        DecodedPosition { file: 0, line: 1, column: 6 }
    );

    // GLI = 9 → still file 0, line 1, column 10 (the last column of line 1).
    let pos = decoder.decode_global_position_index(9).expect("GLI 9 valid");
    assert_eq!(
        pos,
        DecodedPosition { file: 0, line: 1, column: 10 }
    );
}

#[test]
fn decode_global_position_index_line_transition_resets_column() {
    let decoder = GlobalPositionDecoder::from_line_lengths(three_file_fixture());

    // GLI = 10 → first column of line 2 in file 0 (line transitions reset column to 1).
    let pos = decoder
        .decode_global_position_index(10)
        .expect("GLI 10 valid");
    assert_eq!(
        pos,
        DecodedPosition { file: 0, line: 2, column: 1 }
    );

    // GLI = 29 → last column of line 2 in file 0.
    let pos = decoder
        .decode_global_position_index(29)
        .expect("GLI 29 valid");
    assert_eq!(
        pos,
        DecodedPosition { file: 0, line: 2, column: 20 }
    );
}

#[test]
fn decode_global_position_index_file_transition() {
    let decoder = GlobalPositionDecoder::from_line_lengths(three_file_fixture());

    // GLI = 30 → first column of file 1's line 1 (crossed inter-file boundary).
    let pos = decoder
        .decode_global_position_index(30)
        .expect("GLI 30 valid (first byte of file 1)");
    assert_eq!(
        pos,
        DecodedPosition { file: 1, line: 1, column: 1 }
    );

    // GLI = 35 → first column of file 1's line 2 (5 columns on line 1 → next file
    // 1 offset is 5 into line 2's [0, 15) range).
    let pos = decoder
        .decode_global_position_index(35)
        .expect("GLI 35 valid");
    assert_eq!(
        pos,
        DecodedPosition { file: 1, line: 2, column: 1 }
    );

    // GLI = 49 → file 1, line 2, column 15 (last column of line 2).
    let pos = decoder
        .decode_global_position_index(49)
        .expect("GLI 49 valid");
    assert_eq!(
        pos,
        DecodedPosition { file: 1, line: 2, column: 15 }
    );

    // GLI = 50 → file 1, line 3, column 1.
    let pos = decoder
        .decode_global_position_index(50)
        .expect("GLI 50 valid");
    assert_eq!(
        pos,
        DecodedPosition { file: 1, line: 3, column: 1 }
    );

    // GLI = 74 → last column of last line of file 1.
    let pos = decoder
        .decode_global_position_index(74)
        .expect("GLI 74 valid");
    assert_eq!(
        pos,
        DecodedPosition { file: 1, line: 3, column: 25 }
    );

    // GLI = 75 → file 2 line 1 column 1.
    let pos = decoder
        .decode_global_position_index(75)
        .expect("GLI 75 valid");
    assert_eq!(
        pos,
        DecodedPosition { file: 2, line: 1, column: 1 }
    );

    // GLI = 81 → last addressable column.
    let pos = decoder
        .decode_global_position_index(81)
        .expect("GLI 81 valid");
    assert_eq!(
        pos,
        DecodedPosition { file: 2, line: 1, column: 7 }
    );
}

#[test]
fn decode_global_position_index_past_end_is_rejected() {
    let decoder = GlobalPositionDecoder::from_line_lengths(three_file_fixture());

    // GLI = 82 is the first address past the final file.
    let err = decoder
        .decode_global_position_index(82)
        .expect_err("GLI 82 is past end of last file");
    assert!(
        matches!(err, DecodeError::OutOfRange { .. }),
        "expected OutOfRange, got {err:?}",
    );

    // GLI = 100_000 is far past the end.
    let err = decoder
        .decode_global_position_index(100_000)
        .expect_err("GLI 100000 is far past the trace");
    assert!(
        matches!(err, DecodeError::OutOfRange { .. }),
        "expected OutOfRange, got {err:?}",
    );
}

#[test]
fn decode_global_position_index_empty_decoder_errors_cleanly() {
    let decoder = GlobalPositionDecoder::from_line_lengths(Vec::new());

    let err = decoder
        .decode_global_position_index(0)
        .expect_err("decoder with no files must reject all GLIs");
    assert!(
        matches!(err, DecodeError::NoFiles),
        "expected NoFiles, got {err:?}",
    );
}

#[test]
fn decode_global_position_index_file_with_no_lines_is_skipped_cleanly() {
    // File 0 has zero lines (length 0); file 1 carries the real data.
    // GLI 0 must land on file 1's first column because file 0 contributes
    // no positions to the global range.
    let decoder = GlobalPositionDecoder::from_line_lengths(vec![vec![], vec![3, 4]]);

    let pos = decoder
        .decode_global_position_index(0)
        .expect("GLI 0 must decode to file 1");
    assert_eq!(
        pos,
        DecodedPosition { file: 1, line: 1, column: 1 }
    );

    let pos = decoder
        .decode_global_position_index(2)
        .expect("GLI 2 must decode");
    assert_eq!(
        pos,
        DecodedPosition { file: 1, line: 1, column: 3 }
    );

    let pos = decoder
        .decode_global_position_index(3)
        .expect("GLI 3 must decode to next line");
    assert_eq!(
        pos,
        DecodedPosition { file: 1, line: 2, column: 1 }
    );
}

#[test]
fn column_aware_step_record_surfaces_column_some_on_column_aware_trace() {
    // M4 deliverable: the new column-aware step shape must surface
    // `column: Some(N)` when the decoder resolved a per-line table
    // (i.e. the trace is column-aware).  The test pins the exact
    // shape end-to-end through the bulk decode entry point that
    // downstream consumers (db-backend bulk ingest) drive.
    let decoder = GlobalPositionDecoder::from_line_lengths(three_file_fixture());

    let glis = [0u64, 5, 10, 30, 75, 81];
    let records = decoder.decode_many(&glis).expect("all GLIs in range");
    assert_eq!(
        records,
        vec![
            ColumnAwareStepRecord { file: 0, line: 1, column: Some(1) },
            ColumnAwareStepRecord { file: 0, line: 1, column: Some(6) },
            ColumnAwareStepRecord { file: 0, line: 2, column: Some(1) },
            ColumnAwareStepRecord { file: 1, line: 1, column: Some(1) },
            ColumnAwareStepRecord { file: 2, line: 1, column: Some(1) },
            ColumnAwareStepRecord { file: 2, line: 1, column: Some(7) },
        ]
    );
}

#[test]
fn decode_many_resolves_cleanly_across_a_trailing_empty_file() {
    // File 0 carries real per-line data; file 1 is registered but
    // contributes zero positions to the global space (sum of its
    // line_lengths == 0).  decode_many must walk file 0's GLIs to the
    // correct (file, line, column) tuples without being thrown by the
    // trailing empty file — the binary search should ignore the
    // zero-size suffix.
    //
    // The plan also calls for proving `column = None` on a
    // per-line-table-absent fallback.  As implemented, the
    // `FileHasNoLineTable` arm in `decode_many` is unreachable through
    // the `from_line_lengths` public constructor: an empty
    // `line_lengths[fid]` produces `file_size[fid] == 0`, so the
    // decoder's binary search skips that file before consulting its
    // (empty) per-line table.  The arm exists as defensive code that
    // mirrors the Nim reader's `"file F has no line-length table"`
    // branch — see `global_position_decoder.rs::DecodeError::FileHasNoLineTable`
    // for the documented dead-code rationale.  The
    // `ColumnAwareStepRecord::line_only` constructor (asserted in
    // `column_aware_step_record_surfaces_column_some_on_column_aware_trace`'s
    // companion below) gives downstream consumers a typed shape for
    // line-only steps without having to reach a non-existent runtime
    // path.
    let decoder = GlobalPositionDecoder::from_line_lengths(vec![vec![3], vec![]]);
    let records = decoder
        .decode_many(&[0, 2])
        .expect("GLIs inside file 0 must resolve");
    assert_eq!(
        records,
        vec![
            ColumnAwareStepRecord { file: 0, line: 1, column: Some(1) },
            ColumnAwareStepRecord { file: 0, line: 1, column: Some(3) },
        ]
    );
}

#[test]
fn column_aware_step_record_line_only_constructor_surfaces_column_none() {
    // ColumnAwareStepRecord::line_only is the canonical "I have no
    // column data for this step" shape — what a consumer routes to
    // when it has a line-only step (legacy or via a future
    // FileHasNoLineTable fallback path).  Pin its construction so the
    // additive ColumnAwareStepRecord shape can't silently drift back
    // to a Required<u32> column.
    let rec = ColumnAwareStepRecord::line_only(2, 7);
    assert_eq!(
        rec,
        ColumnAwareStepRecord { file: 2, line: 7, column: None }
    );
    // `from_decoded` is the column-bearing mirror — surface both
    // constructors so consumers can pick the right shape without
    // probing the Option directly.
    let decoded = DecodedPosition { file: 2, line: 7, column: 14 };
    let bearing = ColumnAwareStepRecord::from_decoded(decoded);
    assert_eq!(
        bearing,
        ColumnAwareStepRecord { file: 2, line: 7, column: Some(14) }
    );
}

#[test]
fn decoder_exposes_per_file_metadata_for_consumer_introspection() {
    // The decoder must surface its derived cumulative tables so
    // downstream tooling (db-backend bulk ingest, future inline-decoder
    // paths) can sanity-check the trace without re-walking the spec
    // algorithm by hand.
    let decoder = GlobalPositionDecoder::from_line_lengths(three_file_fixture());

    assert_eq!(decoder.file_count(), 3);
    assert_eq!(decoder.file_base(0), Some(0));
    assert_eq!(decoder.file_base(1), Some(30));
    assert_eq!(decoder.file_base(2), Some(75));
    assert_eq!(decoder.file_base(3), None);
    assert_eq!(decoder.file_size(0), Some(30));
    assert_eq!(decoder.file_size(1), Some(45));
    assert_eq!(decoder.file_size(2), Some(7));
    assert_eq!(decoder.total_positions(), 82);
}
