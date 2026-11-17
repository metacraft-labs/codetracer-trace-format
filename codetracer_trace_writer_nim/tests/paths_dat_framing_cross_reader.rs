//! The Nim writer and the Rust reader agree on every `paths.dat` record shape.
//!
//! `paths.dat` has three record layouts, selected by `meta.dat` flags
//! (`codetracer-trace-format-spec/internal-files.md`):
//!
//! * **bare** — neither bit: the record is the path bytes, its length taken
//!   from `paths.off`;
//! * **Layout A** — bit 4 (`FLAG_HAS_COLUMN_AWARE_STEPS`):
//!   `path_len + path + line_count + line_lengths × line_count`;
//! * **line-count table** — bit 14 (`FLAG_HAS_LINE_COUNT_TABLE`):
//!   `path_len + path + line_count`.
//!
//! The layouts overlap: a bare record whose first byte happens to be no larger
//! than what follows it also decodes under either framed layout, into the wrong
//! path and with no error. So agreement cannot be inferred from a clean decode;
//! each arm here checks the exact path, the flag the header declares, and the
//! per-file data the layout carries, for a short path and for one long enough
//! (over 127 bytes) that its length prefix is a two-byte varint.
//!
//! No mocks: the real Nim writer is driven through its FFI and the container is
//! read back with the Rust `InterningTablesReader` and split-stream reader.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use codetracer_trace_reader::interning_tables_reader::InterningTablesReader;
use codetracer_trace_types::{Line, TraceLowLevelEvent};
use codetracer_trace_writer::column_aware::decode_path_record_layout_a;
use codetracer_trace_writer::meta_dat::{meta_dat_has_column_aware_steps, meta_dat_has_line_count_table};
use codetracer_trace_writer_nim::{NimTraceWriter, TraceEventsFileFormat};

static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

fn nim_lock() -> MutexGuard<'static, ()> {
    NIM_TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Layout {
    Bare,
    LayoutA,
    LineCountTable,
}

/// Per-file data registered for the two paths: line lengths under Layout A
/// (the second file deliberately has none, so the writers record the
/// conventional table for it), line counts under bit 14.
const LINE_LENGTHS: [&[u32]; 2] = [&[12, 40, 1, 7, 300], &[]];
const LINE_COUNTS: [u64; 2] = [5, 200_000];

struct ReadBack {
    column_aware: bool,
    line_count_table: bool,
    paths: Vec<String>,
    line_counts: Vec<Option<u64>>,
    layout_a_line_lengths: Vec<Vec<u32>>,
    path_events: Vec<PathBuf>,
}

fn two_paths(dir: &Path) -> [PathBuf; 2] {
    let short = dir.join("a.src");
    let long_dir = dir.join("d".repeat(90));
    let long = long_dir.join(format!("{}.src", "long_file_name_".repeat(4)));
    assert!(long.as_os_str().len() > 127, "the long path must need a two-byte length prefix");
    [short, long]
}

fn write_and_read(layout: Layout) -> (ReadBack, [PathBuf; 2]) {
    let dir = tempfile::tempdir().expect("tempdir");
    let program = format!("paths_framing_{layout:?}");
    let paths = two_paths(dir.path());

    let mut w = NimTraceWriter::new(&program, &[], TraceEventsFileFormat::Ctfs);
    w.begin_writing_trace_events(&dir.path().join("e.json")).expect("begin_events");
    w.begin_writing_trace_metadata(&dir.path().join("m.json")).expect("begin_metadata");
    w.begin_writing_trace_paths(&dir.path().join("p.json")).expect("begin_paths");
    match layout {
        Layout::Bare => {}
        Layout::LayoutA => w.enable_column_aware_steps(),
        Layout::LineCountTable => w.enable_line_count_table().expect("enable_line_count_table"),
    }
    for (i, p) in paths.iter().enumerate() {
        match layout {
            Layout::Bare => {
                w.ensure_path_id(p);
            }
            Layout::LayoutA => {
                w.register_path_with_line_lengths(p, LINE_LENGTHS[i])
                    .expect("register_path_with_line_lengths");
            }
            Layout::LineCountTable => {
                w.register_path_with_line_count(p, LINE_COUNTS[i]).expect("register_path_with_line_count");
            }
        }
    }
    w.register_step(&paths[0], Line(1));
    w.register_step(&paths[1], Line(1));
    w.register_step(&paths[0], Line(2));
    w.finish_writing_trace_events().expect("finish_events");
    w.finish_writing_trace_metadata().expect("finish_metadata");
    w.finish_writing_trace_paths().expect("finish_paths");
    w.close().expect("close");
    drop(w);

    let ct = dir.path().join(format!("{program}.ct"));
    let mut reader = codetracer_ctfs::CtfsReader::open(&ct).expect("open container");
    let meta = reader.read_file("meta.dat").expect("meta.dat");
    let raw_paths = reader.read_file("paths.dat").expect("paths.dat");
    let raw_off = reader.read_file("paths.off").expect("paths.off");
    let tables = InterningTablesReader::open(&mut reader).expect("open tables").expect("paths.dat present");

    let count = tables.path_count();
    let mut layout_a_line_lengths = Vec::new();
    if meta_dat_has_column_aware_steps(&meta) {
        let offsets: Vec<usize> = raw_off
            .chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().unwrap()) as usize)
            .collect();
        for i in 0..count {
            let (_, lls) = decode_path_record_layout_a(&raw_paths[offsets[i]..offsets[i + 1]]).expect("Layout A record");
            layout_a_line_lengths.push(lls);
        }
    }
    let path_events = codetracer_trace_reader::ctfs_reader::read_trace_from_ctfs(&ct)
        .expect("split-stream read")
        .into_iter()
        .filter_map(|ev| match ev {
            TraceLowLevelEvent::Path(p) => Some(p),
            _ => None,
        })
        .collect();
    let rb = ReadBack {
        column_aware: meta_dat_has_column_aware_steps(&meta),
        line_count_table: meta_dat_has_line_count_table(&meta),
        paths: (0..count as u64).map(|i| tables.path_str(i).expect("path resolves")).collect(),
        line_counts: (0..count as u64).map(|i| tables.line_count(i)).collect(),
        layout_a_line_lengths,
        path_events,
    };
    drop(dir);
    (rb, paths)
}

fn expected_names(paths: &[PathBuf; 2]) -> Vec<String> {
    paths.iter().map(|p| p.to_string_lossy().into_owned()).collect()
}

#[test]
fn bare_records_read_back_as_the_registered_paths() {
    let _g = nim_lock();
    let (rb, paths) = write_and_read(Layout::Bare);
    assert!(!rb.column_aware && !rb.line_count_table, "a plain writer declares neither framed layout");
    assert_eq!(rb.paths, expected_names(&paths));
    assert_eq!(rb.line_counts, vec![None, None], "a bare container records no size");
    assert_eq!(rb.path_events, paths.to_vec());
}

#[test]
fn layout_a_records_read_back_with_their_line_lengths() {
    let _g = nim_lock();
    let (rb, paths) = write_and_read(Layout::LayoutA);
    assert!(
        rb.column_aware && !rb.line_count_table,
        "a column-aware writer declares bit 4 and only bit 4"
    );
    assert_eq!(rb.paths, expected_names(&paths));
    // No table is recorded as the conventional one, whose only encoding is
    // `line_count = 0` with no line lengths (`internal-files.md` §"`paths.dat`
    // Layout A").
    assert_eq!(rb.layout_a_line_lengths, vec![LINE_LENGTHS[0].to_vec(), Vec::new()]);
    assert_eq!(rb.line_counts, vec![None, None], "bit 4 alone is not the bit-14 table");
    assert_eq!(rb.path_events, paths.to_vec());
}

#[test]
fn line_count_table_records_read_back_with_their_counts() {
    let _g = nim_lock();
    let (rb, paths) = write_and_read(Layout::LineCountTable);
    assert!(
        rb.line_count_table && !rb.column_aware,
        "a line-count-table writer declares bit 14 and only bit 14"
    );
    assert_eq!(rb.paths, expected_names(&paths), "the framing must not leak into the path");
    assert_eq!(rb.line_counts, vec![Some(LINE_COUNTS[0]), Some(LINE_COUNTS[1])]);
    assert_eq!(rb.path_events, paths.to_vec());
}
