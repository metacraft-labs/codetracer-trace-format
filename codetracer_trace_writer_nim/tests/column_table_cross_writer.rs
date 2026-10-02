//! The two writers decide a column-aware file's table at its first mention by
//! the same rules, producing the same container byte for byte, and refuse a
//! line past the conventional table with the same words.
//!
//! Spec: `codetracer-trace-format-spec/internal-files.md` §"`paths.dat` Layout
//! A": a given table is recorded as given, except that one whose lines hold
//! nothing gives its first line one position; an empty table, or none (the
//! path first mentioned by a step, a function or an id request), records the
//! conventional table of 100000 lines of 1024 positions, on which a column
//! above 1024 is recorded at 1024 and a line above 100000 is refused. "The two
//! writers apply these rules identically."
//!
//! The recording below exercises every branch: a real table, `[0]`, `[0, 0]`,
//! an explicitly empty table, a path first named by a step, one first named by
//! a function, a recorder-built conventional table, columns past 1024 folded
//! into a step and as a stand-alone column move, line 0, and the last line of a
//! conventional file. Every internal file of the two containers is compared.
//!
//! No mocks: both containers are produced by the real writers and read back
//! through the real CTFS reader.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use codetracer_ctfs::CtfsReader;
use codetracer_trace_types::{Line, ThreadId, TraceLowLevelEvent, TypeKind};
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::ctfs_writer::{conventional_line_diagnostic, late_column_table_diagnostic, CtfsTraceWriter};
use codetracer_trace_writer::trace_writer::TraceWriter;
use codetracer_trace_writer_nim::{NimTraceWriter, TraceEventsFileFormat};

static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

fn nim_lock() -> MutexGuard<'static, ()> {
    NIM_TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

const PROGRAM: &str = "column_tables";
const RECORDING_ID: &str = "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb";
const WORKDIR: &str = "/work/column_tables";
const REAL: &str = "/src/app.py";
const EMPTY_FILE: &str = "/src/pkg/__init__.py";
const BLANK_FILE: &str = "/src/blank.py";
const NO_TABLE: &str = "/src/no_table.py";
const STEPPED: &str = "<frozen importlib._bootstrap>";
const DECLARED: &str = "/src/declared_only.py";
const BUILT: &str = "/src/recorder_built.py";

fn nim(dir: &Path) -> NimTraceWriter {
    let mut w = NimTraceWriter::new(PROGRAM, &[], TraceEventsFileFormat::Ctfs);
    w.set_recording_id(RECORDING_ID).expect("nim set_recording_id");
    w.set_workdir(Path::new(WORKDIR));
    w.begin_writing_trace_events(&dir.join("trace.json")).expect("nim begin_events");
    w.begin_writing_trace_metadata(&dir.join("trace_metadata.json"))
        .expect("nim begin_metadata");
    w.begin_writing_trace_paths(&dir.join("trace_paths.json")).expect("nim begin_paths");
    w.enable_column_aware_steps();
    w
}

fn rust(dir: &Path) -> CtfsTraceWriter {
    let mut w = CtfsTraceWriter::new(PROGRAM, &[]);
    w.set_recording_id(RECORDING_ID);
    AbstractTraceWriter::set_workdir(&mut w, Path::new(WORKDIR));
    w.enable_column_aware_steps();
    TraceWriter::begin_writing_trace_events(&mut w, &dir.join(PROGRAM)).expect("rust begin_events");
    w
}

/// Close the Nim writer; its error, if it failed.
fn nim_close(mut w: NimTraceWriter) -> Option<String> {
    w.finish_writing_trace_events().expect("nim finish_events");
    w.finish_writing_trace_metadata().expect("nim finish_metadata");
    w.finish_writing_trace_paths().expect("nim finish_paths");
    w.close().err().map(|e| e.to_string())
}

/// Finish the Rust writer; its error, if it failed.
fn rust_finish(mut w: CtfsTraceWriter) -> Option<String> {
    TraceWriter::finish_writing_trace_events(&mut w).err().map(|e| e.to_string())
}

fn files(ct: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut r = CtfsReader::open(ct).unwrap_or_else(|e| panic!("open {}: {e:?}", ct.display()));
    r.list_files()
        .into_iter()
        .map(|name| {
            let bytes = r.read_file(&name).unwrap_or_else(|e| panic!("{name}: {e:?}"));
            (name, bytes)
        })
        .collect()
}

/// The recorder-facing calls both writers receive.
trait Recorder {
    fn table(&mut self, path: &str, t: &[u32]);
    fn start(&mut self, path: &str, line: i64);
    fn step(&mut self, path: &str, line: i64, column: Option<i64>);
    fn function(&mut self, name: &str, path: &str, line: i64);
    fn thread_switch(&mut self, thread: u64);
    fn column_move(&mut self, delta: i64);
    fn none_type(&mut self);
}

impl Recorder for NimTraceWriter {
    fn table(&mut self, path: &str, t: &[u32]) {
        self.register_path_with_line_lengths(Path::new(path), t).expect("nim register");
    }
    fn start(&mut self, path: &str, line: i64) {
        NimTraceWriter::start(self, Path::new(path), Line(line));
    }
    fn step(&mut self, path: &str, line: i64, column: Option<i64>) {
        self.register_step_with_column(Path::new(path), Line(line), column.map(Line));
    }
    fn function(&mut self, name: &str, path: &str, line: i64) {
        self.ensure_function_id(name, Path::new(path), Line(line));
    }
    fn thread_switch(&mut self, thread: u64) {
        self.register_thread_switch(thread);
    }
    fn column_move(&mut self, delta: i64) {
        self.write_delta_column(delta);
    }
    fn none_type(&mut self) {
        self.ensure_type_id(TypeKind::None, "None");
    }
}

impl Recorder for CtfsTraceWriter {
    fn table(&mut self, path: &str, t: &[u32]) {
        TraceWriter::register_path_with_line_lengths(self, Path::new(path), t).expect("rust register");
    }
    fn start(&mut self, path: &str, line: i64) {
        TraceWriter::start(self, Path::new(path), Line(line));
    }
    fn step(&mut self, path: &str, line: i64, column: Option<i64>) {
        AbstractTraceWriter::register_step_with_column(self, Path::new(path), Line(line), column.map(Line));
    }
    fn function(&mut self, name: &str, path: &str, line: i64) {
        AbstractTraceWriter::ensure_function_id(self, name, Path::new(path), Line(line));
    }
    fn thread_switch(&mut self, thread: u64) {
        AbstractTraceWriter::add_event(self, TraceLowLevelEvent::ThreadSwitch(ThreadId(thread)));
    }
    fn column_move(&mut self, delta: i64) {
        self.register_column_step(delta).expect("rust column move");
    }
    fn none_type(&mut self) {
        AbstractTraceWriter::ensure_type_id(self, TypeKind::None, "None");
    }
}

fn valid_recording(w: &mut impl Recorder) {
    // The Rust writer interns the `None` type its `start` needs and the Nim
    // writer does not; declaring it keeps `types.dat` out of this comparison's
    // way, since it is not what the comparison is about.
    w.none_type();
    w.table(REAL, &[12, 0, 40]);
    w.table(EMPTY_FILE, &[0]);
    w.table(BLANK_FILE, &[0, 0]);
    w.table(NO_TABLE, &[]);
    w.table(BUILT, &vec![1024; 100_000]);
    // The same table again, and no table, are lookups.
    w.table(REAL, &[12, 0, 40]);
    w.table(REAL, &[]);
    w.start(REAL, 1);
    w.function("declared_only", DECLARED, 4);
    w.step(REAL, 3, Some(30));
    w.step(EMPTY_FILE, 1, None);
    w.step(BLANK_FILE, 1, Some(1));
    w.step(NO_TABLE, 9, Some(5000));
    w.step(STEPPED, 2, Some(1500));
    w.step(STEPPED, 6, Some(1001));
    w.thread_switch(3);
    w.column_move(100);
    w.step(BUILT, 100_000, Some(2048));
    w.step(STEPPED, 0, None);
    w.step(REAL, 1, Some(5));
}

#[test]
fn both_writers_decide_tables_alike_and_yield_the_same_container() {
    let _g = nim_lock();
    let dir = tempfile::tempdir().expect("tempdir");
    let (nd, rd): (PathBuf, PathBuf) = (dir.path().join("nim"), dir.path().join("rust"));
    std::fs::create_dir_all(&nd).unwrap();
    std::fs::create_dir_all(&rd).unwrap();

    let mut n = nim(&nd);
    valid_recording(&mut n);
    assert_eq!(nim_close(n), None, "the valid Nim recording must close");
    let nim_ct = nd.join(format!("{PROGRAM}.ct"));

    let mut r = rust(&rd);
    valid_recording(&mut r);
    assert!(r.refusals().is_empty(), "{:?}", r.refusals());
    let tables = r.line_lengths().to_vec();
    assert_eq!(rust_finish(r), None, "the valid Rust recording must finish");
    let rust_ct = rd.join(PROGRAM).with_extension("ct");

    // What the fixture is about, before the bytes: [0] -> [1], [0, 0] ->
    // [1, 0], and no table -> the conventional table.
    assert_eq!(tables[1], vec![1]);
    assert_eq!(tables[2], vec![1, 0]);
    for f in [3, 4, 5] {
        assert_eq!(tables[f].len(), 100_000, "file {f}");
    }

    let (a, b) = (files(&nim_ct), files(&rust_ct));
    assert_eq!(
        a.keys().collect::<Vec<_>>(),
        b.keys().collect::<Vec<_>>(),
        "the two containers hold different files"
    );
    let differing: Vec<&String> = a.keys().filter(|k| a[*k] != b[*k]).collect();
    assert!(differing.is_empty(), "files differ between the writers: {differing:?}");
    assert!(a["paths.dat"].len() > 400_000, "four conventional tables are in paths.dat");
    let len = |p: &Path| std::fs::metadata(p).expect("container").len();
    assert_eq!(len(&nim_ct), len(&rust_ct), "the containers differ in size");
}

#[test]
fn both_writers_refuse_a_line_past_the_conventional_table_alike() {
    let _g = nim_lock();
    let dir = tempfile::tempdir().expect("tempdir");
    let expected = conventional_line_diagnostic(Path::new(STEPPED), 100_001);
    let mut n = nim(dir.path());
    let mut r = rust(dir.path());
    for w in [&mut n as &mut dyn Recorder, &mut r as &mut dyn Recorder] {
        w.table(REAL, &[5, 5]);
        w.start(REAL, 1);
        w.step(STEPPED, 100_001, None);
        w.step(REAL, 2, None);
    }
    let nim_close = nim_close(n).expect("the Nim recording must fail");
    let rust_finish = rust_finish(r).expect("the Rust recording must fail");
    assert!(nim_close.contains(&expected), "nim close: {nim_close}\nexpected: {expected}");
    assert!(rust_finish.contains(&expected), "rust finish: {rust_finish}\nexpected: {expected}");
}

#[test]
fn both_writers_refuse_a_table_after_the_file_was_interned_alike() {
    let _g = nim_lock();
    let dir = tempfile::tempdir().expect("tempdir");
    let expected = late_column_table_diagnostic(Path::new(STEPPED), 100_000, 2);
    let mut n = nim(dir.path());
    let mut r = rust(dir.path());
    n.register_path_with_line_lengths(Path::new(REAL), &[5, 5]).expect("nim");
    TraceWriter::register_path_with_line_lengths(&mut r, Path::new(REAL), &[5, 5]).expect("rust");
    for w in [&mut n as &mut dyn Recorder, &mut r as &mut dyn Recorder] {
        w.start(REAL, 1);
        w.step(STEPPED, 1, None);
        w.step(REAL, 2, None);
    }
    let nim_err = n
        .register_path_with_line_lengths(Path::new(STEPPED), &[3, 4])
        .expect_err("the Nim writer must refuse")
        .to_string();
    let rust_err = TraceWriter::register_path_with_line_lengths(&mut r, Path::new(STEPPED), &[3, 4])
        .expect_err("the Rust writer must refuse")
        .to_string();
    assert_eq!(nim_err, expected, "the Nim writer's refusal");
    assert_eq!(rust_err, expected, "the Rust writer's refusal");
    let nim_close = nim_close(n).expect("the Nim recording must fail");
    let rust_finish = rust_finish(r).expect("the Rust recording must fail");
    assert!(nim_close.contains(&expected), "nim close: {nim_close}");
    assert!(rust_finish.contains(&expected), "rust finish: {rust_finish}");
}
