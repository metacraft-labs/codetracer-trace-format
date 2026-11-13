//! A function names the version of its declaration file that was current when
//! it was registered, in both writers.
//!
//! `funcs.dat` is written once the position space is complete, but a bare
//! path resolves to its newest version when it is named
//! (`internal-files.md` §"`paths.dat` path versions"), so a version registered
//! after the function must not move the function into the new version's range.
//! The same operations go through the Nim writer (its C ABI) and the Rust
//! writer; `funcs.dat` must be byte-identical and decode to the addresses the
//! rule gives.
//!
//! No mocks: real writers, real containers, read with the shipped reader.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use codetracer_ctfs::CtfsReader;
use codetracer_trace_reader::interning_tables_reader::InterningTablesReader;
use codetracer_trace_types::Line;
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::trace_writer::TraceWriter;
use codetracer_trace_writer_nim::{NimTraceWriter, TraceEventsFileFormat};

static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

fn nim_lock() -> MutexGuard<'static, ()> {
    NIM_TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

const PROGRAM: &str = "fn_versions";
const GAME: &str = "/src/game.gd";
const UTIL: &str = "/src/util.gd";

/// `game.gd` (12 lines) and `util.gd` (6); `before` declared in `game.gd`;
/// `game.gd` reloaded as 15 lines; `after` declared in it; a step.
fn record(nim: bool, dir: &Path) -> PathBuf {
    let game = PathBuf::from(GAME);
    let util = PathBuf::from(UTIL);
    if nim {
        let mut w = NimTraceWriter::new(PROGRAM, &[], TraceEventsFileFormat::Ctfs);
        w.begin_writing_trace_events(&dir.join("e.json")).expect("nim begin_events");
        w.enable_line_count_table().expect("nim enable_line_count_table");
        w.register_path_with_line_count(&game, 12).expect("nim game");
        w.register_path_with_line_count(&util, 6).expect("nim util");
        assert_eq!(w.ensure_function_id("before", &game, Line(3)).0, 0);
        w.register_path_version(&game, 15).expect("nim version");
        assert_eq!(w.ensure_function_id("after", &game, Line(4)).0, 1);
        w.register_step(&game, Line(1));
        w.finish_writing_trace_events().expect("nim finish_events");
        w.close().expect("nim close");
        drop(w);
        dir.join(format!("{PROGRAM}.ct"))
    } else {
        let mut w = CtfsTraceWriter::new(PROGRAM, &[]);
        TraceWriter::begin_writing_trace_events(&mut w, &dir.join(PROGRAM)).expect("rust begin_events");
        w.enable_line_count_table().expect("rust enable_line_count_table");
        w.register_path_with_line_count(&game, 12).expect("rust game");
        w.register_path_with_line_count(&util, 6).expect("rust util");
        assert_eq!(AbstractTraceWriter::ensure_function_id(&mut w, "before", &game, Line(3)).0, 0);
        w.register_path_version(&game, 15).expect("rust version");
        assert_eq!(AbstractTraceWriter::ensure_function_id(&mut w, "after", &game, Line(4)).0, 1);
        AbstractTraceWriter::register_step(&mut w, &game, Line(1));
        TraceWriter::finish_writing_trace_events(&mut w).expect("rust finish_events");
        dir.join(format!("{PROGRAM}.ct"))
    }
}

fn function_addresses(ct: &Path) -> (Vec<u8>, Vec<u64>) {
    let mut r = CtfsReader::open(ct).expect("open");
    let funcs = r.read_file("funcs.dat").expect("funcs.dat");
    let tables = InterningTablesReader::open(&mut r).expect("tables").expect("interning tables");
    let addresses = (0..tables.func_count() as u64)
        .map(|id| tables.func(id).expect("func").global_line_index)
        .collect();
    (funcs, addresses)
}

#[test]
fn a_function_registered_before_a_version_stays_in_the_version_it_named() {
    let _g = nim_lock();
    let dir = tempfile::tempdir().expect("tempdir");
    let nim_dir = dir.path().join("nim");
    let rust_dir = dir.path().join("rust");
    std::fs::create_dir_all(&nim_dir).unwrap();
    std::fs::create_dir_all(&rust_dir).unwrap();
    let (nim_funcs, nim_addr) = function_addresses(&record(true, &nim_dir));
    let (rust_funcs, rust_addr) = function_addresses(&record(false, &rust_dir));
    // `before`: line 3 of version 0, base 0. `after`: line 4 of version 2,
    // after 12 + 6 lines.
    let want = vec![2u64, 12 + 6 + 3];
    assert_eq!(nim_addr, want, "the Nim writer's funcs.dat addresses");
    assert_eq!(rust_addr, want, "the Rust writer's funcs.dat addresses");
    assert_eq!(nim_funcs, rust_funcs, "funcs.dat must be byte-identical");
}
