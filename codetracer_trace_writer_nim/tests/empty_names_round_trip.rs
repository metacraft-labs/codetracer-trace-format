//! An empty name is a name: both writers record it, both readers return it.
//!
//! The spec gives interned names no minimum length (`internal-files.md`
//! §"Interning Tables"): `varnames.dat` records are raw bytes and `types.dat` /
//! `funcs.dat` carry a length prefix, so a zero-length name is a well-formed
//! record. Recorders write them -- the Solana recorder registers empty type
//! names.
//!
//! The Nim reader's C ABI used to return a null pointer for an empty name,
//! the same value it returns for a failed lookup, and `NimTraceReaderHandle`
//! turned the null into an error with an empty message: a recording with one
//! empty type name failed to open with `type 14:`. Now an empty name is a
//! non-null zero-length buffer, and `read_nim_buffer` frees every non-null
//! buffer, empty or not.
//!
//! Asserted, for a container from EACH writer (the Nim writer through this
//! crate, the Rust `CtfsTraceWriter`): an empty type name, variable name and
//! function name -- each next to a non-empty one -- read back as `""` (the
//! non-empty ones as themselves) through both the Nim reader
//! (`NimTraceReaderHandle`) and the pure-Rust `InterningTablesReader`.
//!
//! No mocks: real writers, real containers on disk, real readers.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use codetracer_ctfs::CtfsReader;
use codetracer_trace_reader::interning_tables_reader::InterningTablesReader;
use codetracer_trace_types::{Line, TypeKind};
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::trace_writer::TraceWriter;
use codetracer_trace_writer_nim::{NimTraceReaderHandle, NimTraceWriter, TraceEventsFileFormat};

static NIM_LOCK: Mutex<()> = Mutex::new(());
const PROGRAM: &str = "empty_names";
const SRC: &str = "/src/a.nr";

/// The ids each writer returned, in registration order: (empty, named).
struct Ids {
    types: (u64, u64),
    vars: (u64, u64),
    funcs: (u64, u64),
}

fn write_nim(dir: &Path) -> (PathBuf, Ids) {
    let mut w = NimTraceWriter::new(PROGRAM, &[], TraceEventsFileFormat::Ctfs);
    w.begin_writing_trace_events(&dir.join("trace.json")).expect("begin_events");
    w.begin_writing_trace_metadata(&dir.join("trace_metadata.json")).expect("begin_metadata");
    w.begin_writing_trace_paths(&dir.join("trace_paths.json")).expect("begin_paths");
    let ids = Ids {
        types: (
            w.ensure_type_id(TypeKind::Int, "").0 as u64,
            w.ensure_type_id(TypeKind::Int, "i64").0 as u64,
        ),
        vars: (w.ensure_variable_id("").0 as u64, w.ensure_variable_id("x").0 as u64),
        funcs: (
            w.ensure_function_id("", Path::new(SRC), Line(3)).0 as u64,
            w.ensure_function_id("main", Path::new(SRC), Line(1)).0 as u64,
        ),
    };
    w.register_step(Path::new(SRC), Line(1));
    w.finish_writing_trace_events().expect("finish_events");
    w.finish_writing_trace_metadata().expect("finish_metadata");
    w.finish_writing_trace_paths().expect("finish_paths");
    w.close().expect("close");
    drop(w);
    (dir.join(format!("{PROGRAM}.ct")), ids)
}

fn write_rust(dir: &Path) -> (PathBuf, Ids) {
    let mut w = CtfsTraceWriter::new(PROGRAM, &[]);
    let out = dir.join(PROGRAM);
    TraceWriter::begin_writing_trace_events(&mut w, &out).expect("begin_events");
    let ids = Ids {
        types: (
            AbstractTraceWriter::ensure_type_id(&mut w, TypeKind::Int, "").0 as u64,
            AbstractTraceWriter::ensure_type_id(&mut w, TypeKind::Int, "i64").0 as u64,
        ),
        vars: (
            AbstractTraceWriter::ensure_variable_id(&mut w, "").0 as u64,
            AbstractTraceWriter::ensure_variable_id(&mut w, "x").0 as u64,
        ),
        funcs: (
            AbstractTraceWriter::ensure_function_id(&mut w, "", Path::new(SRC), Line(3)).0 as u64,
            AbstractTraceWriter::ensure_function_id(&mut w, "main", Path::new(SRC), Line(1)).0 as u64,
        ),
    };
    AbstractTraceWriter::register_step(&mut w, Path::new(SRC), Line(1));
    TraceWriter::finish_writing_trace_events(&mut w).expect("finish_events");
    assert!(w.refusals().is_empty(), "the Rust writer refused: {:?}", w.refusals());
    (out.with_extension("ct"), ids)
}

fn assert_both_readers(writer: &str, ct: &Path, ids: &Ids) {
    let nim = NimTraceReaderHandle::open(ct.to_str().unwrap()).unwrap_or_else(|e| panic!("{writer}-written: the Nim reader did not open it: {e}"));
    let mut c = CtfsReader::open(ct).expect("open");
    let rust = InterningTablesReader::open(&mut c).expect("tables").expect("tables present");

    for (id, want) in [(ids.types.0, ""), (ids.types.1, "i64")] {
        let n = nim
            .type_name(id)
            .unwrap_or_else(|e| panic!("{writer}-written, Nim reader, type {id}: error '{e}'"));
        assert_eq!(n, want, "{writer}-written, Nim reader, type {id}");
        let r = rust
            .type_record(id)
            .unwrap_or_else(|e| panic!("{writer}-written, Rust reader, type {id}: {e}"));
        assert_eq!(r.lang_type, want.as_bytes(), "{writer}-written, Rust reader, type {id}");
    }
    for (id, want) in [(ids.vars.0, ""), (ids.vars.1, "x")] {
        let n = nim
            .varname(id)
            .unwrap_or_else(|e| panic!("{writer}-written, Nim reader, varname {id}: error '{e}'"));
        assert_eq!(n, want, "{writer}-written, Nim reader, varname {id}");
        let r = rust
            .varname(id)
            .unwrap_or_else(|e| panic!("{writer}-written, Rust reader, varname {id}: {e}"));
        assert_eq!(r, want.as_bytes(), "{writer}-written, Rust reader, varname {id}");
    }
    for (id, want) in [(ids.funcs.0, ""), (ids.funcs.1, "main")] {
        let n = nim
            .function(id)
            .unwrap_or_else(|e| panic!("{writer}-written, Nim reader, function {id}: error '{e}'"));
        assert_eq!(n, want, "{writer}-written, Nim reader, function {id}");
        let r = rust
            .func(id)
            .unwrap_or_else(|e| panic!("{writer}-written, Rust reader, function {id}: {e}"));
        assert_eq!(r.name, want.as_bytes(), "{writer}-written, Rust reader, function {id}");
    }
}

#[test]
fn empty_names_round_trip_through_both_writers_and_both_readers() {
    let _g = NIM_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let nim_dir = dir.path().join("nim");
    let rust_dir = dir.path().join("rust");
    std::fs::create_dir_all(&nim_dir).unwrap();
    std::fs::create_dir_all(&rust_dir).unwrap();
    let (nim_ct, nim_ids) = write_nim(&nim_dir);
    let (rust_ct, rust_ids) = write_rust(&rust_dir);
    assert_both_readers("Nim", &nim_ct, &nim_ids);
    assert_both_readers("Rust", &rust_ct, &rust_ids);
}
