//! A Nim-written container's call arguments must survive the Rust reader.
//!
//! # The gap this closes
//!
//! Before this file, **no test anywhere read a Nim-written container through
//! the Rust split-stream reader.** That is the path every recorder in the
//! fleet actually produces and `codetracer_trace_util convert` actually
//! consumes, and it was broken:
//!
//! `calls.dat` frames arguments as a count followed by
//! `(varname_id, len, value)` triples. The Nim writer fills them the way the
//! spec now requires — one entry per argument, each with its own interned
//! name. The pure-Rust writer instead emitted ONE synthetic entry under
//! `varname_id` 0 holding the whole argument vector as a single serialized
//! blob, and its reader was written to match its own writer:
//!
//! ```ignore
//! // only the first is the canonical args blob for Rust-written records
//! if args.is_empty() {
//!     args.extend_from_slice(&data[pos..pos + arg_len]);
//! }
//! ```
//!
//! Against a Nim-written record that keeps **only the first argument**,
//! discards every later one, and throws away all names. The decode then failed
//! on a type mismatch — a single value's CBOR map read as a vector — which is
//! the only reason anyone noticed. Had the shapes happened to agree it would
//! have been silent argument loss.
//!
//! `trace-events.md` §"Call Stream (`calls.dat`)" now pins the per-argument
//! form, and both writers emit it.
//!
//! # What this asserts
//!
//! 1. A call's arguments come back with their **names**, which is what the
//!    collapsed form destroyed and what a count-only assertion cannot see.
//! 2. **Every** argument comes back, not just the first — the specific
//!    off-by-design of the old reader.
//! 3. Order is preserved, so a reader can match an argument to a parameter.
//! 4. A zero-argument call still reads back as zero arguments, so (2) cannot
//!    pass by inventing entries.
//!
//! # Mocking policy justification (workspace CLAUDE.md)
//!
//! **Nothing is mocked.** A REAL `NimTraceWriter` drives the REAL Nim static
//! library through the REAL C FFI to write a REAL `.ct`, which is then decoded
//! by the REAL Rust `read_trace_from_ctfs`. The entire question is whether one
//! implementation's container is legible to the other's reader, so substituting
//! either end would test the substitute.

use std::sync::Mutex;

use codetracer_trace_types::{Line, TraceLowLevelEvent, TypeKind, ValueRecord};
use codetracer_trace_writer_nim::{NimTraceWriter, TraceEventsFileFormat};

/// The Nim runtime is not thread-safe — its global state lives behind a single
/// lock. Serialize this binary's writers through it, as the other Nim-backed
/// suites in this crate do.
static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

/// Write a container whose single call carries `arg_names`, then decode it with
/// the Rust reader and return that call's `(name, value)` pairs.
fn call_args_round_trip(program: &str, arg_names: &[&str]) -> Vec<(String, i64)> {
    let dir = tempfile::tempdir().expect("tempdir");

    let mut w = NimTraceWriter::new(program, &[], TraceEventsFileFormat::Ctfs);
    w.begin_writing_trace_events(&dir.path().join("events.json")).expect("begin_events");
    w.begin_writing_trace_metadata(&dir.path().join("meta.json")).expect("begin_metadata");
    w.begin_writing_trace_paths(&dir.path().join("paths.json")).expect("begin_paths");

    let source = dir.path().join(format!("{program}.src"));
    w.register_path_with_line_count(&source, 16).expect("register_path_with_line_count");
    w.register_function("callee", &source, Line(1));
    let tid = w.ensure_type_id(TypeKind::Int, "Int");

    w.register_step(&source, Line(1));
    // Stage each argument, then open the call that consumes them. The values
    // are distinct so a reordering cannot compare equal by accident.
    for (i, name) in arg_names.iter().enumerate() {
        w.arg(
            name,
            ValueRecord::Int {
                i: 100 + i as i64,
                type_id: tid,
            },
        );
    }
    w.register_call(codetracer_trace_types::FunctionId(0), vec![]);
    w.register_step(&source, Line(2));
    w.register_return(ValueRecord::None { type_id: tid });

    w.finish_writing_trace_events().expect("finish_events");
    w.finish_writing_trace_metadata().expect("finish_metadata");
    w.finish_writing_trace_paths().expect("finish_paths");
    w.close().expect("close");
    assert!(
        w.discarded_record_counts().is_empty(),
        "the fixture must not rely on a discarded record: {:?}",
        w.discarded_record_counts()
    );
    drop(w);

    let ct_path = dir.path().join(format!("{program}.ct"));
    assert!(ct_path.exists(), "no .ct at {}", ct_path.display());

    // THE PATH UNDER TEST: a Nim-written container, the Rust reader.
    let events = codetracer_trace_reader::ctfs_reader::read_trace_from_ctfs(&ct_path)
        .unwrap_or_else(|e| panic!("the Rust reader could not read a Nim-written container: {e}"));

    let tables = codetracer_trace_reader::interning_tables_reader::open_interning_tables(&ct_path)
        .expect("interning tables open")
        .expect("interning tables present");

    let mut found = None;
    for ev in &events {
        if let TraceLowLevelEvent::Call(call) = ev {
            assert!(found.is_none(), "the fixture opens exactly one call");
            found = Some(
                call.args
                    .iter()
                    .map(|a| {
                        let name = tables
                            .varname_str(a.variable_id.0 as u64)
                            .expect("a call argument's name resolves in varnames.dat");
                        let i = match &a.value {
                            ValueRecord::Int { i, .. } => *i,
                            other => panic!("expected an Int argument, got {other:?}"),
                        };
                        (name, i)
                    })
                    .collect::<Vec<_>>(),
            );
        }
    }

    drop(dir);
    found.expect("the decoded trace carries no Call event at all")
}

#[test]
fn every_call_argument_survives_with_its_name() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    // THREE arguments: the old reader kept the first and dropped the rest, so
    // two would already catch it and three shows the order as well.
    let got = call_args_round_trip("nim_call_args_three", &["board", "depth", "flags"]);

    assert_eq!(
        got,
        vec![("board".to_string(), 100), ("depth".to_string(), 101), ("flags".to_string(), 102),],
        "every argument must come back, in order, WITH its name — the collapsed \
         form kept only the first and named none of them"
    );
}

#[test]
fn a_single_call_argument_keeps_its_name() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    // The one-argument case is where the two conventions are least
    // distinguishable by arity, so the name is the only thing separating them.
    let got = call_args_round_trip("nim_call_args_one", &["only"]);

    assert_eq!(got, vec![("only".to_string(), 100)]);
}

#[test]
fn a_call_with_no_arguments_reads_back_with_none() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    let got = call_args_round_trip("nim_call_args_none", &[]);

    assert!(got.is_empty(), "a call that was given no arguments must not acquire any, got {got:?}");
}
