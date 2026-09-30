//! A value staged before the first step must still reach the trace.
//!
//! # The shape under test
//!
//! Recorders that learn a call's arguments before they learn a source position
//! stage the arguments first and emit the step afterwards. `NimTraceWriter::arg`
//! registers each argument as a step variable AND stages it for the call
//! record, so after a `Call` that precedes any `register_step` the writer holds
//! values with no step to attach them to.
//!
//! The writer carries those forward to the next step rather than dropping them.
//! This pins that it really does, because the alternative is invisible: the
//! container is well-formed either way, `values.dat` has exactly as many
//! records as there are steps, and every one of them decodes. They are simply
//! empty. Measured in the BEAM recorder, an Elixir recording came out with six
//! steps, six value records, and nothing in any of them.
//!
//! # What this asserts
//!
//! 1. A value staged BEFORE the first step is in the container afterwards.
//! 2. It is attached to a step, reachable by name — not merely present in
//!    `varnames.dat`, which it would be even if the value were lost, since
//!    interning happens when the name is first seen.
//! 3. The control: a value staged AFTER a step lands too, so a failure of (1)
//!    is specifically about ordering rather than about values in general.
//! 4. The source path reads back as exactly the path that was registered, so a
//!    container whose `paths.dat` framing disagrees with its `meta.dat` flags
//!    fails here rather than passing with a misread path.
//!
//! No mocks: every arm drives the real Nim writer through its FFI and reads the
//! container back with the Rust reader.

use std::sync::{Mutex, MutexGuard, PoisonError};

use codetracer_trace_types::{FunctionId, Line, TraceLowLevelEvent, TypeKind, ValueRecord};
use codetracer_trace_writer_nim::{NimTraceWriter, TraceEventsFileFormat};

static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

/// Serialise the arms. A panicking arm poisons the lock; the next arm takes it
/// anyway, so one failure is reported as one failure and not as every arm.
fn nim_lock() -> MutexGuard<'static, ()> {
    NIM_TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

/// How the writer is configured before its first path is registered.
#[derive(Clone, Copy, PartialEq)]
enum Mode {
    LineOnly,
    /// Column-aware, opted into before any path: every `paths.dat` record of a
    /// column-aware trace is Layout A, so the opt-in cannot follow one.
    ColumnAware,
}

/// Drive a writer and return the `(name, value)` pairs the Rust reader finds.
fn record(program: &str, mode: Mode, drive: impl FnOnce(&mut NimTraceWriter, &std::path::Path)) -> Vec<(String, ValueRecord)> {
    let dir = tempfile::tempdir().expect("tempdir");

    let mut w = NimTraceWriter::new(program, &[], TraceEventsFileFormat::Ctfs);
    w.begin_writing_trace_events(&dir.path().join("e.json")).expect("begin_events");
    w.begin_writing_trace_metadata(&dir.path().join("m.json")).expect("begin_metadata");
    w.begin_writing_trace_paths(&dir.path().join("p.json")).expect("begin_paths");
    if mode == Mode::ColumnAware {
        w.enable_column_aware_steps();
    }

    let source = dir.path().join(format!("{program}.src"));
    if mode == Mode::ColumnAware {
        w.register_path_with_line_lengths(&source, &[]).expect("register_path_with_line_lengths");
    } else {
        w.register_path_with_line_count(&source, 16).expect("register_path_with_line_count");
    }
    w.register_function("callee", &source, Line(1));

    drive(&mut w, &source);

    w.finish_writing_trace_events().expect("finish_events");
    w.finish_writing_trace_metadata().expect("finish_metadata");
    w.finish_writing_trace_paths().expect("finish_paths");
    w.close().expect("close");
    drop(w);

    let ct_path = dir.path().join(format!("{program}.ct"));
    let events = codetracer_trace_reader::ctfs_reader::read_trace_from_ctfs(&ct_path).expect("read back");

    let mut names: Vec<String> = Vec::new();
    let mut paths: Vec<std::path::PathBuf> = Vec::new();
    let mut out: Vec<(String, ValueRecord)> = Vec::new();
    for ev in events {
        match ev {
            TraceLowLevelEvent::Path(p) => paths.push(p),
            TraceLowLevelEvent::VariableName(n) | TraceLowLevelEvent::Variable(n) => names.push(n),
            TraceLowLevelEvent::Value(full) => {
                let name = names
                    .get(full.variable_id.0)
                    .cloned()
                    .unwrap_or_else(|| format!("<unknown:{}>", full.variable_id.0));
                out.push((name, full.value));
            }
            _ => {}
        }
    }
    assert!(
        paths.iter().any(|p| p == &source),
        "the registered source path must read back unchanged; the reader decoded {paths:?}, \
         expected {source:?} among them"
    );
    drop(dir);
    out
}

#[test]
fn a_value_staged_before_the_first_step_is_carried_into_it() {
    let _guard = nim_lock();

    let values = record("nim_value_before_step", Mode::LineOnly, |w, source| {
        let tid = w.ensure_type_id(TypeKind::Int, "Int");
        // The recorder's shape: arguments known before any position is.
        w.arg("_arg0", ValueRecord::Int { i: 7, type_id: tid });
        w.register_call(FunctionId(0), vec![]);
        // Positions arrive afterwards.
        w.register_step(source, Line(1));
        w.register_step(source, Line(2));
        w.register_return(ValueRecord::None { type_id: tid });
    });

    let found: Vec<&ValueRecord> = values.iter().filter(|(n, _)| n == "_arg0").map(|(_, v)| v).collect();
    assert!(
        !found.is_empty(),
        "a value staged before the first step must be carried into it, not dropped. \
         The container is well-formed either way — one value record per step, all \
         decodable — so an empty one is indistinguishable from a step that genuinely \
         had no variables. Decoded values were: {values:?}"
    );
    assert!(
        matches!(found[0], ValueRecord::Int { i: 7, .. }),
        "the carried value must be the one that was staged, got {:?}",
        found[0]
    );
}

#[test]
fn a_value_survives_a_call_and_return_that_precede_every_step() {
    let _guard = nim_lock();

    // The BEAM shape: a whole call completes — arguments staged, call opened,
    // call returned — before the runtime reports any source position at all.
    let values = record("nim_value_call_return_before_step", Mode::LineOnly, |w, source| {
        let tid = w.ensure_type_id(TypeKind::Int, "Int");
        w.arg("_arg0", ValueRecord::Int { i: 7, type_id: tid });
        w.register_call(FunctionId(0), vec![]);
        w.register_return(ValueRecord::None { type_id: tid });
        w.register_step(source, Line(1));
        w.register_step(source, Line(2));
    });

    assert!(
        values.iter().any(|(n, _)| n == "_arg0"),
        "an argument staged before a call that returns before any step must still \
         reach the trace; decoded {values:?}"
    );
}

/// The same shape, for a COLUMN-AWARE writer.
///
/// This arm used to clear the staged values whether or not anything had taken
/// them. Both of its branches can decline — one needs a resolvable definition
/// site, the other needs an existing step to hang a column on — so a recorder
/// whose first event is a call, before any position is known, had its
/// arguments dropped with nothing written and nothing reported.
#[test]
fn a_column_aware_writer_also_carries_values_staged_before_the_first_step() {
    let _guard = nim_lock();

    let values = record("nim_value_before_step_col", Mode::ColumnAware, |w, source| {
        let tid = w.ensure_type_id(TypeKind::Int, "Int");
        w.arg("_arg0", ValueRecord::Int { i: 7, type_id: tid });
        w.register_call(FunctionId(0), vec![]);
        w.register_return(ValueRecord::None { type_id: tid });
        w.register_step(source, Line(1));
        w.register_step(source, Line(2));
    });

    assert!(
        values.iter().any(|(n, _)| n == "_arg0"),
        "a column-aware writer must carry values staged before its first step, \
         not discard them because neither orphan branch could take them yet; \
         decoded {values:?}"
    );
}

#[test]
fn a_value_staged_after_a_step_lands_too() {
    let _guard = nim_lock();

    // The control. Without it, a failure above could be read as "values never
    // land" rather than "values land only when a step came first".
    let values = record("nim_value_after_step", Mode::LineOnly, |w, source| {
        let tid = w.ensure_type_id(TypeKind::Int, "Int");
        w.register_step(source, Line(1));
        w.register_variable_with_full_value("after", ValueRecord::Int { i: 9, type_id: tid });
        w.register_step(source, Line(2));
    });

    assert!(
        values.iter().any(|(n, _)| n == "after"),
        "a value staged after a step must land; decoded {values:?}"
    );
}
