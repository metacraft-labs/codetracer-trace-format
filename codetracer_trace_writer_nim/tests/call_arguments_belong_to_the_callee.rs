//! A call's arguments are the callee's variables, not the caller's.
//!
//! # The shape under test
//!
//! A line-driven recorder (the Python recorder is the one that found this) has
//! the caller's step still OPEN when it learns that a call is starting: the
//! line event for `rest = factorial(n - 1)` has been reported, and the call's
//! entry arrives before any further position does. The recorder then stages
//! the callee's arguments with `NimTraceWriter::arg` and opens the call.
//!
//! `trace-events.md` §"Recorder Integration — Staging Values" says values that
//! are not visible at the open step attach to the next step the writer emits.
//! For a call's arguments that is the callee's first step. Attaching them to
//! the open caller step instead gives the caller two values for one name in a
//! recursive call (`n = 5` and `n = 4` on the caller's line), and in any call
//! shows the caller a variable it does not have.
//!
//! # What this asserts
//!
//! 1. The caller's open step carries only the caller's own variables.
//! 2. The callee's first step carries the arguments.
//! 3. The call record still carries the arguments (the call-trace pane's
//!    `f(n=4)` rendering).
//! 4. The control: when no step is open at `arg()` time, the arguments still
//!    reach the callee's first step.
//!
//! No mocks: every arm drives the real Nim writer through its FFI and reads the
//! container back with the Rust reader.

use std::sync::{Mutex, MutexGuard, PoisonError};

use codetracer_trace_types::{Line, TraceLowLevelEvent, TypeKind, ValueRecord};
use codetracer_trace_writer_nim::{NimTraceWriter, TraceEventsFileFormat};

static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

fn nim_lock() -> MutexGuard<'static, ()> {
    NIM_TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One decoded step: its line and the `(name, int)` values attached to it.
#[derive(Debug)]
struct DecodedStep {
    line: i64,
    values: Vec<(String, i64)>,
}

struct Decoded {
    steps: Vec<DecodedStep>,
    call_args: Vec<Vec<(String, i64)>>,
}

fn int_of(v: &ValueRecord) -> i64 {
    match v {
        ValueRecord::Int { i, .. } => *i,
        other => panic!("expected an Int value, got {other:?}"),
    }
}

fn record(program: &str, drive: impl FnOnce(&mut NimTraceWriter, &std::path::Path)) -> Decoded {
    let dir = tempfile::tempdir().expect("tempdir");

    let mut w = NimTraceWriter::new(program, &[], TraceEventsFileFormat::Ctfs);
    w.begin_writing_trace_events(&dir.path().join("e.json")).expect("begin_events");
    w.begin_writing_trace_metadata(&dir.path().join("m.json")).expect("begin_metadata");
    w.begin_writing_trace_paths(&dir.path().join("p.json")).expect("begin_paths");

    let source = dir.path().join(format!("{program}.src"));
    w.register_path_with_line_count(&source, 16).expect("register_path_with_line_count");

    drive(&mut w, &source);

    w.finish_writing_trace_events().expect("finish_events");
    w.finish_writing_trace_metadata().expect("finish_metadata");
    w.finish_writing_trace_paths().expect("finish_paths");
    w.close().expect("close");
    drop(w);

    let ct_path = dir.path().join(format!("{program}.ct"));
    let events = codetracer_trace_reader::ctfs_reader::read_trace_from_ctfs(&ct_path).expect("read back");

    let mut names: Vec<String> = Vec::new();
    let mut steps: Vec<DecodedStep> = Vec::new();
    let mut call_args: Vec<Vec<(String, i64)>> = Vec::new();
    for ev in events {
        match ev {
            TraceLowLevelEvent::VariableName(n) | TraceLowLevelEvent::Variable(n) => names.push(n),
            TraceLowLevelEvent::Step(s) => steps.push(DecodedStep {
                line: s.line.0,
                values: Vec::new(),
            }),
            TraceLowLevelEvent::Value(full) => {
                let name = names
                    .get(full.variable_id.0)
                    .cloned()
                    .unwrap_or_else(|| format!("<unknown:{}>", full.variable_id.0));
                steps
                    .last_mut()
                    .expect("a value decoded before any step")
                    .values
                    .push((name, int_of(&full.value)));
            }
            TraceLowLevelEvent::Call(call) => call_args.push(
                call.args
                    .iter()
                    .map(|a| {
                        let name = names
                            .get(a.variable_id.0)
                            .cloned()
                            .unwrap_or_else(|| format!("<unknown:{}>", a.variable_id.0));
                        (name, int_of(&a.value))
                    })
                    .collect(),
            ),
            _ => {}
        }
    }
    drop(dir);
    Decoded { steps, call_args }
}

fn step_at(decoded: &Decoded, line: i64) -> &DecodedStep {
    decoded
        .steps
        .iter()
        .find(|s| s.line == line)
        .unwrap_or_else(|| panic!("no step at line {line}; decoded steps: {:?}", decoded.steps))
}

/// `factorial(5)` at the moment it calls `factorial(4)`: the caller's step on
/// line 4 is open, holding `n = 5`, when the callee's argument `n = 4` arrives.
#[test]
fn an_argument_staged_while_the_caller_step_is_open_goes_to_the_callee() {
    let _guard = nim_lock();

    let decoded = record("args_open_caller_step", |w, source| {
        let tid = w.ensure_type_id(TypeKind::Int, "Int");
        let fid = w.ensure_function_id("factorial", source, Line(1));
        // The caller's line, with its own `n`.
        w.register_step(source, Line(4));
        w.register_variable_with_full_value("n", ValueRecord::Int { i: 5, type_id: tid });
        // The callee's argument, staged while that step is still open.
        let arg = w.arg("n", ValueRecord::Int { i: 4, type_id: tid });
        w.register_call(fid, vec![arg]);
        // The callee's definition line, then its body.
        w.register_step(source, Line(1));
        w.register_step(source, Line(2));
        w.register_return(ValueRecord::Int { i: 24, type_id: tid });
    });

    let caller = step_at(&decoded, 4);
    assert_eq!(
        caller.values,
        vec![("n".to_string(), 5)],
        "the caller's step must carry only the caller's own `n`; the callee's argument \
         was attached to it. Decoded steps: {:?}",
        decoded.steps
    );

    let callee_entry = step_at(&decoded, 1);
    assert_eq!(
        callee_entry.values,
        vec![("n".to_string(), 4)],
        "the callee's first step must carry its argument. Decoded steps: {:?}",
        decoded.steps
    );

    assert!(
        decoded.call_args.iter().any(|args| args == &vec![("n".to_string(), 4)]),
        "the call record must still carry the argument; decoded call args: {:?}",
        decoded.call_args
    );
}

/// The control: the caller's step was already flushed (by a return) when the
/// argument is staged, so there is no open step to misattach it to.
#[test]
fn an_argument_staged_with_no_step_open_also_goes_to_the_callee() {
    let _guard = nim_lock();

    let decoded = record("args_no_open_step", |w, source| {
        let tid = w.ensure_type_id(TypeKind::Int, "Int");
        let helper = w.ensure_function_id("helper", source, Line(10));
        let fid = w.ensure_function_id("factorial", source, Line(1));
        w.register_step(source, Line(4));
        w.register_call(helper, vec![]);
        w.register_step(source, Line(10));
        w.register_return(ValueRecord::Int { i: 0, type_id: tid });
        let arg = w.arg("n", ValueRecord::Int { i: 4, type_id: tid });
        w.register_call(fid, vec![arg]);
        w.register_step(source, Line(1));
        w.register_step(source, Line(2));
        w.register_return(ValueRecord::Int { i: 24, type_id: tid });
    });

    let callee_entry = step_at(&decoded, 1);
    assert_eq!(
        callee_entry.values,
        vec![("n".to_string(), 4)],
        "the callee's first step must carry its argument. Decoded steps: {:?}",
        decoded.steps
    );
    assert!(
        step_at(&decoded, 10).values.is_empty(),
        "the argument must not land on the helper's step. Decoded steps: {:?}",
        decoded.steps
    );
}
