//! Records the Nim backend cannot persist must be VISIBLY dropped, never
//! silently dropped.
//!
//! # Purpose
//!
//! Guard one defect class: **silent incompleteness.**  A dozen
//! [`NimTraceWriter`] operations have no counterpart in the Nim C API —
//! `drop_variables`, `drop_variable`, `register_compound_value`,
//! `register_cell_value`, `assign_compound_item`, `assign_cell`,
//! `register_variable`, `bind_variable`, `assign`, `register_asm`,
//! `drop_last_step`.  Each was a bare no-op carrying the comment
//! `// Not exposed in the Nim C API — no-op`.
//!
//! `add_event` dispatches `TraceLowLevelEvent::DropVariables` straight into
//! `drop_variables`, so a recorder that emitted those records produced a
//! trace with none of them in it — and said nothing.  The recording came out
//! quietly incomplete and was indistinguishable, to the user and to any
//! downstream assertion, from a complete one.  That is the same lie as a
//! test that reports success while doing nothing, moved one layer down: the
//! artifact claims to be a faithful recording and is not.
//!
//! # What this test pins
//!
//! 1. A discarded record is COUNTED and attributable to the operation that
//!    lost it (`discard_is_counted_and_named`).
//! 2. The `add_event` dispatch path — the one recorders actually use — is
//!    covered, not just the direct method call
//!    (`add_event_drop_variables_is_counted_not_swallowed`).
//! 3. An operation the backend really does support does NOT get counted, so
//!    the counter cannot pass by over-reporting
//!    (`supported_operations_are_not_counted_as_discards`).
//! 4. Strict mode turns the discard into a hard error
//!    (`strict_mode_refuses_to_produce_an_incomplete_trace`).
//! 5. The strict-mode env parsing is exactly as documented
//!    (`strict_env_parsing_is_conservative`).
//!
//! # What this test does NOT claim
//!
//! It does not claim the records are persisted — they are not.  Persisting
//! them needs new entry points in `codetracer-trace-format-nim`'s C API and
//! matching encoder support on the Nim side; that is a format-surface change
//! and is deliberately out of scope here.  What changed is that the loss is
//! now stated instead of hidden.  If those entry points are added later, the
//! assertions below are what will show the counters going to zero.
//!
//! # Mocking policy justification (workspace AGENTS.md)
//!
//! **Nothing is mocked.**  Every assertion runs against a REAL
//! [`NimTraceWriter`] driving the REAL Nim static library through the REAL
//! FFI boundary, writing to a REAL temporary directory — the same object a
//! recorder constructs in production.  A fake writer would be worthless
//! here: the entire question is what the actual Rust↔Nim wrapper does with
//! a record the Nim side cannot take, so substituting anything for that
//! wrapper would test the substitute.
//!
//! Strict mode is exercised through [`NimTraceWriter::set_strict`] rather
//! than by setting `CODETRACER_NIM_TRACE_WRITER_STRICT` in the process
//! environment.  That is not a mock — it is the same field the environment
//! latches into — and it avoids a race: the Nim runtime forces every test in
//! this binary through one process (see `NIM_TEST_LOCK`), so a test mutating
//! a global env var would change behaviour under its neighbours' feet.

use std::path::Path;
use std::sync::Mutex;

/// Every byte the writer produced under `dir`, concatenated.
///
/// The two tests below assert that a variable name reaches the trace. They
/// deliberately do NOT name the output file: which container the Nim writer
/// finally emits (and under what name) is its business, and a test that
/// hardcodes the filename fails for the wrong reason the moment that changes
/// — a false red is only marginally better than a false green. Scanning
/// everything the writer wrote asks the question that actually matters.
fn all_bytes_written_under(dir: &Path) -> Vec<u8> {
    fn walk(dir: &Path, out: &mut Vec<u8>) {
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else if let Ok(mut bytes) = std::fs::read(&path) {
                out.append(&mut bytes);
                out.push(b'\n');
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, &mut out);
    assert!(
        !out.is_empty(),
        "the writer produced no files at all under {}, so the assertions \
         below would prove nothing",
        dir.display()
    );
    out
}

use codetracer_trace_types::{Line, Place, TraceLowLevelEvent, TypeId, ValueRecord, VariableId};
use codetracer_trace_writer_nim::{strict_from_env_value, NimTraceWriter, TraceEventsFileFormat};

/// The Nim runtime is **not** thread-safe — its global state lives behind a
/// single lock.  Serialize every test in this binary, exactly as
/// `tests/thread_events.rs` does.
static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

/// Take the Nim lock, tolerating poisoning.
///
/// `strict_mode_refuses_to_produce_an_incomplete_trace` panics BY DESIGN
/// while holding this lock, which poisons it.  A plain `.unwrap()` would then
/// make every later test in this binary fail for an unrelated reason — and a
/// cascade of confusing failures is its own kind of dishonest reporting.
fn nim_lock() -> std::sync::MutexGuard<'static, ()> {
    NIM_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn make_writer(program_basename: &str) -> (tempfile::TempDir, NimTraceWriter) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut writer = NimTraceWriter::new(program_basename, &[], TraceEventsFileFormat::Binary);

    let metadata_path = dir.path().join("trace_metadata.json");
    writer.begin_writing_trace_metadata(&metadata_path).expect("begin_metadata");
    writer.finish_writing_trace_metadata().expect("finish_metadata");

    let events_path = dir.path().join("trace.json");
    writer.begin_writing_trace_events(&events_path).expect("begin_events");

    let paths_path = dir.path().join("trace_paths.json");
    writer.begin_writing_trace_paths(&paths_path).expect("begin_paths");
    writer.finish_writing_trace_paths().expect("finish_paths");

    (dir, writer)
}

/// A record the backend cannot persist is counted, and the count names the
/// operation that lost it.
#[test]
fn discard_is_counted_and_named() {
    let _guard = nim_lock();
    let (_dir, mut writer) = make_writer("discard_counted");

    assert!(
        writer.discarded_record_counts().is_empty(),
        "a fresh writer must not claim to have discarded anything"
    );

    writer.drop_variables(&["a".to_string(), "b".to_string()]);
    writer.drop_variable("c");
    writer.register_compound_value(Place(0), ValueRecord::Int { i: 1, type_id: TypeId(0) });

    let counts = writer.discarded_record_counts();
    assert_eq!(
        counts.get("drop_variables").copied(),
        Some(1),
        "a `drop_variables` call that persists nothing must be counted; \
         counts were {counts:?}"
    );
    assert_eq!(counts.get("drop_variable").copied(), Some(1), "{counts:?}");
    assert_eq!(counts.get("register_compound_value").copied(), Some(1), "{counts:?}");
    assert_eq!(writer.discarded_record_total(), 3);
}

/// The path recorders actually use.  `add_event(DropVariables(..))` must not
/// be able to vanish.
#[test]
fn add_event_drop_variables_is_counted_not_swallowed() {
    let _guard = nim_lock();
    let (_dir, mut writer) = make_writer("discard_add_event");

    writer.start(Path::new("/tmp/discard_add_event.rb"), Line(1));
    writer.register_step(Path::new("/tmp/discard_add_event.rb"), Line(2));

    writer.add_event(TraceLowLevelEvent::DropVariables(vec![VariableId(0), VariableId(1)]));

    assert_eq!(
        writer.discarded_record_counts().get("drop_variables").copied(),
        Some(1),
        "`add_event(DropVariables)` dispatches into `drop_variables`, which \
         cannot persist the record.  Before this was counted, the record was \
         dropped and the trace looked complete: counts were {:?}",
        writer.discarded_record_counts()
    );
}

/// The counter must not over-report: an operation the backend genuinely
/// supports has to leave it untouched, or "zero discards" would stop meaning
/// "complete trace".
#[test]
fn supported_operations_are_not_counted_as_discards() {
    let _guard = nim_lock();
    let (_dir, mut writer) = make_writer("discard_supported");

    writer.start(Path::new("/tmp/discard_supported.rb"), Line(1));
    writer.register_step(Path::new("/tmp/discard_supported.rb"), Line(2));
    writer.register_path(Path::new("/tmp/discard_supported.rb"));
    // Genuinely handled inside Nim rather than dropped.
    writer.register_variable_name("x");

    assert!(
        writer.discarded_record_counts().is_empty(),
        "supported operations must not be reported as losses; counts were {:?}",
        writer.discarded_record_counts()
    );
}

/// `register_variable_name` is left uncounted on the grounds that it loses
/// nothing.  Check that claim rather than trusting it: the name must survive
/// into the trace, which it does because `add_event`'s `VariableName` arm
/// mirrors it into the writer's variable table and every later record that
/// needs it passes it to the FFI by name.
#[test]
fn register_variable_name_is_uncounted_because_the_name_really_survives() {
    let _guard = nim_lock();
    let (dir, mut writer) = make_writer("varname_survives");

    let program = dir.path().join("varname_survives.rb");
    writer.start(&program, Line(1));
    writer.register_path(&program);
    writer.register_step(&program, Line(2));

    // The dispatch a recorder actually drives.
    writer.add_event(TraceLowLevelEvent::VariableName("interesting_name".to_string()));
    writer.add_event(TraceLowLevelEvent::Value(
        codetracer_trace_types::FullValueRecord {
            variable_id: VariableId(0),
            value: ValueRecord::Int { i: 7, type_id: TypeId(0) },
        },
    ));

    assert!(
        writer.discarded_record_counts().is_empty(),
        "neither VariableName nor Value may be counted as a discard; got {:?}",
        writer.discarded_record_counts()
    );
    writer.finish_writing_trace_events().expect("finish_events");
    writer.close().expect("close");

    // The name is only genuinely safe if it reaches the written trace.  If
    // `register_variable_name` being a no-op DID lose it, the value record
    // would have been filed under the synthetic `var_0` fallback instead.
    let written = all_bytes_written_under(dir.path());
    let haystack = String::from_utf8_lossy(&written);
    assert!(
        haystack.contains("interesting_name"),
        "the variable name did not reach the trace, so treating \
         `register_variable_name` as a no-op DOES lose data and it must be \
         counted as a discard"
    );
}

/// `register_full_value` is likewise uncounted.  That is only honest if the
/// method actually persists the value — it is public on the `TraceWriter`
/// trait, so an external caller can reach it directly without going through
/// `add_event`.
#[test]
fn register_full_value_persists_rather_than_silently_dropping() {
    let _guard = nim_lock();
    let (dir, mut writer) = make_writer("full_value_persists");

    let program = dir.path().join("full_value_persists.rb");
    writer.start(&program, Line(1));
    writer.register_path(&program);
    writer.register_step(&program, Line(2));
    writer.add_event(TraceLowLevelEvent::VariableName("direct_call_name".to_string()));

    // The direct call, NOT via add_event.
    writer.register_full_value(
        VariableId(0),
        ValueRecord::Int { i: 1234, type_id: TypeId(0) },
    );

    assert!(
        writer.discarded_record_counts().is_empty(),
        "register_full_value must not report a discard; got {:?}",
        writer.discarded_record_counts()
    );
    writer.finish_writing_trace_events().expect("finish_events");
    writer.close().expect("close");

    let written = all_bytes_written_under(dir.path());
    let haystack = String::from_utf8_lossy(&written);
    assert!(
        haystack.contains("direct_call_name"),
        "a direct `register_full_value` call did not reach the trace.  It is \
         a public trait method, so leaving it a no-op means an external \
         caller loses values with `discarded_record_counts()` still empty — \
         i.e. \"zero discards\" stops meaning \"complete trace\""
    );
}

/// The tally must survive `close()`.
///
/// The close-time summary suppresses its own repeat from `Drop`.  If it did
/// that by CLEARING the counters — which is the obvious way to write it — then
/// the natural completeness check a caller would write,
/// `writer.close()?; assert!(writer.discarded_record_counts().is_empty())`,
/// would pass however much had been thrown away.  A self-erasing completeness
/// report is the same defect this file exists to prevent.
#[test]
fn the_discard_tally_survives_close() {
    let _guard = nim_lock();
    let (_dir, mut writer) = make_writer("discard_survives_close");

    writer.drop_variables(&["a".to_string()]);
    assert_eq!(writer.discarded_record_total(), 1, "precondition");

    writer.finish_writing_trace_events().expect("finish_events");
    writer.close().expect("close");

    assert_eq!(
        writer.discarded_record_total(),
        1,
        "the discard tally was reset by `close()`, so a caller asking \
         \"was this trace complete?\" after closing gets told \"yes\".  \
         counts were {:?}",
        writer.discarded_record_counts()
    );
    assert_eq!(
        writer.discarded_record_counts().get("drop_variables").copied(),
        Some(1),
        "the per-operation attribution must survive close() too"
    );
}

/// Strict mode refuses to produce a knowingly incomplete trace.
#[test]
#[should_panic(expected = "cannot persist a `drop_variables` record")]
fn strict_mode_refuses_to_produce_an_incomplete_trace() {
    let _guard = nim_lock();
    let (_dir, mut writer) = make_writer("discard_strict");
    writer.set_strict(true);
    writer.drop_variables(&["a".to_string()]);
}

/// The documented spellings, and nothing else, enable strict mode.
#[test]
fn strict_env_parsing_is_conservative() {
    assert!(!strict_from_env_value(None), "unset must not be strict");
    assert!(strict_from_env_value(Some("1")));
    assert!(strict_from_env_value(Some("true")));
    assert!(strict_from_env_value(Some("TRUE")));
    assert!(!strict_from_env_value(Some("0")));
    assert!(!strict_from_env_value(Some("")));
    assert!(!strict_from_env_value(Some("yes")));
}
