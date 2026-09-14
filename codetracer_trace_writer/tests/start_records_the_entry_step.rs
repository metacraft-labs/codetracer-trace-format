//! `start()` emits the entry step, the `<toplevel>` function, and its opening call.
//!
//! ---------------------------------------------------------------------------
//! WHY THIS TEST EXISTS
//! ---------------------------------------------------------------------------
//!
//! The two reference writers disagreed about what `start(path, line)` does, in opposite
//! directions, and neither was wrong by its own documentation:
//!
//!   * this one — `AbstractTraceWriter::start` — registered `<toplevel>` and its call and emitted
//!     NO STEP;
//!   * the Nim C ABI's `trace_writer_start` emitted the step (its docstring says *"Record the
//!     initial step (entry point)"*) and registered NO `<toplevel>`.
//!
//! A container written by one and read by a consumer written against the other is off by one step,
//! or has no call-tree root, and nothing in the container says so. `codetracer-trace-format-spec`'s
//! `trace-events.md` now pins the contract — all three records, in order — and this test is that
//! contract asserted against this writer.
//!
//! ---------------------------------------------------------------------------
//! WHAT IS ASSERTED, AND WHY EACH PART CAN FAIL
//! ---------------------------------------------------------------------------
//!
//! 1. `start` alone produces a `Step`. This is the half that was missing; before the fix it is 0.
//! 2. …at the path and line `start` was given, not merely *a* step somewhere.
//! 3. `<toplevel>` is interned FIRST, so it is `function_id` 0 — which readers root the call tree
//!    at. Asserting the name without the id would pass for a writer that interned a user function
//!    first and pushed the root to id 1.
//! 4. Its `Call` is emitted, and before the step: the entry step belongs INSIDE the root frame, and
//!    a step emitted before the call would sit outside every frame.
//! 5. **The counting control.** A recorder that calls `start` and then `register_step` N times
//!    produces N + 1 steps. Asserting only "there is a step" would pass for a writer that emitted
//!    the entry step and dropped every later one; asserting the arithmetic is what makes the extra
//!    step an OFFSET rather than a coincidence.

use std::path::Path;

use codetracer_trace_types::{Line, TOP_LEVEL_FUNCTION_ID, TraceLowLevelEvent};
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::non_streaming_trace_writer::NonStreamingTraceWriter;

const SOURCE: &str = "/srv/entry.rb";
const ENTRY_LINE: i64 = 7;

fn steps(events: &[TraceLowLevelEvent]) -> Vec<(usize, i64)> {
    events
        .iter()
        .filter_map(|e| match e {
            TraceLowLevelEvent::Step(s) => Some((s.path_id.0, s.line.0)),
            _ => None,
        })
        .collect()
}

fn writer() -> NonStreamingTraceWriter {
    NonStreamingTraceWriter::new("entry-step-probe", &[])
}

#[test]
fn start_emits_the_entry_step_at_the_position_it_was_given() {
    let mut w = writer();
    w.start(Path::new(SOURCE), Line(ENTRY_LINE));

    let found = steps(&w.events);
    assert_eq!(
        found.len(),
        1,
        "start() emitted {} steps; the spec says it emits exactly one, the entry step. \
         Events were: {:?}",
        found.len(),
        w.events
    );
    assert_eq!(
        found[0].1, ENTRY_LINE,
        "the entry step is at line {}, not the line start() was given ({ENTRY_LINE})",
        found[0].1
    );
}

#[test]
fn start_interns_toplevel_first_and_calls_it_before_the_step() {
    let mut w = writer();
    w.start(Path::new(SOURCE), Line(ENTRY_LINE));

    let mut toplevel_at = None;
    let mut call_at = None;
    let mut step_at = None;
    for (i, e) in w.events.iter().enumerate() {
        match e {
            TraceLowLevelEvent::Function(f) if f.name == "<toplevel>" && toplevel_at.is_none() => toplevel_at = Some(i),
            TraceLowLevelEvent::Call(c) if call_at.is_none() => {
                assert_eq!(
                    c.function_id, TOP_LEVEL_FUNCTION_ID,
                    "the first Call names function {:?}, not the toplevel's id {:?}; readers root \
                     the call tree at function_id 0",
                    c.function_id, TOP_LEVEL_FUNCTION_ID
                );
                call_at = Some(i)
            }
            TraceLowLevelEvent::Step(_) if step_at.is_none() => step_at = Some(i),
            _ => {}
        }
    }

    let toplevel_at = toplevel_at.expect("start() interned no <toplevel> function");
    let call_at = call_at.expect("start() emitted no Call");
    let step_at = step_at.expect("start() emitted no Step — this is the half that was missing");

    assert!(
        toplevel_at < call_at,
        "the <toplevel> function must be interned before the call that names it"
    );
    assert!(
        call_at < step_at,
        "the entry step must come AFTER the opening call, so it sits inside the root frame \
         rather than outside every frame"
    );
}

#[test]
fn a_recording_holds_one_more_step_than_the_recorder_emitted() {
    // THE COUNTING CONTROL. "There is a step" would pass for a writer that emitted the entry step
    // and dropped every later one; the arithmetic is what makes the extra step an offset.
    for emitted in [0usize, 1, 5] {
        let mut w = writer();
        w.start(Path::new(SOURCE), Line(ENTRY_LINE));
        for i in 0..emitted {
            w.register_step(Path::new(SOURCE), Line(100 + i as i64));
        }
        let found = steps(&w.events);
        assert_eq!(
            found.len(),
            emitted + 1,
            "a recorder that emitted {emitted} steps produced {} in the container; the spec says \
             start() adds the entry step, so it should be {}",
            found.len(),
            emitted + 1
        );
    }
}
