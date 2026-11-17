//! A column-aware container reads back to the lines its steps were recorded
//! at, from either writer.
//!
//! In a column-aware trace (`meta.dat` bit 4) a step's `global_position_index`
//! addresses a `(line, column)` pair in a space where a file with a per-line
//! table is `sum(line_lengths)` addresses wide and a file without one is the
//! conventional `DEFAULT_LINES_PER_FILE` (`trace-events.md` §"Source Location
//! Addressing"). The split-stream reader resolved every address through the
//! line-only space instead, so a step at column 9 of line 1 came back as
//! line 9, and every file after the first was placed at the wrong base.
//!
//! The fixture mixes a tabled file with long lines, a second tabled file, and
//! an untabled file, and steps at columns other than 1 so a column read as a
//! line cannot pass. Both writers' containers are read back by the Rust
//! split-stream reader and compared with the recorded (path, line) pairs.
//!
//! No mocks: real writers, the real reader.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use codetracer_ctfs::CtfsReader;
use codetracer_trace_types::{Line, TraceLowLevelEvent};
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::trace_writer::TraceWriter;
use codetracer_trace_writer_nim::{NimTraceWriter, TraceEventsFileFormat};

static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

fn nim_lock() -> MutexGuard<'static, ()> {
    NIM_TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

/// `(file, line, column)`; file 2 has no per-line table.
const STEPS: &[(usize, i64, i64)] = &[(0, 1, 1), (0, 1, 9), (1, 1, 1), (1, 2, 5), (1, 3, 2), (2, 42, 1), (0, 2, 3)];

fn paths() -> [PathBuf; 3] {
    [PathBuf::from("/src/a.js"), PathBuf::from("/src/b.js"), PathBuf::from("/src/c.js")]
}

fn tables() -> [Vec<u32>; 3] {
    [vec![12, 6], vec![64, 70, 3], vec![]]
}

fn write(nim: bool, dir: &Path) -> PathBuf {
    let ps = paths();
    let ts = tables();
    if nim {
        let mut w = NimTraceWriter::new("col_read", &[], TraceEventsFileFormat::Ctfs);
        w.begin_writing_trace_events(&dir.join("e.json")).unwrap();
        w.begin_writing_trace_metadata(&dir.join("m.json")).unwrap();
        w.begin_writing_trace_paths(&dir.join("p.json")).unwrap();
        w.enable_column_aware_steps();
        for (p, t) in ps.iter().zip(ts.iter()) {
            w.register_path_with_line_lengths(p, t).unwrap();
        }
        w.start(&ps[0], Line(1));
        for (f, l, c) in STEPS {
            w.register_step_with_column(&ps[*f], Line(*l), Some(Line(*c)));
        }
        w.finish_writing_trace_events().unwrap();
        w.finish_writing_trace_metadata().unwrap();
        w.finish_writing_trace_paths().unwrap();
        w.close().expect("nim close");
        drop(w);
        dir.join("col_read.ct")
    } else {
        let mut w = CtfsTraceWriter::new("col_read", &[]);
        w.enable_column_aware_steps();
        let out = dir.join("col_read");
        TraceWriter::begin_writing_trace_events(&mut w, &out).unwrap();
        for (p, t) in ps.iter().zip(ts.iter()) {
            w.register_path_with_line_lengths(p, t);
        }
        TraceWriter::start(&mut w, &ps[0], Line(1));
        for (f, l, c) in STEPS {
            AbstractTraceWriter::register_step_with_column(&mut w, &ps[*f], Line(*l), Some(Line(*c)));
        }
        TraceWriter::finish_writing_trace_events(&mut w).expect("rust finish");
        out.with_extension("ct")
    }
}

#[test]
fn column_aware_steps_read_back_at_their_recorded_lines_from_both_writers() {
    let _g = nim_lock();
    // The entry step `start` emits, then the recorded steps.
    let mut want: Vec<(usize, i64)> = vec![(0, 1)];
    want.extend(STEPS.iter().map(|(f, l, _)| (*f, *l)));
    for nim in [true, false] {
        let label = if nim { "nim" } else { "rust" };
        let dir = tempfile::tempdir().unwrap();
        let ct = write(nim, dir.path());
        let mut r = CtfsReader::open(&ct).unwrap();
        let events = codetracer_trace_reader::split_stream_reader::read_trace_from_split_streams(&mut r)
            .unwrap_or_else(|e| panic!("{label}: split-stream read failed: {e}"));
        let got: Vec<(usize, i64)> = events
            .iter()
            .filter_map(|e| match e {
                TraceLowLevelEvent::Step(s) => Some((s.path_id.0, s.line.0)),
                _ => None,
            })
            .collect();
        assert_eq!(got, want, "{label}: steps resolved to the wrong (path, line)");
    }
}

/// The column of each step, which the event sequence has no field for, is
/// reported beside it — the same columns from either writer.
#[test]
fn column_aware_steps_report_their_recorded_columns_from_both_writers() {
    let _g = nim_lock();
    // The entry step `start` emits is at column 1.
    let mut want: Vec<u64> = vec![1];
    want.extend(STEPS.iter().map(|(_, _, c)| *c as u64));
    for nim in [true, false] {
        let label = if nim { "nim" } else { "rust" };
        let dir = tempfile::tempdir().unwrap();
        let ct = write(nim, dir.path());
        let mut r = CtfsReader::open(&ct).unwrap();
        let t = codetracer_trace_reader::split_stream_reader::read_trace_with_details(&mut r)
            .unwrap_or_else(|e| panic!("{label}: split-stream read failed: {e}"));
        let step_events: Vec<usize> = t
            .events
            .iter()
            .enumerate()
            .filter_map(|(i, e)| matches!(e, TraceLowLevelEvent::Step(_)).then_some(i))
            .collect();
        let at: Vec<usize> = t.details.step_columns.iter().map(|c| c.event_index).collect();
        assert_eq!(at, step_events, "{label}: one column per Step event");
        let got: Vec<u64> = t.details.step_columns.iter().map(|c| c.column).collect();
        assert_eq!(got, want, "{label}: steps report the wrong columns");
    }
}
