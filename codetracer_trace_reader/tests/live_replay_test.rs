//! Integration test: read trace events while recording is still in progress.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;

use codetracer_trace_reader::streaming_ctfs_reader::StreamingCtfsReader;
use codetracer_trace_types::{Line, PathId, StepRecord, TraceLowLevelEvent};
use codetracer_trace_writer::abstract_trace_writer::AbstractTraceWriter;
use codetracer_trace_writer::ctfs_writer::CtfsTraceWriter;
use codetracer_trace_writer::trace_writer::TraceWriter;

#[test]
fn test_live_replay_during_recording() {
    let dir = tempfile::tempdir().unwrap();
    let ct_path = dir.path().join("live_trace");

    let recording_done = Arc::new(AtomicBool::new(false));
    let recording_done_clone = recording_done.clone();
    let ct_path_writer = ct_path.clone();
    // The writer adds .ct extension automatically.
    let ct_path_reader = ct_path.with_extension("ct");

    // Writer thread: records events with streaming flushes.
    let writer_handle = thread::spawn(move || {
        // Use a small flush threshold (1 KiB) to force frequent flushes
        // so the reader can observe partial progress.
        let mut writer = CtfsTraceWriter::with_flush_threshold("test", &[], 1024);
        TraceWriter::begin_writing_trace_events(&mut writer, &ct_path_writer).unwrap();

        let mut events_written = 0usize;
        for batch in 0..10 {
            // Write 20 step events per batch.
            for i in 0..20 {
                let event = TraceLowLevelEvent::Step(StepRecord {
                    path_id: PathId(0),
                    line: Line((batch * 20 + i) as i64),
                });
                AbstractTraceWriter::add_event(&mut writer, event);
                events_written += 1;
            }
            // Small delay between batches to let reader catch up.
            thread::sleep(Duration::from_millis(50));
        }

        TraceWriter::finish_writing_trace_events(&mut writer).unwrap();
        recording_done_clone.store(true, Ordering::Release);
        events_written
    });

    // Reader thread: polls for new events while recording is in progress.
    let reader_handle = thread::spawn(move || {
        // Wait for the writer to create the file and write some initial data.
        thread::sleep(Duration::from_millis(200));

        let mut reader = StreamingCtfsReader::open(&ct_path_reader).unwrap();
        let mut total_events = 0usize;
        let mut polls = 0usize;
        let mut saw_events_before_done = false;

        loop {
            match reader.poll_new_events() {
                Ok(new_events) => {
                    if !new_events.is_empty() {
                        total_events += new_events.len();
                        if !recording_done.load(Ordering::Acquire) {
                            saw_events_before_done = true;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("poll_new_events error (poll {}): {}", polls, e);
                }
            }

            polls += 1;

            if reader.check_finalized().unwrap_or(false) {
                // Do one final poll to get remaining events.
                if let Ok(final_events) = reader.poll_new_events() {
                    total_events += final_events.len();
                }
                break;
            }

            if polls > 400 {
                panic!("Reader timed out after {} polls", polls);
            }

            thread::sleep(Duration::from_millis(25));
        }

        (total_events, saw_events_before_done, polls)
    });

    let events_written = writer_handle.join().unwrap();
    let (events_read, saw_early, polls) = reader_handle.join().unwrap();

    eprintln!(
        "Live replay test: {} events written, {} events read, {} polls, saw_early={}",
        events_written, events_read, polls, saw_early
    );

    assert_eq!(
        events_read, events_written,
        "Reader should see all {} events, but saw {}",
        events_written, events_read
    );
    assert!(
        saw_early,
        "Reader should have seen events BEFORE recording finished"
    );
}
