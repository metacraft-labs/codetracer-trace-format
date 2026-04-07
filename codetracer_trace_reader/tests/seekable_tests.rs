//! Integration tests for the streaming seekable Zstd reader and writer.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use codetracer_trace_types::*;

use codetracer_trace_reader::seekable_reader;
use codetracer_trace_writer::streaming_writer::{StreamingTraceWriter, DEFAULT_FRAME_SIZE};

/// Helper: generate N distinct test events.
fn make_test_events(n: usize) -> Vec<TraceLowLevelEvent> {
    (0..n)
        .map(|i| {
            TraceLowLevelEvent::Step(StepRecord {
                path_id: PathId(0),
                line: Line(i as i64 + 1),
            })
        })
        .collect()
}

/// Helper: write events using the streaming writer and return the path + offsets.
fn write_trace_file(
    dir: &tempfile::TempDir,
    events: &[TraceLowLevelEvent],
    frame_size: Option<u32>,
) -> (std::path::PathBuf, Vec<codetracer_trace_writer::streaming_writer::EventOffset>) {
    let path = dir.path().join("trace.events");
    let mut writer = match frame_size {
        Some(fs) => StreamingTraceWriter::with_frame_size(fs),
        None => StreamingTraceWriter::new(),
    };
    writer.begin(&path).unwrap();
    for event in events {
        writer.write_event(event).unwrap();
    }
    writer.finish().unwrap();
    let offsets = writer.event_offsets().to_vec();
    (path, offsets)
}

#[test]
fn test_read_all_events_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let events = make_test_events(50);
    let (path, _offsets) = write_trace_file(&dir, &events, None);

    let mut file = File::open(&path).unwrap();
    let read_back = seekable_reader::read_all_events(&mut file).unwrap();

    assert_eq!(read_back.len(), events.len());
    for (orig, read) in events.iter().zip(read_back.iter()) {
        match (orig, read) {
            (
                TraceLowLevelEvent::Step(StepRecord { line: l1, .. }),
                TraceLowLevelEvent::Step(StepRecord { line: l2, .. }),
            ) => assert_eq!(l1, l2),
            _ => panic!("Event type mismatch"),
        }
    }
}

#[test]
fn test_seekable_read_at_offset() {
    let dir = tempfile::tempdir().unwrap();
    let events = make_test_events(100);
    let (path, offsets) = write_trace_file(&dir, &events, None);

    // Read events starting from event 50
    let offset_50 = offsets[50].decompressed_offset;
    let offset_60 = offsets[60].decompressed_offset;

    let mut file = File::open(&path).unwrap();
    let subset = seekable_reader::read_events_at_offset(&mut file, offset_50, offset_60).unwrap();

    assert_eq!(subset.len(), 10, "Expected exactly 10 events in range [50..60)");
    for (i, event) in subset.iter().enumerate() {
        match event {
            TraceLowLevelEvent::Step(StepRecord { line, .. }) => {
                assert_eq!(*line, Line((50 + i) as i64 + 1));
            }
            _ => panic!("Expected Step event"),
        }
    }
}

#[test]
fn test_seekable_read_last_events() {
    let dir = tempfile::tempdir().unwrap();
    let events = make_test_events(100);
    // Write using the streaming writer to get offset tracking
    let path = dir.path().join("trace.events");
    let mut writer = StreamingTraceWriter::new();
    writer.begin(&path).unwrap();
    for event in &events {
        writer.write_event(event).unwrap();
    }
    let total_decompressed = writer.current_decompressed_offset();
    let offset_95 = writer.event_offsets()[95].decompressed_offset;
    writer.finish().unwrap();
    let mut file = File::open(&path).unwrap();
    let last5 =
        seekable_reader::read_events_at_offset(&mut file, offset_95, total_decompressed)
            .unwrap();

    assert_eq!(last5.len(), 5, "Expected exactly 5 events at end");
    for (i, event) in last5.iter().enumerate() {
        match event {
            TraceLowLevelEvent::Step(StepRecord { line, .. }) => {
                assert_eq!(*line, Line((95 + i) as i64 + 1));
            }
            _ => panic!("Expected Step event"),
        }
    }
}

#[test]
fn test_event_iterator_no_full_materialization() {
    let dir = tempfile::tempdir().unwrap();
    let events = make_test_events(75);
    let (path, _offsets) = write_trace_file(&dir, &events, None);

    let mut file = File::open(&path).unwrap();

    // Use read_all_events to count (the iterator functions return borrowed data
    // that requires the borrow to outlive the iterator, so we verify the concept
    // through EventIterator::new with a pre-built decoder).
    // For the integration test, we build the decoder manually.
    let stream_len = file.seek(SeekFrom::End(0)).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();

    let mut header_buf = [0u8; 8];
    file.read_exact(&mut header_buf).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();

    let input2 = fscommon::StreamSlice::new(&mut file, 8, stream_len).unwrap();
    let decoder = zeekstd::DecodeOptions::new(input2).into_decoder().unwrap();
    let iter = seekable_reader::EventIterator::new(decoder);

    let mut count: i64 = 0;
    for result in iter {
        let event = result.unwrap();
        match event {
            TraceLowLevelEvent::Step(StepRecord { line, .. }) => {
                count += 1;
                assert_eq!(line, Line(count));
            }
            _ => panic!("Expected Step event"),
        }
    }
    assert_eq!(count, 75);
}

#[test]
fn test_smaller_frame_size() {
    let dir = tempfile::tempdir().unwrap();
    // Write many events with small frame size to force multiple frames
    let events = make_test_events(500);
    // Use a very small frame size (256 bytes) to guarantee many frames
    let (path, offsets) = write_trace_file(&dir, &events, Some(256));

    assert_eq!(offsets.len(), 500);

    // Verify the file size is reasonable (it should exist and be non-empty)
    let metadata = std::fs::metadata(&path).unwrap();
    assert!(metadata.len() > 0);

    // Read back all events to verify correctness
    let mut file = File::open(&path).unwrap();
    let read_back = seekable_reader::read_all_events(&mut file).unwrap();
    assert_eq!(read_back.len(), 500);

    // Verify seeking works with the small frames
    let offset_200 = offsets[200].decompressed_offset;
    let offset_210 = offsets[210].decompressed_offset;
    drop(file);
    let mut file = File::open(&path).unwrap();
    let subset =
        seekable_reader::read_events_at_offset(&mut file, offset_200, offset_210).unwrap();
    assert_eq!(subset.len(), 10);
    for (i, event) in subset.iter().enumerate() {
        match event {
            TraceLowLevelEvent::Step(StepRecord { line, .. }) => {
                assert_eq!(*line, Line((200 + i) as i64 + 1));
            }
            _ => panic!("Expected Step event"),
        }
    }
}

#[test]
fn test_default_frame_size_is_64k() {
    assert_eq!(DEFAULT_FRAME_SIZE, 64 * 1024);
}

#[test]
fn test_event_offset_tracking() {
    let dir = tempfile::tempdir().unwrap();
    let events = make_test_events(20);
    let path = dir.path().join("trace.events");
    let mut writer = StreamingTraceWriter::new();
    writer.begin(&path).unwrap();
    for event in &events {
        writer.write_event(event).unwrap();
    }

    let offsets = writer.event_offsets().to_vec();
    let total = writer.current_decompressed_offset();

    assert_eq!(offsets.len(), 20);
    assert_eq!(writer.event_count(), 20);

    // Offsets must be monotonically increasing
    for i in 1..offsets.len() {
        assert!(
            offsets[i].decompressed_offset > offsets[i - 1].decompressed_offset,
            "Offsets must be strictly increasing"
        );
        assert_eq!(offsets[i].event_index, i);
    }

    // First offset is 0
    assert_eq!(offsets[0].decompressed_offset, 0);
    assert_eq!(offsets[0].event_index, 0);

    // Total decompressed offset is past the last event
    assert!(total > offsets.last().unwrap().decompressed_offset);

    writer.finish().unwrap();
}

#[test]
fn test_flush_frame_mid_stream() {
    let dir = tempfile::tempdir().unwrap();
    let events = make_test_events(40);
    let path = dir.path().join("trace.events");
    let mut writer = StreamingTraceWriter::new();
    writer.begin(&path).unwrap();

    // Write 20 events, flush, write 20 more
    for event in &events[..20] {
        writer.write_event(event).unwrap();
    }
    writer.flush_frame().unwrap();

    for event in &events[20..] {
        writer.write_event(event).unwrap();
    }
    writer.finish().unwrap();

    // Verify all 40 events are readable
    let mut file = File::open(&path).unwrap();
    let read_back = seekable_reader::read_all_events(&mut file).unwrap();
    assert_eq!(read_back.len(), 40);
    for (i, event) in read_back.iter().enumerate() {
        match event {
            TraceLowLevelEvent::Step(StepRecord { line, .. }) => {
                assert_eq!(*line, Line(i as i64 + 1));
            }
            _ => panic!("Expected Step event"),
        }
    }
}
