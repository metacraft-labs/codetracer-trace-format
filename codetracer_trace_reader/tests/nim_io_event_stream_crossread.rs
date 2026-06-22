//! M24a-3 cross-read proof: a NEW Nim-written production bundle's `events.dat`
//! is read by the canonical Rust `IoEventStreamReader`, and the decoded I/O
//! event records (kind / step_id / metadata / content) equal the records the Nim
//! writer recorded.
//!
//! This is the load-bearing byte-compatibility test for the M24a-3 deliverable:
//! the Nim multi-stream writer now emits the SPEC-canonical
//! `events.dat`/`events.idx` chunked layout (chunked Zstd records +
//! a `[chunk_size: u32][offset: u64]...` index, with the `has_io_event_stream`
//! meta.dat flag set). If that layout were not byte-compatible with the Rust
//! reader, this decode would diverge or error.
//!
//! Driven by the Nim test `tests/test_nim_io_event_stream_crossread.nim`, which
//! builds the Nim writer, produces the bundle + a sidecar of decoded per-record
//! I/O events, and runs this test with two env vars set:
//!
//! - `CT_NIM_IO_EVENT_FIXTURE` — path to the Nim-written `<bundle>.ct`.
//! - `CT_NIM_IO_EVENT_FIXTURE_EVENTS` — path to the `<bundle>.ct.events.txt`
//!   sidecar (one line per record:
//!   `kind=<u8>;step_id=<u64>;metadata=<hex>;content=<hex>`).
//!
//! When the env vars are absent (e.g. the Rust suite run on its own) the test
//! is a no-op: there is no Nim fixture to cross-read.

use codetracer_trace_reader::io_event_stream_reader::open_io_event_stream;
use codetracer_trace_writer::event_stream::IoEventRecord;

/// Parse one sidecar line into the expected `IoEventRecord`.
fn parse_line(line: &str) -> IoEventRecord {
    let line = line.trim_end_matches(['\n', '\r']);
    let mut kind: Option<u8> = None;
    let mut step_id: Option<u64> = None;
    let mut metadata: Option<Vec<u8>> = None;
    let mut content: Option<Vec<u8>> = None;
    for field in line.split(';') {
        let (key, val) = field
            .split_once('=')
            .unwrap_or_else(|| panic!("malformed sidecar field: {field:?}"));
        match key {
            "kind" => kind = Some(val.parse::<u8>().expect("parse kind")),
            "step_id" => step_id = Some(val.parse::<u64>().expect("parse step_id")),
            "metadata" => metadata = Some(decode_hex(val)),
            "content" => content = Some(decode_hex(val)),
            other => panic!("unexpected sidecar field key: {other:?}"),
        }
    }
    IoEventRecord {
        kind: kind.expect("sidecar missing kind"),
        step_id: step_id.expect("sidecar missing step_id"),
        metadata: metadata.expect("sidecar missing metadata"),
        content: content.expect("sidecar missing content"),
    }
}

fn decode_hex(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "odd-length hex in sidecar: {s:?}");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("parse hex byte"))
        .collect()
}

#[test]
fn nim_events_dat_read_by_rust_reader() {
    let fixture = match std::env::var("CT_NIM_IO_EVENT_FIXTURE") {
        Ok(p) if !p.is_empty() => p,
        _ => {
            eprintln!(
                "nim_events_dat_read_by_rust_reader: CT_NIM_IO_EVENT_FIXTURE unset — \
                 skipping (run via the Nim driver test for the cross-read proof)"
            );
            return;
        }
    };
    let events_path = std::env::var("CT_NIM_IO_EVENT_FIXTURE_EVENTS")
        .expect("CT_NIM_IO_EVENT_FIXTURE set but CT_NIM_IO_EVENT_FIXTURE_EVENTS missing");

    // Expected per-record I/O events the Nim FFI reader decoded out of the same
    // bundle (one line per record, in stream order).
    let expected: Vec<IoEventRecord> = std::fs::read_to_string(&events_path)
        .expect("read sidecar events")
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(parse_line)
        .collect();
    assert!(!expected.is_empty(), "sidecar must list at least one I/O event");

    // Open the Nim-written events.dat with the canonical Rust reader. The
    // has_io_event_stream flag (set by the Nim writer) must be honored, so this
    // returns Some(reader) rather than None.
    let mut io = open_io_event_stream(std::path::Path::new(&fixture))
        .expect("open_io_event_stream on Nim bundle must succeed")
        .expect("Nim bundle must expose an I/O event stream (has_io_event_stream flag set)");

    assert_eq!(
        io.count() as usize,
        expected.len(),
        "Rust reader's record count must equal the Nim-recorded I/O event count"
    );

    // Read every record and compare to the sidecar (byte-for-byte on kind /
    // step_id / metadata / content).
    for (i, exp) in expected.iter().enumerate() {
        let got = io.read(i as u64).expect("Rust reader read I/O event record");
        assert_eq!(
            &got, exp,
            "I/O event {i}: Rust IoEventStreamReader must decode the Nim-written \
             events.dat to the exact (kind, step_id, metadata, content) record \
             the Nim reader decoded — byte-compatible"
        );
    }

    // Also exercise the paginated read path (the event-log pane's primary
    // access pattern) and confirm it matches the per-record reads.
    let page = io.read_all().expect("read_all over Nim events.dat");
    assert_eq!(page, expected, "read_page over the Nim bundle must match the sidecar");

    // Spot-check seeking into the last record (independent per-chunk decode over
    // the Nim-produced chunk boundaries).
    let last = expected.len() as u64 - 1;
    let last_rec = io.read(last).expect("seek to last Nim I/O event record");
    assert_eq!(&last_rec, expected.last().unwrap());
}
