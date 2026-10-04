//! A compact container's chunk is read as its content, even when that content
//! begins with the four bytes of a zstd frame's magic number
//! (`ctfs-container.md` §1f: "the profile is the declaration, and four bytes
//! of content that happen to spell `28 B5 2F FD` are content").
//!
//! The chunk is a `calls.dat` chunk whose first record is chosen so that its
//! encoding starts with the magic: a 40-byte record (length varint `0x28`)
//! whose `function_id` 6069 is the varint `B5 2F` and whose `parent_key` -127
//! is the zigzag varint `FD 01`. The full container is built by the real
//! call-stream encoder and `CtfsWriter`, converted by the real conversion,
//! and read by the real reader. No mocks.

use codetracer_ctfs::compact::Profile;
use codetracer_ctfs::{CompressionMethod, CtfsReader, CtfsWriter, MemberBytes};
use codetracer_trace_reader::ChunkForm;
use codetracer_trace_reader::call_stream_reader::CallStreamReader;
use codetracer_trace_writer::call_stream::{CallStreamRecord, encode_call_stream};
use codetracer_trace_writer::compact_profile::{compact_members_of, encode_compact};

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];

fn record(call_key: u64) -> CallStreamRecord {
    CallStreamRecord {
        call_key,
        function_id: 6069,
        parent_key: -127,
        first_step_id: 0,
        last_step_id: 0,
        depth: 0,
        args: Vec::new(),
        // 11 bytes of other fields and 29 of return value: a 40-byte record.
        return_value: vec![0xf6; 29],
        raised_exception: Vec::new(),
        children: Vec::new(),
    }
}

fn full_container() -> Vec<u8> {
    let records: Vec<CallStreamRecord> = (0..3).map(record).collect();
    let calls = encode_call_stream(&records, 2, 3).expect("encode calls");
    let mut w = CtfsWriter::create_in_memory(4096, 31, CompressionMethod::None).expect("in-memory container");
    for (name, bytes) in [("calls.dat", &calls.dat), ("calls.idx", &calls.idx)] {
        let h = w.add_file(name).expect("add member");
        w.write(h, bytes).expect("write");
    }
    w.finish_to_bytes().expect("finish")
}

fn read_all(reader: &mut CtfsReader) -> Vec<CallStreamRecord> {
    CallStreamReader::open(reader)
        .expect("opens")
        .expect("present")
        .read_all()
        .expect("reads")
}

#[test]
fn a_chunk_whose_content_spells_the_zstd_magic_is_read_as_content() {
    let full = full_container();
    let compact = encode_compact(&compact_members_of(&full).expect("convert")).expect("lay out");

    let mut reader = CtfsReader::from_bytes(compact).expect("open compact");
    assert_eq!(reader.profile(), Profile::Compact);
    let dat = reader.read_file("calls.dat").expect("calls.dat");
    assert_eq!(
        dat[..4],
        ZSTD_MAGIC,
        "the fixture's first chunk must begin with the zstd magic, or this test proves nothing"
    );

    let records = read_all(&mut reader);
    assert_eq!(records, (0..3).map(record).collect::<Vec<_>>());
    assert_eq!(read_all(&mut CtfsReader::from_bytes(full).expect("open full")), records);

    // Control: the same bytes taken as frames are refused, so the answer
    // above came from reading each chunk as content.
    let idx = reader.read_file("calls.idx").expect("calls.idx");
    let as_frames = CallStreamReader::from_member_as(&[], MemberBytes::from(dat), &idx, ChunkForm::Framed);
    assert!(as_frames.is_err(), "the content is not a zstd frame");
}
