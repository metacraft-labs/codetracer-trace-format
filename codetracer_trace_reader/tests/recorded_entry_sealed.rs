//! Real sealed CTFS corruption controls, cloned from the genuine Nim producer.
//! No mocks: actual public CTFS writer and actual split-call codec materialize each image.
#[path = "common/entry_fixture_capture.rs"]
mod entry_fixture_capture;
use codetracer_ctfs::{CompressionMethod, CtfsReader, CtfsWriter};
use codetracer_trace_reader::{call_stream_reader::CallStreamReader, recorded_entry_reader::RecordedEntryReader};
use codetracer_trace_writer::{
    call_stream::encode_call_stream,
    recorded_entry_identity::{decode_entry_identity, encode_entry_identity},
};
use std::path::Path;
fn original() -> CtfsReader {
    let captured = entry_fixture_capture::capture();
    let root = captured.path();
    CtfsReader::open(&Path::new(&root).join("marked.ct")).unwrap()
}
fn rebuild(entry: Vec<u8>, duplicate: bool, calls: Option<(Vec<u8>, Vec<u8>)>) -> CtfsReader {
    let mut old = original();
    let mut writer = CtfsWriter::create_in_memory(old.block_size(), old.max_entries(), CompressionMethod::None).unwrap();
    for name in old.list_files() {
        let bytes = match (name.as_str(), calls.as_ref()) {
            ("entry.dat", _) => entry.clone(),
            ("calls.dat", Some((data, _))) => data.clone(),
            ("calls.idx", Some((_, index))) => index.clone(),
            _ => old.read_file(&name).unwrap(),
        };
        let handle = writer.add_file(&name).unwrap();
        writer.write(handle, &bytes).unwrap();
    }
    if duplicate {
        let handle = writer.add_file("entry.dat").unwrap();
        writer.write(handle, &entry).unwrap();
    }
    CtfsReader::from_owned_bytes(writer.finish_to_bytes().unwrap()).unwrap()
}
#[test]
fn sealed_canonical_domain_identity_and_unique_membership_refusals() {
    let good = original().read_file("entry.dat").unwrap();
    let expected = original().recorded_entry().unwrap();
    let mut unchanged = rebuild(good.clone(), false, None);
    assert_eq!(unchanged.recorded_entry().unwrap(), expected);
    let old_events = codetracer_trace_reader::split_stream_reader::read_trace_from_split_streams(&mut original()).unwrap();
    let new_events = codetracer_trace_reader::split_stream_reader::read_trace_from_split_streams(&mut unchanged).unwrap();
    assert_eq!(serde_json::to_value(old_events).unwrap(), serde_json::to_value(new_events).unwrap());
    let mut bad = vec![Vec::new(), good[..7].to_vec(), zstd::encode_all(std::io::Cursor::new(&good), 1).unwrap()];
    let mut version = good.clone();
    version[4] = 2;
    bad.push(version);
    let mut reserved = good.clone();
    reserved[6] = 1;
    bad.push(reserved);
    let mut trailing = good.clone();
    trailing.push(0);
    bad.push(trailing);
    let mut noncanonical = good[..8].to_vec();
    noncanonical.extend([0x81, 0, 1, 1]);
    bad.push(noncanonical);
    for bytes in bad {
        let error = rebuild(bytes, false, None).recorded_entry().unwrap_err();
        assert!(error.contains("entry.dat"), "{error}");
    }
    assert!(rebuild(good.clone(), true, None).recorded_entry().unwrap_err().contains("duplicate"));
    for role in 0..3 {
        let mut entry = decode_entry_identity(&good).unwrap();
        match role {
            0 => entry.call_key = 100,
            1 => entry.function_id = 100,
            _ => entry.entry_step = 100,
        }
        assert!(rebuild(encode_entry_identity(entry).unwrap(), false, None).recorded_entry().is_err());
    }
}
#[test]
fn actual_finalized_parent_chain_depth_ranges_and_duplicate_child_refuse() {
    let mut old = original();
    let good = old.read_file("entry.dat").unwrap();
    let records = CallStreamReader::open(&mut old).unwrap().unwrap().read_all().unwrap();
    let expected = original().recorded_entry().unwrap();
    let encoded = encode_call_stream(&records, 16, 1).unwrap();
    let mut reencoded = rebuild(good.clone(), false, Some((encoded.dat, encoded.idx)));
    assert_eq!(reencoded.recorded_entry().unwrap(), expected);
    let old_events = codetracer_trace_reader::split_stream_reader::read_trace_from_split_streams(&mut original()).unwrap();
    let new_events = codetracer_trace_reader::split_stream_reader::read_trace_from_split_streams(&mut reencoded).unwrap();
    assert_eq!(serde_json::to_value(old_events).unwrap(), serde_json::to_value(new_events).unwrap());
    for role in 0..6 {
        let mut changed = records.clone();
        match role {
            0 => changed[0].children.push(1),
            1 => changed[0].children.clear(),
            2 => changed[1].parent_key = 1,
            3 => changed[1].depth += 1,
            4 => changed[1].last_step_id = u64::MAX,
            _ => changed[1].function_id = 1u64 << 32,
        }
        let encoded = encode_call_stream(&changed, 16, 1).unwrap();
        let error = rebuild(good.clone(), false, Some((encoded.dat, encoded.idx)))
            .recorded_entry()
            .unwrap_err();
        assert!(error.contains("entry.dat"), "{error}");
    }
}

#[test]
fn declared_root_table_requires_its_actual_root_and_allows_nonliteral_outermost() {
    let mut old = original();
    let good = old.read_file("entry.dat").unwrap();
    let records = CallStreamReader::open(&mut old).unwrap().unwrap().read_all().unwrap();
    assert_eq!(records[0].call_key, 0);
    let mut omitted = records[1..].to_vec();
    omitted[0].parent_key = -1;
    omitted[0].depth = 0;
    let encoded = encode_call_stream(&omitted, 16, 1).unwrap();
    assert!(
        rebuild(good.clone(), false, Some((encoded.dat, encoded.idx)))
            .recorded_entry()
            .unwrap_err()
            .contains("entry.dat")
    );
    let mut writer = CtfsWriter::create_in_memory(old.block_size(), old.max_entries(), CompressionMethod::None).unwrap();
    for name in old.list_files() {
        let mut bytes = old.read_file(&name).unwrap();
        if name == "funcs.dat" {
            let positions: Vec<_> = bytes
                .windows(10)
                .enumerate()
                .filter(|(_, v)| *v == b"<toplevel>")
                .map(|(i, _)| i)
                .collect();
            assert_eq!(positions.len(), 1);
            bytes[positions[0]..positions[0] + 10].copy_from_slice(b"plain-root");
        }
        let handle = writer.add_file(&name).unwrap();
        writer.write(handle, &bytes).unwrap();
    }
    let mut renamed = CtfsReader::from_owned_bytes(writer.finish_to_bytes().unwrap()).unwrap();
    assert_eq!(renamed.recorded_entry().unwrap(), original().recorded_entry().unwrap());
}
