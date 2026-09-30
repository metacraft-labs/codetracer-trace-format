//! A container whose `funcs.dat`/`types.dat` hold bare names is refused, by
//! both readers, with the same message.
//!
//! The spec's `funcs.dat` record is `global_line_index, name_len, name` and its
//! `types.dat` record `kind, lang_type_len, lang_type, specific_info`
//! (`internal-files.md` "Interning Tables"); bit 12 of `meta.dat` only says the
//! tables are present. The Nim writer wrote BARE NAMES there until b891a0f
//! (2026-09-15), with bit 12 clear, at schema versions 4 and 5. The Nim reader
//! refuses such records, naming the shape and the remedy; the Rust reader used
//! bit 12 as a layout switch and returned the bare bytes as names, so the same
//! container read in one and was refused by the other.
//!
//! The container is built from its parts: a v4 `meta.dat` with bit 12 clear,
//! one structured record and one bare-name record in each table. Both readers
//! must read the structured record and refuse the bare one with byte-identical
//! messages.
//!
//! No mocks: the real CTFS writer builds the container; the real Rust reader
//! and the real Nim reader (through its C ABI) read it.

use codetracer_ctfs::CtfsWriter;
use codetracer_trace_reader::interning_tables_reader::InterningTablesReader;
use codetracer_trace_writer::interning_tables::{encode_func_record, encode_type_record};
use codetracer_trace_writer::meta_dat::encode_meta_dat;
use codetracer_trace_writer_nim::NimTraceReaderHandle;

fn table(records: &[Vec<u8>]) -> (Vec<u8>, Vec<u8>) {
    let mut dat = Vec::new();
    let mut off = 0u64.to_le_bytes().to_vec();
    for r in records {
        dat.extend_from_slice(r);
        off.extend_from_slice(&(dat.len() as u64).to_le_bytes());
    }
    (dat, off)
}

const BARE_FN: &str = "res://game/player.gd::_physics_process_with_a_long_enough_name";
const BARE_TYPE: &str = "PackedStringArrayWithALongEnoughNameToOverrunTheRecord";

fn bare_name_container(dir: &std::path::Path) -> std::path::PathBuf {
    let ct = dir.join("bare.ct");
    let mut w = CtfsWriter::create(&ct, 4096, 31).unwrap();
    let meta = encode_meta_dat(
        "01949fcc-7d92-7e9c-aaaa-bbbbbbbbbbbb",
        "bare",
        &[],
        "",
        "",
        &["/src/game.gd".to_string()],
        0,
    );
    let h = w.add_file("meta.dat").unwrap();
    w.write(h, &meta).unwrap();
    let mut structured_fn = Vec::new();
    encode_func_record(2, b"structured_fn", &mut structured_fn);
    let mut structured_ty = Vec::new();
    encode_type_record(
        7,
        b"Int",
        &[0xA1, 0x64, b'k', b'i', b'n', b'd', 0x64, b'N', b'o', b'n', b'e'],
        &mut structured_ty,
    );
    for (name, records) in [
        ("paths", vec![b"/src/game.gd".to_vec()]),
        ("funcs", vec![structured_fn, BARE_FN.as_bytes().to_vec()]),
        ("types", vec![structured_ty, BARE_TYPE.as_bytes().to_vec()]),
        ("varnames", vec![b"v".to_vec()]),
    ] {
        let (dat, off) = table(&records);
        let h = w.add_file(&format!("{name}.dat")).unwrap();
        w.write(h, &dat).unwrap();
        let h = w.add_file(&format!("{name}.off")).unwrap();
        w.write(h, &off).unwrap();
    }
    w.close().unwrap();
    ct
}

#[test]
fn both_readers_refuse_a_bare_name_record_with_the_same_message() {
    let dir = tempfile::tempdir().unwrap();
    let ct = bare_name_container(dir.path());

    let mut r = codetracer_ctfs::CtfsReader::open(&ct).unwrap();
    let rust = InterningTablesReader::open(&mut r).expect("tables open").expect("tables present");
    let nim = NimTraceReaderHandle::open(ct.to_str().unwrap()).expect("nim reader opens");

    // The controls: the structured records read in both.
    assert_eq!(rust.func(0).expect("rust: structured func").name, b"structured_fn");
    assert_eq!(nim.function(0).expect("nim: structured func"), "structured_fn");
    assert_eq!(rust.type_record(0).expect("rust: structured type").lang_type, b"Int");

    let rust_err = rust.func(1).expect_err("rust must refuse a bare-name funcs.dat record");
    let nim_err = nim.function(1).expect_err("nim must refuse a bare-name funcs.dat record").to_string();
    assert!(
        rust_err.contains("bare name") && rust_err.contains("b891a0f") && rust_err.contains("re-record"),
        "rust: {rust_err}"
    );
    assert_eq!(rust_err, nim_err, "the two readers refuse the same record with different messages");

    let rust_ty = rust.type_record(1).expect_err("rust must refuse a bare-name types.dat record");
    assert!(
        rust_ty.starts_with("types.dat record 1 is not the spec's structured record"),
        "rust: {rust_ty}"
    );
    let nim_ty = nim.type_name(1).expect_err("nim must refuse a bare-name types.dat record").to_string();
    assert_eq!(rust_ty, nim_ty, "types.dat: the two readers refuse with different messages");
}
