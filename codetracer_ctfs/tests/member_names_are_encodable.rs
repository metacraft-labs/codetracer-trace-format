//! A CTFS member name is 1 to 12 characters from `0-9 a-z . / -`
//! (`ctfs-container.md` §3). Every writer refuses any other name when the
//! member is created, naming it, instead of storing the base40 packing of a
//! different name; a reader does not answer a lookup of one with whatever
//! member its packing happens to match.
//!
//! `REFUSED` and `ACCEPTED` are the Nim writer's sets
//! (`tests/test_member_names_are_encodable.nim`), which
//! `codetracer_trace_writer_nim/tests/member_names_are_refused_alike.rs`
//! drives both writers over. No mocks: the real CTFS writers and readers.

use codetracer_ctfs::compact::{encode_compact_container, WholeFileCompression};
use codetracer_ctfs::{CompressionMethod, ConcurrentCtfsReader, ConcurrentCtfsWriter, CtfsReader, CtfsWriter};

/// Each breaks §3 one way: empty, an underscore and 13 characters, a capital,
/// 13 characters, a space, a NUL, a non-ASCII character, an underscore alone,
/// a control character.
const REFUSED: [&str; 9] = [
    "",
    "event_log.dat",
    "Meta.dat",
    "abcdefghijklm",
    "a b",
    "steps.dat\0",
    "naïve.dat",
    "a_b",
    "tab\tname",
];
const ACCEPTED: [&str; 6] = ["steps.dat", "a", "abcdefghijkl", "a/b-c.12", "0", "step-map.ns"];

fn names(name: &str, message: &str) -> bool {
    message.contains(&format!("{name:?}"))
}

#[test]
fn every_writer_refuses_an_unencodable_name_by_name() {
    let dir = tempfile::tempdir().unwrap();
    for name in REFUSED {
        let mut w = CtfsWriter::create_in_memory(4096, 31, CompressionMethod::None).unwrap();
        let e = w.add_file(name).err().unwrap_or_else(|| panic!("CtfsWriter::add_file accepted {name:?}"));
        assert!(names(name, &e.to_string()), "the refusal does not name {name:?}: {e}");

        let cw = ConcurrentCtfsWriter::create(&dir.path().join("c.ct"), 4096, 31).unwrap();
        let e = cw
            .add_file(name)
            .err()
            .unwrap_or_else(|| panic!("ConcurrentCtfsWriter::add_file accepted {name:?}"));
        assert!(names(name, &e.to_string()), "the refusal does not name {name:?}: {e}");

        let e = encode_compact_container(&[(name, b"x")], WholeFileCompression::None)
            .err()
            .unwrap_or_else(|| panic!("encode_compact_container accepted {name:?}"));
        assert!(names(name, &e.to_string()), "the refusal does not name {name:?}: {e}");
    }
    for name in ACCEPTED {
        let mut w = CtfsWriter::create_in_memory(4096, 31, CompressionMethod::None).unwrap();
        w.add_file(name).unwrap_or_else(|e| panic!("{name}: {e}"));
        let cw = ConcurrentCtfsWriter::create(&dir.path().join("c.ct"), 4096, 31).unwrap();
        cw.add_file(name).unwrap_or_else(|e| panic!("{name}: {e}"));
        encode_compact_container(&[(name, b"x")], WholeFileCompression::None).unwrap_or_else(|e| panic!("{name}: {e}"));
    }
}

#[test]
fn a_reader_does_not_answer_for_a_mangled_name() {
    // "abcdefghijklm" is "abcdefghijkl" and one character more; the packing
    // of the longer one, were it made, would be the shorter one's.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.ct");
    let mut w = CtfsWriter::create(&path, 4096, 31).unwrap();
    let h = w.add_file("abcdefghijkl").unwrap();
    w.write(h, &[1, 2, 3]).unwrap();
    w.close().unwrap();
    let compact = encode_compact_container(&[("abcdefghijkl", &[1, 2, 3])], WholeFileCompression::None).unwrap();

    let mut full = CtfsReader::open(&path).unwrap();
    let mut compact = CtfsReader::from_bytes(compact).unwrap();
    let concurrent = ConcurrentCtfsReader::open(&path).unwrap();
    for name in REFUSED {
        for (profile, r) in [("full", &mut full), ("compact", &mut compact)] {
            assert_eq!(r.file_size(name), None, "{profile}: file_size answered for {name:?}");
            let e = r
                .read_file(name)
                .err()
                .unwrap_or_else(|| panic!("{profile}: read_file answered for {name:?}"));
            assert!(names(name, &e.to_string()), "{profile}: the refusal does not name {name:?}: {e}");
            assert!(r.read_member(name).is_err(), "{profile}: read_member answered for {name:?}");
        }
        assert_eq!(concurrent.file_size(name), None, "concurrent: file_size answered for {name:?}");
        assert!(concurrent.read_file(name).is_err(), "concurrent: read_file answered for {name:?}");
    }
    assert_eq!(full.read_file("abcdefghijkl").unwrap(), [1, 2, 3]);
    assert_eq!(compact.read_file("abcdefghijkl").unwrap(), [1, 2, 3]);
}
