//! The Rust and Nim CTFS writers refuse exactly the same member names
//! (`ctfs-container.md` §3: 1 to 12 characters from `0-9 a-z . / -`), and
//! a name either refuses is not stored as the packing of another name.
//!
//! The name set is the Nim writer's (`tests/test_member_names_are_encodable.nim`),
//! less the name holding a NUL, which a C string cannot carry. The Nim writer
//! is driven through its C ABI (`ct_container_create`,
//! `ct_container_append_files`), the Rust one through `CtfsWriter::add_file`.
//!
//! No mocks: both writers are the real ones, and the containers they accept
//! are read back through the real CTFS reader.

use std::path::Path;

use codetracer_ctfs::{CompressionMethod, CtfsReader, CtfsWriter};
use codetracer_trace_writer_nim::{container_append_files, container_create};

/// Names §3 refuses, each one way: empty, an underscore and 13 characters, a
/// capital, 13 characters, a space, a non-ASCII character, an underscore
/// alone, a control character.
const REFUSED: [&str; 8] = ["", "event_log.dat", "Meta.dat", "abcdefghijklm", "a b", "naïve.dat", "a_b", "tab\tname"];
const ACCEPTED: [&str; 6] = ["steps.dat", "a", "abcdefghijkl", "a/b-c.12", "0", "step-map.ns"];

fn nim_accepts(dir: &Path, k: usize, name: &str) -> bool {
    let path = dir.join(format!("nim-{k}.ct"));
    container_create(&path, 0).expect("ct_container_create");
    let accepted = container_append_files(&path, &[(name, b"x")]).is_ok();
    if accepted {
        let mut r = CtfsReader::open(&path).expect("open the Nim container");
        assert_eq!(r.list_files(), [name], "the Nim writer stored {name:?} under another name");
        assert_eq!(r.read_file(name).expect("read back"), b"x");
    }
    accepted
}

fn rust_accepts(name: &str) -> bool {
    let mut w = CtfsWriter::create_in_memory(4096, 31, CompressionMethod::None).expect("in-memory container");
    let Ok(h) = w.add_file(name) else {
        return false;
    };
    w.write(h, b"x").expect("write");
    let r = CtfsReader::from_bytes(w.finish_to_bytes().expect("finish")).expect("open the Rust container");
    assert_eq!(r.list_files(), [name], "the Rust writer stored {name:?} under another name");
    true
}

#[test]
fn both_writers_refuse_exactly_the_names_section_3_refuses() {
    let dir = tempfile::tempdir().expect("tempdir");
    let all = REFUSED.iter().map(|n| (*n, false)).chain(ACCEPTED.iter().map(|n| (*n, true)));
    let mut disagreements = Vec::new();
    for (k, (name, expected)) in all.enumerate() {
        let (nim, rust) = (nim_accepts(dir.path(), k, name), rust_accepts(name));
        if (nim, rust) != (expected, expected) {
            disagreements.push(format!("{name:?}: nim accepts {nim}, rust accepts {rust}, §3 says {expected}"));
        }
    }
    assert!(disagreements.is_empty(), "{}", disagreements.join("\n"));
}
