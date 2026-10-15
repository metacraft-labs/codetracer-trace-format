//! The writer refuses to finish a container that carries some but not all of
//! the four interning tables (`internal-files.md` §"Interning Tables": all
//! four or none).
//!
//! `CtfsWriter::add_file` takes any storable name, so a caller can create
//! `paths.dat`/`paths.off` and nothing else; a one-off converter in the Nim
//! implementation did exactly that, and the result was committed as a
//! fixture that the Rust readers refused. Asserted: `close`,
//! `finish_to_bytes` and `ConcurrentCtfsWriter::close` all return the
//! refusal for every partial set tried, and finish a container with all
//! four tables, with none, or with `markers` alone.
//!
//! No mocks: real containers written by the real writers, in memory and on
//! disk.

use codetracer_ctfs::interning_set::{interning_set_refusal, INTERNING_TABLE_MEMBERS};
use codetracer_ctfs::{CompressionMethod, ConcurrentCtfsWriter, CtfsReader, CtfsWriter};

const PATHS_ONLY: &str = "the container carries paths.dat, paths.off but not funcs.dat, funcs.off, types.dat, \
                          types.off, varnames.dat, varnames.off: the interning tables paths, funcs, types and \
                          varnames are carried all four or none (internal-files.md \"Interning Tables\")";

/// Add `members`, each an empty table half (`.off` holds the offset 0).
fn add_members(w: &mut CtfsWriter, members: &[&str]) {
    for m in members {
        let h = w.add_file(m).unwrap();
        if m.ends_with(".off") {
            w.write(h, &0u64.to_le_bytes()).unwrap();
        }
    }
}

fn finished(members: &[&str]) -> Result<Vec<u8>, String> {
    let mut w = CtfsWriter::create_in_memory(4096, 31, CompressionMethod::None).unwrap();
    add_members(&mut w, members);
    w.finish_to_bytes().map_err(|e| e.to_string())
}

#[test]
fn finish_to_bytes_refuses_paths_without_the_other_three() {
    let got = finished(&["paths.dat", "paths.off"]).map(|_| "finished");
    assert_eq!(got, Err(PATHS_ONLY.to_string()));
}

#[test]
fn finish_to_bytes_refuses_every_strict_subset() {
    for mask in 1u32..255 {
        let members: Vec<&str> = INTERNING_TABLE_MEMBERS
            .iter()
            .enumerate()
            .filter(|(i, _)| mask & (1 << i) != 0)
            .map(|(_, n)| *n)
            .collect();
        let want = interning_set_refusal(|n| members.contains(&n)).expect("a strict subset");
        assert_eq!(finished(&members).map(|_| "finished"), Err(want), "subset {members:?}");
    }
}

#[test]
fn close_refuses_a_partial_set_on_disk() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("partial.ct");
    let mut w = CtfsWriter::create(&path, 4096, 31).unwrap();
    add_members(&mut w, &["paths.dat", "paths.off"]);
    assert_eq!(w.close().map_err(|e| e.to_string()), Err(PATHS_ONLY.to_string()));

    let path = dir.path().join("concurrent.ct");
    let w = ConcurrentCtfsWriter::create(&path, 4096, 31).unwrap();
    for m in ["paths.dat", "paths.off"] {
        let mut f = w.add_file(m).unwrap();
        f.write(&w, &0u64.to_le_bytes()).unwrap();
        f.flush(&w).unwrap();
    }
    let w = std::sync::Arc::into_inner(w).unwrap();
    assert_eq!(w.close().map_err(|e| e.to_string()), Err(PATHS_ONLY.to_string()));
}

#[test]
fn all_four_none_and_markers_alone_finish() {
    assert!(finished(&INTERNING_TABLE_MEMBERS).is_ok());
    assert!(finished(&[]).is_ok());
    assert!(finished(&["markers.dat", "markers.off"]).is_ok());

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("all.ct");
    let mut w = CtfsWriter::create(&path, 4096, 31).unwrap();
    add_members(&mut w, &INTERNING_TABLE_MEMBERS);
    w.close().unwrap();
    let r = CtfsReader::open(&path).unwrap();
    for m in INTERNING_TABLE_MEMBERS {
        assert!(r.file_size(m).is_some(), "{m} is in the finished container");
    }
}
