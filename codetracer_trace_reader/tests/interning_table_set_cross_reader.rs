//! The Rust and Nim trace readers refuse a container that carries some but
//! not all of the four interning tables, with the same message, and both open
//! one that carries all four or none (`internal-files.md` §"Interning Tables":
//! all four or none).
//!
//! The two disagreed once: the Nim reader took an absent table as empty, so a
//! container with `paths.dat` alone opened there and was refused here, and the
//! checks that ran only through Nim passed it. Every strict subset of the
//! eight members is written to a file and read by both readers — this
//! crate's `InterningTablesReader::open` and the sibling
//! `codetracer-trace-format-nim` checkout's `openNewTrace`, built through that
//! repo's own dev shell — and the two verdicts must be equal, byte for byte.
//!
//! The containers are written with `CtfsWriter`, which refuses to finish a
//! partial set; each member is written under a placeholder name and the root
//! entry's name is then set to the table member's, which is what a container
//! from a writer without that refusal looks like. No mocks: real files, both
//! production readers.

#[path = "../../codetracer_ctfs/tests/nim_adjudicator/mod.rs"]
mod nim_adjudicator;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use codetracer_ctfs::interning_set::{INTERNING_TABLE_MEMBERS, interning_set_refusal};
use codetracer_ctfs::{CompressionMethod, CtfsReader, CtfsWriter, base40_encode};
use codetracer_trace_reader::interning_tables_reader::InterningTablesReader;

/// One line per file: `<path>\tOK` or `<path>\t<refusal>`.
const NIM_DRIVER: &str = r#"
import std/os
import results
import codetracer_trace_writer/new_trace_reader

for path in commandLineParams():
  let r = openNewTrace(path)
  if r.isOk:
    echo path, "\tOK"
  else:
    echo path, "\t", r.error
"#;

const ROOT_ENTRIES: usize = 31;
const ENTRY_SIZE: usize = 24;
const ENTRIES_AT: usize = 16;

/// A container holding exactly `members`, each an empty table half (a `.off`
/// member holds the single offset 0).
fn container_with(members: &[&str]) -> Vec<u8> {
    let mut w = CtfsWriter::create_in_memory(4096, ROOT_ENTRIES as u32, CompressionMethod::None).unwrap();
    for (i, m) in members.iter().enumerate() {
        let h = w.add_file(&format!("placeholder{i}")).unwrap();
        if m.ends_with(".off") {
            w.write(h, &0u64.to_le_bytes()).unwrap();
        }
    }
    let mut bytes = w.finish_to_bytes().unwrap();
    for (i, m) in members.iter().enumerate() {
        let from = base40_encode(&format!("placeholder{i}")).unwrap().to_le_bytes();
        let to = base40_encode(m).unwrap().to_le_bytes();
        let slot = (0..ROOT_ENTRIES)
            .map(|k| ENTRIES_AT + k * ENTRY_SIZE + 16)
            .find(|&at| bytes[at..at + 8] == from)
            .expect("the placeholder's root entry");
        bytes[slot..slot + 8].copy_from_slice(&to);
    }
    bytes
}

fn rust_verdict(path: &Path) -> String {
    let mut reader = CtfsReader::open(path).unwrap();
    match InterningTablesReader::open(&mut reader) {
        Ok(_) => "OK".to_string(),
        Err(e) => e,
    }
}

fn nim_verdicts(exe: &Path, files: &[PathBuf]) -> BTreeMap<PathBuf, String> {
    let out = Command::new(exe).args(files).output().expect("the Nim driver runs");
    assert!(out.status.success(), "the Nim driver failed: {}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(|line| {
            let (path, verdict) = line.split_once('\t').expect("<path>\\t<verdict>");
            (PathBuf::from(path), verdict.to_string())
        })
        .collect()
}

#[test]
fn both_readers_refuse_every_partial_set_in_the_same_words() {
    let work = tempfile::tempdir().unwrap();
    let mut expected: BTreeMap<PathBuf, String> = BTreeMap::new();
    for mask in 0u32..256 {
        let members: Vec<&str> = INTERNING_TABLE_MEMBERS
            .iter()
            .enumerate()
            .filter(|(i, _)| mask & (1 << i) != 0)
            .map(|(_, n)| *n)
            .collect();
        let path = work.path().join(format!("set-{mask:03}.ct"));
        std::fs::write(&path, container_with(&members)).unwrap();
        let want = interning_set_refusal(|n| members.contains(&n)).unwrap_or_else(|| "OK".to_string());
        expected.insert(path, want);
    }

    // The Rust reader, against the refusal the set calls for.
    for (path, want) in &expected {
        assert_eq!(&rust_verdict(path), want, "Rust reader on {}", path.display());
    }
    let refused = expected.values().filter(|v| *v != "OK").count();
    assert_eq!(refused, 254, "every strict subset is refused; all eight and none open");

    // The Nim reader, against the Rust reader's verdicts.
    let Some(exe) = nim_adjudicator::build_driver(work.path(), "interning_set_driver", NIM_DRIVER) else {
        return;
    };
    let files: Vec<PathBuf> = expected.keys().cloned().collect();
    let nim = nim_verdicts(&exe, &files);
    assert_eq!(nim.len(), expected.len(), "the Nim driver answered for every file");
    for (path, want) in &expected {
        assert_eq!(nim.get(path), Some(want), "Nim reader on {}", path.display());
    }
}
