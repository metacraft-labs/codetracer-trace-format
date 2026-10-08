//! A block size a container may not declare is refused before the writer
//! touches the file: no file is created, and one already there keeps its
//! bytes. A full container's block size is 1024, 2048 or 4096
//! (`ctfs-container.md` §1).
//!
//! No mocks: the writers create real files in a temporary directory.

use codetracer_ctfs::{ConcurrentCtfsWriter, CtfsError, CtfsWriter};

#[test]
fn a_refused_block_size_creates_no_file() {
    let dir = tempfile::tempdir().unwrap();
    for bs in [512u32, 8192, 4104, 0] {
        let path = dir.path().join(format!("c{bs}.ct"));
        let r = CtfsWriter::create(&path, bs, 31);
        assert!(matches!(r, Err(CtfsError::InvalidBlockSize(v)) if v == bs), "block size {bs} is refused");
        assert!(!path.exists(), "a refused block size {bs} must leave no file at {}", path.display());
        let r = ConcurrentCtfsWriter::create(&path, bs, 31);
        assert!(r.is_err(), "the concurrent writer refuses block size {bs}");
        assert!(!path.exists(), "the concurrent writer must leave no file for block size {bs}");
    }
}

#[test]
fn a_refused_block_size_leaves_an_existing_file_as_it_was() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("existing.ct");
    std::fs::write(&path, b"not to be touched").unwrap();
    assert!(CtfsWriter::create(&path, 8192, 31).is_err());
    assert!(ConcurrentCtfsWriter::create(&path, 8192, 31).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"not to be touched");
}

#[test]
fn an_accepted_block_size_creates_the_container() {
    let dir = tempfile::tempdir().unwrap();
    for bs in [1024u32, 2048, 4096] {
        let path = dir.path().join(format!("ok{bs}.ct"));
        CtfsWriter::create(&path, bs, 31).unwrap().close().unwrap();
        assert!(path.exists());
    }
}
