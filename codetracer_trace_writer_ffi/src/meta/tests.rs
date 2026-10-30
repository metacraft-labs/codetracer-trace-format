//! Real C ABI calls over owned files; no mocked writer or filesystem.
//! Rejection must preserve the sealed container, while the generic streaming
//! writer retains its documented ability to leave a refused partial set.
use super::{ct_container_append_files, ct_container_create};
use std::ffi::CString;
use std::path::Path;

const TABLES: [&str; 8] = [
    "paths.dat",
    "paths.off",
    "funcs.dat",
    "funcs.off",
    "types.dat",
    "types.off",
    "varnames.dat",
    "varnames.off",
];

fn create(path: &Path) {
    let path = CString::new(path.to_str().unwrap()).unwrap();
    assert_eq!(unsafe { ct_container_create(path.as_ptr(), 0) }, 0);
}

fn append(path: &Path, names: &[&str]) -> i32 {
    let path = CString::new(path.to_str().unwrap()).unwrap();
    let names: Vec<_> = names.iter().map(|name| CString::new(*name).unwrap()).collect();
    let pointers: Vec<_> = names.iter().map(|name| name.as_ptr()).collect();
    // A present empty table has empty data and its single zero offset.
    let data: Vec<Vec<u8>> = names
        .iter()
        .map(|name| if name.to_bytes().ends_with(b".off") { vec![0; 8] } else { vec![] })
        .collect();
    let contents: Vec<_> = data.iter().map(|bytes| bytes.as_ptr()).collect();
    let lengths: Vec<_> = data.iter().map(Vec::len).collect();
    unsafe { ct_container_append_files(path.as_ptr(), pointers.as_ptr(), contents.as_ptr(), lengths.as_ptr(), names.len()) }
}

#[test]
fn refused_partial_batch_preserves_existing_container_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("existing.ct");
    create(&path);
    assert_eq!(append(&path, &["keep.dat"]), 0);
    let before = std::fs::read(&path).unwrap();
    assert_ne!(append(&path, &["paths.dat", "paths.off"]), 0);
    assert!(
        std::fs::read(&path).unwrap() == before,
        "a rejected batch changed the complete container bytes"
    );
}

#[test]
fn refused_bad_tables_namespace_preserves_empty_container_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("bad_tables.ct");
    create(&path);
    let before = std::fs::read(&path).unwrap();
    // Same missing member as the unchanged nineteen-scenario C host.
    let names = [
        "meta.dat",
        "paths.dat",
        "paths.off",
        "funcs.dat",
        "funcs.off",
        "types.dat",
        "types.off",
        "varnames.off",
    ];
    assert_ne!(append(&path, &names), 0);
    assert!(
        std::fs::read(&path).unwrap() == before,
        "a refused seven-of-eight set changed the complete container bytes"
    );
}

#[test]
fn complete_tables_and_unrelated_members_remain_accepted() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("complete.ct");
    create(&path);
    assert_eq!(append(&path, &TABLES), 0);
    assert_eq!(append(&path, &["extra.dat"]), 0);
    let mut reader = codetracer_ctfs::CtfsReader::from_bytes(std::fs::read(&path).unwrap()).unwrap();
    let mut expected: Vec<_> = TABLES.iter().map(|name| name.to_string()).collect();
    expected.push("extra.dat".to_string());
    assert_eq!(reader.list_files(), expected);
    for name in TABLES {
        assert_eq!(reader.read_file(name).unwrap(), if name.ends_with(".off") { vec![0; 8] } else { vec![] });
    }
}

#[test]
fn preflight_checks_existing_and_new_names_together() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("completed.ct");
    // The generic live writer's existing refusal is intentionally different:
    // it completes its already-written members on disk, then returns an error.
    let mut writer = codetracer_ctfs::CtfsWriter::create(&path, 4096, 31).unwrap();
    for name in &TABLES[..2] {
        let handle = writer.add_file(name).unwrap();
        writer.write(handle, if name.ends_with(".off") { &[0; 8] } else { &[] }).unwrap();
    }
    assert!(matches!(writer.close(), Err(codetracer_ctfs::CtfsError::PartialInterningTableSet(_))));
    assert_eq!(append(&path, &TABLES[2..]), 0, "the resulting namespace carries all eight members");
    let mut reader = codetracer_ctfs::CtfsReader::from_bytes(std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(reader.list_files(), TABLES.map(str::to_string));
}
