//! Not a correctness test — a measurement, run with `cargo test -- --nocapture`.
//! Compares the compact-profile size against this crate's existing v4 full
//! writer for the same member set.
use codetracer_ctfs::{encode_compact_container, CompactMember, EncryptionMethod, WholeFileCompression};
use codetracer_ctfs::{CtfsReader, CtfsWriter};
use tempfile::NamedTempFile;

#[test]
fn measure_compact_vs_full_v4() {
    let members_data: Vec<(&str, Vec<u8>)> = vec![
        ("meta.json", b"{\"recordingId\":\"01949fcc-7d92-7e9c-8ccc-eeeeeeeeeeee\",\"program\":\"demo\"}".to_vec()),
        ("events.log", (0..2000u32).map(|i| (i % 251) as u8).collect()),
        ("paths.json", b"[\"/src/a.py\",\"/src/b.py\",\"/src/c.py\"]".to_vec()),
        ("calls.dat", (0..3000u32).map(|i| ((i * 7) % 251) as u8).collect()),
        ("values.dat", (0..1500u32).map(|i| ((i * 3) % 251) as u8).collect()),
    ];

    // Compact profile.
    let compact_members: Vec<CompactMember> =
        members_data.iter().map(|(n, d)| CompactMember::new(*n, d.clone())).collect();
    let compact_image =
        encode_compact_container(&compact_members, WholeFileCompression::None, EncryptionMethod::None).unwrap();

    // Full profile (v4, this crate's existing writer, block_size=4096).
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    {
        let mut w = CtfsWriter::create(&path, 4096, 31).unwrap();
        for (name, data) in &members_data {
            let h = w.add_file(name).unwrap();
            w.write(h, data).unwrap();
        }
        w.close().unwrap();
    }
    let full_size = std::fs::metadata(&path).unwrap().len();

    // Sanity: full profile still reads back correctly.
    let mut r = CtfsReader::open(&path).unwrap();
    for (name, data) in &members_data {
        assert_eq!(&r.read_file(name).unwrap(), data);
    }

    let raw_bytes: u64 = members_data.iter().map(|(_, d)| d.len() as u64).sum();
    eprintln!(
        "raw member bytes = {}\ncompact container = {} bytes\nfull (v4) container = {} bytes\nreduction = {:.1}%",
        raw_bytes,
        compact_image.len(),
        full_size,
        100.0 * (1.0 - compact_image.len() as f64 / full_size as f64)
    );
}
