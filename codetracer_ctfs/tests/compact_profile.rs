//! The version-6 container (`ctfs-container.md` §1-§1d): the compact profile
//! read back through `CtfsReader` exactly as the full container it was laid
//! out from, whole-file zstd undone before any offset is used, and every
//! refusal §1c and §1d require made, naming the offending value.
//!
//! The full containers are written by the production `CtfsWriter` with their
//! members interleaved (so their blocks are not contiguous), the compact ones
//! by `compact::encode_compact_container` from the members the real reader
//! returns. Every container is read through both constructors of the real
//! reader, from bytes and from a file. No mocks.

use codetracer_ctfs::compact::{
    self, compress_image, encode_compact_container, read_compact_directory, Profile, WholeFileCompression, COMPACT_DIRECTORY_OFFSET,
    COMPACT_ENTRY_SIZE, V6_HEADER_SIZE,
};
use codetracer_ctfs::{CompressionMethod, CtfsReader, CtfsWriter};

const BS: u32 = 4096;

fn bytes(seed: u64, n: usize) -> Vec<u8> {
    let mut s = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..n)
        .map(|_| {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            (s >> 33) as u8
        })
        .collect()
}

/// A full container whose members are written interleaved, and the members.
fn full_container() -> (Vec<u8>, Vec<(&'static str, Vec<u8>)>) {
    let members: Vec<(&'static str, Vec<u8>)> = vec![
        ("meta.dat", bytes(1, 149)),
        ("empty.dat", Vec::new()),
        ("steps.dat", bytes(2, 3 * BS as usize - 17)),
        ("values.dat", bytes(3, 40 * BS as usize + 5)),
        ("step-map.ns", bytes(4, 416)),
    ];
    let mut w = CtfsWriter::create_in_memory(BS, 32, CompressionMethod::None).unwrap();
    let handles: Vec<_> = members.iter().map(|(n, _)| w.add_file(n).unwrap()).collect();
    let mut at = vec![0usize; members.len()];
    while at.iter().zip(&members).any(|(a, (_, d))| *a < d.len()) {
        for (i, (_, d)) in members.iter().enumerate() {
            let end = (at[i] + 3000).min(d.len());
            if at[i] < end {
                w.write(handles[i], &d[at[i]..end]).unwrap();
                at[i] = end;
            }
        }
    }
    (w.finish_to_bytes().unwrap(), members)
}

/// The compact image of `full_container`, laid out from what the reader
/// returns for it.
fn compact_container(compression: WholeFileCompression) -> (Vec<u8>, Vec<(&'static str, Vec<u8>)>) {
    let (full, members) = full_container();
    let listed = CtfsReader::from_bytes(full).unwrap().members().unwrap();
    let refs: Vec<(&str, &[u8])> = listed.iter().map(|(n, b)| (n.as_str(), b.as_slice())).collect();
    (encode_compact_container(&refs, compression).unwrap(), members)
}

/// Both readers of `data`: from bytes, and from a file holding them.
fn readers(data: &[u8]) -> (tempfile::TempDir, [CtfsReader; 2]) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.ct");
    std::fs::write(&path, data).unwrap();
    let from_file = CtfsReader::open(&path).unwrap();
    (dir, [CtfsReader::from_bytes(data.to_vec()).unwrap(), from_file])
}

fn refusal(data: Vec<u8>) -> String {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.ct");
    std::fs::write(&path, &data).unwrap();
    let from_file = CtfsReader::open(&path).err().map(|e| e.to_string());
    let from_bytes = CtfsReader::from_bytes(data).err().map(|e| e.to_string());
    assert_eq!(from_file, from_bytes, "both constructors refuse alike");
    from_bytes.expect("the container must be refused")
}

fn assert_reads_back(reader: &mut CtfsReader, members: &[(&str, Vec<u8>)]) {
    assert_eq!(reader.list_files(), members.iter().map(|(n, _)| n.to_string()).collect::<Vec<_>>());
    for (name, content) in members {
        assert_eq!(reader.file_size(name), Some(content.len() as u64), "{name}");
        assert_eq!(reader.read_file(name).unwrap(), *content, "{name}");
        assert_eq!(reader.read_member(name).unwrap().to_vec(), *content, "{name}");
        let mut buf = vec![0u8; 100];
        let off = content.len() / 2;
        let n = reader.read_at(name, off as u64, &mut buf).unwrap();
        assert_eq!(&buf[..n], &content[off..(off + 100).min(content.len())], "{name} at {off}");
    }
    assert!(reader.read_file("absent.dat").is_err());
}

#[test]
fn a_compact_container_reads_back_as_the_full_one_it_was_laid_out_from() {
    let (image, members) = compact_container(WholeFileCompression::None);
    let n = members.len();
    let payload: usize = members.iter().map(|(_, b)| b.len()).sum();
    assert_eq!(image.len(), 28 + 24 * n + payload, "Size = 28 + 24*N + sum(length): no padding");
    let (_dir, mut readers) = readers(&image);
    for r in &mut readers {
        assert_eq!(r.profile(), Profile::Compact);
        assert_eq!(r.whole_file_compression(), WholeFileCompression::None);
        assert_eq!(r.block_size(), 0);
        assert_reads_back(r, &members);
    }
    // Re-laying the compact container out reproduces it byte for byte.
    let again = readers[0].members().unwrap();
    let refs: Vec<(&str, &[u8])> = again.iter().map(|(n, b)| (n.as_str(), b.as_slice())).collect();
    assert_eq!(encode_compact_container(&refs, WholeFileCompression::None).unwrap(), image);
}

#[test]
fn a_whole_file_zstd_container_is_reconstructed_before_it_is_read() {
    let (image, members) = compact_container(WholeFileCompression::Zstd);
    let stored = compress_image(&image, 3).unwrap();
    assert_eq!(stored[..V6_HEADER_SIZE], image[..V6_HEADER_SIZE], "the header is stored as plaintext");
    let (_dir, mut readers) = readers(&stored);
    for r in &mut readers {
        assert_eq!(r.whole_file_compression(), WholeFileCompression::Zstd);
        assert_reads_back(r, &members);
    }
    // The image is the reconstructed one and still declares its scheme: a
    // decoder told it has been reconstructed reads it, one not told refuses.
    assert!(read_compact_directory(&image, true).is_ok());
    let err = read_compact_directory(&image, false).unwrap_err().to_string();
    assert!(err.contains("zstd") && err.contains("reconstruct"), "{err}");
    // A body that is not the zstd it is declared to be is refused.
    let mut broken = stored.clone();
    broken[V6_HEADER_SIZE + 1] ^= 0xff;
    assert!(refusal(broken).contains("does not decode"));
}

#[test]
fn a_version_6_full_container_reads_back_as_its_version_5_twin() {
    let (v5, members) = full_container();
    // Block 0 rewritten for the 24-byte header: the entries move 8 bytes along,
    // every block keeps its number.
    let mut v6 = v5.clone();
    v6[5] = 6;
    let entries = &v5[16..16 + 32 * 24];
    v6[16..24].fill(0); // Profile full, Compression none, reserved zero.
    v6[24..24 + entries.len()].copy_from_slice(entries);
    let (_dir, mut readers) = readers(&v6);
    for r in &mut readers {
        assert_eq!(r.profile(), Profile::Full);
        assert_eq!(r.block_size(), BS);
        assert_reads_back(r, &members);
    }
}

/// §1c: an unknown version, profile or scheme, a non-zero reserved byte and a
/// version-6 header too short for its fields are refused, naming the value.
#[test]
fn every_version_6_header_field_it_does_not_implement_is_refused_by_value() {
    let (image, _) = compact_container(WholeFileCompression::None);
    let with = |at: usize, v: u8| {
        let mut b = image.clone();
        b[at] = v;
        b
    };
    for (data, needle) in [
        (with(5, 7), "version 7"),
        (with(16, 2), "profile 2"),
        (with(17, 9), "scheme 9"),
        (with(20, 1), "offset 20 is 1"),
        (image[..20].to_vec(), "only 20 bytes"),
    ] {
        let err = refusal(data);
        assert!(err.contains(needle), "{needle}: {err}");
    }
}

fn put_u64(b: &mut [u8], at: usize, v: u64) {
    b[at..at + 8].copy_from_slice(&v.to_le_bytes());
}

fn get_u64(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}

/// §1d's checks, each by its own perturbation of a valid image.
#[test]
fn every_directory_check_refuses_by_name() {
    let (image, members) = compact_container(WholeFileCompression::None);
    let n = members.len();
    let entry = |i: usize| COMPACT_DIRECTORY_OFFSET + i * COMPACT_ENTRY_SIZE;
    let edit = |f: &dyn Fn(&mut Vec<u8>)| {
        let mut b = image.clone();
        f(&mut b);
        b
    };
    let cases: Vec<(Vec<u8>, &str)> = vec![
        (edit(&|b| b[24..28].copy_from_slice(&u32::MAX.to_le_bytes())), "check 1"),
        (
            edit(&|b| {
                let v = get_u64(b, entry(0) + 8);
                put_u64(b, entry(0) + 8, v + 1)
            }),
            "check 2",
        ),
        (
            edit(&|b| {
                let v = get_u64(b, entry(2) + 8);
                put_u64(b, entry(2) + 8, v - 1)
            }),
            "check 3",
        ),
        (edit(&|b| b.push(0)), "check 4"),
        (edit(&|b| put_u64(b, entry(1), 0)), "check 5"),
        (edit(&|b| put_u64(b, entry(1), 40u64.pow(12))), "check 5"),
        (edit(&|b| put_u64(b, entry(1), 40 * 11)), "check 5"),
        (
            edit(&|b| {
                let v = get_u64(b, entry(2));
                put_u64(b, entry(3), v)
            }),
            "check 6",
        ),
        (edit(&|b| b[8..12].copy_from_slice(&4096u32.to_le_bytes())), "BlockSize 4096"),
        (edit(&|b| b[12..16].copy_from_slice(&1u32.to_le_bytes())), "MaxRootEntries 1"),
        (edit(&|b| b[7] = 1), "MaxShards 1"),
    ];
    assert!(n >= 4);
    for (data, needle) in cases {
        let err = refusal(data);
        assert!(err.contains(needle), "{needle}: {err}");
    }
}

/// The property checks 2-4 exist for: no single perturbed `Offset` or
/// `Length` field is accepted, so none can serve a shifted or short member.
#[test]
fn no_single_perturbed_offset_or_length_is_accepted() {
    let (image, members) = compact_container(WholeFileCompression::None);
    let mut tried = 0;
    for i in 0..members.len() {
        for field in [8usize, 16] {
            let at = COMPACT_DIRECTORY_OFFSET + i * COMPACT_ENTRY_SIZE + field;
            let v = get_u64(&image, at);
            for delta in [1i64, -1, 7, -4096, 1 << 20] {
                let Some(w) = v.checked_add_signed(delta) else { continue };
                let mut b = image.clone();
                put_u64(&mut b, at, w);
                assert!(CtfsReader::from_bytes(b).is_err(), "entry {i} field {field} {v} -> {w} was accepted");
                tried += 1;
            }
        }
    }
    assert!(tried > 40);
    assert!(CtfsReader::from_bytes(image).is_ok(), "control");
}

#[test]
fn the_encoder_refuses_names_the_directory_could_not_carry() {
    for (members, needle) in [
        (vec![("Meta.dat", &b""[..])], "base40"),
        (vec![("a-name-too-long", &b""[..])], "too long"),
        (vec![("", &b""[..])], "representable"),
        (vec![("a.dat", &b"1"[..]), ("a.dat", &b"2"[..])], "duplicate"),
    ] {
        let err = encode_compact_container(&members, WholeFileCompression::None).unwrap_err().to_string();
        assert!(err.contains(needle), "{needle}: {err}");
    }
    let empty = encode_compact_container(&[], WholeFileCompression::None).unwrap();
    assert_eq!(empty.len(), 28);
    assert!(compact::read_compact_directory(&empty, false).unwrap().is_empty());
}
