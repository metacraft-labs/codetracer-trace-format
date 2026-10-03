//! `CtfsReader::from_bytes`: a container already in memory reads back exactly
//! as the same bytes do from a file, and is refused where they are refused.
//!
//! The containers are written by the production `CtfsWriter` and read through
//! both constructors of the real reader; the file-backed one is the oracle.
//! Members are written interleaved, a few bytes at a time, so their data blocks
//! are not contiguous in the container: a reader that read a run of blocks as
//! one range where the blocks are not consecutive would return another
//! member's bytes. No mocks.

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

/// Members of every layout: empty, a single direct block, a few mapped
/// blocks, and one past a level-1 mapping block (511 data blocks), which
/// needs a level-2 block. Returns the container and each member's content.
fn container() -> (Vec<u8>, Vec<(&'static str, Vec<u8>)>) {
    let members: Vec<(&'static str, Vec<u8>)> = vec![
        ("empty.dat", Vec::new()),
        ("small.dat", bytes(1, 100)),
        ("three.dat", bytes(2, 3 * BS as usize - 17)),
        ("big.dat", bytes(3, 600 * BS as usize + 5)),
        ("other.dat", bytes(4, 40 * BS as usize + 1)),
    ];
    let mut w = CtfsWriter::create_in_memory(BS, 32, CompressionMethod::None).unwrap();
    let handles: Vec<_> = members.iter().map(|(n, _)| w.add_file(n).unwrap()).collect();
    let mut at = vec![0usize; members.len()];
    let step = 3000;
    while at.iter().zip(&members).any(|(a, (_, d))| *a < d.len()) {
        for (i, (_, d)) in members.iter().enumerate() {
            let end = (at[i] + step).min(d.len());
            if at[i] < end {
                w.write(handles[i], &d[at[i]..end]).unwrap();
                at[i] = end;
            }
        }
    }
    (w.finish_to_bytes().unwrap(), members)
}

fn file_reader(data: &[u8]) -> (tempfile::TempDir, CtfsReader) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.ct");
    std::fs::write(&path, data).unwrap();
    let r = CtfsReader::open(&path).unwrap();
    (dir, r)
}

#[test]
fn every_member_reads_back_from_bytes_as_from_a_file() {
    let (data, members) = container();
    let (_dir, mut from_file) = file_reader(&data);
    let mut from_bytes = CtfsReader::from_bytes(data).unwrap();
    assert_eq!(from_bytes.list_files(), from_file.list_files());
    assert_eq!(from_bytes.block_size(), BS);
    for (name, content) in &members {
        assert_eq!(from_file.read_file(name).unwrap(), *content, "{name} from a file");
        assert_eq!(from_bytes.read_file(name).unwrap(), *content, "{name} from bytes");
        assert_eq!(from_bytes.file_size(name), Some(content.len() as u64));
    }
}

#[test]
fn ranges_read_back_from_bytes_as_from_a_file() {
    let (data, members) = container();
    let (_dir, mut from_file) = file_reader(&data);
    let mut from_bytes = CtfsReader::from_bytes(data).unwrap();
    for (name, content) in &members {
        for (off, len) in [(0usize, 1usize), (5, 9000), (BS as usize - 3, 7), (content.len().saturating_sub(10), 50)] {
            let mut a = vec![0u8; len];
            let mut b = vec![0u8; len];
            let got_a = from_file.read_at(name, off as u64, &mut a).unwrap();
            let got_b = from_bytes.read_at(name, off as u64, &mut b).unwrap();
            let want = len.min(content.len().saturating_sub(off));
            assert_eq!((got_a, got_b), (want, want), "{name} at {off}");
            assert_eq!(&b[..want], &content[off.min(content.len())..][..want], "{name} at {off}");
            assert_eq!(a, b);
        }
    }
}

/// A truncated container is refused from bytes with the refusal the file
/// reader gives: the bound on block numbers is the length of the bytes.
#[test]
fn a_truncated_container_is_refused_from_bytes_as_from_a_file() {
    let (data, _) = container();
    let cut = &data[..data.len() - 2 * BS as usize - 100];
    let (_dir, mut from_file) = file_reader(cut);
    let mut from_bytes = CtfsReader::from_bytes(cut.to_vec()).unwrap();
    let mut refused = 0;
    for name in ["small.dat", "three.dat", "big.dat", "other.dat"] {
        let a = from_file.read_file(name).map_err(|e| e.to_string());
        let b = from_bytes.read_file(name).map_err(|e| e.to_string());
        assert_eq!(a, b, "{name}");
        if let Err(e) = b {
            assert!(e.contains("out of bounds"), "{name}: {e}");
            refused += 1;
        }
    }
    assert!(refused > 0, "the cut must reach at least one member, or nothing was tested");
}

#[test]
fn bytes_that_are_not_a_container_are_refused() {
    assert!(CtfsReader::from_bytes(Vec::new()).is_err());
    assert!(CtfsReader::from_bytes(vec![0u8; 8192]).is_err());
    let (mut data, _) = container();
    data[5] = 4;
    assert!(CtfsReader::from_bytes(data).is_err(), "a version-4 header");
}
