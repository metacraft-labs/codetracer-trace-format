//! §5d block-number bounds for the canonical Rust CTFS readers.
//!
//! `CTFS-Binary-Format.md` §5d says a **reader** must accept a container whose
//! length is not a whole number of blocks — the state a crash *inside* an
//! append's tail write leaves — and ignore the bytes past the last whole
//! block. What makes accepting that safe is flooring: `whole_blocks` is
//! `floor(len / block_size)`, never rounded up, so the incomplete final block
//! is unaddressable.
//!
//! Flooring only helps if the bound is applied on **every** path from a block
//! number to bytes, and there are three: the entry's mapping root, each
//! mapping block walked to resolve a data block, and the **data block itself**.
//! The last is the easy one to miss, because the final data block's copy is
//! clamped to the entry's `Size` — so a short read out of the partial region
//! *succeeds*. Bounding byte offsets against the file length (which is all
//! these two readers used to do, via `read_exact` / `pread`) therefore turns a
//! truncated container into wrong content instead of an error.
//!
//! The same gap was found and closed in the wasm recorder's Go reader (M57),
//! in `codetracer-trace-format-nim`'s `readInternalFile`, the db-backend's
//! `ctfs_container.rs` and the native recorder's `ctfs_nim.nim` (M58). This is
//! the canonical Rust crate's half (M59) — the one every other Rust consumer
//! of the format depends on, and the one that *fabricated* content rather than
//! merely serving it out of the partial region.
//!
//! # NO MOCKS
//!
//! Every container below is written by the production `CtfsWriter` onto a real
//! filesystem and then damaged the way a crash damages it: bytes appended past
//! a sealed container (an interrupted tail write) or bytes removed from the end
//! (a truncated container). Nothing here stubs a file, a reader or an I/O
//! error. The tests parse the container's own block-0 entry array and mapping
//! blocks with plain byte arithmetic, so the block numbers they assert on come
//! from the bytes rather than from the reader under test.

use std::fs;
use std::path::{Path, PathBuf};

use codetracer_ctfs::{base40_encode, ConcurrentCtfsReader, CtfsReader, CtfsWriter};
use tempfile::TempDir;

const BS: usize = 4096;
const MAX_ROOT_ENTRIES: u32 = 64;

/// Deterministic, non-repeating content. A container full of identical bytes
/// would let a reader that served the wrong block still compare equal.
fn deterministic_bytes(seed: u64, n: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push((state >> 33) as u8);
    }
    out
}

fn u64_le(bytes: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(bytes[off..off + 8].try_into().unwrap())
}

/// Parse an entry straight out of block 0. Independent of the readers under
/// test, so a reader that resolves the wrong block cannot also decide what the
/// right block was.
fn entry_of(container: &[u8], name: &str) -> (u64, u64) {
    let encoded = base40_encode(name).unwrap();
    for i in 0..MAX_ROOT_ENTRIES as usize {
        let off = 16 + i * 24;
        if u64_le(container, off + 16) == encoded {
            return (u64_le(container, off), u64_le(container, off + 8));
        }
    }
    panic!("no entry named {name} in block 0");
}

/// Resolve a level-1 data block pointer directly out of the mapping block.
/// Only valid for files that fit in a level-1 mapping block (511 data blocks),
/// which is all this helper is used for.
fn level1_data_block(container: &[u8], map_block: u64, block_index: u64) -> u64 {
    assert!(block_index < (BS as u64 / 8) - 1, "block index {block_index} needs a multi-level walk");
    u64_le(container, map_block as usize * BS + block_index as usize * 8)
}

/// Seal a container holding `meta.dat` and `z.dat`, written by the production
/// writer. `z.dat` is sized so its last data block carries only 100 bytes — the
/// clamped read that a missing bound satisfies out of a partial block — and is
/// added second so it owns the final block of the container.
fn sealed_two_stream_container(dir: &Path) -> (PathBuf, Vec<u8>, Vec<u8>) {
    const TAIL_BYTES: usize = 100;
    let path = dir.join("cut.ct");
    let survivor = deterministic_bytes(11, 9000);
    let lost = deterministic_bytes(12, 3 * BS + TAIL_BYTES);

    let mut w = CtfsWriter::create(&path, BS as u32, MAX_ROOT_ENTRIES).unwrap();
    let meta = w.add_file("meta.dat").unwrap();
    w.write(meta, &survivor).unwrap();
    let z = w.add_file("z.dat").unwrap();
    w.write(z, &lost).unwrap();
    w.close().unwrap();

    let sealed = fs::read(&path).unwrap();
    assert_eq!(
        sealed.len() % BS,
        0,
        "the sealed container is {} bytes, not a block multiple; the fixture cannot isolate the partial region",
        sealed.len()
    );
    (path, survivor, lost)
}

/// The shape both truncation fixtures assert on, read out of the container
/// rather than assumed: `z.dat`'s last data block must be the container's last
/// block, or a cut cannot place it in the partial region without also
/// destroying `meta.dat`.
fn assert_z_owns_the_last_block(container: &[u8]) -> u64 {
    let (meta_size, meta_map) = entry_of(container, "meta.dat");
    let (z_size, z_map) = entry_of(container, "z.dat");
    assert_eq!(meta_size, 9000);
    assert_eq!(z_size, (3 * BS + 100) as u64);

    let last = level1_data_block(container, z_map, 3);
    let total_blocks = (container.len() / BS) as u64;
    assert_eq!(
        last,
        total_blocks - 1,
        "the writer did not put z.dat's last data block at the end of the {} block container (it is block {}); \
         adjust the fixture layout rather than deleting the test",
        total_blocks,
        last
    );
    for i in 0..3u64 {
        let b = level1_data_block(container, meta_map, i.min(2));
        assert!(b < last, "meta.dat's data block {b} is not below z.dat's last block {last}");
    }
    assert!(meta_map < last && z_map < last);
    last
}

// ---------------------------------------------------------------------------
// The accept half: a partial tail must cost the reader nothing.
// ---------------------------------------------------------------------------

/// A crash inside an append's tail write leaves a whole block plus a fragment
/// past a sealed container. Block 0 is still the previous, complete one and
/// every pointer in it is below the previous end of file, so both readers must
/// return every stream byte-exact. This is the test that keeps the bounds
/// below from turning into "refuse anything that is not a block multiple",
/// which is the answer M57 removed.
///
/// `snapshot.mem` is 600 data blocks, past the 511-block level-1 threshold, so
/// the agreement covers §4's multi-level walk and not just a flat file.
#[test]
fn both_readers_read_a_partial_tail_byte_exact() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("torn.ct");

    let meta = deterministic_bytes(77, 9000);
    let snapshot = deterministic_bytes(78, BS * 600);
    let mut w = CtfsWriter::create(&path, BS as u32, MAX_ROOT_ENTRIES).unwrap();
    let h_meta = w.add_file("meta.dat").unwrap();
    w.write(h_meta, &meta).unwrap();
    let h_snap = w.add_file("snapshot.mem").unwrap();
    w.write(h_snap, &snapshot).unwrap();
    w.close().unwrap();

    let sealed = fs::read(&path).unwrap();
    assert_eq!(
        sealed.len() % BS,
        0,
        "the sealed container is {} bytes, not a block multiple",
        sealed.len()
    );

    // A tail write that died part-way: a whole block plus a fragment.
    let mut torn = sealed.clone();
    torn.extend_from_slice(&deterministic_bytes(79, BS + 777));
    fs::write(&path, &torn).unwrap();
    assert_eq!(torn.len() % BS, 777, "the fixture did not leave a 777-byte partial tail");

    let mut r = CtfsReader::open(&path).unwrap();
    assert_eq!(
        r.read_file("meta.dat").unwrap(),
        meta,
        "CtfsReader lost meta.dat to an unreferenced partial tail"
    );
    assert_eq!(
        r.read_file("snapshot.mem").unwrap(),
        snapshot,
        "CtfsReader lost snapshot.mem to an unreferenced partial tail"
    );

    let c = ConcurrentCtfsReader::open(&path).unwrap();
    assert_eq!(
        c.read_file("meta.dat").unwrap(),
        meta,
        "ConcurrentCtfsReader lost meta.dat to an unreferenced partial tail"
    );
    assert_eq!(
        c.read_file("snapshot.mem").unwrap(),
        snapshot,
        "ConcurrentCtfsReader lost snapshot.mem to an unreferenced partial tail"
    );

    // The unreferenced tail must not have invented a stream.
    let mut names = r.list_files();
    names.sort();
    assert_eq!(names, vec!["meta.dat".to_string(), "snapshot.mem".to_string()]);
}

/// The negative control. Without it every assertion below could be passing
/// against a fixture that failed to damage anything, or against a reader that
/// simply refuses every container.
#[test]
fn a_cleanly_sealed_container_is_unaffected_by_the_bounds() {
    let dir = TempDir::new().unwrap();
    let (path, survivor, lost) = sealed_two_stream_container(dir.path());

    let mut r = CtfsReader::open(&path).unwrap();
    assert_eq!(r.read_file("meta.dat").unwrap(), survivor);
    assert_eq!(r.read_file("z.dat").unwrap(), lost);

    let c = ConcurrentCtfsReader::open(&path).unwrap();
    assert_eq!(c.read_file("meta.dat").unwrap(), survivor);
    assert_eq!(c.read_file("z.dat").unwrap(), lost);
}

// ---------------------------------------------------------------------------
// Path 3 of 3: the data block. The one that was missing in both readers.
// ---------------------------------------------------------------------------

/// The measured M58/M59 signature: 12 388 bytes returned with no error.
///
/// The container is cut so `z.dat`'s last data block becomes the *first
/// partial* block, with exactly the 100 bytes the entry's `Size` asks for
/// present. `read_exact` therefore succeeds and, without a bound on the block
/// **number**, the whole stream is served out of bytes the container does not
/// own. `meta.dat`, whose blocks are all below the cut, must survive untouched
/// — a reader that refuses the whole container discards readable data over
/// unreferenced bytes.
#[test]
fn read_file_refuses_a_data_block_in_the_partial_region() {
    let dir = TempDir::new().unwrap();
    let (path, survivor, lost) = sealed_two_stream_container(dir.path());
    let sealed = fs::read(&path).unwrap();
    let last = assert_z_owns_the_last_block(&sealed);

    let cut = last as usize * BS + 100;
    fs::write(&path, &sealed[..cut]).unwrap();
    assert_eq!(cut % BS, 100);
    assert_eq!(
        (cut / BS) as u64,
        last,
        "z.dat's last data block {last} is still inside the container's whole blocks"
    );

    let mut r = CtfsReader::open(&path).unwrap();
    assert_eq!(r.read_file("meta.dat").unwrap(), survivor, "the surviving stream came back changed");

    match r.read_file("z.dat") {
        Ok(body) => panic!(
            "CtfsReader returned {} bytes with no error for a stream whose last data block ({last}) lies outside \
             the container's {} whole blocks; the partial region was served as content (expected {} bytes)",
            body.len(),
            cut / BS,
            lost.len()
        ),
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("out of bounds"),
                "the refusal does not say the block is out of bounds: {msg}"
            );
            assert!(msg.contains("truncated"), "the refusal does not say the container is truncated: {msg}");
            assert!(msg.contains("z.dat"), "the refusal does not name the lost stream: {msg}");
        }
    }
}

/// The same fixture through `ConcurrentCtfsReader`, which reaches the data
/// block through `pread` rather than `read_exact`. Before M59 this reader was
/// strictly worse than the one above: its `pread` reported a short positional
/// read as a full one, so it did not even need the 100 bytes to be present.
#[test]
fn concurrent_read_file_refuses_a_data_block_in_the_partial_region() {
    let dir = TempDir::new().unwrap();
    let (path, survivor, lost) = sealed_two_stream_container(dir.path());
    let sealed = fs::read(&path).unwrap();
    let last = assert_z_owns_the_last_block(&sealed);

    let cut = last as usize * BS + 100;
    fs::write(&path, &sealed[..cut]).unwrap();

    let c = ConcurrentCtfsReader::open(&path).unwrap();
    assert_eq!(c.read_file("meta.dat").unwrap(), survivor, "the surviving stream came back changed");

    match c.read_file("z.dat") {
        Ok(body) => panic!(
            "ConcurrentCtfsReader returned {} bytes with no error for a stream whose last data block ({last}) lies \
             outside the container's {} whole blocks (expected {} bytes)",
            body.len(),
            cut / BS,
            lost.len()
        ),
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("out of bounds"),
                "the refusal does not say the block is out of bounds: {msg}"
            );
            assert!(msg.contains("truncated"), "the refusal does not say the container is truncated: {msg}");
        }
    }
}

/// The fabrication, isolated. The container is cut on a **block boundary**, so
/// `z.dat`'s last data block is entirely past EOF and there is nothing at all
/// to read there.
///
/// `CtfsReader` at least failed here (`read_exact` → `UnexpectedEof`).
/// `ConcurrentCtfsReader` did not: `pread_compat::pread` was
/// `file.read_at(buf, offset)?; Ok(buf.len())` — one non-looping positional
/// read whose short count was discarded and reported as a full read, into a
/// buffer pre-zeroed by `vec![0u8; to_read]`. It returned 12 388 bytes,
/// success, and 100 trailing zeros: the only reader in the workspace known to
/// invent content rather than merely serve it from the wrong place.
///
/// The assertion is deliberately in two parts. `is_err()` is the bound; the
/// zero-tail check reports the fabrication if the bound is ever removed again,
/// so a future reader of a failure here sees *both* defects rather than only
/// the block number.
///
/// It does **not** work in the other direction, and it should not be relied on
/// to: reinstating the old non-looping `pread` on its own leaves this test
/// green, because the bound refuses the block before any positional read is
/// issued. The `pread` defect is guarded by the unit tests in
/// `pread_compat.rs` (three of which go red against the old implementation),
/// not by this file.
#[test]
fn a_block_aligned_truncation_is_not_answered_with_fabricated_zeros() {
    let dir = TempDir::new().unwrap();
    let (path, survivor, lost) = sealed_two_stream_container(dir.path());
    let sealed = fs::read(&path).unwrap();
    let last = assert_z_owns_the_last_block(&sealed);

    let cut = last as usize * BS;
    fs::write(&path, &sealed[..cut]).unwrap();
    assert_eq!(cut % BS, 0, "this fixture must cut on a block boundary");

    let c = ConcurrentCtfsReader::open(&path).unwrap();
    assert_eq!(c.read_file("meta.dat").unwrap(), survivor, "the surviving stream came back changed");

    match c.read_file("z.dat") {
        Ok(body) => {
            let zeros = body.iter().rev().take_while(|b| **b == 0).count();
            panic!(
                "ConcurrentCtfsReader returned {} bytes with no error for a stream whose last data block ({last}) is \
                 entirely past the end of the {}-byte container, with {} trailing zero bytes; the expected content \
                 ends with {:?}. A short positional read was reported as a full one and the pre-zeroed buffer was \
                 served as content.",
                body.len(),
                cut,
                zeros,
                &lost[lost.len() - 4..]
            );
        }
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("out of bounds"),
                "the refusal does not say the block is out of bounds: {msg}"
            );
            assert!(msg.contains("truncated"), "the refusal does not say the container is truncated: {msg}");
        }
    }

    // The same input through the seek-based reader, so the two agree.
    let mut r = CtfsReader::open(&path).unwrap();
    assert_eq!(r.read_file("meta.dat").unwrap(), survivor);
    let err = r.read_file("z.dat").unwrap_err().to_string();
    assert!(
        err.contains("out of bounds") && err.contains("truncated"),
        "CtfsReader's refusal does not name the truncation: {err}"
    );
}

/// `read_at` resolves blocks through the same walk, and it is the entry point
/// the streaming consumers use. A bound applied only in `read_file` would leave
/// the partial region reachable through the other door.
#[test]
fn read_at_is_bounded_on_both_readers() {
    let dir = TempDir::new().unwrap();
    let (path, survivor, _) = sealed_two_stream_container(dir.path());
    let sealed = fs::read(&path).unwrap();
    let last = assert_z_owns_the_last_block(&sealed);
    fs::write(&path, &sealed[..last as usize * BS + 100]).unwrap();

    // Offset 3 * BS lands squarely in the lost block.
    let mut buf = [0u8; 100];

    let mut r = CtfsReader::open(&path).unwrap();
    assert!(
        r.read_at("z.dat", (3 * BS) as u64, &mut buf).is_err(),
        "CtfsReader::read_at served the partial region"
    );
    // The surviving stream is still readable through the same door.
    let mut ok = [0u8; 64];
    let n = r.read_at("meta.dat", 8000, &mut ok).unwrap();
    assert_eq!(&ok[..n], &survivor[8000..8000 + n]);

    let c = ConcurrentCtfsReader::open(&path).unwrap();
    assert!(
        c.read_at("z.dat", (3 * BS) as u64, &mut buf).is_err(),
        "ConcurrentCtfsReader::read_at served the partial region"
    );
    let n = c.read_at("meta.dat", 8000, &mut ok).unwrap();
    assert_eq!(&ok[..n], &survivor[8000..8000 + n]);
}

// ---------------------------------------------------------------------------
// Paths 1 and 2 of 3: the mapping root and the mapping blocks.
// ---------------------------------------------------------------------------

/// The bound §5d says is the one applied "in two of three places".
///
/// With this writer's layout a mapping block landing in the partial region is
/// an **agreement and hardening** gap rather than a demonstrated wrong-bytes
/// path: mapping blocks are allocated after the data blocks they cover, so a
/// cut that removes a mapping block also removes something below it. What the
/// bound buys is that the reader refuses *by name*, naming the block and the
/// truncation, the way the Nim and Go readers do — instead of surfacing a bare
/// `failed to fill whole buffer`, which is indistinguishable from a corrupt
/// container and tells a consumer nothing about which stream it lost.
///
/// Red without the bound: the refusal arrives as an `UnexpectedEof` that names
/// neither the block nor the truncation, and (in `ConcurrentCtfsReader`, before
/// the `pread` fix) as a zero pointer, which is the writer's "unallocated"
/// sentinel — so a truncated stream reported itself *absent*.
#[test]
fn a_mapping_root_in_the_partial_region_is_refused_by_name() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("map.ct");

    // `a.dat` is an exact multiple of the block size, so `close` allocates it
    // no further block and `b.dat`'s mapping root is the higher block number.
    let a = deterministic_bytes(21, 3 * BS);
    let b = deterministic_bytes(22, 100);
    let mut w = CtfsWriter::create(&path, BS as u32, MAX_ROOT_ENTRIES).unwrap();
    let h_a = w.add_file("a.dat").unwrap();
    w.write(h_a, &a).unwrap();
    let h_b = w.add_file("b.dat").unwrap();
    w.write(h_b, &b).unwrap();
    w.close().unwrap();

    let sealed = fs::read(&path).unwrap();
    let (_, a_map) = entry_of(&sealed, "a.dat");
    let (_, b_map) = entry_of(&sealed, "b.dat");
    let a_blocks: Vec<u64> = (0..3).map(|i| level1_data_block(&sealed, a_map, i)).collect();
    assert!(
        a_map < b_map && a_blocks.iter().all(|blk| *blk < b_map),
        "the fixture cannot isolate b.dat's mapping root: a.dat occupies map {a_map} data {a_blocks:?} and b.dat's root is {b_map}"
    );

    // Cut exactly at b.dat's mapping root, so it is the first block outside
    // the container's whole blocks and everything a.dat needs survives.
    fs::write(&path, &sealed[..b_map as usize * BS]).unwrap();

    let mut r = CtfsReader::open(&path).unwrap();
    assert_eq!(
        r.read_file("a.dat").unwrap(),
        a,
        "a.dat, whose blocks all survived the cut, came back changed"
    );
    let err = r.read_file("b.dat").unwrap_err().to_string();
    assert!(
        err.contains("mapping root"),
        "CtfsReader's refusal does not say it is the mapping root: {err}"
    );
    assert!(
        err.contains("out of bounds") && err.contains("truncated"),
        "CtfsReader's refusal does not name the truncation: {err}"
    );
    assert!(err.contains("b.dat"), "CtfsReader's refusal does not name the stream: {err}");

    let c = ConcurrentCtfsReader::open(&path).unwrap();
    assert_eq!(
        c.read_file("a.dat").unwrap(),
        a,
        "a.dat, whose blocks all survived the cut, came back changed"
    );
    let err = c.read_file("b.dat").unwrap_err().to_string();
    assert!(
        err.contains("mapping root"),
        "ConcurrentCtfsReader's refusal does not say it is the mapping root: {err}"
    );
    assert!(
        err.contains("out of bounds") && err.contains("truncated"),
        "ConcurrentCtfsReader's refusal does not name the truncation: {err}"
    );
}

/// The second of the three paths, in both of the shapes it takes: a mapping
/// block reached by following the **chain** up a level, and one reached by
/// **descending** from a higher-level block. `snapshot.mem` is 600 data blocks,
/// so its walk goes through a level-2 block and then a level-1 sub-block, and
/// cutting at each in turn exercises one bound apiece.
///
/// The same caveat as the mapping-root test applies: with this writer's layout
/// these are agreement and hardening bounds, not a demonstrated wrong-bytes
/// path. Without them the reader still refuses, but with a bare `failed to fill
/// whole buffer` that names neither the block nor the truncation — which is
/// what the assertions below redden on.
#[test]
fn a_chained_mapping_block_in_the_partial_region_is_refused_by_name() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("chain.ct");

    let snapshot = deterministic_bytes(31, BS * 600);
    let mut w = CtfsWriter::create(&path, BS as u32, MAX_ROOT_ENTRIES).unwrap();
    let h = w.add_file("snapshot.mem").unwrap();
    w.write(h, &snapshot).unwrap();
    w.close().unwrap();

    let sealed = fs::read(&path).unwrap();
    let (_, root) = entry_of(&sealed, "snapshot.mem");
    // The chain pointer lives in the last slot of the level-1 root block; the
    // level-2 block's slot 0 names the level-1 sub-block below it.
    let usable = BS / 8 - 1;
    let level2 = u64_le(&sealed, root as usize * BS + usable * 8);
    assert!(
        level2 > 0,
        "the fixture did not produce a level-2 mapping block; snapshot.mem fits in one level"
    );
    let level1_sub = u64_le(&sealed, level2 as usize * BS);
    assert!(
        level1_sub > level2,
        "the fixture's level-1 sub-block ({level1_sub}) is not above the level-2 block ({level2})"
    );
    // Everything the level-1 root addresses must survive both cuts, or the test
    // would be measuring the data-block bound again.
    for i in 0..usable as u64 {
        assert!(
            level1_data_block(&sealed, root, i) < level2,
            "a level-1 data block sits above the level-2 mapping block"
        );
    }

    // Path 2a: the chain pointer to the level-2 block.
    fs::write(&path, &sealed[..level2 as usize * BS]).unwrap();
    for err in [
        CtfsReader::open(&path).unwrap().read_file("snapshot.mem").unwrap_err().to_string(),
        ConcurrentCtfsReader::open(&path)
            .unwrap()
            .read_file("snapshot.mem")
            .unwrap_err()
            .to_string(),
    ] {
        assert!(err.contains("chain pointer"), "the refusal does not say it is a chain pointer: {err}");
        assert!(
            err.contains("out of bounds") && err.contains("truncated"),
            "the refusal does not name the truncation: {err}"
        );
    }

    // Path 2b: the child pointer down to the level-1 sub-block. The level-2
    // block itself now survives, so only the descent can catch this.
    fs::write(&path, &sealed[..level1_sub as usize * BS]).unwrap();
    for err in [
        CtfsReader::open(&path).unwrap().read_file("snapshot.mem").unwrap_err().to_string(),
        ConcurrentCtfsReader::open(&path)
            .unwrap()
            .read_file("snapshot.mem")
            .unwrap_err()
            .to_string(),
    ] {
        assert!(
            err.contains("child block pointer"),
            "the refusal does not say it is a child block pointer: {err}"
        );
        assert!(
            err.contains("out of bounds") && err.contains("truncated"),
            "the refusal does not name the truncation: {err}"
        );
    }
}

// ---------------------------------------------------------------------------
// The mapping root's null branch: the one bound site nothing pinned.
// ---------------------------------------------------------------------------

/// `entry.map_block == 0` is the fourth way a block number reaches bytes, and
/// it is the only one whose *null* branch has no guard of its own.
///
/// The chain, child and data-block pointers are each checked against `0`
/// explicitly, by name, before `BlockBound::check` ever sees them. The mapping
/// root is not: its sole protection is the `block == 0` arm inside
/// `check_mapping_root`. Drop that arm and `0` sails through the numeric bound
/// — `0 < whole_blocks` for any non-empty container — and the walk reads its
/// *mapping* out of block 0, which is the header and the root directory. The
/// entry's `Size` then clamps the copy, so the read **succeeds** and hands back
/// the container's own metadata as the stream's content.
///
/// That is the campaign's signature — missing data converted into plausible
/// data — and it survives a truncation fixture untouched, because the container
/// here is a clean block multiple. Only a null mapping root provokes it.
///
/// The writer's `0` means "unallocated" (`CTFS-Binary-Format.md` §4, "Null
/// pointers during allocation"), so this is the shape a container takes when
/// an entry landed in block 0 but the mapping block it names never did.
#[test]
fn a_null_mapping_root_is_refused_by_name_rather_than_served_as_block_zero() {
    let dir = TempDir::new().unwrap();
    let (path, survivor, _lost) = sealed_two_stream_container(dir.path());

    let mut damaged = fs::read(&path).unwrap();
    let (z_size, z_map) = entry_of(&damaged, "z.dat");
    assert!(z_map != 0 && z_size > 0, "the fixture's z.dat is already null-mapped");

    // Zero the mapping root of z.dat, and nothing else.
    let encoded = base40_encode("z.dat").unwrap();
    let mut zeroed = false;
    for i in 0..MAX_ROOT_ENTRIES as usize {
        let off = 16 + i * 24;
        if u64_le(&damaged, off + 16) == encoded {
            damaged[off + 8..off + 16].copy_from_slice(&0u64.to_le_bytes());
            zeroed = true;
        }
    }
    assert!(zeroed, "no z.dat entry to damage");
    fs::write(&path, &damaged).unwrap();

    // The container is still a whole number of blocks: no bound that keys off
    // the file length can catch this, which is why it needs its own guard.
    assert_eq!(damaged.len() % BS, 0, "the fixture accidentally truncated the container");
    let whole_blocks = (damaged.len() / BS) as u64;

    // The block index that would fabricate, found in the bytes rather than
    // hardcoded. Read as a mapping block, block 0's slot `i` is bytes
    // `i * 8 ..`, i.e. the header and then the root directory's entry fields —
    // sizes, mapping roots, encoded names. Most decode to absurd block numbers
    // that the *data-block* bound rejects anyway, which is why asserting only
    // on a refusal is not enough here: some slot holds another stream's
    // `map_block`, a small in-bounds number, and that index reads back real
    // blocks belonging to a different stream, successfully.
    let fabricating_index = (0..z_size.div_ceil(BS as u64))
        .find(|i| {
            let slot = u64_le(&damaged, *i as usize * 8);
            slot != 0 && slot < whole_blocks
        })
        .expect(
            "no slot of block 0 decodes to an in-bounds block number, so this fixture cannot \
             exercise the fabricating path; adjust the fixture rather than deleting the test",
        );

    let mut probes: Vec<(String, Result<Vec<u8>, codetracer_ctfs::CtfsError>)> = Vec::new();
    probes.push(("CtfsReader::read_file".into(), CtfsReader::open(&path).unwrap().read_file("z.dat")));
    probes.push((
        "ConcurrentCtfsReader::read_file".into(),
        ConcurrentCtfsReader::open(&path).unwrap().read_file("z.dat"),
    ));
    // The same null, reached at the offset that resolves rather than the one
    // that happens to decode to garbage.
    let at = fabricating_index * BS as u64;
    let mut buf = vec![0u8; 64];
    probes.push((
        format!("CtfsReader::read_at(block index {fabricating_index})"),
        CtfsReader::open(&path)
            .unwrap()
            .read_at("z.dat", at, &mut buf)
            .map(|n| buf[..n].to_vec()),
    ));
    let mut buf2 = vec![0u8; 64];
    probes.push((
        format!("ConcurrentCtfsReader::read_at(block index {fabricating_index})"),
        ConcurrentCtfsReader::open(&path)
            .unwrap()
            .read_at("z.dat", at, &mut buf2)
            .map(|n| buf2[..n].to_vec()),
    ));

    // Pass 1, over *every* probe: did anything come back at all?
    //
    // Structural before message, and across the whole probe set rather than
    // within each one. Removing the guard leaves the walk reading the root
    // directory as a mapping, which still refuses at the indices whose slots
    // decode to absurd block numbers — so a loop that checked probe 1's
    // wording first would report a wording mismatch and never reach the probe
    // that actually gets bytes back. The harm is the fabrication, so the
    // fabrication is what a regression has to report.
    let mut errs: Vec<(String, String)> = Vec::new();
    for (which, got) in probes {
        match got {
            Err(e) => errs.push((which, e.to_string())),
            Ok(content) => {
                let source = (0..whole_blocks as usize)
                    .find(|b| damaged[b * BS..b * BS + content.len()] == content[..]);
                panic!(
                    "{which} served {} bytes for a stream whose mapping root is null, and reported \
                     success; those bytes are the container's own block {:?} — content belonging to \
                     another stream, handed back as z.dat",
                    content.len(),
                    source
                )
            }
        }
    }

    // Pass 2: every refusal has to say which stream was lost and why.
    for (which, err) in errs {
        assert!(
            err.contains("z.dat"),
            "{which}'s refusal does not name the stream that was lost: {err}"
        );
        assert!(
            err.contains("mapping root"),
            "{which}'s refusal does not say it was the mapping root: {err}"
        );
        assert!(
            err.contains("null"),
            "{which}'s refusal does not say the pointer is null: {err}"
        );
        // The container is intact apart from one null pointer. A refusal that
        // blames truncation sends whoever reads it — or a repair tool — after
        // damage that is not there.
        assert!(
            !err.contains("truncated"),
            "{which} blames truncation for a null mapping root in a container that is \
             a whole number of blocks: {err}"
        );
    }

    // The damage cost exactly what was damaged.
    assert_eq!(
        CtfsReader::open(&path).unwrap().read_file("meta.dat").unwrap(),
        survivor,
        "CtfsReader lost meta.dat to a null mapping root on the other stream"
    );
    assert_eq!(
        ConcurrentCtfsReader::open(&path).unwrap().read_file("meta.dat").unwrap(),
        survivor,
        "ConcurrentCtfsReader lost meta.dat to a null mapping root on the other stream"
    );
}
