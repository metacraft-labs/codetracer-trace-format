//! The Rust writers over a mapping that does not resolve
//! (`CTFS-Binary-Format.md` §4, and the write-side note under §5d).
//!
//! # What this pins, and why it is here rather than only in Nim
//!
//! M61a closed the *read* side of the null-data-block defect in nine Nim
//! readers: a mapping slot holding `0` resolved to block **0**, so the reader
//! served the container's own header and root directory as a stream's content.
//! M61b closed the *write* side, where the same zero is destructive rather
//! than merely wrong — `codetracer-trace-format-nim`'s `writeToFile` and the
//! recorder's hand-maintained copy both computed `block_offset(0) == 0` and
//! wrote the caller's payload **over the header and the entire root
//! directory**, returning success while doing it.
//!
//! "A container's header must survive a mapping this writer cannot resolve" is
//! a property of the *format*, not of one implementation, so it has to be
//! asserted of each writer independently. Asserting it only in Nim would leave
//! the claim "the Rust writers do not have this hole" as a reading of the
//! source rather than a measurement. These tests are that measurement.
//!
//! # The structural reason the answer differs
//!
//! `CtfsWriter` and `ConcurrentCtfsWriter` never resolve a mapping in order to
//! write through it. Each keeps the file's partial block in an in-memory
//! buffer and reuses a `pending_block`, so the mapping is only ever *inserted*
//! into. The one place either of them walks the chain is
//! `CtfsWriter::open_append`, which resolves the last data block to read the
//! partial buffer back — and `navigate_to_data_block` refuses a null pointer at
//! every level *including* level 1, so the walk errors instead of returning
//! block 0.
//!
//! The Nim `writeToFile` has no per-file buffer and therefore re-resolves the
//! current partial block on every call, which is what put an unchecked
//! `lookup_data_block` result on its write path. Same format, same walk,
//! different exposure — and the tests below are what keeps the Rust side on
//! the right one if that design ever changes.
//!
//! # What failure actually looks like here — read this before adding a case
//!
//! **The Rust failure mode is the opposite direction from the Nim one, and the
//! names of these tests said otherwise until review corrected them.**
//! `open_append` performs *no writes*: it opens the file `read(true)
//! .write(true)` without truncating, reads the header, the entry and the last
//! data-block chain, and only then constructs a `BufWriter` into which nothing
//! has yet been written. There is no `Drop` impl. So block 0 **cannot** change
//! during `open_append`, for any input, corrupt or not — which means the
//! `after[..BS] == before[..BS]` assertions below are cheap invariants that
//! hold vacuously, *not* the thing being tested. They are kept because they
//! cost nothing and would catch a future `open_append` that did start writing;
//! they must never be mistaken for the load-bearing assertion.
//!
//! Stripping the level-1 check does not make Rust write over block 0; it makes
//! Rust read block 0 **into the stream**. Measured on a mutated build: block 0
//! was unchanged apart from the two `FileEntry.size` bytes, the magic was
//! intact, and `steps.dat` came back with `c0 de 72 ac` — the CTFS magic —
//! spliced in where its own payload belonged. That is the M61a *read*-side
//! defect surfacing through a writer's reopen. The assertions that carry
//! weight for the WRITER are therefore the `.err().expect(...)` refusals and
//! the `msg.contains` checks — nothing else in the first two cases can fail.
//!
//! `the_refused_stream_never_reads_back_containing_block_zero` pins the other
//! half, and it is worth being precise about which: it exercises the **reader**
//! guard (`reader.rs`'s level-1 check, the Rust counterpart of M61a), not the
//! writer's. Verified by mutation in both directions — stripping the *writer*
//! check leaves it green, while stripping the *reader* check turns it red with
//! `the damaged stream read back as 4100 bytes … contains the CTFS magic:
//! true`. So the three writer cases and this one are independent gates, and a
//! regression in either layer reddens exactly one test.
//!
//! # Two gaps this file does NOT cover
//!
//! 1. `read_last_data_block_chain` only runs when `has_partial` is true, so for
//!    a container whose `entry.size` is an exact block multiple `open_append`
//!    validates nothing at all.
//! 2. A later `append` reaches `insert_data_block_chain`, which reads a `0`
//!    chain pointer as "not allocated yet" and allocates a replacement. It
//!    cannot distinguish "unallocated" from "corrupted", so on a crash-damaged
//!    container it silently orphans the existing level-2+ subtree — data loss
//!    on the write path, reached through a null mapping pointer, untested.
//!
//! Neither threatens the header or the root directory, which is why "the Rust
//! writers do not have the M61b hole" stands; but "the Rust writers have no
//! null-pointer hole at all" would be false.
//!
//! # NO MOCKS
//!
//! Every case writes a real container with the production `CtfsWriter` onto a
//! real filesystem, damages it the way a crash actually damages it (one u64
//! zeroed in a real mapping block), and drives the production reopen path over
//! the result.

use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use codetracer_ctfs::{CtfsReader, CtfsWriter};
use tempfile::TempDir;

const BS: usize = 4096;
const MAX_ROOT_ENTRIES: u32 = 16;

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

/// Write one stream per `(name, content)` pair and seal the container.
fn sealed_container(path: &Path, files: &[(&str, Vec<u8>)]) {
    let mut w = CtfsWriter::create(path, BS as u32, MAX_ROOT_ENTRIES).unwrap();
    for (name, content) in files {
        let h = w.add_file(name).unwrap();
        w.write(h, content).unwrap();
    }
    w.close().unwrap();
}

/// `(size, map_block)` of the root entry carrying `name`.
fn entry_of(image: &[u8], name: &str) -> (u64, u64) {
    let encoded = codetracer_ctfs::base40_encode(name).unwrap();
    for i in 0..MAX_ROOT_ENTRIES as usize {
        let off = 16 + i * 24;
        let name_val = u64::from_le_bytes(image[off + 16..off + 24].try_into().unwrap());
        if name_val == encoded {
            return (
                u64::from_le_bytes(image[off..off + 8].try_into().unwrap()),
                u64::from_le_bytes(image[off + 8..off + 16].try_into().unwrap()),
            );
        }
    }
    panic!("no root entry named {name}");
}

fn read_image(path: &Path) -> Vec<u8> {
    let mut f = fs::File::open(path).unwrap();
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).unwrap();
    buf
}

/// Overwrite one u64 in the container, in place, without touching anything else.
fn poke_u64(path: &Path, offset: u64, value: u64) {
    let mut f = fs::OpenOptions::new().write(true).open(path).unwrap();
    f.seek(SeekFrom::Start(offset)).unwrap();
    f.write_all(&value.to_le_bytes()).unwrap();
    f.sync_all().unwrap();
}

/// Where block 0 first differs, and to what — what a failing case has to say.
fn block0_damage(before: &[u8], after: &[u8]) -> String {
    let n = BS.min(before.len()).min(after.len());
    let first = (0..n).find(|&i| before[i] != after[i]);
    match first {
        None => "block 0 is unchanged".to_string(),
        Some(first) => {
            let last = (0..n).rev().find(|&i| before[i] != after[i]).unwrap();
            let end = (first + 16).min(n);
            format!(
                "block 0 bytes {first}..{last} were rewritten: was {:02x?} now {:02x?}",
                &before[first..end],
                &after[first..end]
            )
        }
    }
}

// ---------------------------------------------------------------------------

#[test]
fn open_append_refuses_a_null_level_1_slot_rather_than_resolving_it_to_block_zero() {
    // A complete, sealed, block-multiple container — no truncation anywhere —
    // with one level-1 mapping slot zeroed: the state a crash between the
    // mapping-block flush and the data-block flush leaves.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("t.ct");
    // `steps.dat` ends 4 bytes into its second block, so reopening it has to
    // resolve data block 1 to restore the partial buffer.
    let steps = deterministic_bytes(2, BS + 4);
    let meta = deterministic_bytes(1, BS * 2);
    sealed_container(&path, &[("meta.dat", meta.clone()), ("steps.dat", steps.clone())]);

    let image = read_image(&path);
    assert_eq!(image.len() % BS, 0, "the fixture is not block-aligned");
    let (size, root) = entry_of(&image, "steps.dat");
    assert_eq!(size, steps.len() as u64);
    assert_ne!(root, 0);

    poke_u64(&path, root * BS as u64 + 8, 0);
    let before = read_image(&path);

    let opened = CtfsWriter::open_append(&path);

    // Cheap invariant, NOT the load-bearing assertion: `open_append` performs
    // no writes, so this holds vacuously today (see the module header). It is
    // here to catch a future `open_append` that starts writing before it has
    // validated the mapping.
    let after = read_image(&path);
    assert_eq!(
        &after[..BS],
        &before[..BS],
        "reopening for append rewrote the container header and root directory — {}",
        block0_damage(&before, &after)
    );
    assert_eq!(&after[..5], &[0xC0, 0xDE, 0x72, 0xAC, 0xE2], "block 0 lost the CTFS magic");

    let err = opened.err().expect("open_append accepted a null level-1 mapping slot");
    let msg = err.to_string();
    assert!(
        msg.contains("null data block pointer"),
        "the refusal does not name the null data block: {msg}"
    );

    // And the container is still readable, which is the whole point of
    // refusing: the undamaged member survives a refused append untouched.
    let mut r = CtfsReader::open(&path).unwrap();
    assert_eq!(r.read_file("meta.dat").unwrap(), meta);
}

#[test]
fn open_append_refuses_a_null_chain_pointer_rather_than_resolving_it_to_block_zero() {
    // The same destination reached through a different null: the level-2 chain
    // pointer. `usable == 511`, so data block 511 is the first that needs it.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("t.ct");
    let meta = deterministic_bytes(1, BS);
    let big = deterministic_bytes(3, BS * 512 + 4);
    sealed_container(&path, &[("meta.dat", meta.clone()), ("big.dat", big)]);

    let image = read_image(&path);
    let (_, root) = entry_of(&image, "big.dat");
    let usable = BS / 8 - 1;
    let chain_off = root * BS as u64 + (usable * 8) as u64;
    assert_ne!(
        u64::from_le_bytes(image[chain_off as usize..chain_off as usize + 8].try_into().unwrap()),
        0,
        "the fixture does not actually use a level-2 chain"
    );
    poke_u64(&path, chain_off, 0);
    let before = read_image(&path);

    let opened = CtfsWriter::open_append(&path);

    // Cheap invariant, not the load-bearing assertion — see the module header.
    let after = read_image(&path);
    assert_eq!(
        &after[..BS],
        &before[..BS],
        "reopening for append rewrote the container header and root directory — {}",
        block0_damage(&before, &after)
    );
    let err = opened.err().expect("open_append accepted a null chain pointer");
    let msg = err.to_string();
    assert!(
        msg.contains("null chain pointer"),
        "the refusal does not name the null chain pointer: {msg}"
    );
}

#[test]
fn an_undamaged_reopen_and_append_still_works() {
    // The control: the refusals above must not be the writer refusing to
    // append at all. A mid-block append through `open_append` has to succeed
    // and the stream has to read back as the concatenation.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("t.ct");
    let meta = deterministic_bytes(1, BS * 2);
    let head = deterministic_bytes(2, BS + 100);
    sealed_container(&path, &[("meta.dat", meta.clone()), ("steps.dat", head.clone())]);

    let tail = deterministic_bytes(11, 300);
    let mut w = CtfsWriter::open_append(&path).expect("open_append refused a healthy container");
    let h = w.find_file("steps.dat").expect("find_file could not locate steps.dat");
    w.append(h, &tail).unwrap();
    w.close().unwrap();

    let mut expected = head.clone();
    expected.extend_from_slice(&tail);

    let mut r = CtfsReader::open(&path).unwrap();
    assert_eq!(r.read_file("steps.dat").unwrap(), expected);
    assert_eq!(r.read_file("meta.dat").unwrap(), meta);
}

#[test]
fn the_refused_stream_never_reads_back_containing_block_zero() {
    // The assertion that actually carries weight for the Rust side, added in
    // review because the block-0 equality checks above cannot fail here.
    //
    // The Rust consequence of an unresolved mapping is not that block 0 is
    // overwritten but that block 0 is served AS the stream. This pins the
    // outcome from the reader's side, on the same damaged fixture the level-1
    // case builds: `steps.dat` must not come back at all, and in particular
    // must not come back carrying the container magic. With the level-1 check
    // stripped, a mutated build returns `steps.dat` with `c0 de 72 ac` where
    // its own payload belongs — this test is what says so out loud.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("t.ct");
    let meta = deterministic_bytes(1, BS * 2);
    let steps = deterministic_bytes(2, BS + 4);
    sealed_container(&path, &[("meta.dat", meta.clone()), ("steps.dat", steps.clone())]);

    let image = read_image(&path);
    let (_, root) = entry_of(&image, "steps.dat");
    poke_u64(&path, root * BS as u64 + 8, 0);

    // Drive the reopen for realism, but deliberately do NOT assert on it: that
    // refusal is the level-1 case's job, and asserting it here too would make
    // this test fail on the writer's behaviour before it ever reached the
    // reader property it exists to pin.
    let _ = CtfsWriter::open_append(&path);

    // And the damaged stream is not readable as content either. The undamaged
    // member must still read, so this cannot pass by the reader being broken.
    let mut r = CtfsReader::open(&path).unwrap();
    assert_eq!(
        r.read_file("meta.dat").unwrap(),
        meta,
        "the undamaged member stopped reading — this test would pass vacuously"
    );

    match r.read_file("steps.dat") {
        Err(_) => {}
        Ok(got) => panic!(
            "the damaged stream read back as {} bytes instead of being refused; \
             first 8 bytes {:02x?}, contains the CTFS magic: {}",
            got.len(),
            &got[..8.min(got.len())],
            got.windows(4).any(|w| w == [0xC0, 0xDE, 0x72, 0xAC])
        ),
    }
}
