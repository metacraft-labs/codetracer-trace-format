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
//! # The second null-pointer hole: an append that allocates over a damaged slot
//!
//! The first four cases reach the mapping through `open_append`'s
//! `read_last_data_block_chain`, which only runs when the entry has a partial
//! tail. For a container whose `entry.size` is an exact block multiple
//! `open_append` therefore validates *nothing*, and the first thing to touch
//! the mapping is the next `append` — which reaches `insert_data_block_chain`
//! and `navigate_and_insert`, both of which read a `0` pointer as **"not
//! allocated yet"** and allocate a replacement.
//!
//! Those two sites cannot tell "unallocated" from "corrupted" by looking at the
//! pointer alone, and until this file's last four cases existed they did not
//! try: appending to a crash-damaged container overwrote the only pointer to
//! the existing level-2+ subtree, orphaning every data block under it, and
//! returned `Ok`. It is a different failure from the header destruction above —
//! block 0 is never touched — but it is the same zero, on the same write path,
//! and it loses data.
//!
//! They *can* tell the two apart from the index. Both writers fill a mapping in
//! strictly increasing block-index order, so a pointer is legitimately null
//! exactly when the index being placed is the **first index that pointer
//! covers**: the chain pointer to level `k+1` may be null only when the rebased
//! index at that level is `0`, and a level-`k` child pointer may be null only
//! when the remainder within that child is `0`. Anything else means an earlier
//! index already passed through the same pointer, so a null there is damage.
//! That rule is now normative in `CTFS-Binary-Format.md` §4 under "Null
//! pointers during allocation", because both implementations of the walk had
//! the same hole and the spec did not say which reading was right.
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

use codetracer_ctfs::{CtfsError, CtfsReader, CtfsWriter};
use tempfile::TempDir;

mod nim_adjudicator;
use nim_adjudicator::{nim_checker, run_nim_checker};

const BS: usize = 4096;
const MAX_ROOT_ENTRIES: u32 = 16;
/// Data pointers per mapping block: the last of the `BS / 8` entries is the
/// chain pointer. 511 for the default 4096-byte block.
const USABLE: usize = BS / 8 - 1;

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

// ---------------------------------------------------------------------------
// The allocation half: a null pointer that `insert_data_block_chain` and
// `navigate_and_insert` used to read as "not allocated yet".
// ---------------------------------------------------------------------------

/// The u64 at entry `index` of `block`.
fn ptr_at(image: &[u8], block: u64, index: usize) -> u64 {
    let off = block as usize * BS + index * 8;
    u64::from_le_bytes(image[off..off + 8].try_into().unwrap())
}

/// Reopen `path`, append `tail` to `name`, and seal it — the whole sequence a
/// consumer of a closed container performs, as one result. Nothing here pokes
/// at internals: `open_append` / `find_file` / `append` / `close` are the
/// crate's entire public reopen surface.
fn reopen_and_append(path: &Path, name: &str, tail: &[u8]) -> Result<(), CtfsError> {
    let mut w = CtfsWriter::open_append(path)?;
    let h = w.find_file(name).expect("find_file could not locate the stream");
    w.append(h, tail)?;
    w.close()
}

/// The refusal text, for the assertions that check *which* rule refused.
fn refusal_message(outcome: &Result<(), CtfsError>) -> String {
    match outcome {
        Ok(()) => "the append succeeded".to_string(),
        Err(e) => e.to_string(),
    }
}

/// What a stream reads back as, in one line, for a failure message.
fn readback(path: &Path, name: &str) -> String {
    match CtfsReader::open(path) {
        Err(e) => format!("the container no longer opens: {e}"),
        Ok(mut r) => match r.read_file(name) {
            Err(e) => format!("refused by the reader: {e}"),
            Ok(got) => format!("{} bytes", got.len()),
        },
    }
}

#[test]
fn an_append_through_a_null_chain_pointer_is_refused_rather_than_orphaning_the_subtree() {
    // A complete, sealed container whose stream is an exact block multiple —
    // which is precisely the shape `open_append` does not validate, because
    // `read_last_data_block_chain` only runs for a partial tail. The first code
    // to look at the mapping is the append's own allocator.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("t.ct");
    let meta = deterministic_bytes(1, BS);
    // 512 data blocks: indices 0..510 live in the level-1 root, and index 511
    // is the first that needs the level-2 chain, so the chain pointer exists
    // and covers 511 real data blocks.
    let big = deterministic_bytes(3, BS * 512);
    sealed_container(&path, &[("meta.dat", meta.clone()), ("big.dat", big.clone())]);

    let image = read_image(&path);
    let (size, root) = entry_of(&image, "big.dat");
    assert_eq!(size % BS as u64, 0, "the fixture must be an exact block multiple");
    let old_l2 = ptr_at(&image, root, USABLE);
    assert_ne!(old_l2, 0, "the fixture does not actually use a level-2 chain");

    let chain_off = root * BS as u64 + (USABLE * 8) as u64;
    poke_u64(&path, chain_off, 0);
    let before = read_image(&path);

    let outcome = reopen_and_append(&path, "big.dat", &deterministic_bytes(9, BS));

    let after = read_image(&path);
    let new_l2 = ptr_at(&after, root, USABLE);

    if outcome.is_ok() {
        panic!(
            "the append reported success on a container whose level-2 chain pointer was null. \
             The writer wrote a fresh mapping block into root[{USABLE}] (was {old_l2} before the \
             damage, 0 after it, now {new_l2}), so the {USABLE} data blocks the old level-2 \
             subtree at block {old_l2} mapped are no longer referenced by anything in the \
             container and cannot be recovered from it. big.dat now {}",
            readback(&path, "big.dat")
        );
    }

    // Checked before the wording, deliberately: refusing is only worth anything
    // if it also leaves the evidence in place, and the pointer that a repair
    // tool would use to find block `old_l2` again must not have been replaced by
    // a fresh, empty mapping block. Dropping the chain-pointer rule but keeping
    // the child one still produces *a* refusal — from one level further down,
    // after the damage has been done — so an assertion on the message alone
    // would call that a pass of the wrong kind.
    assert_eq!(
        new_l2, 0,
        "the append was refused but still rewrote root[{USABLE}] to {new_l2}, destroying the only \
         thing that says the level-2 subtree at block {old_l2} ever existed"
    );
    let msg = refusal_message(&outcome);
    assert!(
        msg.contains("null chain pointer"),
        "the refusal does not name the null chain pointer: {msg}"
    );
    // And nothing else moved either. A refused append must leave the container
    // byte-identical — the writer allocates its data block before it walks the
    // mapping, so "refused" has to mean the allocation never reached the file,
    // not merely that block 0 survived. The Nim writer is held to the same
    // property (it rolls the provisional allocation back), which is what keeps
    // the two implementations agreeing about what a refusal leaves behind.
    assert_eq!(
        after.len(),
        before.len(),
        "the refused append changed the container's length from {} to {}",
        before.len(),
        after.len()
    );
    assert_eq!(after, before, "{}", block0_damage(&before, &after));

    // The undamaged member is untouched, so nothing above passed vacuously.
    let mut r = CtfsReader::open(&path).unwrap();
    assert_eq!(r.read_file("meta.dat").unwrap(), meta);
}

#[test]
fn an_append_through_a_null_level_2_child_is_refused_rather_than_orphaning_the_subtree() {
    // The same zero one step further down the walk: a level-2 block's child
    // pointer, reached by `navigate_and_insert` rather than by the chain loop
    // in `insert_data_block_chain`. Two separate sites, so two cases.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("t.ct");
    let meta = deterministic_bytes(1, BS);
    // 1023 data blocks. Rebased at level 2, index i maps to r = i - 511, and
    // the level-2 block's entry is r / 511. Index 1022 gives r = 511, i.e.
    // entry 1 — so the level-2 block has two children, and the next append
    // (index 1023, r = 512) descends through entry 1 at a non-zero remainder.
    let big = deterministic_bytes(3, BS * 1023);
    sealed_container(&path, &[("meta.dat", meta.clone()), ("big.dat", big)]);

    let image = read_image(&path);
    let (_, root) = entry_of(&image, "big.dat");
    let l2 = ptr_at(&image, root, USABLE);
    assert_ne!(l2, 0, "the fixture does not use a level-2 chain");
    let old_child = ptr_at(&image, l2, 1);
    assert_ne!(old_child, 0, "the fixture does not use a second level-2 child");

    let child_off = l2 * BS as u64 + 8;
    poke_u64(&path, child_off, 0);
    let before = read_image(&path);

    let outcome = reopen_and_append(&path, "big.dat", &deterministic_bytes(9, BS));

    let after = read_image(&path);
    let new_child = ptr_at(&after, l2, 1);

    if outcome.is_ok() {
        panic!(
            "the append reported success on a container whose level-2 child pointer was null. \
             The writer wrote a fresh mapping block into block {l2} entry 1 (was {old_child} \
             before the damage, 0 after it, now {new_child}), orphaning the level-1 subtree at \
             block {old_child}. big.dat now {}",
            readback(&path, "big.dat")
        );
    }

    // Structure before wording, for the reason given in the case above.
    assert_eq!(
        new_child, 0,
        "the append was refused but still rewrote block {l2} entry 1 to {new_child}, orphaning \
         the level-1 subtree at block {old_child}"
    );
    let msg = refusal_message(&outcome);
    assert!(
        msg.contains("null mapping pointer"),
        "the refusal does not name the null mapping pointer: {msg}"
    );
    // And nothing else moved either. A refused append must leave the container
    // byte-identical — the writer allocates its data block before it walks the
    // mapping, so "refused" has to mean the allocation never reached the file,
    // not merely that block 0 survived. The Nim writer is held to the same
    // property (it rolls the provisional allocation back), which is what keeps
    // the two implementations agreeing about what a refusal leaves behind.
    assert_eq!(
        after.len(),
        before.len(),
        "the refused append changed the container's length from {} to {}",
        before.len(),
        after.len()
    );
    assert_eq!(after, before, "{}", block0_damage(&before, &after));

    let mut r = CtfsReader::open(&path).unwrap();
    assert_eq!(r.read_file("meta.dat").unwrap(), meta);
}

/// The control for both cases above, and the one that says the new refusals do
/// not simply stop the writer from ever extending a mapping.
///
/// It deliberately crosses the level-1/level-2 boundary **inside the reopened
/// session**: the sealed container has 510 data blocks (all in the level-1
/// root, no chain pointer at all), and the append takes it to 513, so index 511
/// is the first to need level 2 and the chain pointer is legitimately null when
/// the reopened writer reaches it. That is the exact state the new rule has to
/// keep allowing, and it is where a rule stated one index too strictly would
/// show up.
#[test]
fn a_reopened_append_may_still_create_the_level_2_chain_it_needs() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("t.ct");
    let meta = deterministic_bytes(1, BS * 2);
    let head = deterministic_bytes(4, BS * 510);
    sealed_container(&path, &[("meta.dat", meta.clone()), ("big.dat", head.clone())]);

    let image = read_image(&path);
    let (_, root) = entry_of(&image, "big.dat");
    assert_eq!(
        ptr_at(&image, root, USABLE),
        0,
        "the fixture must start with no level-2 chain, or it does not test creating one"
    );

    let tail = deterministic_bytes(5, BS * 3);
    reopen_and_append(&path, "big.dat", &tail).expect("a healthy reopened append was refused");

    let mut expected = head;
    expected.extend_from_slice(&tail);

    let after = read_image(&path);
    assert_ne!(
        ptr_at(&after, root, USABLE),
        0,
        "the reopened append did not create the level-2 chain it needed"
    );

    let mut r = CtfsReader::open(&path).unwrap();
    assert_eq!(r.read_file("big.dat").unwrap(), expected, "the extended stream is wrong");
    assert_eq!(r.read_file("meta.dat").unwrap(), meta);
}

/// The cross-implementation half, and the reason it is on the *control* rather
/// than on a refusal: a refusal is a fact about one writer, whereas the bytes a
/// successful reopened append leaves behind are a claim about the **format**,
/// and §5d makes agreement between implementations the acceptance criterion for
/// that.
///
/// The fixture is deliberately not a pure function of its inputs. The container
/// is written, sealed, closed, reopened from disk — recovering `NextFreeBlock`
/// from the file length rather than from any in-memory state — and only then
/// extended across the level-1/level-2 boundary of §4. Its bytes therefore
/// depend on the on-disk state carried across the close, which is what makes an
/// agreement here worth asserting; a fixture built in one pass would be
/// satisfied by any writer that is merely self-consistent.
#[test]
fn the_nim_reader_agrees_with_what_a_reopened_append_wrote_across_a_level_boundary() {
    if nim_checker().is_none() {
        return;
    }
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("reopened.ct");

    let meta = deterministic_bytes(21, BS * 2);
    let head = deterministic_bytes(22, BS * 510);
    sealed_container(&path, &[("meta.dat", meta.clone()), ("big.dat", head.clone())]);

    // Two separate reopen-append cycles, so the second one inherits a mapping
    // the first one extended rather than one a single session built.
    let tail1 = deterministic_bytes(23, BS * 3);
    reopen_and_append(&path, "big.dat", &tail1).expect("first reopened append refused");
    let tail2 = deterministic_bytes(24, BS + 77);
    reopen_and_append(&path, "big.dat", &tail2).expect("second reopened append refused");

    let mut expected = head;
    expected.extend_from_slice(&tail1);
    expected.extend_from_slice(&tail2);

    let (out, ok) = run_nim_checker(
        dir.path(),
        &path,
        &[("meta.dat", &meta), ("big.dat", &expected)],
        &["absent.dat"],
    );
    assert!(
        ok,
        "the independent Nim reader does not agree with the container two reopened appends left \
         behind, so this crate's §4 walk across the level-1/level-2 boundary differs from the \
         other implementation's rather than merely being self-consistent:\n{out}"
    );
}
