//! The CTFS **compact profile** body: version-6, `Profile = 1`.
//!
//! Spec: `ctfs-container.md` §1d (normative for every offset here), with
//! §1/§1a/§1b/§1c for the shared version-6 header and §3 for the base40 name
//! packing. The only runnable reference encoder/decoder for this profile is
//! `codetracer-trace-format-nim`'s `src/codetracer_ctfs/compact.nim`
//! (`encodeCompactContainer` / `readCompactDirectory`); this module is a
//! from-scratch Rust port of the same layout and the same six reader-side
//! refusals, built to be byte-identical to that encoder's output.
//!
//! ```text
//! Compact container (version 6, profile = 1):
//!   [0 .. 23]                     ContainerHeaderV6 (24 bytes)
//!   [24 .. 27]                    MemberCount, N (u32 LE)
//!   [28 .. 28 + 24*N - 1]         Directory: N 24-byte (name, offset, length)
//!   [28 + 24*N .. Size - 1]       The members' bytes, concatenated
//!
//!   Size = 28 + 24*N + sum(length)
//! ```
//!
//! **No alignment, deliberately.** Nothing here is padded to a block, a page
//! or a word: a compact container is read whole and issues no ranged read, so
//! there is nothing for alignment to serve (§1d). This module therefore has
//! no block arithmetic in it at all.
//!
//! **Why the decoder checks contiguity and the total, not bounds.** A decoder
//! that checked only `offset + length <= size` would accept a directory with
//! one perturbed `Offset` and serve a SHIFTED member, or one perturbed
//! `Length` and serve a SHORT member — successfully, with nothing to
//! indicate it. Checks 2-4 below make a single perturbed `Offset` or `Length`
//! unrepresentable: any wrong value breaks either contiguity with its
//! neighbour or the total.

use crate::base40::base40_encode;
use crate::header::{EncryptionMethod, MAGIC};

/// Version byte for a container that carries `Profile` and `Compression`
/// (`ctfs-container.md` §1).
pub const CTFS_VERSION_V6: u8 = 6;

/// Size of the version-6 header: 24 bytes (§1).
pub const V6_HEADER_SIZE: usize = 24;
/// Offset of the `Profile` byte in a version-6 header (§1).
pub const V6_PROFILE_OFFSET: usize = 16;
/// Offset of the `Compression` byte in a version-6 header (§1).
pub const V6_COMPRESSION_OFFSET: usize = 17;
/// Offset of the first of the six MUST-be-zero reserved bytes (§1).
pub const V6_RESERVED_OFFSET: usize = 18;

/// Offset of the u32 LE member count: 24, immediately after the header.
pub const COMPACT_MEMBER_COUNT_OFFSET: usize = V6_HEADER_SIZE;
/// Offset of the first directory entry: 28.
pub const COMPACT_DIRECTORY_OFFSET: usize = V6_HEADER_SIZE + 4;
/// `(name: u64, offset: u64, length: u64)` — 24 bytes, the same stride a
/// `FileEntry` occupies, carrying different fields.
pub const COMPACT_DIR_ENTRY_SIZE: usize = 24;
pub const COMPACT_DIR_NAME_OFFSET: usize = 0;
pub const COMPACT_DIR_OFFSET_OFFSET: usize = 8;
pub const COMPACT_DIR_LENGTH_OFFSET: usize = 16;
/// The size of a compact container with no members: 28 bytes.
pub const COMPACT_EMPTY_SIZE: usize = COMPACT_DIRECTORY_OFFSET;

/// `ctfs-container.md` §1a: the closed set of container body layouts a
/// version-6 header may declare.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CtfsProfile {
    Full = 0,
    Compact = 1,
}

/// `ctfs-container.md` §1b: the closed set of WHOLE-FILE compression
/// schemes. Distinct from the per-member `CompressionMethod` in `header.rs`,
/// which carries a third, explicitly unimplemented value (`Lz4`) that this
/// closed set does not repeat.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WholeFileCompression {
    None = 0,
    Zstd = 1,
}

/// `ctfs-container.md` §1e: the recommended default profile-selection
/// threshold, measured against RAW STREAM BYTES (the logical size of the
/// members before any compression), never against compressed size.
pub const DEFAULT_RAW_BYTE_THRESHOLD: u64 = 1 << 20; // 1 MiB

/// Which profile a writer should choose for a recording whose raw (logical,
/// pre-compression) byte count is `raw_bytes`. §1e: below the threshold,
/// compact; at or above it, full. This is a RECOMMENDED default, not
/// something a reader may depend on — the profile is always read from the
/// header, never inferred from size.
pub fn choose_profile_for_raw_bytes(raw_bytes: u64) -> CtfsProfile {
    choose_profile_for_raw_bytes_with_threshold(raw_bytes, DEFAULT_RAW_BYTE_THRESHOLD)
}

/// As `choose_profile_for_raw_bytes`, with an explicit threshold — so a
/// different figure can be tested, or argued against the default, without
/// touching the decision rule itself.
pub fn choose_profile_for_raw_bytes_with_threshold(raw_bytes: u64, threshold: u64) -> CtfsProfile {
    if raw_bytes < threshold {
        CtfsProfile::Compact
    } else {
        CtfsProfile::Full
    }
}

/// One member of a compact container: a name (§3) and its raw payload.
///
/// §1d / §1e: a compact container's members are stored AS WRITTEN. Member
/// formats that carry independent per-member zstd framing (chunked
/// compressed tables, seekable-zstd streams) must be inflated by the caller
/// before being handed to the encoder — see `encode_compact_container`'s
/// `reject_framed_members` behaviour.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactMember {
    pub name: String,
    pub payload: Vec<u8>,
}

impl CompactMember {
    pub fn new(name: impl Into<String>, payload: Vec<u8>) -> Self {
        CompactMember {
            name: name.into(),
            payload,
        }
    }
}

/// A validated directory entry (reader side).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactDirEntry {
    pub name: String,
    pub encoded_name: u64,
    pub offset: u64,
    pub length: u64,
}

/// A validated compact-container directory. Constructing one of these is the
/// only way this module hands out a member: every read goes through §1d's
/// six checks in `read_compact_directory`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactDirectory {
    pub entries: Vec<CompactDirEntry>,
    pub size: u64,
}

fn write_u32_le(buf: &mut [u8], offset: usize, val: u32) {
    buf[offset..offset + 4].copy_from_slice(&val.to_le_bytes());
}

fn write_u64_le(buf: &mut [u8], offset: usize, val: u64) {
    buf[offset..offset + 8].copy_from_slice(&val.to_le_bytes());
}

fn read_u32_le(buf: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap())
}

fn read_u64_le(buf: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap())
}

/// Byte offset of directory entry `index` from the start of the image.
pub fn compact_dir_entry_offset(index: usize) -> usize {
    COMPACT_DIRECTORY_OFFSET + index * COMPACT_DIR_ENTRY_SIZE
}

/// `ctfs-container.md` §1d: `Size = 28 + 24*N + sum(length)`. This is the
/// layout's whole claim, so the encoder and any measurement tooling compute
/// it from one place.
pub fn compact_container_size(members: &[CompactMember]) -> u64 {
    let mut total = COMPACT_DIRECTORY_OFFSET as u64 + members.len() as u64 * COMPACT_DIR_ENTRY_SIZE as u64;
    for m in members {
        total += m.payload.len() as u64;
    }
    total
}

const BASE40_ALPHABET: &[u8] = b"\x000123456789abcdefghijklmnopqrstuvwxyz./-";

/// Decode a base40 `u64` exactly as `codetracer_ctfs/base40.nim`'s
/// `base40Decode` does: a FIXED 12 iterations (never fewer, never an early
/// break), keeping any embedded `'\0'` that falls before a later non-padding
/// character. The existing `crate::base40::base40_decode` instead stops at
/// the first zero digit, which is the right shape for a well-formed name but
/// is NOT what §1d check 5 needs to catch "a padding character before a
/// non-padding one": that trap is only visible if the decode keeps going
/// past the embedded `'\0'` and the round-trip re-encode is compared against
/// the ORIGINAL value. This function exists so that comparison is possible.
fn base40_decode_raw(mut val: u64) -> String {
    let mut chars = [0u8; 12];
    let mut last_non_zero: i32 = -1;
    for (i, c) in chars.iter_mut().enumerate() {
        let idx = (val % 40) as usize;
        val /= 40;
        if idx == 0 {
            *c = 0;
        } else {
            *c = BASE40_ALPHABET[idx];
            last_non_zero = i as i32;
        }
    }
    if last_non_zero < 0 {
        String::new()
    } else {
        chars[..=(last_non_zero as usize)].iter().map(|&b| b as char).collect()
    }
}

/// §3: a name is representable iff it is 1..=12 characters, every one of
/// them from `0-9`, `a-z`, `.`, `/`, `-` — no space, no capitals, and no
/// embedded padding.
fn base40_encodable(name: &str) -> bool {
    if name.is_empty() || name.chars().count() > 12 {
        return false;
    }
    name.chars()
        .all(|c| c.is_ascii_digit() || c.is_ascii_lowercase() || c == '.' || c == '/' || c == '-')
}

/// `ctfs-container.md` §1d check 5: a name is non-zero and round-trips
/// through §3's packing.
///
/// Two things this refuses that a bare decode does not: a `u64` at or above
/// `40^12`, which names nothing (the 12 base-40 digits cannot represent it,
/// so re-encoding what they decode to can never reproduce it), and a packing
/// with a padding digit before a non-padding one, whose decoded string
/// carries an embedded NUL and so is never `base40_encodable`.
pub fn name_is_well_formed(encoded: u64) -> bool {
    if encoded == 0 {
        return false;
    }
    let decoded = base40_decode_raw(encoded);
    base40_encodable(&decoded) && base40_encode(&decoded).map(|e| e == encoded).unwrap_or(false)
}

/// Decode a directory entry's name word for display/consumption. Callers
/// that need to know whether the name is well-formed MUST check
/// `name_is_well_formed` first — this just decodes.
pub fn decode_name(encoded: u64) -> String {
    base40_decode_raw(encoded)
}

/// The four-byte zstd frame magic number, little-endian on the wire:
/// `28 B5 2F FD`.
const ZSTD_FRAME_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Best-effort detector for "this member's bytes begin with a zstd frame".
///
/// This is a HEURISTIC, not a proof: §1d / §1f are explicit that a reader
/// must never use this signature to decide whether to inflate, because four
/// bytes of legitimate content can happen to spell it. The obligation this
/// guards is different and sits on the WRITER: §1e says a compact container
/// "MUST NOT contain a zstd frame in any member whose format is not itself a
/// compressed format", and the chunked-compressed-table / seekable-zstd
/// formats that legitimately carry such frames (§1f) are supposed to be
/// INFLATED before reaching this encoder. This module has no knowledge of
/// which member names use those formats (that catalogue lives in
/// `internal-files.md` and the stream writers, outside this crate's
/// container layer), so the best it can do without that catalogue is refuse
/// a payload that starts with the frame magic, surfacing the mistake rather
/// than silently copying real per-member zstd framing into a profile whose
/// whole point is to have none. A caller that KNOWS a member's content
/// legitimately starts with those four bytes must pre-validate and is not
/// forced through this check; see `encode_compact_container_unchecked`.
fn looks_like_zstd_framed(payload: &[u8]) -> bool {
    payload.len() >= 4 && payload[0..4] == ZSTD_FRAME_MAGIC
}

/// Lay `members` out as a version-6 compact container (`ctfs-container.md`
/// §1d). Member payloads are copied VERBATIM — the profile stores a member
/// as written, and byte-exactness on the payload is what makes the
/// round-trip a proof rather than an equivalence.
///
/// Refuses (as `encode_compact_container_unchecked` does not):
/// - a name not representable in the base40 alphabet;
/// - two members with the same name;
/// - a member whose payload begins with a zstd frame magic number (§1e's
///   writer-side obligation; see `looks_like_zstd_framed` for the caveat).
pub fn encode_compact_container(
    members: &[CompactMember],
    compression: WholeFileCompression,
    encryption: EncryptionMethod,
) -> Result<Vec<u8>, String> {
    for m in members {
        if looks_like_zstd_framed(&m.payload) {
            return Err(format!(
                "member '{}' begins with the zstd frame magic number (28 B5 2F FD): the compact \
                 profile stores members as written (§1d) with no per-member zstd framing, and a \
                 member in that format (a chunked compressed table or a seekable-zstd stream, §1f) \
                 must be inflated before it is handed to the compact encoder (§1e) rather than \
                 copied verbatim",
                m.name
            ));
        }
    }
    encode_compact_container_unchecked(members, compression, encryption)
}

/// As `encode_compact_container`, but skips the zstd-frame heuristic — for a
/// caller that has already established (or deliberately wants to test) that
/// a payload's leading bytes are not per-member zstd framing.
pub fn encode_compact_container_unchecked(
    members: &[CompactMember],
    compression: WholeFileCompression,
    encryption: EncryptionMethod,
) -> Result<Vec<u8>, String> {
    let mut seen: Vec<(String, u64)> = Vec::with_capacity(members.len());
    for m in members {
        if !base40_encodable(&m.name) {
            return Err(format!(
                "member name '{}' is not representable in the base40 alphabet of ctfs-container.md \
                 §3 (1..12 characters from 0-9 a-z . / -): encoding it would silently collide with a \
                 shorter name",
                m.name
            ));
        }
        let encoded = base40_encode(&m.name).map_err(|e| format!("{}", e))?;
        for (prev_name, prev_encoded) in &seen {
            if *prev_encoded == encoded {
                return Err(format!(
                    "duplicate member name '{}' (also carried by '{}') in a compact container: §1d \
                     check 6 requires the N names to be distinct",
                    m.name, prev_name
                ));
            }
        }
        seen.push((m.name.clone(), encoded));
    }

    let total = compact_container_size(members);
    if total > usize::MAX as u64 {
        return Err(format!(
            "compact container would be {} bytes, past what this platform can address",
            total
        ));
    }
    let mut image = vec![0u8; total as usize];

    image[0..5].copy_from_slice(&MAGIC);
    image[5] = CTFS_VERSION_V6;
    image[6] = encryption as u8;
    image[7] = 0u8;
    // §1a: a compact container MUST write max_shards = 0 — it has no block
    // number space to partition, so "one shard" would be a second spelling
    // of "no sharding".
    write_u32_le(&mut image, 8, 0u32);
    // §1d: BlockSize = 0. There are no blocks, and writing 4096 "because it
    // is the default" would spell "there are no blocks" as a block size.
    write_u32_le(&mut image, 12, 0u32);
    // §1d: MaxRootEntries = 0. There is no FileEntry array for a maximum to
    // bound.
    image[V6_PROFILE_OFFSET] = CtfsProfile::Compact as u8;
    image[V6_COMPRESSION_OFFSET] = compression as u8;
    // bytes 18..24 stay zero: §1 makes a non-zero value there a refusal.

    write_u32_le(&mut image, COMPACT_MEMBER_COUNT_OFFSET, members.len() as u32);
    let mut payload_off = COMPACT_DIRECTORY_OFFSET + members.len() * COMPACT_DIR_ENTRY_SIZE;
    for (i, m) in members.iter().enumerate() {
        let e = compact_dir_entry_offset(i);
        let encoded = base40_encode(&m.name).map_err(|err| format!("{}", err))?;
        write_u64_le(&mut image, e + COMPACT_DIR_NAME_OFFSET, encoded);
        write_u64_le(&mut image, e + COMPACT_DIR_OFFSET_OFFSET, payload_off as u64);
        write_u64_le(&mut image, e + COMPACT_DIR_LENGTH_OFFSET, m.payload.len() as u64);
        if !m.payload.is_empty() {
            image[payload_off..payload_off + m.payload.len()].copy_from_slice(&m.payload);
        }
        payload_off += m.payload.len();
    }

    if payload_off != image.len() {
        return Err(format!(
            "encoder produced {} bytes but filled {}: §1d's Size identity does not hold, which \
             would mean the container has padding in it",
            image.len(),
            payload_off
        ));
    }
    Ok(image)
}

fn has_ctfs_magic(data: &[u8]) -> bool {
    data.len() >= 5 && data[0..5] == MAGIC
}

/// Parse and VALIDATE the directory of a compact container, applying all six
/// of `ctfs-container.md` §1d's checks and naming the offending value in
/// every refusal.
///
/// `body_reconstructed`: a container under a whole-file compression scheme
/// is stored as `header || compress(rest)`; a reader reconstructs it as
/// `header || decompress(rest)` before this function is meaningful to call.
/// The reconstructed image KEEPS the original header and so still DECLARES
/// its scheme (§1a) — the field is not the gate, so the caller states
/// whether reconstruction has happened, defaulting to "not done", which
/// turns handing this function stored-compressed bytes into a named refusal
/// rather than a directory read out of a compressed body.
pub fn read_compact_directory(data: &[u8], body_reconstructed: bool) -> Result<CompactDirectory, String> {
    if !has_ctfs_magic(data) {
        return Err("not a CTFS container: the first five bytes are not the magic".to_string());
    }
    if data.len() < 6 {
        return Err(format!(
            "CTFS container is {} bytes, short of the version byte at offset 5",
            data.len()
        ));
    }
    if data[5] != CTFS_VERSION_V6 {
        return Err(format!(
            "CTFS header declares version {}, not the version-6 header the compact profile needs \
             (ctfs-container.md §1a)",
            data[5]
        ));
    }
    if data.len() < V6_HEADER_SIZE {
        return Err(format!(
            "CTFS header declares version {} but is only {} bytes, short of the {}-byte \
             version-6 header",
            CTFS_VERSION_V6,
            data.len(),
            V6_HEADER_SIZE
        ));
    }
    let profile_byte = data[V6_PROFILE_OFFSET];
    let profile = match profile_byte {
        0 => CtfsProfile::Full,
        1 => CtfsProfile::Compact,
        other => {
            return Err(format!(
                "CTFS header declares Profile {}, which is not one of the closed set {{0, 1}} \
                 (ctfs-container.md §1a)",
                other
            ))
        }
    };
    if profile != CtfsProfile::Compact {
        return Err(format!(
            "container declares profile {:?}, not Compact: ctfs-container.md §1d describes the \
             compact body only",
            profile
        ));
    }

    for (i, &byte) in data.iter().enumerate().take(V6_HEADER_SIZE).skip(V6_RESERVED_OFFSET) {
        if byte != 0 {
            return Err(format!(
                "CTFS version-6 Reserved byte at offset {} is {}, not 0: §1 requires every \
                 reserved byte to be zero",
                i, byte
            ));
        }
    }

    let compression_byte = data[V6_COMPRESSION_OFFSET];
    let scheme = match compression_byte {
        0 => WholeFileCompression::None,
        1 => WholeFileCompression::Zstd,
        other => {
            return Err(format!(
                "CTFS header declares whole-file Compression {}, which is not one of the closed set \
                 {{0=none, 1=zstd}} (ctfs-container.md §1b)",
                other
            ))
        }
    };
    if scheme != WholeFileCompression::None && !body_reconstructed {
        return Err(format!(
            "compact container declares whole-file compression scheme {:?}: its body must be \
             reconstructed as header || decompress(rest) before the directory is read \
             (ctfs-container.md §1a), and the caller says it has not been",
            scheme
        ));
    }

    let block_size = read_u32_le(data, 8);
    if block_size != 0 {
        return Err(format!(
            "compact container declares BlockSize {}, not 0: §1d requires 0 because the profile \
             has no blocks, and a block size in a layout with no blocks is a second spelling of \
             one state",
            block_size
        ));
    }
    let max_root_entries = read_u32_le(data, 12);
    if max_root_entries != 0 {
        return Err(format!(
            "compact container declares MaxRootEntries {}, not 0: §1d requires 0 because there is \
             no FileEntry array for a maximum to bound",
            max_root_entries
        ));
    }
    let max_shards = data[7];
    if max_shards != 0 {
        return Err(format!(
            "compact container declares MaxShards {}, not 0: §1a requires 0 because the profile \
             has no block-number space to partition",
            max_shards
        ));
    }

    if data.len() < COMPACT_DIRECTORY_OFFSET {
        return Err(format!(
            "compact container is {} bytes, short of the {} a header and member count occupy",
            data.len(),
            COMPACT_DIRECTORY_OFFSET
        ));
    }

    let count = read_u32_le(data, COMPACT_MEMBER_COUNT_OFFSET);
    // §1d check 1: the directory itself fits.
    let dir_end = COMPACT_DIRECTORY_OFFSET as u64 + count as u64 * COMPACT_DIR_ENTRY_SIZE as u64;
    if dir_end > data.len() as u64 {
        return Err(format!(
            "compact container declares {} members, whose directory would end at byte {} of a \
             {}-byte container (§1d check 1)",
            count,
            dir_end,
            data.len()
        ));
    }

    let mut dir = CompactDirectory {
        entries: Vec::with_capacity(count as usize),
        size: data.len() as u64,
    };
    let mut expected = dir_end; // §1d check 2: the first member starts right here.
    for i in 0..count as usize {
        let e = compact_dir_entry_offset(i);
        let encoded = read_u64_le(data, e + COMPACT_DIR_NAME_OFFSET);
        let offset = read_u64_le(data, e + COMPACT_DIR_OFFSET_OFFSET);
        let length = read_u64_le(data, e + COMPACT_DIR_LENGTH_OFFSET);

        // §1d check 5.
        if !name_is_well_formed(encoded) {
            return Err(format!(
                "compact directory entry {} carries name word {}, which does not round-trip \
                 through the base40 packing of ctfs-container.md §3 (§1d check 5)",
                i, encoded
            ));
        }
        let name = decode_name(encoded);

        // §1d checks 2 and 3, as one: the member begins where its
        // predecessor ended, and the first begins where the directory ended.
        if offset != expected {
            return Err(format!(
                "compact directory entry {} ('{}') declares offset {} but the members are \
                 contiguous and the previous one ended at {} (§1d check {}): a gap would be \
                 padding and an overlap or a jump would serve a shifted member",
                i,
                name,
                offset,
                expected,
                if i == 0 { "2" } else { "3" }
            ));
        }
        if length > data.len() as u64 || offset + length > data.len() as u64 {
            return Err(format!(
                "compact directory entry {} ('{}') declares {} bytes at offset {}, past the end \
                 of a {}-byte container",
                i,
                name,
                length,
                offset,
                data.len()
            ));
        }

        // §1d check 6.
        for prev in &dir.entries {
            if prev.encoded_name == encoded {
                return Err(format!(
                    "compact directory names '{}' twice, at entries {} and earlier (§1d check 6)",
                    name, i
                ));
            }
        }

        dir.entries.push(CompactDirEntry {
            name,
            encoded_name: encoded,
            offset,
            length,
        });
        expected = offset + length;
    }

    // §1d check 4: nothing follows the last member.
    if expected != data.len() as u64 {
        return Err(format!(
            "compact container is {} bytes but its {} members end at {}: §1d requires Size = 28 + \
             24*N + sum(length), so the {}-byte difference is padding or truncation (§1d check 4)",
            data.len(),
            count,
            expected,
            data.len() as i64 - expected as i64
        ));
    }

    Ok(dir)
}

/// Index of `name` in a validated directory, or `None`. A linear search over
/// one `u64` per member: §1d states the directory is NOT sorted, so this is
/// the only correct lookup.
pub fn find_compact_member(dir: &CompactDirectory, name: &str) -> Option<usize> {
    if !base40_encodable(name) {
        return None;
    }
    let encoded = base40_encode(name).ok()?;
    dir.entries.iter().position(|e| e.encoded_name == encoded)
}

/// A member's bytes, sliced out of a validated image.
pub fn compact_member_bytes(data: &[u8], dir: &CompactDirectory, name: &str) -> Result<Vec<u8>, String> {
    let idx = find_compact_member(dir, name).ok_or_else(|| format!("internal file not found: {}", name))?;
    let e = &dir.entries[idx];
    Ok(data[e.offset as usize..(e.offset + e.length) as usize].to_vec())
}

/// Every member of a compact container, in directory order.
pub fn decode_compact_container(data: &[u8], body_reconstructed: bool) -> Result<Vec<CompactMember>, String> {
    let dir = read_compact_directory(data, body_reconstructed)?;
    let mut members = Vec::with_capacity(dir.entries.len());
    for e in &dir.entries {
        let payload = data[e.offset as usize..(e.offset + e.length) as usize].to_vec();
        members.push(CompactMember {
            name: e.name.clone(),
            payload,
        });
    }
    Ok(members)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(name: &str, payload: &[u8]) -> CompactMember {
        CompactMember::new(name, payload.to_vec())
    }

    // -----------------------------------------------------------------
    // Round trips, N = 0, 1, >= 2, with the size identity checked each time.
    // -----------------------------------------------------------------

    #[test]
    fn round_trip_n0() {
        let members: Vec<CompactMember> = vec![];
        let image = encode_compact_container(&members, WholeFileCompression::None, EncryptionMethod::None).unwrap();
        assert_eq!(image.len(), COMPACT_EMPTY_SIZE);
        assert_eq!(compact_container_size(&members), COMPACT_EMPTY_SIZE as u64);

        let dir = read_compact_directory(&image, false).unwrap();
        assert_eq!(dir.entries.len(), 0);
        assert_eq!(dir.size, COMPACT_EMPTY_SIZE as u64);

        let decoded = decode_compact_container(&image, false).unwrap();
        assert_eq!(decoded, members);
    }

    #[test]
    fn round_trip_n1() {
        let members = vec![member("meta.dat", b"hello world, this is meta")];
        let image = encode_compact_container(&members, WholeFileCompression::None, EncryptionMethod::None).unwrap();
        assert_eq!(image.len() as u64, compact_container_size(&members));

        let dir = read_compact_directory(&image, false).unwrap();
        assert_eq!(dir.entries.len(), 1);
        assert_eq!(dir.entries[0].offset, COMPACT_DIRECTORY_OFFSET as u64 + COMPACT_DIR_ENTRY_SIZE as u64);

        let decoded = decode_compact_container(&image, false).unwrap();
        assert_eq!(decoded, members);
    }

    #[test]
    fn round_trip_n_ge_2() {
        let members = vec![
            member("meta.dat", b"metadata-contents"),
            member("steps.dat", b""), // empty member, exercised deliberately
            member("t00000000001", &[0u8, 1, 2, 3, 255, 254, 0, 0, 9]),
            member("a.b/c-d", b"x"),
        ];
        let image = encode_compact_container(&members, WholeFileCompression::None, EncryptionMethod::None).unwrap();
        assert_eq!(image.len() as u64, compact_container_size(&members));

        let dir = read_compact_directory(&image, false).unwrap();
        assert_eq!(dir.entries.len(), members.len());
        // Contiguity, directory order == input order, per §1d.
        let mut expected_offset = COMPACT_DIRECTORY_OFFSET as u64 + (members.len() * COMPACT_DIR_ENTRY_SIZE) as u64;
        for (entry, m) in dir.entries.iter().zip(members.iter()) {
            assert_eq!(entry.name, m.name);
            assert_eq!(entry.offset, expected_offset);
            assert_eq!(entry.length, m.payload.len() as u64);
            expected_offset += m.payload.len() as u64;
        }
        assert_eq!(expected_offset, image.len() as u64);

        let decoded = decode_compact_container(&image, false).unwrap();
        assert_eq!(decoded, members);
    }

    // -----------------------------------------------------------------
    // Each of the six refusals, provoked independently.
    // -----------------------------------------------------------------

    fn sample_image() -> (Vec<u8>, Vec<CompactMember>) {
        let members = vec![
            member("meta.dat", b"0123456789"),
            member("steps.dat", b"abcdefg"),
            member("values.dat", b"xy"),
        ];
        let image = encode_compact_container(&members, WholeFileCompression::None, EncryptionMethod::None).unwrap();
        (image, members)
    }

    #[test]
    fn baseline_sample_is_valid() {
        let (image, _members) = sample_image();
        read_compact_directory(&image, false).expect("unperturbed sample must validate");
    }

    #[test]
    fn refusal_1_directory_does_not_fit() {
        let (mut image, members) = sample_image();
        // Bump the declared count past what the file can hold a directory for.
        let bogus_count = (members.len() + 50) as u32;
        image[COMPACT_MEMBER_COUNT_OFFSET..COMPACT_MEMBER_COUNT_OFFSET + 4].copy_from_slice(&bogus_count.to_le_bytes());
        let err = read_compact_directory(&image, false).unwrap_err();
        assert!(err.contains(&bogus_count.to_string()), "error should name the offending count: {}", err);
        assert!(err.contains("check 1"), "error should cite check 1: {}", err);
    }

    #[test]
    fn refusal_2_first_member_does_not_start_right_after_directory() {
        let (mut image, _members) = sample_image();
        let e0 = compact_dir_entry_offset(0);
        let bogus_offset: u64 = 9999;
        image[e0 + COMPACT_DIR_OFFSET_OFFSET..e0 + COMPACT_DIR_OFFSET_OFFSET + 8].copy_from_slice(&bogus_offset.to_le_bytes());
        let err = read_compact_directory(&image, false).unwrap_err();
        assert!(err.contains(&bogus_offset.to_string()), "error should name the offending offset: {}", err);
        assert!(err.contains("check 2"), "error should cite check 2: {}", err);
    }

    #[test]
    fn refusal_3_members_not_contiguous() {
        let (mut image, _members) = sample_image();
        let e1 = compact_dir_entry_offset(1);
        let bogus_offset: u64 = 12345;
        image[e1 + COMPACT_DIR_OFFSET_OFFSET..e1 + COMPACT_DIR_OFFSET_OFFSET + 8].copy_from_slice(&bogus_offset.to_le_bytes());
        let err = read_compact_directory(&image, false).unwrap_err();
        assert!(err.contains(&bogus_offset.to_string()), "error should name the offending offset: {}", err);
        assert!(err.contains("check 3"), "error should cite check 3: {}", err);
    }

    #[test]
    fn refusal_4_nothing_follows_the_last_member() {
        let (mut image, members) = sample_image();
        let last = members.len() - 1;
        let e_last = compact_dir_entry_offset(last);
        // Shrink the last member's declared length without touching its
        // offset, so checks 2/3 stay satisfied and only check 4 fires.
        let bogus_length: u64 = 1;
        image[e_last + COMPACT_DIR_LENGTH_OFFSET..e_last + COMPACT_DIR_LENGTH_OFFSET + 8]
            .copy_from_slice(&bogus_length.to_le_bytes());
        let err = read_compact_directory(&image, false).unwrap_err();
        assert!(err.contains("check 4"), "error should cite check 4: {}", err);
    }

    #[test]
    fn refusal_4_n0_size_must_be_28() {
        let members: Vec<CompactMember> = vec![];
        let mut image = encode_compact_container(&members, WholeFileCompression::None, EncryptionMethod::None).unwrap();
        image.push(0); // one trailing byte past the empty container's 28
        let err = read_compact_directory(&image, false).unwrap_err();
        assert!(err.contains("check 4"), "error should cite check 4: {}", err);
    }

    #[test]
    fn refusal_5_name_zero() {
        let (mut image, _members) = sample_image();
        let e0 = compact_dir_entry_offset(0);
        image[e0 + COMPACT_DIR_NAME_OFFSET..e0 + COMPACT_DIR_NAME_OFFSET + 8].copy_from_slice(&0u64.to_le_bytes());
        let err = read_compact_directory(&image, false).unwrap_err();
        assert!(err.contains("check 5"), "error should cite check 5: {}", err);
        assert!(err.contains('0'), "error should name the offending value: {}", err);
    }

    #[test]
    fn refusal_5_name_above_40_pow_12() {
        let (mut image, _members) = sample_image();
        // 40^12 names nothing: it is one past the largest representable
        // packing. u64::MAX is comfortably past it too.
        let bogus_name: u64 = u64::MAX;
        let e0 = compact_dir_entry_offset(0);
        image[e0 + COMPACT_DIR_NAME_OFFSET..e0 + COMPACT_DIR_NAME_OFFSET + 8].copy_from_slice(&bogus_name.to_le_bytes());
        let err = read_compact_directory(&image, false).unwrap_err();
        assert!(err.contains("check 5"), "error should cite check 5: {}", err);
        assert!(err.contains(&bogus_name.to_string()), "error should name the offending value: {}", err);
    }

    #[test]
    fn refusal_5_padding_before_non_padding() {
        let (mut image, _members) = sample_image();
        // Construct a name word whose digit 0 (the first character) is the
        // padding index 0 and whose digit 1 (the second character) is a
        // real character ('0', alphabet index 1): 0 + 1*40 = 40. §1d: a
        // padding character before a non-padding one is refused by check 5,
        // because `base40Encode`/`base40_encode` can never produce it (a
        // well-formed name's padding only ever trails) and decoding it
        // yields a string with an embedded NUL that is not base40-encodable.
        let bogus_name: u64 = 40;
        assert!(!name_is_well_formed(bogus_name), "fixture itself must be the trap this test names");
        let e0 = compact_dir_entry_offset(0);
        image[e0 + COMPACT_DIR_NAME_OFFSET..e0 + COMPACT_DIR_NAME_OFFSET + 8].copy_from_slice(&bogus_name.to_le_bytes());
        let err = read_compact_directory(&image, false).unwrap_err();
        assert!(err.contains("check 5"), "error should cite check 5: {}", err);
        assert!(err.contains(&bogus_name.to_string()), "error should name the offending value: {}", err);
    }

    #[test]
    fn refusal_6_duplicate_names() {
        let (mut image, _members) = sample_image();
        let e0 = compact_dir_entry_offset(0);
        let e1 = compact_dir_entry_offset(1);
        let name0 = read_u64_le(&image, e0 + COMPACT_DIR_NAME_OFFSET);
        image[e1 + COMPACT_DIR_NAME_OFFSET..e1 + COMPACT_DIR_NAME_OFFSET + 8].copy_from_slice(&name0.to_le_bytes());
        let err = read_compact_directory(&image, false).unwrap_err();
        assert!(err.contains("check 6"), "error should cite check 6: {}", err);
    }

    #[test]
    fn encoder_refuses_duplicate_names() {
        let members = vec![member("meta.dat", b"a"), member("meta.dat", b"b")];
        let err = encode_compact_container(&members, WholeFileCompression::None, EncryptionMethod::None).unwrap_err();
        assert!(err.contains("meta.dat"));
    }

    #[test]
    fn encoder_refuses_unrepresentable_name() {
        let members = vec![member("HAS CAPS", b"a")];
        let err = encode_compact_container(&members, WholeFileCompression::None, EncryptionMethod::None).unwrap_err();
        assert!(err.contains("HAS CAPS"));
    }

    #[test]
    fn encoder_refuses_zstd_framed_member() {
        let mut payload = vec![0x28u8, 0xB5, 0x2F, 0xFD];
        payload.extend_from_slice(b"fake-frame-body");
        let members = vec![member("steps.dat", &payload)];
        let err = encode_compact_container(&members, WholeFileCompression::None, EncryptionMethod::None).unwrap_err();
        assert!(err.contains("zstd frame"));
        // The unchecked variant must still accept it (used by tests above
        // that deliberately exercise other paths and by a caller that has
        // already confirmed the content is not framed).
        encode_compact_container_unchecked(&members, WholeFileCompression::None, EncryptionMethod::None).unwrap();
    }

    // -----------------------------------------------------------------
    // base40 round trip, including the alphabet trap.
    // -----------------------------------------------------------------

    #[test]
    fn base40_alphabet_trap() {
        // Index 0 is '\0' padding; index 1 is '0', not '\0'.
        assert_eq!(decode_name(0), "");
        assert_eq!(base40_encode("0").unwrap(), 1);
        assert_eq!(decode_name(1), "0");

        // No space, no capitals.
        assert!(!base40_encodable(" "));
        assert!(!base40_encodable("A"));
        assert!(!base40_encodable("Z"));
        assert!(base40_encode(" ").is_err());
        assert!(base40_encode("A").is_err());

        // A padding character before a non-padding one must be refused.
        assert!(!name_is_well_formed(40)); // digit0=0 (pad), digit1=1 ('0')
    }

    #[test]
    fn base40_round_trip_every_character() {
        for c in "0123456789abcdefghijklmnopqrstuvwxyz./-".chars() {
            let s = c.to_string();
            let encoded = base40_encode(&s).unwrap();
            assert!(name_is_well_formed(encoded), "char '{}' -> {} must be well-formed", c, encoded);
            assert_eq!(decode_name(encoded), s);
        }
    }

    #[test]
    fn base40_round_trip_max_length_name() {
        let name = "abcdefghijkl"; // 12 chars
        let encoded = base40_encode(name).unwrap();
        assert!(name_is_well_formed(encoded));
        assert_eq!(decode_name(encoded), name);
    }

    #[test]
    fn choose_profile_threshold() {
        assert_eq!(choose_profile_for_raw_bytes(DEFAULT_RAW_BYTE_THRESHOLD - 1), CtfsProfile::Compact);
        assert_eq!(choose_profile_for_raw_bytes(DEFAULT_RAW_BYTE_THRESHOLD), CtfsProfile::Full);
        assert_eq!(choose_profile_for_raw_bytes(0), CtfsProfile::Compact);
    }
}
