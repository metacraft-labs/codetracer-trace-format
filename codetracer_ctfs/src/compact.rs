//! The version-6 header fields and the **compact profile** body
//! (`codetracer-trace-format-spec/ctfs-container.md` §1, §1a-§1d).
//!
//! ```text
//! Compact container (version 6, profile = 1):
//!   [0 .. 23]                     ContainerHeaderV6 (24 bytes)
//!   [24 .. 27]                    MemberCount, N (u32 LE)
//!   [28 .. 28 + 24*N - 1]         Directory: N (name: u64, offset: u64, length: u64)
//!   [28 + 24*N .. Size - 1]       The members' bytes, concatenated in directory order
//!
//!   Size = 28 + 24*N + sum(length)
//! ```
//!
//! A directory answers "where does this member start and how long is it",
//! which is all a container that is resident before its first query needs, so
//! nothing here knows about blocks. [`read_compact_directory`] makes every
//! check §1d lists, naming the offending value: checks 2-4 (contiguity and the
//! total) are what make a single perturbed `Offset` or `Length` unable to serve
//! a shifted or short member under a right name, which a bounds check alone
//! would allow.
//!
//! The header fields are closed sets and §1c makes refusing an unknown value
//! normative: an unknown profile is not `full`, an unknown scheme is not
//! `none`, a non-zero reserved byte is not ignored, and a header too short to
//! carry a field the version declares is a refusal rather than an absent
//! field.

use crate::header::MAGIC;
use crate::CtfsError;

/// The container version that carries `Profile` and `Compression`.
pub const VERSION_6: u8 = 6;
/// The version-6 header's size.
pub const V6_HEADER_SIZE: usize = 24;
/// Offset of the `Profile` byte.
pub const PROFILE_OFFSET: usize = 16;
/// Offset of the whole-file `Compression` byte.
pub const COMPRESSION_OFFSET: usize = 17;
/// Offset and length of the reserved bytes that must be zero.
pub const RESERVED_OFFSET: usize = 18;
pub const RESERVED_LEN: usize = 6;
/// Offset of a compact container's `MemberCount`.
pub const COMPACT_MEMBER_COUNT_OFFSET: usize = V6_HEADER_SIZE;
/// Offset of a compact container's first directory entry.
pub const COMPACT_DIRECTORY_OFFSET: usize = V6_HEADER_SIZE + 4;
/// `(name, offset, length)`, three little-endian `u64`s.
pub const COMPACT_ENTRY_SIZE: usize = 24;

/// Which body follows a version-6 header (§1a).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    /// Block 0, the `FileEntry` array and the block map.
    Full = 0,
    /// A directory and the members concatenated (§1d).
    Compact = 1,
}

/// The scheme the container image after the header is stored under (§1b).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WholeFileCompression {
    /// Stored as-is.
    None = 0,
    /// One zstd frame over the image from offset 24 to its end.
    Zstd = 1,
}

fn refused(message: String) -> CtfsError {
    CtfsError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, message))
}

fn u32_at(data: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(data[at..at + 4].try_into().unwrap_or_default())
}

fn u64_at(data: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(data[at..at + 8].try_into().unwrap_or_default())
}

/// Parse byte 16 of a version-6 header against the closed set of §1a.
pub fn parse_profile(value: u8) -> Result<Profile, CtfsError> {
    match value {
        0 => Ok(Profile::Full),
        1 => Ok(Profile::Compact),
        _ => Err(refused(format!("unknown CTFS container profile {value} (§1a: 0 full, 1 compact)"))),
    }
}

/// Parse byte 17 of a version-6 header against the closed set of §1b.
pub fn parse_whole_file_compression(value: u8) -> Result<WholeFileCompression, CtfsError> {
    match value {
        0 => Ok(WholeFileCompression::None),
        1 => Ok(WholeFileCompression::Zstd),
        _ => Err(refused(format!(
            "unknown CTFS whole-file compression scheme {value} (§1b: 0 none, 1 zstd)"
        ))),
    }
}

/// The version-6 header of `data`, or `None` when `data` is a version-5
/// container (full, no whole-file scheme: an inference from a known version,
/// not a default). Any other version, a bad magic, a header too short for the
/// fields its version declares, an unknown profile or scheme and a non-zero
/// reserved byte are refused by name.
pub fn read_v6_header(data: &[u8]) -> Result<Option<(Profile, WholeFileCompression)>, CtfsError> {
    if data.len() < 6 {
        return Err(refused(format!("CTFS container too short for a header ({} bytes)", data.len())));
    }
    if data[..5] != MAGIC {
        return Err(CtfsError::InvalidMagic);
    }
    match data[5] {
        crate::header::VERSION => return Ok(None),
        VERSION_6 => {}
        v => return Err(CtfsError::InvalidVersion(v)),
    }
    if data.len() < V6_HEADER_SIZE {
        return Err(refused(format!("CTFS version-6 header is only {} bytes, not 24 (§1c)", data.len())));
    }
    let profile = parse_profile(data[PROFILE_OFFSET])?;
    let compression = parse_whole_file_compression(data[COMPRESSION_OFFSET])?;
    let reserved = &data[RESERVED_OFFSET..RESERVED_OFFSET + RESERVED_LEN];
    if let Some((k, value)) = reserved.iter().enumerate().find(|(_, b)| **b != 0) {
        return Err(refused(format!(
            "CTFS reserved byte at offset {} is {value}, not 0 (§1)",
            RESERVED_OFFSET + k
        )));
    }
    Ok(Some((profile, compression)))
}

/// The container image a stored object stands for: `header || decompress(rest)`
/// under [`WholeFileCompression::Zstd`], the object itself otherwise
/// (`ctfs-container.md` §1a). The image keeps the stored header, so it still
/// declares its scheme.
pub fn reconstruct_image(stored: Vec<u8>) -> Result<Vec<u8>, CtfsError> {
    match read_v6_header(&stored)? {
        Some((_, WholeFileCompression::Zstd)) => {
            let body = crate::zstd_compat::decode_all(&stored[V6_HEADER_SIZE..])
                .map_err(|e| refused(format!("the zstd container body does not decode: {e}")))?;
            let mut image = Vec::with_capacity(V6_HEADER_SIZE + body.len());
            image.extend_from_slice(&stored[..V6_HEADER_SIZE]);
            image.extend_from_slice(&body);
            Ok(image)
        }
        _ => Ok(stored),
    }
}

/// The stored object for a version-6 `image` that declares
/// [`WholeFileCompression::Zstd`]: the header as it is, then one zstd frame
/// over the rest that pledges its content size, as every frame this crate
/// writes does. The inverse of [`reconstruct_image`].
pub fn compress_image(image: &[u8], level: i32) -> Result<Vec<u8>, CtfsError> {
    if read_v6_header(image)?.map(|(_, c)| c) != Some(WholeFileCompression::Zstd) {
        return Err(refused("the image does not declare the zstd scheme".to_string()));
    }
    let mut stored = image[..V6_HEADER_SIZE].to_vec();
    stored.extend_from_slice(&crate::zstd_frame::compress_pledged(&image[V6_HEADER_SIZE..], level, "the container body").map_err(refused)?);
    Ok(stored)
}

/// Append the content of the one zstd frame `frame` to `out`: how a framed
/// member's chunk is stored in a compact container (`ctfs-container.md` §1f).
/// The frame must declare its content size, as every frame this format writes
/// does, and decode to exactly that many bytes; `what` names the chunk in the
/// refusal.
pub fn append_frame_content(frame: &[u8], what: &str, out: &mut Vec<u8>) -> Result<(), String> {
    if frame.is_empty() {
        return Err(format!("{what}: an empty frame"));
    }
    let declared = crate::zstd_frame::declared_content_size(frame).ok_or_else(|| format!("{what}: the frame does not declare its content size"))?;
    let content = crate::zstd_compat::decode_all(frame).map_err(|e| format!("{what}: the frame does not decode: {e}"))?;
    if content.len() as u64 != declared {
        return Err(format!("{what}: the frame does not decode to its declared {declared} bytes"));
    }
    out.extend_from_slice(&content);
    Ok(())
}

/// Whether a directory name word is a name §3's packing can produce: non-zero,
/// below `40^12`, and with no padding digit before a character (§1d check 5).
pub fn name_is_well_formed(encoded: u64) -> bool {
    if encoded == 0 {
        return false;
    }
    let mut rest = encoded;
    let mut padding_seen = false;
    for _ in 0..12 {
        let digit = rest % 40;
        rest /= 40;
        if digit == 0 {
            padding_seen = true;
        } else if padding_seen {
            return false;
        }
    }
    rest == 0
}

/// One validated directory entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactEntry {
    pub name: String,
    pub encoded_name: u64,
    pub offset: u64,
    pub length: u64,
}

/// Parse and validate the directory of a compact container image, applying
/// every check of §1d and naming the offending value. `image` must be the
/// container image — a stored object under a whole-file scheme is passed
/// through [`reconstruct_image`] first, and the caller says so with
/// `body_reconstructed`; a scheme the caller has not undone is refused rather
/// than read as a directory.
pub fn read_compact_directory(image: &[u8], body_reconstructed: bool) -> Result<Vec<CompactEntry>, CtfsError> {
    if !matches!(read_v6_header(image)?, Some((Profile::Compact, _))) {
        return Err(refused("not a compact-profile container (§1d)".to_string()));
    }
    if image[COMPRESSION_OFFSET] != WholeFileCompression::None as u8 && !body_reconstructed {
        return Err(refused(
            "compact container declares the zstd scheme: reconstruct header || decompress(rest) first (§1a)".to_string(),
        ));
    }
    // §1d and §1a: there are no blocks, no FileEntry array and no shards.
    for (field, value) in [
        ("BlockSize", u32_at(image, 8)),
        ("MaxRootEntries", u32_at(image, 12)),
        ("MaxShards", image[7] as u32),
    ] {
        if value != 0 {
            return Err(refused(format!("compact container declares {field} {value}, not 0 (§1d)")));
        }
    }
    let size = image.len() as u64;
    if image.len() < COMPACT_DIRECTORY_OFFSET {
        return Err(refused(format!(
            "compact container is {size} bytes, short of {COMPACT_DIRECTORY_OFFSET} (§1d)"
        )));
    }
    let count = u32_at(image, COMPACT_MEMBER_COUNT_OFFSET);
    // Check 1: the directory itself fits.
    let directory_end = COMPACT_DIRECTORY_OFFSET as u64 + count as u64 * COMPACT_ENTRY_SIZE as u64;
    if directory_end > size {
        return Err(refused(format!(
            "compact directory of {count} members ends at {directory_end}, past {size} (§1d check 1)"
        )));
    }
    let mut entries: Vec<CompactEntry> = Vec::with_capacity(count as usize);
    // Check 2: the first member starts right after the directory.
    let mut expected = directory_end;
    for i in 0..count as usize {
        let at = COMPACT_DIRECTORY_OFFSET + i * COMPACT_ENTRY_SIZE;
        let (encoded, offset, length) = (u64_at(image, at), u64_at(image, at + 8), u64_at(image, at + 16));
        // Check 5.
        if !name_is_well_formed(encoded) {
            return Err(refused(format!(
                "compact directory entry {i}: name word {encoded} is not base40 (§1d check 5)"
            )));
        }
        let name = crate::base40::base40_decode(encoded);
        // Checks 2 and 3, as one.
        if offset != expected {
            return Err(refused(format!(
                "compact directory entry {i} ('{name}'): offset {offset}, not {expected} (§1d check {})",
                if i == 0 { 2 } else { 3 }
            )));
        }
        if length > size - offset {
            return Err(refused(format!(
                "compact directory entry {i} ('{name}'): {length} bytes at {offset}, past {size} (§1d check 4)"
            )));
        }
        // Check 6.
        if entries.iter().any(|e| e.encoded_name == encoded) {
            return Err(refused(format!("compact directory entry {i} repeats the name '{name}' (§1d check 6)")));
        }
        entries.push(CompactEntry {
            name,
            encoded_name: encoded,
            offset,
            length,
        });
        expected = offset + length;
    }
    // Check 4: nothing follows the last member.
    if expected != size {
        return Err(refused(format!(
            "compact container is {size} bytes but its members end at {expected} (§1d check 4)"
        )));
    }
    Ok(entries)
}

/// Lay `members` out as a version-6 compact container image (§1d), payloads
/// copied verbatim, in the order given. `compression` is declared in the
/// header only: what this returns is the image a reader holds, and
/// [`compress_image`] makes the stored object of an image that declares
/// zstd. A name outside §3's alphabet or longer than 12 characters, and a name
/// given twice, are refused.
pub fn encode_compact_container(members: &[(&str, &[u8])], compression: WholeFileCompression) -> Result<Vec<u8>, CtfsError> {
    let mut names: Vec<u64> = Vec::with_capacity(members.len());
    for (name, _) in members {
        let encoded = crate::base40::encode_member_name(name)?;
        if names.contains(&encoded) {
            return Err(refused(format!(
                "duplicate member name '{name}' in a compact container: §1d check 6 requires the names to be distinct"
            )));
        }
        names.push(encoded);
    }
    let count = u32::try_from(members.len()).map_err(|_| refused(format!("{} members do not fit a u32 count", members.len())))?;
    let directory_end = COMPACT_DIRECTORY_OFFSET + members.len() * COMPACT_ENTRY_SIZE;
    let total = directory_end + members.iter().map(|(_, p)| p.len()).sum::<usize>();
    let mut image = Vec::with_capacity(total);
    image.extend_from_slice(&MAGIC);
    image.push(VERSION_6);
    image.push(0); // Encryption: none.
    image.push(0); // MaxShards: §1a requires 0.
    image.extend_from_slice(&0u32.to_le_bytes()); // BlockSize: §1d requires 0.
    image.extend_from_slice(&0u32.to_le_bytes()); // MaxRootEntries: §1d requires 0.
    image.push(Profile::Compact as u8);
    image.push(compression as u8);
    image.extend_from_slice(&[0u8; RESERVED_LEN]);
    image.extend_from_slice(&count.to_le_bytes());
    let mut offset = directory_end as u64;
    for ((_, payload), encoded) in members.iter().zip(&names) {
        image.extend_from_slice(&encoded.to_le_bytes());
        image.extend_from_slice(&offset.to_le_bytes());
        image.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        offset += payload.len() as u64;
    }
    for (_, payload) in members {
        image.extend_from_slice(payload);
    }
    debug_assert_eq!(image.len(), total);
    Ok(image)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_word_is_well_formed_exactly_when_the_packing_can_produce_it() {
        for name in ["meta.dat", "a", "steps.idx", "zzzzzzzzzzzz", "-/.0"] {
            assert!(name_is_well_formed(crate::base40::base40_encode(name).unwrap()), "{name}");
        }
        assert!(!name_is_well_formed(0));
        assert!(!name_is_well_formed(40u64.pow(12)), "past the 12 digits");
        assert!(!name_is_well_formed(40 * 5), "padding before a character");
    }

    #[test]
    fn the_layout_is_the_size_identity_with_no_padding() {
        let image = encode_compact_container(&[("meta.dat", b"abc"), ("e.dat", b""), ("x", b"12345")], WholeFileCompression::None).unwrap();
        assert_eq!(image.len(), 28 + 24 * 3 + 8);
        let dir = read_compact_directory(&image, false).unwrap();
        let spans: Vec<_> = dir.iter().map(|e| (e.name.as_str(), e.offset, e.length)).collect();
        assert_eq!(spans, [("meta.dat", 100, 3), ("e.dat", 103, 0), ("x", 103, 5)]);
        assert_eq!(&image[100..103], b"abc");
    }
}
