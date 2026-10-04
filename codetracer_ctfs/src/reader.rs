use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use crate::base40::base40_decode;
use crate::block_bounds::BlockBound;
use crate::compact::{self, Profile, WholeFileCompression};
use crate::file_entry::{FileEntry, MemberLayout};
use crate::header::{CompressionMethod, EncryptionMethod, ExtendedHeader, Header};

/// A version-5 header with its extended header: 16 bytes.
const HEADER_SIZE_V5: usize = crate::header::HEADER_SIZE + crate::header::EXTENDED_HEADER_SIZE;
use crate::member::MemberBytes;
use crate::pread_compat::pread_exact;
use crate::CtfsError;

/// Where a [`CtfsReader`]'s container bytes are: a file it reads from as it
/// is asked, or the whole container already in memory (a browser, which is
/// handed bytes; a container fetched over the network). In-memory bytes are
/// shared with the members read out of them ([`CtfsReader::read_member`]).
enum Source {
    File(File),
    Bytes(Arc<Vec<u8>>),
}

impl Source {
    /// The container's length now. A file is asked every time, because a
    /// container can grow between reads (`BlockBound::of`).
    fn len(&self) -> Result<u64, CtfsError> {
        Ok(match self {
            Source::File(f) => f.metadata()?.len(),
            Source::Bytes(b) => b.len() as u64,
        })
    }

    /// Append the `len` bytes at `offset` to `out`.
    fn append_at(&self, out: &mut Vec<u8>, len: usize, offset: u64) -> Result<(), CtfsError> {
        match self {
            Source::Bytes(b) => {
                out.extend_from_slice(Self::slice(b, offset, len)?);
                Ok(())
            }
            Source::File(f) => {
                let start = out.len();
                out.resize(start + len, 0);
                Ok(pread_exact(f, &mut out[start..], offset)?)
            }
        }
    }

    /// `len` bytes of an in-memory container at `offset`, or `UnexpectedEof`.
    fn slice(b: &[u8], offset: u64, len: usize) -> Result<&[u8], CtfsError> {
        usize::try_from(offset)
            .ok()
            .and_then(|start| b.get(start..start.checked_add(len)?))
            .ok_or_else(|| {
                CtfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("read of {len} bytes at offset {offset} runs past the container's {} bytes", b.len()),
                ))
            })
    }

    /// Fill `buf` from byte `offset`. Running past the end is an
    /// `UnexpectedEof`, from either source.
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), CtfsError> {
        match self {
            Source::File(f) => Ok(pread_exact(f, buf, offset)?),
            Source::Bytes(b) => {
                buf.copy_from_slice(Self::slice(b, offset, buf.len())?);
                Ok(())
            }
        }
    }
}

/// Mapping blocks a file-backed read has already fetched, so resolving the
/// data blocks of one member reads each mapping block once rather than once
/// per data block. Lives for one call: a container being written can change
/// its mapping blocks between calls.
#[derive(Default)]
struct MappingBlocks {
    blocks: Vec<(u64, Vec<u8>)>,
}

/// One level of the chain per slot is enough to never re-read a block while
/// walking a member in order.
const MAPPING_BLOCKS_KEPT: usize = 6;

/// Reader for CTFS containers.
pub struct CtfsReader {
    source: Source,
    block_size: u32,
    entries: Vec<FileEntry>,
    /// For a compact-profile container, the image offset of each entry's
    /// bytes (`entries[i].size` of them); `None` for a full-profile one.
    compact_offsets: Option<Vec<u64>>,
    profile: Profile,
    whole_file_compression: WholeFileCompression,
    compression: CompressionMethod,
    encryption: EncryptionMethod,
}

/// Compute the capacity of a single level in the chain.
/// Level 1: usable data blocks (direct pointers)
/// Level 2: usable^2 data blocks (via usable level-1 sub-blocks)
/// Level k: usable^k
fn level_capacity(usable: u64, level: u32) -> u64 {
    usable.saturating_pow(level)
}

impl CtfsReader {
    /// Open an existing CTFS container: version 5, or version 6 in either
    /// profile (`ctfs-container.md` §1a). Refuses every other version, and
    /// every version-6 header field it does not implement, naming the value
    /// it found (§1c).
    ///
    /// A full-profile container stored as-is is read from the file as it is
    /// asked. A compact one, and one stored under a whole-file scheme, is read
    /// whole and opened as by [`from_bytes`](Self::from_bytes): the compact
    /// profile is for a container that is resident before its first query,
    /// and a whole-file scheme has to be undone before any offset holds.
    pub fn open(path: &Path) -> Result<Self, CtfsError> {
        let mut file = File::open(path)?;
        let mut head = Vec::with_capacity(compact::V6_HEADER_SIZE);
        (&mut file).take(compact::V6_HEADER_SIZE as u64).read_to_end(&mut head)?;
        let v6 = compact::read_v6_header(&head)?;
        if matches!(v6, Some((profile, compression)) if profile == Profile::Compact || compression != WholeFileCompression::None) {
            return Self::from_bytes(std::fs::read(path)?);
        }
        let header_size = if v6.is_some() { compact::V6_HEADER_SIZE } else { HEADER_SIZE_V5 };
        let mut r = std::io::BufReader::new(&mut file);
        std::io::Seek::seek(&mut r, std::io::SeekFrom::Start(0))?;
        let (header, ext_header, entries) = Self::read_root(&mut r, header_size)?;
        Ok(CtfsReader {
            source: Source::File(file),
            block_size: ext_header.block_size,
            entries,
            compact_offsets: None,
            profile: Profile::Full,
            whole_file_compression: WholeFileCompression::None,
            compression: header.compression,
            encryption: header.encryption,
        })
    }

    /// Open a container that is already in memory. Every check [`open`]
    /// makes is made, and members read back exactly as they do from a file:
    /// the bound on block numbers is the length of `bytes`.
    ///
    /// A version-6 container is read in the profile it declares; one stored
    /// under a whole-file scheme is reconstructed first, as
    /// `header || decompress(rest)` (`ctfs-container.md` §1a). A compact
    /// container's directory is checked as §1d requires before any member is
    /// served.
    ///
    /// [`open`]: Self::open
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, CtfsError> {
        let Some((profile, compression)) = compact::read_v6_header(&bytes)? else {
            return Self::full_from_bytes(bytes, HEADER_SIZE_V5, WholeFileCompression::None);
        };
        let image = compact::reconstruct_image(bytes)?;
        match profile {
            Profile::Full => Self::full_from_bytes(image, compact::V6_HEADER_SIZE, compression),
            Profile::Compact => {
                let directory = compact::read_compact_directory(&image, true)?;
                let entries = directory
                    .iter()
                    .map(|e| FileEntry {
                        size: e.length,
                        map_block: 0,
                        name: e.encoded_name,
                    })
                    .collect();
                Ok(CtfsReader {
                    encryption: EncryptionMethod::from_byte(image[6]),
                    source: Source::Bytes(Arc::new(image)),
                    block_size: 0,
                    entries,
                    compact_offsets: Some(directory.iter().map(|e| e.offset).collect()),
                    profile,
                    whole_file_compression: compression,
                    compression: CompressionMethod::None,
                })
            }
        }
    }

    fn full_from_bytes(image: Vec<u8>, header_size: usize, whole_file_compression: WholeFileCompression) -> Result<Self, CtfsError> {
        let (header, ext_header, entries) = Self::read_root(&mut image.as_slice(), header_size)?;
        Ok(CtfsReader {
            source: Source::Bytes(Arc::new(image)),
            block_size: ext_header.block_size,
            entries,
            compact_offsets: None,
            profile: Profile::Full,
            whole_file_compression,
            compression: header.compression,
            encryption: header.encryption,
        })
    }

    /// The header, the extended header and the root directory of a
    /// full-profile container whose header is `header_size` bytes: 16
    /// through version 5, 24 at version 6, whose last eight bytes the caller
    /// has already checked.
    fn read_root(r: &mut impl Read, header_size: usize) -> Result<(Header, ExtendedHeader, Vec<FileEntry>), CtfsError> {
        let header = Header::read_from_any_version(r)?;
        let ext_header = ExtendedHeader::read_from(r)?;
        // The version-6 fields, which the caller has checked.
        let mut v6_fields = [0u8; compact::V6_HEADER_SIZE - HEADER_SIZE_V5];
        r.read_exact(&mut v6_fields[..header_size - HEADER_SIZE_V5])?;
        let mut entries = Vec::new();
        for _ in 0..ext_header.max_root_entries {
            entries.push(FileEntry::read_from(r)?);
        }
        Ok((header, ext_header, entries))
    }

    /// The body layout the container declares: [`Profile::Full`] for every
    /// version-5 container.
    pub fn profile(&self) -> Profile {
        self.profile
    }

    /// The whole-file scheme the container was stored under. The reader holds
    /// the reconstructed image either way.
    pub fn whole_file_compression(&self) -> WholeFileCompression {
        self.whole_file_compression
    }

    /// Get the compression method from the container header.
    pub fn compression(&self) -> CompressionMethod {
        self.compression
    }

    /// Get the encryption method from the container header.
    pub fn encryption(&self) -> EncryptionMethod {
        self.encryption
    }

    /// Get the block size of this container: `0` for a compact one, which
    /// has no blocks.
    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    /// Get the maximum number of root entries (files) this container supports.
    pub fn max_entries(&self) -> u32 {
        self.entries.len() as u32
    }

    /// List all file names in the container.
    pub fn list_files(&self) -> Vec<String> {
        self.entries.iter().filter(|e| !e.is_empty()).map(|e| base40_decode(e.name)).collect()
    }

    /// Get the size of a named file, or None if not found.
    pub fn file_size(&self, name: &str) -> Option<u64> {
        self.find_entry(name).map(|e| e.size)
    }

    /// Read an entire file's contents.
    ///
    /// Refuses — rather than serving out of the partial region — a stream whose
    /// mapping root, mapping blocks or data blocks fall outside the container's
    /// whole blocks. See `block_bounds` and `CTFS-Binary-Format.md` §5d: the
    /// `read_exact` below is clamped to what is left of the entry, so a stream
    /// whose last, short data block landed past the last whole block used to be
    /// read back *successfully* out of bytes the container does not own.
    pub fn read_file(&mut self, name: &str) -> Result<Vec<u8>, CtfsError> {
        let (size, runs) = self.member_runs(name)?;
        let mut data = Vec::with_capacity(size);
        for (offset, len) in runs {
            self.source.append_at(&mut data, len, offset)?;
        }
        Ok(data)
    }

    /// A member's bytes, without copying them out of a container that is in
    /// memory: the [`MemberBytes`] shares the container's buffer and reads
    /// across the member's blocks wherever they lie. From a file, the member
    /// is read whole, as [`read_file`](Self::read_file) reads it. Every check
    /// `read_file` makes is made here, before the member is returned.
    pub fn read_member(&mut self, name: &str) -> Result<MemberBytes, CtfsError> {
        match &self.source {
            Source::File(_) => Ok(MemberBytes::from(self.read_file(name)?)),
            Source::Bytes(bytes) => {
                let image = Arc::clone(bytes);
                let (_, runs) = self.member_runs(name)?;
                let mut stretches = Vec::with_capacity(runs.len());
                for (offset, len) in runs {
                    // `Source::slice` is the same bound `read_file` applies.
                    Source::slice(&image, offset, len)?;
                    stretches.push((offset as usize, len));
                }
                Ok(MemberBytes::in_image(image, stretches))
            }
        }
    }

    /// A member's size and its bytes as `(container offset, length)` runs of
    /// physically consecutive blocks, in member order.
    fn member_runs(&mut self, name: &str) -> Result<(usize, Vec<(u64, usize)>), CtfsError> {
        crate::base40::encode_member_name(name)?;
        let index = self.find_index(name).ok_or_else(|| CtfsError::FileNotFound(name.to_string()))?;
        let entry = self.entries[index];

        if entry.size == 0 {
            return Ok((0, Vec::new()));
        }
        if let Some(offsets) = &self.compact_offsets {
            // The directory was checked at open: the member lies in the image.
            return Ok((entry.size as usize, vec![(offsets[index], entry.size as usize)]));
        }

        let bound = BlockBound::with_len(self.source.len()?, self.block_size);
        let bs = self.block_size as u64;
        let size = usize::try_from(entry.size).map_err(|_| {
            CtfsError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("internal file {name} claims {} bytes, more than this target can address", entry.size),
            ))
        })?;
        let num_blocks = entry.size.div_ceil(bs);
        // Each data block is resolved -- and bounds-checked -- before any byte
        // of it is read; then runs of physically consecutive blocks are read
        // in one call each.
        let mut maps = MappingBlocks::default();
        // Sized by what the container can hold, not by what the entry claims:
        // a corrupt `Size` is refused block by block below, not allocated.
        let mut physical = Vec::with_capacity(num_blocks.min(bound.whole_blocks()) as usize);
        for block_idx in 0..num_blocks {
            physical.push(self.resolve_block(&entry, block_idx, name, &bound, &mut maps)?);
        }
        let mut runs = Vec::new();
        let mut run_start = 0usize;
        for i in 1..=physical.len() {
            if i < physical.len() && physical[i] == physical[i - 1] + 1 {
                continue;
            }
            let len = (i * bs as usize).min(size) - run_start * bs as usize;
            runs.push((physical[run_start] * bs, len));
            run_start = i;
        }
        Ok((size, runs))
    }

    /// Read from an arbitrary position within a file.
    ///
    /// Returns the number of bytes actually read (may be less than buf.len()
    /// if the read extends past the end of the file).
    pub fn read_at(&mut self, name: &str, offset: u64, buf: &mut [u8]) -> Result<usize, CtfsError> {
        crate::base40::encode_member_name(name)?;
        let index = self.find_index(name).ok_or_else(|| CtfsError::FileNotFound(name.to_string()))?;
        let entry = self.entries[index];

        if offset >= entry.size {
            return Ok(0);
        }
        if let Some(offsets) = &self.compact_offsets {
            let n = buf.len().min((entry.size - offset) as usize);
            self.source.read_exact_at(&mut buf[..n], offsets[index] + offset)?;
            return Ok(n);
        }

        let bound = BlockBound::with_len(self.source.len()?, self.block_size);
        let bs = self.block_size as u64;
        let available = (entry.size - offset) as usize;
        let to_read = buf.len().min(available);
        let mut bytes_read = 0;
        let mut maps = MappingBlocks::default();

        while bytes_read < to_read {
            let current_offset = offset + bytes_read as u64;
            let block_idx = current_offset / bs;
            let offset_in_block = (current_offset % bs) as usize;

            let data_block = self.resolve_block(&entry, block_idx, name, &bound, &mut maps)?;
            let block_offset = data_block * bs + offset_in_block as u64;
            let chunk = (bs as usize - offset_in_block).min(to_read - bytes_read);
            self.source.read_exact_at(&mut buf[bytes_read..bytes_read + chunk], block_offset)?;
            bytes_read += chunk;
        }

        Ok(bytes_read)
    }

    /// Resolve a data block index to its physical block number by navigating
    /// the bottom-up chain mapping structure.
    ///
    /// The chain model:
    /// - Start at the root mapping block (always level-1).
    /// - Level 1: entries[0..N-2] are direct data block pointers.
    ///   If block_index < N-1, return entries[block_index].
    /// - If block_index >= N-1, subtract N-1, follow entries[N-1] to level-2 block.
    /// - Level 2: entries[0..N-2] each point to level-1 sub-blocks.
    ///   Each sub-block holds N-1 data pointers, so level-2 capacity = (N-1)^2.
    /// - Continue up: level-k capacity = (N-1)^k.
    ///
    /// Every block number this walk produces is checked against `bound` before
    /// its bytes are read — the mapping root here, the chained and descended
    /// mapping blocks below, and the data block in `navigate_to_data_block`.
    /// That is §5d's "all three paths"; leaving any of them out is what turns a
    /// truncated container into wrong content.
    fn resolve_block(&self, entry: &FileEntry, block_index: u64, name: &str, bound: &BlockBound, maps: &mut MappingBlocks) -> Result<u64, CtfsError> {
        let n = self.block_size as u64 / 8;
        let usable = n - 1;

        // The form is decided from `MapBlock`, never from `Size`
        // (`ctfs-container.md` §2, "Readers").
        let root = match entry.layout() {
            // Reached only with a non-zero `Size` (callers return early on an
            // empty member), which is a null pointer: refused below by name.
            MemberLayout::Empty => 0,
            MemberLayout::Direct(b) => {
                bound.check_direct_member(b, entry.size, name)?;
                return Ok(b);
            }
            MemberLayout::Mapped(m) => m,
        };

        let mut idx = block_index;
        let mut current_level_block = root;
        let mut level = 1u32;

        // Path 1 of 3: the entry's mapping root.
        bound.check_mapping_root(current_level_block, || format!("mapping root block of internal file {name}"))?;

        // Walk up through levels to find which level contains this index
        loop {
            let cap = level_capacity(usable, level);
            if idx < cap {
                break;
            }
            idx -= cap;
            level += 1;
            if level > 5 {
                return Err(CtfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "block index exceeds 5-level mapping capacity",
                )));
            }
            // Follow chain pointer at entries[N-1] to the next higher level
            let chain_ptr = self.read_block_ptr(current_level_block, usable as usize, maps)?;
            if chain_ptr == 0 {
                return Err(CtfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("null chain pointer at block {} following to level {}", current_level_block, level),
                )));
            }
            // Path 2a of 3: a mapping block reached through the chain.
            bound.check(chain_ptr, || format!("chain pointer at level {level} of internal file {name}"))?;
            current_level_block = chain_ptr;
        }

        // Navigate down within this level's block to find the data block
        self.navigate_to_data_block(current_level_block, level, idx, usable, block_index, name, bound, maps)
    }

    /// Navigate within a level-k block to find the data block pointer.
    /// For level 1: return entries[idx].
    /// For level k>1: compute which sub-entry, follow to child, recurse.
    #[allow(clippy::too_many_arguments)]
    fn navigate_to_data_block(
        &self,
        mapping_block: u64,
        level: u32,
        idx_within_level: u64,
        usable: u64,
        block_index: u64,
        name: &str,
        bound: &BlockBound,
        maps: &mut MappingBlocks,
    ) -> Result<u64, CtfsError> {
        if level == 1 {
            let ptr = self.read_block_ptr(mapping_block, idx_within_level as usize, maps)?;
            if ptr == 0 {
                return Err(CtfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("null data block pointer at block {} index {}", mapping_block, idx_within_level),
                )));
            }
            // Path 3 of 3, and the one that was missing. The `read_exact` in
            // `read_file` is clamped to what is left of the entry, so a stream
            // whose last data block landed in the partial region was served
            // successfully out of bytes the container does not own. Check the
            // block NUMBER, before any of its bytes are touched.
            bound.check(ptr, || format!("data block {block_index} of internal file {name}"))?;
            return Ok(ptr);
        }

        let sub_cap = level_capacity(usable, level - 1);
        let entry_idx = idx_within_level / sub_cap;
        let sub_idx = idx_within_level % sub_cap;

        let child_block = self.read_block_ptr(mapping_block, entry_idx as usize, maps)?;
        if child_block == 0 {
            return Err(CtfsError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("null mapping pointer at block {} index {}", mapping_block, entry_idx),
            )));
        }
        // Path 2b of 3: a mapping block reached by descending the hierarchy.
        bound.check(child_block, || format!("child block pointer at level {level} of internal file {name}"))?;

        self.navigate_to_data_block(child_block, level - 1, sub_idx, usable, block_index, name, bound, maps)
    }

    /// Read a u64 pointer at a given entry index within a mapping block.
    /// From a file, the whole mapping block is read once into `maps`.
    fn read_block_ptr(&self, block_num: u64, index: usize, maps: &mut MappingBlocks) -> Result<u64, CtfsError> {
        let bs = self.block_size as usize;
        let at = index * 8;
        let mut buf = [0u8; 8];
        match &self.source {
            Source::Bytes(_) => self.source.read_exact_at(&mut buf, block_num * bs as u64 + at as u64)?,
            Source::File(_) => {
                let slot = match maps.blocks.iter().position(|(b, _)| *b == block_num) {
                    Some(slot) => slot,
                    None => {
                        let mut block = vec![0u8; bs];
                        self.source.read_exact_at(&mut block, block_num * bs as u64)?;
                        if maps.blocks.len() == MAPPING_BLOCKS_KEPT {
                            maps.blocks.remove(0);
                        }
                        maps.blocks.push((block_num, block));
                        maps.blocks.len() - 1
                    }
                };
                buf.copy_from_slice(&maps.blocks[slot].1[at..at + 8]);
            }
        }
        Ok(u64::from_le_bytes(buf))
    }

    /// Read a chunked-compressed file, optionally seeking to a specific GEID.
    ///
    /// If `target_geid` is `None`, all chunks are decompressed and the full
    /// event data is returned.
    ///
    /// If `target_geid` is `Some(geid)`, only the chunk containing that GEID
    /// is decompressed and returned (along with the chunk header metadata).
    pub fn read_file_chunked(&mut self, name: &str, target_geid: Option<u64>) -> Result<Vec<u8>, CtfsError> {
        let raw = self.read_file(name)?;
        match target_geid {
            None => crate::chunked::ChunkedReader::decompress_all(&raw),
            Some(geid) => {
                let (data, _header) = crate::chunked::ChunkedReader::seek_to_geid(&raw, geid)?;
                Ok(data)
            }
        }
    }

    fn find_entry(&self, name: &str) -> Option<&FileEntry> {
        self.find_index(name).map(|i| &self.entries[i])
    }

    fn find_index(&self, name: &str) -> Option<usize> {
        let encoded = crate::base40::encode_member_name(name).ok()?;
        self.entries.iter().position(|e| e.name == encoded && !e.is_empty())
    }

    /// Every member's name and bytes, in the order the container lists them:
    /// the member set a compact container of this recording carries
    /// (`ctfs-container.md` §1d, "Member names are carried unchanged"). Pass
    /// it to [`compact::encode_compact_container`] to lay the container out
    /// in the compact profile. Payloads are as stored: a member whose format
    /// compresses itself is copied compressed.
    pub fn members(&mut self) -> Result<Vec<(String, Vec<u8>)>, CtfsError> {
        let names = self.list_files();
        names
            .into_iter()
            .map(|name| {
                let bytes = self.read_file(&name)?;
                Ok((name, bytes))
            })
            .collect()
    }
}
