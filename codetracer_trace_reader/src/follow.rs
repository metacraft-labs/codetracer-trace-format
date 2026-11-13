//! Reading a container while it is being written (`ctfs-container.md` §6,
//! "Live progress: per-stream following").
//!
//! A writer publishes every chunk as it seals: the chunk's bytes, then the
//! companion-index entry that locates it, then the root entries (`Size`,
//! `MapBlock`) of the members it grew. A following reader re-reads the root
//! entries and extends each stream by the index entries published since it
//! last looked, decoding only the chunk it needs to count the records of the
//! new last chunk. What it already decoded stays decoded.
//!
//! Two things a following reader must not assume:
//!
//! * **That the data member ends where its last indexed chunk ends.** The
//!   chunk's bytes are published before its index entry, so the data member
//!   can already hold a chunk no entry names yet. The last indexed chunk of a
//!   full-profile container ends where its zstd frame ends.
//! * **That what it read stays as it read it, unchecked.** A published chunk
//!   never moves and a member never shrinks; a container in which either
//!   happened is refused, naming the stream, rather than read as if the
//!   earlier answers still held.

use codetracer_ctfs::MemberBytes;

use crate::chunk_codec::ChunkForm;

/// Where chunk `k` of a stream ends in its data member `dat`: at the next
/// chunk's offset, or -- for the last indexed chunk -- at the end of its zstd
/// frame. A stored (compact-profile) chunk, or bytes that do not begin with a
/// whole frame, end at the end of the member; the decode then refuses what is
/// not a chunk.
pub(crate) fn chunk_end(stream: &str, form: ChunkForm, dat: &MemberBytes, offsets: &[u64], k: usize) -> Result<usize, String> {
    if k + 1 < offsets.len() {
        return Ok(offsets[k + 1] as usize);
    }
    let start = offsets[k] as usize;
    if form == ChunkForm::Stored || start >= dat.len() {
        return Ok(dat.len());
    }
    let tail = dat.get(start, dat.len()).map_err(|e| format!("{stream}: chunk {k}: {e}"))?;
    Ok(codetracer_ctfs::zstd_frame::frame_compressed_size(&tail)
        .ok()
        .map_or(dat.len(), |n| start + n))
}

/// Refuse a re-read index that does not extend the one already read: a
/// changed `chunk_size`, a published offset that moved or vanished, offsets
/// that go backwards, or a last chunk that starts past the data member's
/// `dat_len` bytes.
pub(crate) fn check_extends(stream: &str, old_chunk_size: usize, old: &[u64], chunk_size: usize, new: &[u64], dat_len: usize) -> Result<(), String> {
    if chunk_size != old_chunk_size {
        return Err(format!(
            "{stream}: chunk_size changed from {old_chunk_size} to {chunk_size} while the container was followed"
        ));
    }
    if new.len() < old.len() {
        return Err(format!(
            "{stream}: {} published chunk(s) disappeared while the container was followed",
            old.len() - new.len()
        ));
    }
    if let Some(k) = old.iter().zip(new).position(|(a, b)| a != b) {
        return Err(format!(
            "{stream}: published chunk {k} moved from offset {} to {} while the container was followed",
            old[k], new[k]
        ));
    }
    if let Some(k) = new.windows(2).position(|w| w[1] < w[0]) {
        return Err(format!("{stream}: chunk {} starts before chunk {k}", k + 1));
    }
    if let Some(&last) = new.last()
        && last as usize > dat_len
    {
        return Err(format!(
            "{stream}: its last chunk starts at offset {last}, past the {dat_len} bytes of its data member"
        ));
    }
    Ok(())
}

/// The data member `<stem>.dat` and the index `<stem>.idx` of a stream, as
/// published. The root entries are read in one pass, which can observe an
/// index entry whose chunk's root entry it read a moment before the writer
/// published it (`ctfs-container.md` §6, "Writer Protocol": data before the
/// entry that publishes it). Such an index is read again after the root
/// directory is; an entry that still lies past its data member is refused by
/// the caller's [`check_extends`].
pub(crate) fn read_published(reader: &mut codetracer_ctfs::CtfsReader, stem: &str) -> Result<(MemberBytes, Vec<u8>), String> {
    let (dat_name, idx_name) = (format!("{stem}.dat"), format!("{stem}.idx"));
    let mut retried = false;
    loop {
        let idx = reader.read_file(&idx_name).map_err(|e| format!("{idx_name}: {e}"))?;
        let dat = reader.read_member(&dat_name).map_err(|e| format!("{dat_name}: {e}"))?;
        let last = (idx.len() >= 12).then(|| {
            let at = 4 + (idx.len() - 4) / 8 * 8 - 8;
            u64::from_le_bytes(idx[at..at + 8].try_into().expect("eight bytes"))
        });
        if retried || last.is_none_or(|o| o as usize <= dat.len()) {
            return Ok((dat, idx));
        }
        reader.refresh().map_err(|e| e.to_string())?;
        retried = true;
    }
}
