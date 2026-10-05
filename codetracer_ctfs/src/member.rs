//! A member's bytes, as a reader holds them: shared with the container when
//! the container is in memory, read from the container's file the first time
//! they are asked for when it was opened from a path, or owned outright.
//!
//! A member of a full-profile container occupies whole blocks that need not
//! be adjacent: a streaming writer seals one member's chunk between two of
//! another's, so `values.dat`'s blocks interleave with `calls.dat`'s. A
//! [`MemberBytes`] records where each physically contiguous run of the member
//! lies in the container. In memory it answers a byte range out of the
//! container image itself, copying only a range that straddles two runs. From
//! a file it reads the member's runs once, on the first range asked of it, and
//! answers every range out of that copy. Opening a reader therefore copies
//! none of its members, and a member nobody reads is never read.

use std::borrow::Cow;
use std::fs::File;
use std::io;
use std::sync::{Arc, OnceLock};

use crate::pread_compat::pread_exact;

/// One physically contiguous stretch of a member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Run {
    /// Offset of the stretch's first byte within the member.
    member_offset: usize,
    /// Offset of the same byte within the container (`MemberBytes::source`).
    source_offset: u64,
    len: usize,
}

/// Where the runs' bytes are.
#[derive(Debug, Clone)]
enum Source {
    /// A buffer in memory: the container image, or the member alone.
    Image(Arc<Vec<u8>>),
    /// The container's file, which stays open while any member read from it
    /// is alive, and the member read out of it whole once a range is asked
    /// for.
    File { file: Arc<File>, read: Arc<OnceLock<Vec<u8>>> },
}

/// The bytes of one member. Cloning shares them.
#[derive(Debug, Clone)]
pub struct MemberBytes {
    source: Source,
    /// Ascending by `member_offset`, covering `0..len` without gaps.
    runs: Arc<[Run]>,
    len: usize,
}

impl Default for MemberBytes {
    fn default() -> Self {
        MemberBytes::from(Vec::new())
    }
}

impl From<Vec<u8>> for MemberBytes {
    /// A member held in a buffer of its own, as one run. Takes the vector
    /// without copying it.
    fn from(bytes: Vec<u8>) -> Self {
        let len = bytes.len();
        MemberBytes {
            source: Source::Image(Arc::new(bytes)),
            runs: Arc::new([Run {
                member_offset: 0,
                source_offset: 0,
                len,
            }]),
            len,
        }
    }
}

/// The runs spelled by `(container offset, len)` stretches in member order,
/// adjacent stretches merged, and the member's length.
fn runs_of(stretches: impl IntoIterator<Item = (u64, usize)>) -> (Arc<[Run]>, usize) {
    let mut runs: Vec<Run> = Vec::new();
    let mut member_offset = 0usize;
    for (source_offset, len) in stretches {
        match runs.last_mut() {
            Some(last) if last.source_offset + last.len as u64 == source_offset => last.len += len,
            _ => runs.push(Run {
                member_offset,
                source_offset,
                len,
            }),
        }
        member_offset += len;
    }
    if runs.is_empty() {
        runs.push(Run {
            member_offset: 0,
            source_offset: 0,
            len: 0,
        });
    }
    (runs.into(), member_offset)
}

impl MemberBytes {
    /// A member whose bytes lie in `image` as the given `(image_offset, len)`
    /// stretches, in member order. Every stretch must lie inside `image`; the
    /// caller (the container reader) has bounds-checked the blocks they come
    /// from.
    pub(crate) fn in_image(image: Arc<Vec<u8>>, stretches: impl IntoIterator<Item = (usize, usize)>) -> MemberBytes {
        let (runs, len) = runs_of(stretches.into_iter().map(|(at, len)| {
            debug_assert!(at + len <= image.len());
            (at as u64, len)
        }));
        MemberBytes {
            source: Source::Image(image),
            runs,
            len,
        }
    }

    /// A member whose bytes lie in `file` as the given `(file_offset, len)`
    /// stretches, in member order, read the first time a range is asked for.
    /// The caller (the container reader) has bounds-checked the blocks they
    /// come from against the file's length; a file that has been cut short
    /// before the member is read is a read error then, not short content.
    pub(crate) fn in_file(file: Arc<File>, stretches: impl IntoIterator<Item = (u64, usize)>) -> MemberBytes {
        let (runs, len) = runs_of(stretches);
        MemberBytes {
            source: Source::File {
                file,
                read: Arc::new(OnceLock::new()),
            },
            runs,
            len,
        }
    }

    /// The member's length in bytes.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the member is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The bytes `start..end` of the member. Borrowed from an in-memory
    /// container when the range lies in one run, assembled from its runs
    /// otherwise; borrowed from the member read out of its file, which the
    /// first call reads. A range that does not lie inside the member is an
    /// `UnexpectedEof`, as is a file that no longer holds the member's bytes.
    pub fn get(&self, start: usize, end: usize) -> io::Result<Cow<'_, [u8]>> {
        if start > end || end > self.len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("bytes {start}..{end} do not lie inside the member's {} bytes", self.len),
            ));
        }
        let image = match &self.source {
            Source::Image(image) => image,
            Source::File { file, read } => return Ok(Cow::Borrowed(&self.read_from(file, read)?[start..end])),
        };
        // The run holding `start`: the last one that begins at or before it.
        let mut k = self.runs.partition_point(|r| r.member_offset <= start).saturating_sub(1);
        let first = self.runs[k];
        let within = start - first.member_offset;
        if end - start <= first.len - within {
            let at = first.source_offset as usize + within;
            return Ok(Cow::Borrowed(&image[at..at + (end - start)]));
        }
        let mut out = Vec::with_capacity(end - start);
        let mut at = start;
        while at < end {
            let run = self.runs[k];
            let from = at - run.member_offset;
            let take = (run.len - from).min(end - at);
            let offset = run.source_offset as usize + from;
            out.extend_from_slice(&image[offset..offset + take]);
            at += take;
            k += 1;
        }
        Ok(Cow::Owned(out))
    }

    /// The member out of `file`, read run by run on the first call.
    fn read_from<'a>(&self, file: &File, read: &'a OnceLock<Vec<u8>>) -> io::Result<&'a [u8]> {
        if let Some(bytes) = read.get() {
            return Ok(bytes);
        }
        let mut bytes = vec![0u8; self.len];
        for run in self.runs.iter() {
            pread_exact(file, &mut bytes[run.member_offset..run.member_offset + run.len], run.source_offset)?;
        }
        // Another clone may have read it meanwhile; either copy is the member.
        let _ = read.set(bytes);
        Ok(read.get().expect("set above"))
    }

    /// The little-endian `u64` at byte `at` of the member; an error when its
    /// eight bytes do not lie inside it.
    pub fn u64_at(&self, at: usize) -> io::Result<u64> {
        let end = at.saturating_add(8);
        let bytes = self.get(at, end)?;
        let mut word = [0u8; 8];
        word.copy_from_slice(&bytes);
        Ok(u64::from_le_bytes(word))
    }

    /// The whole member as one slice: borrowed unless it is in memory in
    /// more than one run.
    pub fn as_cow(&self) -> io::Result<Cow<'_, [u8]>> {
        self.get(0, self.len)
    }

    /// A copy of the member's bytes.
    pub fn to_vec(&self) -> io::Result<Vec<u8>> {
        Ok(self.as_cow()?.into_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scattered() -> (Vec<u8>, MemberBytes) {
        // The member is 0..30, stored as image[40..50], image[10..20] and
        // image[20..30]; the last two are adjacent and merge into one run.
        let image: Vec<u8> = (0..60u8).collect();
        let member = MemberBytes::in_image(Arc::new(image), [(40, 10), (10, 10), (20, 10)]);
        let expected: Vec<u8> = (40..50u8).chain(10..30).collect();
        (expected, member)
    }

    #[test]
    fn every_range_reads_the_bytes_the_runs_spell() {
        let (expected, m) = scattered();
        assert_eq!(m.runs.len(), 2, "adjacent stretches are one run");
        assert_eq!(m.len(), 30);
        for start in 0..=30 {
            for end in start..=30 {
                assert_eq!(m.get(start, end).unwrap().as_ref(), &expected[start..end], "{start}..{end}");
            }
        }
        assert!(matches!(m.get(2, 9), Ok(Cow::Borrowed(_))), "inside one run, nothing is copied");
        assert!(matches!(m.get(8, 12), Ok(Cow::Owned(_))), "across runs, the range is assembled");
        assert_eq!(m.to_vec().unwrap(), expected);
    }

    #[test]
    fn a_range_outside_the_member_is_refused() {
        let (_, m) = scattered();
        assert!(m.get(0, 31).is_err());
        assert!(m.get(5, 4).is_err());
        assert!(m.u64_at(23).is_err());
        assert_eq!(m.u64_at(6).unwrap(), u64::from_le_bytes([46, 47, 48, 49, 10, 11, 12, 13]));
    }

    #[test]
    fn an_owned_buffer_and_an_empty_member_read_back() {
        let m = MemberBytes::from(vec![1, 2, 3]);
        assert_eq!(m.get(1, 3).unwrap().as_ref(), &[2, 3]);
        let empty = MemberBytes::in_image(Arc::new(vec![9; 4]), []);
        assert!(empty.is_empty());
        assert_eq!(empty.get(0, 0).unwrap().as_ref(), &[] as &[u8]);
        assert!(MemberBytes::default().is_empty());
    }

    /// The same scattered member, read out of a file: every range as the
    /// image spells it, and a file cut short before the member is read is a
    /// read error, not short or zeroed content.
    #[test]
    fn a_member_in_a_file_reads_every_range_and_refuses_a_cut_file() {
        let (expected, _) = scattered();
        let dir = std::env::temp_dir().join(format!("ctfs-member-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("image");
        std::fs::write(&path, (0..60u8).collect::<Vec<u8>>()).unwrap();
        let file = Arc::new(File::open(&path).unwrap());
        let m = MemberBytes::in_file(Arc::clone(&file), [(40, 10), (10, 10), (20, 10)]);
        assert_eq!(m.runs.len(), 2, "adjacent stretches are one run");
        for start in 0..=30 {
            for end in start..=30 {
                assert_eq!(m.get(start, end).unwrap().as_ref(), &expected[start..end], "{start}..{end}");
            }
        }
        assert!(m.get(0, 31).is_err());
        assert_eq!(m.u64_at(6).unwrap(), u64::from_le_bytes([46, 47, 48, 49, 10, 11, 12, 13]));

        assert!(matches!(m.get(8, 12), Ok(Cow::Borrowed(_))), "once read, every range is borrowed");

        // Cut short before the member is read: refused, not zero-filled.
        let unread = MemberBytes::in_file(Arc::clone(&file), [(40, 10), (10, 10), (20, 10)]);
        std::fs::OpenOptions::new().write(true).open(&path).unwrap().set_len(45).unwrap();
        let cut = unread.get(0, 1).unwrap_err();
        assert_eq!(cut.kind(), io::ErrorKind::UnexpectedEof, "{cut}");
        assert_eq!(m.get(0, 30).unwrap().as_ref(), &expected[..], "a member already read is held");
        drop((m, unread));
        drop(file);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
