//! A member's bytes, as a reader holds them: shared with the container when
//! the container is in memory, owned when they were read from a file.
//!
//! A member of a full-profile container occupies whole blocks that need not
//! be adjacent: a streaming writer seals one member's chunk between two of
//! another's, so `values.dat`'s blocks interleave with `calls.dat`'s. A
//! [`MemberBytes`] records where each physically contiguous run of the member
//! lies in the container image and answers a byte range out of the image
//! itself, copying only a range that straddles two runs. Opening a reader
//! from a container already in memory therefore copies none of its members.

use std::borrow::Cow;
use std::sync::Arc;

/// One physically contiguous stretch of a member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Run {
    /// Offset of the stretch's first byte within the member.
    member_offset: usize,
    /// Offset of the same byte within `MemberBytes::image`.
    image_offset: usize,
    len: usize,
}

/// The bytes of one member. Cloning shares them.
#[derive(Debug, Clone)]
pub struct MemberBytes {
    image: Arc<Vec<u8>>,
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
            image: Arc::new(bytes),
            runs: Arc::new([Run {
                member_offset: 0,
                image_offset: 0,
                len,
            }]),
            len,
        }
    }
}

impl MemberBytes {
    /// A member whose `len` bytes lie in `image` as the given
    /// `(image_offset, len)` stretches, in member order. Every stretch must
    /// lie inside `image` and the stretches must add up to `len`; the caller
    /// (the container reader) has bounds-checked the blocks they come from.
    pub(crate) fn in_image(image: Arc<Vec<u8>>, stretches: impl IntoIterator<Item = (usize, usize)>) -> MemberBytes {
        let mut runs: Vec<Run> = Vec::new();
        let mut member_offset = 0usize;
        for (image_offset, len) in stretches {
            debug_assert!(image_offset + len <= image.len());
            match runs.last_mut() {
                Some(last) if last.image_offset + last.len == image_offset => last.len += len,
                _ => runs.push(Run {
                    member_offset,
                    image_offset,
                    len,
                }),
            }
            member_offset += len;
        }
        if runs.is_empty() {
            runs.push(Run {
                member_offset: 0,
                image_offset: 0,
                len: 0,
            });
        }
        MemberBytes {
            image,
            runs: runs.into(),
            len: member_offset,
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

    /// The bytes `start..end` of the member, or `None` when the range does not
    /// lie inside it. Borrowed from the image when the range lies in one run,
    /// assembled from its runs otherwise.
    pub fn get(&self, start: usize, end: usize) -> Option<Cow<'_, [u8]>> {
        if start > end || end > self.len {
            return None;
        }
        // The run holding `start`: the last one that begins at or before it.
        let mut k = self.runs.partition_point(|r| r.member_offset <= start).saturating_sub(1);
        let first = self.runs[k];
        let within = start - first.member_offset;
        if end - start <= first.len - within {
            let at = first.image_offset + within;
            return Some(Cow::Borrowed(&self.image[at..at + (end - start)]));
        }
        let mut out = Vec::with_capacity(end - start);
        let mut at = start;
        while at < end {
            let run = self.runs[k];
            let from = at - run.member_offset;
            let take = (run.len - from).min(end - at);
            out.extend_from_slice(&self.image[run.image_offset + from..run.image_offset + from + take]);
            at += take;
            k += 1;
        }
        Some(Cow::Owned(out))
    }

    /// The little-endian `u64` at byte `at` of the member, or `None` when its
    /// eight bytes do not lie inside it.
    pub fn u64_at(&self, at: usize) -> Option<u64> {
        let bytes = self.get(at, at.checked_add(8)?)?;
        Some(u64::from_le_bytes(bytes.as_ref().try_into().ok()?))
    }

    /// The whole member as one slice: borrowed when it is one run.
    pub fn as_cow(&self) -> Cow<'_, [u8]> {
        self.get(0, self.len).unwrap_or(Cow::Borrowed(&[]))
    }

    /// A copy of the member's bytes.
    pub fn to_vec(&self) -> Vec<u8> {
        self.as_cow().into_owned()
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
        assert!(matches!(m.get(2, 9), Some(Cow::Borrowed(_))), "inside one run, nothing is copied");
        assert!(matches!(m.get(8, 12), Some(Cow::Owned(_))), "across runs, the range is assembled");
        assert_eq!(m.to_vec(), expected);
    }

    #[test]
    fn a_range_outside_the_member_is_none() {
        let (_, m) = scattered();
        assert!(m.get(0, 31).is_none());
        assert!(m.get(5, 4).is_none());
        assert!(m.u64_at(23).is_none());
        assert_eq!(m.u64_at(6), Some(u64::from_le_bytes([46, 47, 48, 49, 10, 11, 12, 13])));
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
}
