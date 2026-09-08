//! The line-only global line index: a prefix sum, and nothing else.
//!
//! A line-only trace addresses a source location with one integer. That integer
//! is a position in a per-trace space built by concatenating the registered
//! files in file-id order — each file owns a contiguous, gap-free range, and a
//! line's address is its file's base plus the 0-based offset of the line inside
//! it:
//!
//! ```text
//!   global_index(file_id, line) = file_base[file_id] + (line - 1)
//!   file_base[0] = 0
//!   file_base[k] = file_base[k-1] + file_size[k-1]
//!
//!   resolve(p): file f with file_base[f] <= p < file_base[f] + file_size[f]
//!               line = p - file_base[f] + 1
//! ```
//!
//! `line` is 1-based, so the `- 1` puts a file's first line at its own base and
//! its last at `file_base + file_size - 1`: the range is exactly the addresses
//! the file's lines occupy, with none left over and none spilling into the next
//! file. This is `codetracer-trace-format-spec/internal-files.md` §"Global Line
//! Index" and `trace-events.md` §"Source Location Addressing"; the same
//! arithmetic is `globalIndex` / `resolve` in the canonical Nim writer's
//! `codetracer_trace_writer/global_line_index.nim`.
//!
//! # Why there is only one scheme
//!
//! The spec's §"The address is a prefix sum, and nothing else" excludes
//! bit-field packings of `(file_id, line)` — `(path_id << 32) | line` and any
//! other fixed stride above bit 32 — on three grounds: they cap both the file
//! count and the lines per file at 2^32; they cost 5 varint bytes for every
//! file after the first against a 2-3 byte budget, inflating the step stream by
//! about half on any multi-file trace; and a container carries no
//! discriminator, so a reader that applies the wrong one does not fail, it
//! answers a location that was never in the trace.
//!
//! That last property is why this module offers no fallback and no sniffing. A
//! position this space cannot hold is refused by name.
//!
//! # `DEFAULT_LINES_PER_FILE`, and what the follow-on removes
//!
//! Sizing a file's range needs that file's line count, which a recorder does not
//! generally have: it sees the lines a program executed, not the lines the file
//! contains, and `paths.dat` records no count in line-only mode. Until it does,
//! every file is given the same generous ceiling, [`DEFAULT_LINES_PER_FILE`] —
//! matching the canonical Nim writer's constant of the same name, so the two
//! writers place a step at the same address.
//!
//! The ceiling is a ceiling, not a guarantee: a file with more than
//! `DEFAULT_LINES_PER_FILE` lines has its later lines addressed inside a
//! following file's range, and they resolve to that file. Real per-file line
//! counts are what remove the ceiling, and they are a format change (a count in
//! the `paths.dat` record) rather than an arithmetic one — [`from_line_counts`]
//! already takes them.
//!
//! [`from_line_counts`]: LinePositionSpace::from_line_counts

use std::fmt;

/// Addresses allocated to a file whose real line count is unknown, which in
/// line-only mode is every file.
///
/// The value matches `DefaultLinesPerFile` in the canonical Nim writer's
/// `codetracer_trace_writer/global_line_index.nim`. It is a count of
/// addressable lines, so a file of exactly this many lines fits: lines
/// `1 ..= DEFAULT_LINES_PER_FILE` occupy the whole slot and line
/// `DEFAULT_LINES_PER_FILE + 1` is the first that spills into the next file's
/// range.
pub const DEFAULT_LINES_PER_FILE: u64 = 100_000;

/// Why a `(file_id, line)` could not be addressed, or an address resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinePositionError {
    /// The trace registers no files, so it has no address space at all.
    NoFiles,
    /// The address lies at or above the top of this trace's space.
    OutOfSpace {
        /// The offending address.
        position: u64,
        /// One past the highest address the space can hold.
        total_lines: u64,
        /// How many files the trace registers.
        file_count: usize,
    },
}

impl fmt::Display for LinePositionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LinePositionError::NoFiles => write!(f, "line-only global_line_index cannot be resolved: the trace registers no paths"),
            LinePositionError::OutOfSpace {
                position,
                total_lines,
                file_count,
            } => write!(
                f,
                "line-only global_line_index {position} is outside this trace's address space \
                 of {total_lines} ({file_count} path(s)). The address is a prefix sum over the \
                 registered files and nothing else — see codetracer-trace-format-spec \
                 trace-events.md \u{a7}\"The address is a prefix sum, and nothing else\" — so \
                 there is no other reading to try"
            ),
        }
    }
}

impl std::error::Error for LinePositionError {}

/// A trace's line-only global position space: one contiguous range per
/// registered file, in file-id order, with no gaps.
///
/// Built once per trace and shared by the encode and the decode sides so they
/// cannot answer differently. See the module header for the arithmetic and for
/// why there is only one scheme.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LinePositionSpace {
    /// `prefix_sum[i]` is the first address of file `i`; the last element is
    /// one past the highest address. Length is `file_count + 1` (and 1 for an
    /// empty space, holding the single element 0).
    prefix_sum: Vec<u64>,
}

impl LinePositionSpace {
    /// An empty space: no files, no addresses.
    pub fn new() -> Self {
        LinePositionSpace { prefix_sum: vec![0] }
    }

    /// The space of a trace with `file_count` files whose real line counts are
    /// unknown — every line-only trace today. Each file gets
    /// [`DEFAULT_LINES_PER_FILE`] addresses.
    pub fn uniform(file_count: usize) -> Self {
        Self::from_line_counts(&vec![DEFAULT_LINES_PER_FILE; file_count])
    }

    /// The space of a trace whose files have the given line counts, in file-id
    /// order. A count of zero is raised to one so every registered file owns at
    /// least its own base address and file ids stay distinguishable.
    ///
    /// This is the constructor the real-line-counts follow-on needs; nothing
    /// else about the scheme changes when the counts become real.
    pub fn from_line_counts(line_counts: &[u64]) -> Self {
        let mut prefix_sum = Vec::with_capacity(line_counts.len() + 1);
        let mut running: u64 = 0;
        prefix_sum.push(0);
        for count in line_counts {
            running = running.saturating_add((*count).max(1));
            prefix_sum.push(running);
        }
        LinePositionSpace { prefix_sum }
    }

    /// Extend the space so file id `file_id` is registered, giving any newly
    /// registered file the [`DEFAULT_LINES_PER_FILE`] slot.
    ///
    /// Appending is base-preserving: file `k`'s base is the sum of the sizes of
    /// the files before it, so registering a later file never moves an earlier
    /// one's addresses. That is what lets a writer address a step the moment it
    /// sees the step, before it knows how many files the trace will end up
    /// having.
    pub fn ensure_file(&mut self, file_id: usize) {
        while self.file_count() <= file_id {
            let top = self.total_lines();
            self.prefix_sum.push(top.saturating_add(DEFAULT_LINES_PER_FILE));
        }
    }

    /// Number of files this space covers.
    pub fn file_count(&self) -> usize {
        self.prefix_sum.len() - 1
    }

    /// One past the highest address this space can hold.
    pub fn total_lines(&self) -> u64 {
        *self.prefix_sum.last().unwrap_or(&0)
    }

    /// The first address of file `file_id`, or `None` when it is not registered.
    pub fn file_base(&self, file_id: usize) -> Option<u64> {
        if file_id >= self.file_count() {
            return None;
        }
        Some(self.prefix_sum[file_id])
    }

    /// How many addresses file `file_id` owns, or `None` when it is not
    /// registered.
    pub fn file_size(&self, file_id: usize) -> Option<u64> {
        if file_id >= self.file_count() {
            return None;
        }
        Some(self.prefix_sum[file_id + 1] - self.prefix_sum[file_id])
    }

    /// The address of `(file_id, line)`: the file's base plus the line's 0-based
    /// in-file offset. `line` is 1-based; a non-positive line is addressed as
    /// line 1, matching the canonical Nim writer's `globalIndex`.
    ///
    /// An unregistered `file_id` is registered on the spot (see
    /// [`ensure_file`](Self::ensure_file)), so a writer that addresses a step
    /// before the path table is complete gets the same answer it would get
    /// afterwards.
    pub fn global_index(&mut self, file_id: usize, line: i64) -> u64 {
        self.ensure_file(file_id);
        let offset = if line <= 1 { 0 } else { (line - 1) as u64 };
        self.prefix_sum[file_id].saturating_add(offset)
    }

    /// The address of `(file_id, line)` without extending the space — `None`
    /// when `file_id` is not registered.
    ///
    /// The read-only sibling of [`global_index`](Self::global_index), for
    /// callers holding a finished space.
    pub fn global_index_of(&self, file_id: usize, line: i64) -> Option<u64> {
        let base = self.file_base(file_id)?;
        let offset = if line <= 1 { 0 } else { (line - 1) as u64 };
        Some(base.saturating_add(offset))
    }

    /// Recover the `(file_id, line)` an address was built from.
    ///
    /// Exact inverse of [`global_index`](Self::global_index) over every line a
    /// registered file has. An address at or above the top of the space is
    /// refused rather than reinterpreted — see the module header.
    pub fn resolve(&self, position: u64) -> Result<(usize, i64), LinePositionError> {
        if self.file_count() == 0 {
            return Err(LinePositionError::NoFiles);
        }
        if position >= self.total_lines() {
            return Err(LinePositionError::OutOfSpace {
                position,
                total_lines: self.total_lines(),
                file_count: self.file_count(),
            });
        }
        // Largest `i` with `prefix_sum[i] <= position`. `prefix_sum[0] == 0` and
        // `position < prefix_sum[file_count]`, so the answer is a real file id.
        let file_id = self.prefix_sum.partition_point(|base| *base <= position) - 1;
        Ok((file_id, (position - self.prefix_sum[file_id]) as i64 + 1))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// Encode and decode are inverses at every line of every file, including
    /// the first and last line of each range — the boundary the off-by-one form
    /// (`file_base + line`) gets wrong.
    #[test]
    fn encode_and_decode_are_inverses_across_file_boundaries() {
        let mut space = LinePositionSpace::from_line_counts(&[10, 10, 10]);
        assert_eq!(space.total_lines(), 30);

        for file_id in 0..3usize {
            for line in 1..=10i64 {
                let p = space.global_index(file_id, line);
                assert_eq!(
                    space.resolve(p),
                    Ok((file_id, line)),
                    "address {p} was written for (file {file_id}, line {line})"
                );
            }
        }

        // The concrete boundary from the spec's note: with counts [10, 10], the
        // tenth line of file 0 must NOT land on file 1.
        assert_eq!(space.global_index(0, 10), 9);
        assert_eq!(space.global_index(1, 1), 10);
        assert_eq!(space.resolve(9), Ok((0, 10)));
        assert_eq!(space.resolve(10), Ok((1, 1)));
    }

    /// A file's range is exactly the addresses its lines occupy: the base is its
    /// own line 1 and nothing is left unused.
    #[test]
    fn a_files_range_holds_exactly_its_own_lines() {
        let space = LinePositionSpace::from_line_counts(&[4, 7]);
        assert_eq!(space.file_base(0), Some(0));
        assert_eq!(space.file_size(0), Some(4));
        assert_eq!(space.file_base(1), Some(4));
        assert_eq!(space.file_size(1), Some(7));
        assert_eq!(space.global_index_of(0, 1), Some(0));
        assert_eq!(space.global_index_of(0, 4), Some(3));
        assert_eq!(space.global_index_of(1, 1), Some(4));
        assert_eq!(space.global_index_of(1, 7), Some(10));
        assert_eq!(space.total_lines(), 11);
    }

    /// The uniform space every line-only trace gets today, and the Nim writer's
    /// stride it must agree with.
    #[test]
    fn the_uniform_space_places_a_file_at_its_own_multiple_of_the_stride() {
        let mut space = LinePositionSpace::uniform(3);
        assert_eq!(space.total_lines(), 3 * DEFAULT_LINES_PER_FILE);
        assert_eq!(space.global_index(0, 1), 0);
        assert_eq!(space.global_index(1, 1), DEFAULT_LINES_PER_FILE);
        assert_eq!(space.global_index(1, 7), DEFAULT_LINES_PER_FILE + 6);
        assert_eq!(space.global_index(2, 42), 2 * DEFAULT_LINES_PER_FILE + 41);
        assert_eq!(space.resolve(DEFAULT_LINES_PER_FILE + 6), Ok((1, 7)));
    }

    /// Registering a later file never moves an earlier file's addresses, so a
    /// writer may address a step before the path table is complete.
    #[test]
    fn appending_a_file_preserves_every_earlier_address() {
        let mut early = LinePositionSpace::new();
        let first = early.global_index(0, 12);
        let second = early.global_index(1, 12);

        let mut late = LinePositionSpace::uniform(5);
        assert_eq!(early.global_index(0, 12), first);
        assert_eq!(late.global_index(0, 12), first);
        assert_eq!(late.global_index(1, 12), second);
    }

    /// An address the space cannot hold is refused by name, not reinterpreted.
    /// The refusal must name the address and the space, because the caller's
    /// only remedy is to find out which trace produced it.
    #[test]
    fn an_address_above_the_space_is_refused_by_name() {
        let space = LinePositionSpace::uniform(2);
        // What `(path_id << 32) | line` produced for (path 1, line 5).
        let shifted = (1u64 << 32) | 5;
        let err = space.resolve(shifted).expect_err("2^32 is far above a two-file space");
        assert_eq!(
            err,
            LinePositionError::OutOfSpace {
                position: shifted,
                total_lines: 200_000,
                file_count: 2,
            }
        );
        let msg = err.to_string();
        assert!(msg.contains("4294967301"), "must name the address: {msg}");
        assert!(msg.contains("200000"), "must name the space: {msg}");
    }

    /// A trace with no files has no space to resolve into and says so, rather
    /// than indexing an empty prefix sum.
    #[test]
    fn a_pathless_trace_is_refused_not_defaulted() {
        let space = LinePositionSpace::new();
        assert_eq!(space.resolve(0), Err(LinePositionError::NoFiles));
        assert_eq!(space.file_count(), 0);
        assert_eq!(space.total_lines(), 0);
    }

    /// Line 0 and line 1 share the file's base address, matching the canonical
    /// Nim writer. A recorder that reports line 0 gets the file's first
    /// address, not the previous file's last.
    #[test]
    fn a_non_positive_line_addresses_the_files_own_base() {
        let mut space = LinePositionSpace::uniform(2);
        assert_eq!(space.global_index(1, 0), DEFAULT_LINES_PER_FILE);
        assert_eq!(space.global_index(1, 1), DEFAULT_LINES_PER_FILE);
        assert_eq!(space.global_index(1, -3), DEFAULT_LINES_PER_FILE);
    }

    /// An empty file still owns one address, so its id survives the round trip
    /// instead of collapsing onto its successor's base.
    #[test]
    fn an_empty_file_still_owns_its_own_address() {
        let space = LinePositionSpace::from_line_counts(&[0, 3]);
        assert_eq!(space.file_size(0), Some(1));
        assert_eq!(space.file_base(1), Some(1));
        assert_eq!(space.resolve(0), Ok((0, 1)));
        assert_eq!(space.resolve(1), Ok((1, 1)));
    }
}
