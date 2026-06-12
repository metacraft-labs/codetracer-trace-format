//! M4 — Rust port of the Nim `decodeGlobalPositionIndex` algorithm.
//!
//! The Nim reader
//! (`codetracer-trace-format-nim/src/codetracer_trace_writer/new_trace_reader.nim`,
//! proc `decodeGlobalPositionIndex` around line 430) resolves a single
//! `global_position_index` varint to `(file, line, column)` via two
//! binary searches: one across the per-file cumulative `file_base`
//! table, then one across the matched file's per-line cumulative
//! `line_base` table.
//!
//! Pre-M4, Rust consumers (chiefly `codetracer/src/db-backend`) routed
//! through the M1 FFI shim
//! (`ct_reader_step_locations_with_columns`) which calls the Nim
//! implementation per step.  That works but ties Rust column-aware
//! tooling to the Nim runtime.  The decoder in this module is a
//! pure-Rust, allocation-free hot path that takes the per-file
//! `line_lengths` tables harvested from `paths.dat` Layout A
//! (`codetracer-trace-format-spec/trace-events.md`
//! §"`paths.dat` per-line offset table") and answers GLI queries
//! natively.
//!
//! Algorithm — matches the spec
//! (`codetracer-trace-format-spec/trace-events.md` §"Decoding
//! `global_position_index`") bit-for-bit:
//!
//! 1. Binary-search the file table on cumulative `file_base` to find
//!    the file `f` such that `file_base[f] <= p < file_base[f] +
//!    file_size[f]`.  Cost `O(log F)`.
//! 2. Compute the in-file offset `q = p - file_base[f]`.
//! 3. Binary-search the file's per-line cumulative-length table for
//!    the line `l` such that `line_base[l] <= q < line_base[l] +
//!    line_lengths[l]`.  Then `column = q - line_base[l] + 1`.  Cost
//!    `O(log L)`.
//!
//! `line` and `column` are both 1-based, matching the spec and the
//! Nim implementation.

use std::fmt;

/// One position resolved from a `global_position_index`.
///
/// Mirrors the Nim `decodeGlobalPositionIndex` return tuple
/// `tuple[file: uint64, line: uint32, column: uint32]`.  `line` and
/// `column` are 1-based; `file` is the path-interning-table index used
/// by the trace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DecodedPosition {
    /// 0-indexed path interning table id.
    pub file: u64,
    /// 1-indexed source line.
    pub line: u32,
    /// 1-indexed source column.
    pub column: u32,
}

/// Column-aware step record exposed by Rust consumers of the new
/// trace reader.
///
/// This is the "new method / `Option<u32>` field" deliverable from
/// the M4 plan
/// (`codetracer-specs/Planned-Features/Column-Aware-Navigation.status.org`).
/// The legacy
/// [`codetracer_trace_types::StepRecord`] only carries `(path_id,
/// line)`; the M4 deliverable explicitly forbids extending it in
/// place because (a) it ships through the legacy CBOR+Zstd event
/// stream and (b) older recorders never populated column data.  This
/// record is the additive surface: `column == Some(N)` whenever the
/// trace is column-aware AND the decoder resolved a per-line table
/// for the file; `column == None` mirrors the legacy line-only
/// semantics.
///
/// The `file` field uses `u64` rather than the legacy
/// [`codetracer_trace_types::PathId`] (`usize` wrapper) because the
/// CTFS multi-stream format the column data comes from already
/// declares paths via a `u64` interning index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnAwareStepRecord {
    /// 0-indexed path interning table id.
    pub file: u64,
    /// 1-indexed source line.
    pub line: u32,
    /// 1-indexed source column when the trace carried column data
    /// (recorder set `meta.has_column_aware_steps`); `None` on
    /// pre-extension or line-only traces.
    pub column: Option<u32>,
}

impl ColumnAwareStepRecord {
    /// Promote a [`DecodedPosition`] to a column-aware step.  The
    /// caller has already confirmed the trace is column-aware (so
    /// `column` is meaningful).
    pub fn from_decoded(pos: DecodedPosition) -> Self {
        Self {
            file: pos.file,
            line: pos.line,
            column: Some(pos.column),
        }
    }

    /// Build a column-aware step from a legacy line-only
    /// `(file, line)` pair.  `column` is `None`.
    pub fn line_only(file: u64, line: u32) -> Self {
        Self { file, line, column: None }
    }
}

/// Errors returned by [`GlobalPositionDecoder::decode_global_position_index`].
///
/// Every variant mirrors a specific failure branch in the Nim
/// `decodeGlobalPositionIndex` implementation so the cross-reader
/// consistency invariant remains visible at the type level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// The decoder was constructed with an empty `line_lengths` slice —
    /// there are no files to search.  Matches the Nim reader's
    /// `"trace has no paths registered"` branch.
    NoFiles,
    /// The requested `global_position_index` lies past the last file's
    /// addressable range.  Matches the Nim reader's
    /// `"global_position_index N out of range for file F"` branch.
    OutOfRange {
        /// The offending GLI.
        position: u64,
        /// Total addressable positions across all files (exclusive
        /// upper bound).
        total_positions: u64,
    },
    /// The file the GLI binary-searched into has no per-line table —
    /// either the recorder did not populate `line_lengths` or the
    /// table was explicitly empty.  Matches the Nim reader's
    /// `"file F has no line-length table"` branch.
    FileHasNoLineTable {
        /// The file id (path interning table index) that failed
        /// resolution.
        file: u64,
    },
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::NoFiles => write!(f, "trace has no paths registered"),
            DecodeError::OutOfRange { position, total_positions } => write!(
                f,
                "global_position_index {position} out of range (trace has {total_positions} addressable positions)",
            ),
            DecodeError::FileHasNoLineTable { file } => {
                write!(f, "file {file} has no line-length table")
            }
        }
    }
}

impl std::error::Error for DecodeError {}

/// Pure-Rust resolver for `global_position_index` → `(file, line,
/// column)` queries.
///
/// Build once from the per-file `line_lengths` tables that the trace's
/// `paths.dat` (Layout A) records carry — typically by harvesting them
/// via the Nim reader's `lineLength` FFI on trace open — then call
/// [`decode_global_position_index`] for each step's GLI.  Per-resolution
/// cost is `O(log F + log L)`; no allocation on the hot path.
///
/// [`decode_global_position_index`]:
///     GlobalPositionDecoder::decode_global_position_index
#[derive(Debug, Clone)]
pub struct GlobalPositionDecoder {
    /// Per-file cumulative `line_base` tables.  `line_base[fid][l]` is
    /// the in-file offset where line `l` (0-indexed) starts.
    line_base: Vec<Vec<u64>>,
    /// Per-file base in the global position space — prefix sum of each
    /// file's `sum(line_lengths)`.  `file_base[fid]` is the GLI of the
    /// first addressable position in file `fid`.
    file_base: Vec<u64>,
    /// Per-file size (sum of that file's `line_lengths`).
    file_size: Vec<u64>,
    /// Sum of all `file_size` entries — exclusive upper bound on any
    /// valid GLI for this trace.
    total_positions: u64,
}

impl GlobalPositionDecoder {
    /// Build a decoder from per-file line-length tables.
    ///
    /// `line_lengths[file_id][line_index_0_based]` is the addressable
    /// column count of that source line — matches the
    /// `NewTraceReader.lineLengths` shape on the Nim side and the
    /// `register_path_with_line_lengths` write-side contract.
    ///
    /// The cumulative `file_base` / `line_base` / `file_size` tables
    /// are computed eagerly because (a) they are tiny relative to the
    /// trace itself (a few hundred KB at most) and (b) eager
    /// construction gives `decode_global_position_index` an
    /// immutable `&self` signature that downstream consumers can hold
    /// across threads.
    pub fn from_line_lengths(line_lengths: Vec<Vec<u32>>) -> Self {
        let file_count = line_lengths.len();
        let mut line_base = Vec::with_capacity(file_count);
        let mut file_base = Vec::with_capacity(file_count);
        let mut file_size = Vec::with_capacity(file_count);
        let mut running_global: u64 = 0;
        for lls in &line_lengths {
            let mut lb = Vec::with_capacity(lls.len());
            let mut sum: u64 = 0;
            for length in lls {
                lb.push(sum);
                sum = sum.saturating_add(u64::from(*length));
            }
            line_base.push(lb);
            file_base.push(running_global);
            file_size.push(sum);
            running_global = running_global.saturating_add(sum);
        }
        GlobalPositionDecoder {
            line_base,
            file_base,
            file_size,
            total_positions: running_global,
        }
    }

    /// Number of files registered in this decoder.
    pub fn file_count(&self) -> usize {
        self.file_base.len()
    }

    /// Cumulative global-position start of file `file_id`, or `None`
    /// when the id is past the registered files.
    pub fn file_base(&self, file_id: usize) -> Option<u64> {
        self.file_base.get(file_id).copied()
    }

    /// Total addressable positions in file `file_id`.
    pub fn file_size(&self, file_id: usize) -> Option<u64> {
        self.file_size.get(file_id).copied()
    }

    /// Total addressable positions across the whole trace — exclusive
    /// upper bound on any valid GLI.
    pub fn total_positions(&self) -> u64 {
        self.total_positions
    }

    /// Resolve a `global_position_index` to `(file, line, column)`.
    ///
    /// Mirrors the Nim `decodeGlobalPositionIndex` algorithm
    /// (`codetracer-trace-format-spec/trace-events.md` §"Decoding
    /// `global_position_index`") bit-for-bit.  See the module-level
    /// docs for the cost analysis and back-compat notes.
    pub fn decode_global_position_index(
        &self,
        position: u64,
    ) -> Result<DecodedPosition, DecodeError> {
        if self.file_base.is_empty() {
            return Err(DecodeError::NoFiles);
        }
        // The decoder treats a trace whose entire address space is
        // empty (every file has `sum(line_lengths) == 0`) the same way
        // the spec does — an out-of-range error for any GLI.  Detect it
        // up front because the binary-search below would otherwise
        // succeed (the candidate file would have `file_size == 0`) and
        // we'd surface a confusing `FileHasNoLineTable` path.
        if self.total_positions == 0 {
            return Err(DecodeError::OutOfRange {
                position,
                total_positions: 0,
            });
        }

        // Binary search for the file: largest fid with file_base[fid]
        // <= position, skipping files with file_size == 0 (they hold no
        // positions of their own — the spec lays files out in id order
        // with zero gaps but allows empty files via empty
        // `line_lengths`).
        let mut lo: isize = 0;
        let mut hi: isize = self.file_base.len() as isize - 1;
        let mut fid: isize = -1;
        while lo <= hi {
            let mid = (lo + hi) / 2;
            // Treat files whose `file_size == 0` as transparent — they
            // contribute no positions, so `file_base[mid] <= position`
            // alone is not enough.  Walk forward until we hit a real
            // file or fall off the end of the table.
            if self.file_base[mid as usize] <= position
                && self.file_size[mid as usize] > 0
                && position
                    < self.file_base[mid as usize].saturating_add(self.file_size[mid as usize])
            {
                fid = mid;
                break;
            } else if self.file_base[mid as usize] <= position {
                fid = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }

        if fid < 0 {
            // Pure prefix-of-the-first-file case: position precedes
            // file 0's base, which can only happen on an empty trace
            // (the spec lays files out starting at file_base[0] == 0).
            return Err(DecodeError::OutOfRange {
                position,
                total_positions: self.total_positions,
            });
        }

        // The binary search above may have landed on a zero-size file
        // when the requested position belongs to a later file.  Walk
        // forward past any zero-size successors to find the real
        // owner.
        let mut owner = fid as usize;
        while owner < self.file_base.len()
            && (self.file_size[owner] == 0
                || position
                    >= self.file_base[owner].saturating_add(self.file_size[owner]))
        {
            owner += 1;
        }
        if owner >= self.file_base.len() {
            return Err(DecodeError::OutOfRange {
                position,
                total_positions: self.total_positions,
            });
        }

        let fid = owner;
        let q = position - self.file_base[fid];
        let lb = &self.line_base[fid];
        if lb.is_empty() {
            return Err(DecodeError::FileHasNoLineTable { file: fid as u64 });
        }

        // Binary search for the line: largest l with lb[l] <= q.
        let mut lo: isize = 0;
        let mut hi: isize = lb.len() as isize - 1;
        let mut line_idx: isize = -1;
        while lo <= hi {
            let mid = (lo + hi) / 2;
            if lb[mid as usize] <= q {
                line_idx = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        if line_idx < 0 {
            // The decoder built `lb[0] = 0` for every file with at
            // least one line, so `q >= 0` always succeeds in the loop
            // above.  Reaching this branch means the file had a
            // populated `line_base` table but somehow lb[0] > q, which
            // would indicate an internal table-construction bug.  Bail
            // out with the same out-of-range signal the spec requires.
            return Err(DecodeError::OutOfRange {
                position,
                total_positions: self.total_positions,
            });
        }

        let line = (line_idx as u32) + 1;
        let column = (q - lb[line_idx as usize]) as u32 + 1;
        Ok(DecodedPosition {
            file: fid as u64,
            line,
            column,
        })
    }

    /// Decode a sequence of `global_position_index` varints to
    /// column-aware step records.  This is the bulk-ingest hot path
    /// downstream consumers (db-backend, future tooling) drive when
    /// streaming an entire trace through a Rust-native decoder.
    ///
    /// Per-element behaviour: on success each slot carries
    /// `column = Some(column)`; on the
    /// [`DecodeError::FileHasNoLineTable`] fallback we surface
    /// `column = None` and `line = 1` — matching the Nim FFI's
    /// behaviour when `decodeGlobalPositionIndex` errors out on a
    /// per-line-table-absent file (the legacy line-only resolver kicks
    /// in there).  Hard errors ([`DecodeError::NoFiles`],
    /// [`DecodeError::OutOfRange`]) short-circuit the whole call so
    /// the caller can surface a single contextual message.
    pub fn decode_many(
        &self,
        positions: &[u64],
    ) -> Result<Vec<ColumnAwareStepRecord>, DecodeError> {
        let mut out = Vec::with_capacity(positions.len());
        for &p in positions {
            match self.decode_global_position_index(p) {
                Ok(pos) => out.push(ColumnAwareStepRecord::from_decoded(pos)),
                Err(DecodeError::FileHasNoLineTable { file }) => {
                    // The per-line table is absent for this file.
                    // Mirror the bulk FFI's "fall back to line-only"
                    // behaviour so the consumer's downstream code can
                    // keep treating this as a step on file F with no
                    // column metadata available.  Line 1 is the
                    // safest default — the legacy line-only resolver
                    // would have done the actual line lookup, but
                    // without a per-line table we have no basis to
                    // pick anything else.
                    out.push(ColumnAwareStepRecord {
                        file,
                        line: 1,
                        column: None,
                    });
                }
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }
}
