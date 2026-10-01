//! `step-map.ns`: the `(path_id, line)` → step-id index a line-only trace
//! carries so a reader can answer "where does line L of file P execute?"
//! without scanning the execution stream.
//!
//! Wire format, version 2 (`codetracer-trace-format-spec/internal-files.md`
//! §"`step-map.ns`"); integers outside the frames are little-endian:
//!
//! ```text
//! Header (26 bytes):
//!   magic: u32 = 0x53544D50 ("STMP")   version: u16 = 2
//!   chunk_count: u32   path_count: u32   line_count: u32   step_count: u64
//! Chunk table, chunk_count x 20 bytes, in key order:
//!   frame_offset: u64 (from the end of the table)
//!   first_path_id: u64   first_line: u32
//! Frames: one zstd frame per chunk, back to back.
//! Chunk content: line records in ascending (path_id, line) order:
//!   path_delta: varint   line: varint (absolute at a chunk's first record and
//!   after a path change, else the delta from the previous line)
//!   count: varint, then (gap: varint, repeat: varint) runs until the repeats
//!   add up to count; the id before a list's first is -1, runs are maximal.
//! ```
//!
//! **Chunking.** Records are appended in key order; when an append brings the
//! chunk's decompressed size to [`STEP_MAP_CHUNK_TARGET`] bytes or more, the
//! chunk closes after that record. Every frame is compressed at level 3 in one
//! shot, declaring its content size. These rules are normative so that two
//! writers produce the same bytes; the reference encoder is
//! `tools/ctfs-measure`'s `encode_packed_mode(.., PACKED_TARGET,
//! PackedMode::RleZstd)`, against which `tests/step_map_v2_known_answer.rs`
//! compares this one.
//!
//! A step id is the step's exec-record index in `steps.dat` — the index its
//! value record has in `values.dat` — so thread and reload records count.
//! The keys are the `(path_id, line)` a step was registered at, not its
//! global line index: a breakpoint request arrives in those coordinates.
//!
//! A trace with no steps still carries the 26-byte header with every count
//! zero and no chunk, so the file's presence says the index was built rather
//! than that steps exist.

use std::collections::BTreeMap;

pub const STEP_MAP_FILE_NAME: &str = "step-map.ns";
pub const STEP_MAP_MAGIC: u32 = 0x5354_4D50;
pub const STEP_MAP_VERSION: u16 = 2;
/// Size of the fixed header.
pub const STEP_MAP_HEADER_SIZE: usize = 26;
/// Size of one chunk-table entry.
pub const STEP_MAP_CHUNK_ENTRY_SIZE: usize = 20;
/// A chunk closes after the record that brings its decompressed size to this.
pub const STEP_MAP_CHUNK_TARGET: usize = 64 * 1024;
/// The zstd level every chunk frame is compressed at.
pub const STEP_MAP_ZSTD_LEVEL: i32 = 3;

/// Accumulates step ids per `(path_id, line)` while a trace is written.
#[derive(Default)]
pub struct StepMapBuilder {
    by_path: BTreeMap<u64, BTreeMap<u32, Vec<u64>>>,
}

fn put_varint(mut v: u64, out: &mut Vec<u8>) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Append `ids`' run-length-coded gap list. The id before the first is -1,
/// so the first gap is the first id plus 1; adjacent runs never share a gap.
fn put_runs(ids: &[u64], out: &mut Vec<u8>) {
    let mut prev: i128 = -1;
    let mut run: Option<(u64, u64)> = None;
    for &id in ids {
        let gap = (id as i128 - prev) as u64;
        prev = id as i128;
        run = match run {
            Some((g, n)) if g == gap => Some((g, n + 1)),
            Some((g, n)) => {
                put_varint(g, out);
                put_varint(n, out);
                Some((gap, 1))
            }
            None => Some((gap, 1)),
        };
    }
    if let Some((g, n)) = run {
        put_varint(g, out);
        put_varint(n, out);
    }
}

impl StepMapBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that the step at exec-record index `step_id` executed `line` of
    /// `path_id`. The line is the registered one, truncated to 32 bits as the
    /// wire field is.
    pub fn record_step(&mut self, path_id: u64, line: i64, step_id: u64) {
        self.by_path.entry(path_id).or_default().entry(line as u32).or_default().push(step_id);
    }

    /// Serialize to the `step-map.ns` bytes. Fails only if zstd does.
    pub fn serialize(&self) -> Result<Vec<u8>, String> {
        // (first key, decompressed content) per chunk.
        let mut chunks: Vec<((u64, u32), Vec<u8>)> = Vec::new();
        let mut current: Option<((u64, u32), Vec<u8>)> = None;
        let (mut prev_path, mut prev_line) = (0u64, 0u32);
        let (mut lines, mut steps) = (0u32, 0u64);
        for (&path, by_line) in &self.by_path {
            for (&line, ids) in by_line {
                let mut ids = ids.clone();
                ids.sort_unstable();
                let (_, content) = current.get_or_insert_with(|| {
                    // A chunk's first record restates its key in full.
                    prev_path = path;
                    prev_line = 0;
                    ((path, line), Vec::new())
                });
                let path_delta = path - prev_path;
                put_varint(path_delta, content);
                put_varint(if path_delta > 0 { line as u64 } else { (line - prev_line) as u64 }, content);
                put_varint(ids.len() as u64, content);
                put_runs(&ids, content);
                prev_path = path;
                prev_line = line;
                lines += 1;
                steps += ids.len() as u64;
                if content.len() >= STEP_MAP_CHUNK_TARGET {
                    chunks.push(current.take().expect("a chunk is open"));
                }
            }
        }
        chunks.extend(current);

        let frames = chunks
            .iter()
            .map(|(_, content)| codetracer_ctfs::compress_pledged(content, STEP_MAP_ZSTD_LEVEL, STEP_MAP_FILE_NAME))
            .collect::<Result<Vec<_>, _>>()?;
        let mut out =
            Vec::with_capacity(STEP_MAP_HEADER_SIZE + chunks.len() * STEP_MAP_CHUNK_ENTRY_SIZE + frames.iter().map(Vec::len).sum::<usize>());
        out.extend_from_slice(&STEP_MAP_MAGIC.to_le_bytes());
        out.extend_from_slice(&STEP_MAP_VERSION.to_le_bytes());
        out.extend_from_slice(&(chunks.len() as u32).to_le_bytes());
        out.extend_from_slice(&(self.by_path.len() as u32).to_le_bytes());
        out.extend_from_slice(&lines.to_le_bytes());
        out.extend_from_slice(&steps.to_le_bytes());
        let mut offset = 0u64;
        for (((path, line), _), frame) in chunks.iter().zip(&frames) {
            out.extend_from_slice(&offset.to_le_bytes());
            out.extend_from_slice(&path.to_le_bytes());
            out.extend_from_slice(&line.to_le_bytes());
            offset += frame.len() as u64;
        }
        for frame in &frames {
            out.extend_from_slice(frame);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The record layout of the spec's example, uncompressed: one line, ids
    /// 0, 2, 4, 6, 7 — the first gap is from -1, and runs are maximal.
    #[test]
    fn a_line_record_is_the_specified_bytes() {
        let mut out = Vec::new();
        put_runs(&[0, 2, 4, 6, 7], &mut out);
        assert_eq!(out, vec![1, 1, 2, 3, 1, 1]);
    }

    #[test]
    fn an_empty_map_is_a_well_formed_zero_count_header() {
        let b = StepMapBuilder::new().serialize().unwrap();
        assert_eq!(b.len(), STEP_MAP_HEADER_SIZE);
        assert_eq!(&b[0..4], &STEP_MAP_MAGIC.to_le_bytes());
        assert_eq!(&b[4..6], &STEP_MAP_VERSION.to_le_bytes());
        assert!(b[6..].iter().all(|x| *x == 0));
    }
}
