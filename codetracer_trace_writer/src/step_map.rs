//! `step-map.ns`: the `(path_id, line)` → step-id index a line-only trace
//! carries so a reader can answer "where does line L of file P execute?"
//! without scanning the execution stream.
//!
//! Wire format (`codetracer-trace-format-spec/internal-files.md`
//! §"`step-map.ns`"), all integers little-endian:
//!
//! ```text
//! Header (18 bytes):
//!   magic: u32 = 0x53544D50 ("STMP")   version: u16 = 1
//!   path_count: u32                     path_table_offset: u64 (= 18)
//! Path table, path_count × 20 bytes, sorted by path_id:
//!   path_id: u64   line_count: u32   lines_offset: u64
//! Line entries, one block per path in path-table order, 32 bytes each,
//! sorted by line:
//!   line: u32   step_count: u32   first_step_id: i64   last_step_id: i64
//!   steps_offset: u64
//! Step-id lists, in line-entry order: step_count × i64, ascending.
//! ```
//!
//! A step id is the step's exec-record index in `steps.dat` — the index its
//! value record has in `values.dat` — so thread and reload records count.
//! The keys are the `(path_id, line)` a step was registered at, not its
//! global line index: a breakpoint request arrives in those coordinates.
//!
//! A trace with no steps still carries a well-formed zero-path map, so the
//! file's presence says the index was built rather than that steps exist.

use std::collections::BTreeMap;

pub const STEP_MAP_FILE_NAME: &str = "step-map.ns";
pub const STEP_MAP_MAGIC: u32 = 0x5354_4D50;
pub const STEP_MAP_VERSION: u16 = 1;

const HEADER_SIZE: usize = 18;
const PATH_ENTRY_SIZE: usize = 20;
const LINE_ENTRY_SIZE: usize = 32;

/// Accumulates step ids per `(path_id, line)` while a trace is written.
#[derive(Default)]
pub struct StepMapBuilder {
    by_path: BTreeMap<u64, BTreeMap<u32, Vec<i64>>>,
}

impl StepMapBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that the step at exec-record index `step_id` executed `line` of
    /// `path_id`. The line is the registered one, truncated to 32 bits as the
    /// wire field is.
    pub fn record_step(&mut self, path_id: u64, line: i64, step_id: u64) {
        self.by_path
            .entry(path_id)
            .or_default()
            .entry(line as u32)
            .or_default()
            .push(step_id as i64);
    }

    /// Serialize to the `step-map.ns` bytes.
    pub fn serialize(&self) -> Vec<u8> {
        let path_count = self.by_path.len();
        let lines_total: usize = self.by_path.values().map(BTreeMap::len).sum();
        let ids_total: usize = self.by_path.values().flat_map(BTreeMap::values).map(Vec::len).sum();
        let total = HEADER_SIZE + path_count * PATH_ENTRY_SIZE + lines_total * LINE_ENTRY_SIZE + ids_total * 8;
        let mut buf = vec![0u8; total];
        let put = |buf: &mut Vec<u8>, off: usize, bytes: &[u8]| buf[off..off + bytes.len()].copy_from_slice(bytes);

        put(&mut buf, 0, &STEP_MAP_MAGIC.to_le_bytes());
        put(&mut buf, 4, &STEP_MAP_VERSION.to_le_bytes());
        put(&mut buf, 6, &(path_count as u32).to_le_bytes());
        put(&mut buf, 10, &(HEADER_SIZE as u64).to_le_bytes());

        let mut line_cursor = HEADER_SIZE + path_count * PATH_ENTRY_SIZE;
        let mut ids_cursor = line_cursor + lines_total * LINE_ENTRY_SIZE;
        for (p, (path_id, lines)) in self.by_path.iter().enumerate() {
            let pe = HEADER_SIZE + p * PATH_ENTRY_SIZE;
            put(&mut buf, pe, &path_id.to_le_bytes());
            put(&mut buf, pe + 8, &(lines.len() as u32).to_le_bytes());
            put(&mut buf, pe + 12, &(line_cursor as u64).to_le_bytes());
            for (line, ids) in lines {
                let mut ids = ids.clone();
                ids.sort_unstable();
                let (first, last) = (ids.first().copied().unwrap_or(0), ids.last().copied().unwrap_or(0));
                put(&mut buf, line_cursor, &line.to_le_bytes());
                put(&mut buf, line_cursor + 4, &(ids.len() as u32).to_le_bytes());
                put(&mut buf, line_cursor + 8, &first.to_le_bytes());
                put(&mut buf, line_cursor + 16, &last.to_le_bytes());
                put(&mut buf, line_cursor + 24, &(ids_cursor as u64).to_le_bytes());
                for id in ids {
                    put(&mut buf, ids_cursor, &id.to_le_bytes());
                    ids_cursor += 8;
                }
                line_cursor += LINE_ENTRY_SIZE;
            }
        }
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_map_is_a_well_formed_zero_path_header() {
        let b = StepMapBuilder::new().serialize();
        assert_eq!(b.len(), HEADER_SIZE);
        assert_eq!(&b[0..4], &STEP_MAP_MAGIC.to_le_bytes());
        assert_eq!(&b[6..10], &0u32.to_le_bytes());
        assert_eq!(&b[10..18], &(HEADER_SIZE as u64).to_le_bytes());
    }
}
