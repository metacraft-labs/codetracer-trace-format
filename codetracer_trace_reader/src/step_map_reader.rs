//! Reader for `step-map.ns` version 2 (`codetracer-trace-format-spec/
//! internal-files.md` §"`step-map.ns`"): the `(path_id, line)` → step-id
//! index of a line-only trace.
//!
//! Two ways in, as the specification describes them:
//!
//! * [`StepMapReader::load_all`] inflates every chunk in order and returns
//!   every list — what a reader that builds `(path, line) -> ids` at open does.
//! * [`StepMapReader::lookup`] binary-searches the chunk table for the last
//!   chunk whose first key is not above the target, inflates that chunk alone
//!   and scans it.
//!
//! Every refusal the specification lists is made, by name: decoded counts that
//! disagree with the header, a chunk whose first record's key is not its table
//! key, keys that do not ascend strictly, a `count`, `gap` or `repeat` of `0`,
//! runs whose repeats overshoot `count`, and a frame that does not decode to
//! its declared size. Each is a map that would answer some breakpoint with the
//! wrong steps. Version 1 and every other version are refused.

use std::collections::BTreeMap;

use codetracer_ctfs::CtfsReader;
use codetracer_trace_writer::step_map::{STEP_MAP_CHUNK_ENTRY_SIZE, STEP_MAP_FILE_NAME, STEP_MAP_HEADER_SIZE, STEP_MAP_MAGIC, STEP_MAP_VERSION};

/// One chunk-table entry, resolved to a byte range of the member.
#[derive(Debug, Clone, Copy)]
struct Chunk {
    start: usize,
    end: usize,
    first: (u64, u32),
}

/// A parsed `step-map.ns`: its header and chunk table, with the frames
/// inflated on demand. The chunk the last [`lookup`](Self::lookup) inflated is
/// kept, so lookups that land in one chunk inflate it once.
#[derive(Debug)]
pub struct StepMapReader {
    bytes: Vec<u8>,
    path_count: u32,
    line_count: u32,
    step_count: u64,
    chunks: Vec<Chunk>,
    decoder: codetracer_ctfs::zstd_compat::Decoder,
    /// The chunk `raw` holds, if any.
    cached_chunk: Option<usize>,
    raw: Vec<u8>,
}

fn u32_at(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}

fn u64_at(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}

fn varint(b: &[u8], pos: &mut usize, chunk: usize) -> Result<u64, String> {
    let mut v: u64 = 0;
    let mut shift = 0u32;
    loop {
        let byte = *b
            .get(*pos)
            .ok_or_else(|| format!("{STEP_MAP_FILE_NAME}: chunk {chunk} ends inside a varint"))?;
        *pos += 1;
        if shift == 63 && byte > 1 || shift > 63 {
            return Err(format!("{STEP_MAP_FILE_NAME}: chunk {chunk} holds a varint wider than 64 bits"));
        }
        v |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(v);
        }
        shift += 7;
    }
}

/// The content size a zstd frame declares in its header, if it declares one
/// (RFC 8878 §3.1.1.1).
fn declared_content_size(frame: &[u8]) -> Option<u64> {
    if frame.len() < 5 || frame[0..4] != [0x28, 0xb5, 0x2f, 0xfd] {
        return None;
    }
    let fhd = frame[4];
    let single_segment = fhd & 0x20 != 0;
    let fcs_size = match fhd >> 6 {
        0 if single_segment => 1,
        0 => return None,
        1 => 2,
        2 => 4,
        _ => 8,
    };
    let dict_size = [0usize, 1, 2, 4][(fhd & 3) as usize];
    let at = 5 + usize::from(!single_segment) + dict_size;
    let field = frame.get(at..at + fcs_size)?;
    let mut buf = [0u8; 8];
    buf[..fcs_size].copy_from_slice(field);
    let v = u64::from_le_bytes(buf);
    Some(if fcs_size == 2 { v + 256 } else { v })
}

/// What [`StepMapReader::each_record`] does with a line record once it has
/// read its key.
enum Visit {
    /// Check the record's runs and move on.
    Skip,
    /// Decode its step ids and hand them over.
    Take,
    /// Stop the scan here.
    Stop,
}

/// Inflate chunk `c` of a member into `raw`, checking it decodes to the size
/// its frame declares.
fn inflate(bytes: &[u8], chunks: &[Chunk], c: usize, decoder: &mut codetracer_ctfs::zstd_compat::Decoder, raw: &mut Vec<u8>) -> Result<(), String> {
    let name = STEP_MAP_FILE_NAME;
    let chunk = chunks[c];
    let frame = &bytes[chunk.start..chunk.end];
    let declared = declared_content_size(frame).ok_or_else(|| format!("{name}: chunk {c}'s frame does not declare its content size"))?;
    decoder
        .decode_into(frame, raw)
        .map_err(|e| format!("{name}: chunk {c}'s frame does not decode: {e}"))?;
    if raw.len() as u64 != declared {
        return Err(format!(
            "{name}: chunk {c}'s frames decode to {} bytes, not the {declared} its frame header declares",
            raw.len()
        ));
    }
    Ok(())
}

impl StepMapReader {
    /// Open the container's `step-map.ns`. `Ok(None)` when the container has
    /// none — a column-aware trace, or one whose writer stopped before close;
    /// a reader then scans the execution stream.
    pub fn open(reader: &mut CtfsReader) -> Result<Option<StepMapReader>, String> {
        if reader.file_size(STEP_MAP_FILE_NAME).is_none() {
            return Ok(None);
        }
        let bytes = reader
            .read_file(STEP_MAP_FILE_NAME)
            .map_err(|e| format!("{STEP_MAP_FILE_NAME} is present but unreadable: {e}"))?;
        StepMapReader::from_bytes(bytes).map(Some)
    }

    /// Parse a member's header and chunk table. Frames are not inflated here.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<StepMapReader, String> {
        let name = STEP_MAP_FILE_NAME;
        if bytes.len() < STEP_MAP_HEADER_SIZE {
            return Err(format!(
                "{name}: {} bytes is shorter than the {STEP_MAP_HEADER_SIZE}-byte header",
                bytes.len()
            ));
        }
        if u32_at(&bytes, 0) != STEP_MAP_MAGIC {
            return Err(format!("{name}: bad magic 0x{:08x}", u32_at(&bytes, 0)));
        }
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != STEP_MAP_VERSION {
            return Err(format!(
                "{name}: version {version} is not read; this reader reads version {STEP_MAP_VERSION} only \
                 (version 1 stored uncompressed i64 lists and is retired)"
            ));
        }
        let chunk_count = u32_at(&bytes, 6) as usize;
        let (path_count, line_count, step_count) = (u32_at(&bytes, 10), u32_at(&bytes, 14), u64_at(&bytes, 18));
        let table_end = chunk_count
            .checked_mul(STEP_MAP_CHUNK_ENTRY_SIZE)
            .and_then(|t| t.checked_add(STEP_MAP_HEADER_SIZE))
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| format!("{name}: the header claims {chunk_count} chunks, more than the member's table can hold"))?;
        if chunk_count == 0 && (path_count, line_count, step_count) != (0, 0, 0) {
            return Err(format!(
                "{name}: the header counts {path_count} paths, {line_count} lines and {step_count} steps but the map has no chunk"
            ));
        }
        let mut chunks = Vec::with_capacity(chunk_count);
        for i in 0..chunk_count {
            let e = STEP_MAP_HEADER_SIZE + i * STEP_MAP_CHUNK_ENTRY_SIZE;
            let start = u64_at(&bytes, e);
            let first = (u64_at(&bytes, e + 8), u32_at(&bytes, e + 16));
            let start = usize::try_from(start)
                .ok()
                .and_then(|s| s.checked_add(table_end))
                .filter(|s| *s <= bytes.len())
                .ok_or_else(|| format!("{name}: chunk {i}'s frame offset {start} lies past the end of the member"))?;
            if let Some(prev) = chunks.last_mut() {
                let prev: &mut Chunk = prev;
                if start < prev.start {
                    return Err(format!("{name}: chunk {i}'s frame begins before chunk {}'s", i - 1));
                }
                if first <= prev.first {
                    return Err(format!(
                        "{name}: chunk keys do not ascend strictly: chunk {i} starts at {first:?}, chunk {} at {:?}",
                        i - 1,
                        prev.first
                    ));
                }
                prev.end = start;
            }
            chunks.push(Chunk {
                start,
                end: bytes.len(),
                first,
            });
        }
        Ok(StepMapReader {
            bytes,
            path_count,
            line_count,
            step_count,
            chunks,
            decoder: codetracer_ctfs::zstd_compat::Decoder::new().map_err(|e| format!("{name}: {e}"))?,
            cached_chunk: None,
            raw: Vec::new(),
        })
    }

    /// Distinct path ids with at least one step, as the header states.
    pub fn path_count(&self) -> u32 {
        self.path_count
    }

    /// Distinct `(path_id, line)` keys, as the header states.
    pub fn line_count(&self) -> u32 {
        self.line_count
    }

    /// Step ids in all lists together, as the header states.
    pub fn step_count(&self) -> u64 {
        self.step_count
    }

    /// Number of chunks.
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Every line's step ids, after checking the decoded counts against the
    /// header.
    pub fn load_all(&self) -> Result<BTreeMap<(u64, u32), Vec<u64>>, String> {
        let mut map = BTreeMap::new();
        let mut last: Option<(u64, u32)> = None;
        let mut steps = 0u64;
        let mut paths = 0u32;
        let mut decoder = codetracer_ctfs::zstd_compat::Decoder::new().map_err(|e| format!("{STEP_MAP_FILE_NAME}: {e}"))?;
        let mut raw = Vec::new();
        for c in 0..self.chunks.len() {
            inflate(&self.bytes, &self.chunks, c, &mut decoder, &mut raw)?;
            self.each_record(c, &raw, |_| Visit::Take, |key, ids| {
                if let Some(prev) = last
                    && key <= prev
                {
                    return Err(format!(
                        "{STEP_MAP_FILE_NAME}: keys do not ascend strictly: {key:?} follows {prev:?} at chunk {c}"
                    ));
                }
                if last.is_none_or(|p| p.0 != key.0) {
                    paths += 1;
                }
                last = Some(key);
                steps += ids.len() as u64;
                map.insert(key, ids);
                Ok(())
            })?;
        }
        for (what, decoded, header) in [
            ("path", paths as u64, self.path_count as u64),
            ("line", map.len() as u64, self.line_count as u64),
            ("step", steps, self.step_count),
        ] {
            if decoded != header {
                return Err(format!(
                    "{STEP_MAP_FILE_NAME}: the header counts {header} {what}s but the chunks hold {decoded}"
                ));
            }
        }
        Ok(map)
    }

    /// One line's step ids, inflating only the chunk that can hold it.
    /// `Ok(None)` when no step ran on that line. The records scanned past are
    /// checked as [`load_all`](Self::load_all) checks them, without
    /// materialising their ids.
    ///
    /// Line 0 is looked up as line 1, the key a writer files a step
    /// registered at line 0 under (`internal-files.md` §"`step-map.ns`",
    /// "Reading").
    pub fn lookup(&mut self, path_id: u64, line: u32) -> Result<Option<Vec<u64>>, String> {
        let target = (path_id, line.max(1));
        let c = self.chunks.partition_point(|ch| ch.first <= target);
        if c == 0 {
            return Ok(None);
        }
        let c = c - 1;
        if self.cached_chunk != Some(c) {
            self.cached_chunk = None;
            inflate(&self.bytes, &self.chunks, c, &mut self.decoder, &mut self.raw)?;
            self.cached_chunk = Some(c);
        }
        let mut found = None;
        self.each_record(
            c,
            &self.raw,
            |key| match key.cmp(&target) {
                std::cmp::Ordering::Less => Visit::Skip,
                std::cmp::Ordering::Equal => Visit::Take,
                std::cmp::Ordering::Greater => Visit::Stop,
            },
            |_, ids| {
                found = Some(ids);
                Ok(())
            },
        )?;
        Ok(found)
    }

    /// Scan the line records of chunk `c`, inflated in `raw`, checking
    /// everything that can be checked within a chunk. `visit` sees each key
    /// first and says whether to skip the record, take its ids — decoded and
    /// handed to `take` — or stop.
    fn each_record(
        &self,
        c: usize,
        raw: &[u8],
        mut visit: impl FnMut((u64, u32)) -> Visit,
        mut take: impl FnMut((u64, u32), Vec<u64>) -> Result<(), String>,
    ) -> Result<(), String> {
        let name = STEP_MAP_FILE_NAME;
        let chunk = self.chunks[c];
        let mut pos = 0usize;
        let (mut path, mut line) = chunk.first;
        let mut first = true;
        while pos < raw.len() {
            let path_delta = varint(raw, &mut pos, c)?;
            let line_field = varint(raw, &mut pos, c)?;
            let key = if first {
                let key = (path.wrapping_add(path_delta), line_field as u32);
                if path_delta != 0 || line_field != chunk.first.1 as u64 {
                    return Err(format!(
                        "{name}: chunk {c}'s first record is ({}, {line_field}), not its table key {:?}",
                        key.0, chunk.first
                    ));
                }
                key
            } else if path_delta > 0 {
                (
                    path.checked_add(path_delta)
                        .ok_or_else(|| format!("{name}: chunk {c}: path id overflows"))?,
                    line_field as u32,
                )
            } else {
                if line_field == 0 {
                    return Err(format!(
                        "{name}: keys do not ascend strictly: chunk {c} repeats ({path}, {line}) with a line delta of 0"
                    ));
                }
                let next = (line as u64) + line_field;
                if next > u32::MAX as u64 {
                    return Err(format!(
                        "{name}: chunk {c}: a line delta takes ({path}, {line}) past the 32-bit line range"
                    ));
                }
                (path, next as u32)
            };
            first = false;
            (path, line) = key;
            let wanted = match visit(key) {
                Visit::Stop => return Ok(()),
                Visit::Skip => false,
                Visit::Take => true,
            };
            let count = varint(raw, &mut pos, c)?;
            if count == 0 {
                return Err(format!("{name}: chunk {c}: line {key:?} has a count of 0"));
            }
            // Bounded by the header before anything is allocated: a list
            // longer than the whole map is a count that disagrees with it.
            if count > self.step_count {
                return Err(format!(
                    "{name}: chunk {c}: line {key:?} claims {count} steps, more than the header's step count of {}",
                    self.step_count
                ));
            }
            let mut ids = if wanted { Vec::with_capacity(count as usize) } else { Vec::new() };
            let mut prev: i128 = -1;
            let mut have = 0u64;
            while have < count {
                let gap = varint(raw, &mut pos, c)?;
                let repeat = varint(raw, &mut pos, c)?;
                if gap == 0 {
                    return Err(format!("{name}: chunk {c}: line {key:?} has a gap of 0"));
                }
                if repeat == 0 {
                    return Err(format!("{name}: chunk {c}: line {key:?} has a repeat of 0"));
                }
                if repeat > count - have {
                    return Err(format!(
                        "{name}: chunk {c}: line {key:?}'s runs overshoot its count of {count} ({} after this run)",
                        have.saturating_add(repeat)
                    ));
                }
                if wanted {
                    for _ in 0..repeat {
                        prev += gap as i128;
                        let id = u64::try_from(prev).map_err(|_| format!("{name}: chunk {c}: line {key:?}'s step ids overflow 64 bits"))?;
                        ids.push(id);
                    }
                } else {
                    prev += gap as i128 * repeat as i128;
                    if prev > u64::MAX as i128 {
                        return Err(format!("{name}: chunk {c}: line {key:?}'s step ids overflow 64 bits"));
                    }
                }
                have += repeat;
            }
            if wanted {
                take(key, ids)?;
            }
        }
        Ok(())
    }
}
