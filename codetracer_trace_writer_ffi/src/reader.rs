//! `ct_reader_*`: a container opened for reading, from a path or from bytes.
//!
//! Opening reads `meta.dat` and the four interning tables, and lays out the
//! position space step positions are resolved in. The streams are opened on
//! first use, so an accessor of a stream a container lacks, or holds
//! malformed, fails where it is called and not at the open.
//!
//! Records are answered as the JSON documents the header describes, or field
//! by field through the structured accessors. Every buffer handed out is a
//! copy the caller releases with `ct_free_buffer`.

use std::os::raw::c_char;
use std::path::PathBuf;

use codetracer_ctfs::CtfsReader;
use codetracer_ctfs::compact::Profile;
use codetracer_trace_reader::call_stream_reader::CallStreamReader;
use codetracer_trace_reader::value_stream_reader::ValueStreamReader;
use codetracer_trace_writer::call_stream::CallStreamRecord;
use codetracer_trace_writer::column_aware::{StepEvent, decode_step_event_declared};
use codetracer_trace_writer::event_stream::IoEventRecord;
use codetracer_trace_writer::meta_dat::{
    FLAG_EXT_HAS_SOURCE_RELOAD, FLAG_HAS_COLUMN_AWARE_STEPS, FLAG_HAS_LINE_COUNT_TABLE, FLAG_SUPPORTS_COLUMN_BREAKPOINTS,
    FLAG_SUPPORTS_COLUMN_MOTIONS, MetaDat, decode_meta_dat,
};
use codetracer_trace_writer::value_stream::ValueStreamEvent;

use crate::{alloc_buffer, cstr_bytes, guarded, path_from_bytes, set_error};

const DEFAULT_LINES_PER_FILE: u64 = 100_000;
const CONVENTIONAL_LINE_LENGTH: u32 = 1024;
const CONVENTIONAL_FILE_SIZE: u64 = DEFAULT_LINES_PER_FILE * CONVENTIONAL_LINE_LENGTH as u64;
/// A `paths.dat` record probed as Layout A states at most this many lines.
const MAX_PROBE_LINE_COUNT: u64 = 1_000_000;

// ---------------------------------------------------------------------------
// Varints
// ---------------------------------------------------------------------------

fn varint(data: &[u8], pos: &mut usize) -> Result<u64, String> {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        let Some(&b) = data.get(*pos) else {
            return Err("truncated varint".to_string());
        };
        *pos += 1;
        if shift >= 64 {
            return Err("varint too long".to_string());
        }
        result |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }
}

// ---------------------------------------------------------------------------
// Interning tables
// ---------------------------------------------------------------------------

/// A `.dat` + `.off` variable-size record table.
#[derive(Default)]
struct Table {
    dat: Vec<u8>,
    off: Vec<u8>,
}

impl Table {
    fn load(ctfs: &mut CtfsReader, name: &str) -> Result<Option<Table>, String> {
        let names = ctfs.list_files();
        let (dat_name, off_name) = (format!("{name}.dat"), format!("{name}.off"));
        if !names.contains(&dat_name) && !names.contains(&off_name) {
            return Ok(None);
        }
        let dat = ctfs
            .read_file(&dat_name)
            .map_err(|e| format!("{dat_name}: failed to read data file: {e}"))?;
        let off = ctfs
            .read_file(&off_name)
            .map_err(|e| format!("{dat_name}: failed to read offset file: {e}"))?;
        if off.len() % 8 != 0 {
            return Err(format!("{dat_name}: offset file size not a multiple of 8"));
        }
        if off.len() < 8 {
            return Err(format!("{dat_name}: offset file too small (needs at least initial offset)"));
        }
        Ok(Some(Table { dat, off }))
    }

    fn count(&self) -> u64 {
        if self.off.len() < 8 { 0 } else { (self.off.len() / 8 - 1) as u64 }
    }

    fn read(&self, index: u64) -> Result<&[u8], String> {
        if index >= self.count() {
            return Err(format!("index out of range: {index}"));
        }
        let at = |i: usize| u64::from_le_bytes(self.off[i * 8..i * 8 + 8].try_into().expect("8 bytes"));
        let (start, end) = (at(index as usize), at(index as usize + 1));
        if end < start || end > self.dat.len() as u64 {
            return Err("record data out of bounds".to_string());
        }
        Ok(&self.dat[start as usize..end as usize])
    }
}

/// Decode a Layout A `paths.dat` record's per-line table. `probe` adds the
/// checks that tell a Layout A record from a bare path.
fn decode_layout_a(raw: &[u8], probe: bool) -> Result<Vec<u32>, String> {
    let mut pos = 0;
    let path_len = varint(raw, &mut pos).map_err(|e| format!("column-aware path_len varint: {e}"))?;
    if path_len > (raw.len() - pos) as u64 {
        return Err("path_bytes truncated".to_string());
    }
    if probe {
        if path_len == (raw.len() - pos) as u64 {
            return Err("not Layout A (no line_count)".to_string());
        }
        if raw[pos..pos + path_len as usize].iter().any(|&b| b < 0x09 || (b > 0x0d && b < 0x20)) {
            return Err("not Layout A (control byte in path)".to_string());
        }
    }
    pos += path_len as usize;
    let line_count = varint(raw, &mut pos).map_err(|e| format!("column-aware line_count varint: {e}"))?;
    if probe && line_count > MAX_PROBE_LINE_COUNT {
        return Err(format!("not Layout A (line_count {line_count} exceeds probe bound)"));
    }
    let mut lls = Vec::with_capacity(line_count.min(1 << 20) as usize);
    let mut prev = 0i64;
    for l in 0..line_count {
        let z = varint(raw, &mut pos).map_err(|e| format!("line_length[{l}]: {e}"))?;
        let d = if z & 1 == 0 { (z >> 1) as i64 } else { !((z >> 1) as i64) };
        let current = if l == 0 { d } else { prev + d };
        if current < 0 {
            return Err(format!("line_length[{l}] negative: {current}"));
        }
        lls.push(current as u32);
        prev = current;
    }
    if probe && pos != raw.len() {
        return Err(format!("not Layout A ({} trailing byte(s))", raw.len() - pos));
    }
    Ok(lls)
}

fn decode_line_count_record(raw: &[u8]) -> Result<(Vec<u8>, u64), String> {
    let mut pos = 0;
    let len = varint(raw, &mut pos).map_err(|e| format!("line-count payload_len varint: {e}"))?;
    if len > (raw.len() - pos) as u64 {
        return Err(format!("payload truncated (payload_len {len}, {} byte(s) left)", raw.len() - pos));
    }
    let payload = raw[pos..pos + len as usize].to_vec();
    pos += len as usize;
    let count = varint(raw, &mut pos).map_err(|e| format!("line_count varint: {e}"))?;
    if count == 0 {
        return Err(format!("line_count is 0 for {}", String::from_utf8_lossy(&payload)));
    }
    if pos != raw.len() {
        return Err(format!("{} trailing byte(s) after line_count", raw.len() - pos));
    }
    Ok((payload, count))
}

fn decode_func_record(data: &[u8]) -> Result<(u64, Vec<u8>), String> {
    let mut pos = 0;
    let gli = varint(data, &mut pos)?;
    let len = varint(data, &mut pos)?;
    if ((data.len() - pos) as u64) < len {
        return Err(format!(
            "funcs.dat record is truncated: declares a {len}-byte name with only {} bytes left",
            data.len() - pos
        ));
    }
    Ok((gli, data[pos..pos + len as usize].to_vec()))
}

fn decode_type_record(data: &[u8]) -> Result<Vec<u8>, String> {
    if data.is_empty() {
        return Err("types.dat record is empty: it must carry at least the kind byte".to_string());
    }
    let mut pos = 1;
    let len = varint(data, &mut pos)?;
    if ((data.len() - pos) as u64) < len {
        return Err(format!(
            "types.dat record is truncated: declares a {len}-byte lang_type with only {} bytes left",
            data.len() - pos
        ));
    }
    Ok(data[pos..pos + len as usize].to_vec())
}

/// Check an alternate source view record: `path_id`, `view_kind`, then the
/// name, content and sourcemap, each length-prefixed.
fn decode_source_view(raw: &[u8]) -> Result<(), String> {
    let mut pos = 0;
    varint(raw, &mut pos).map_err(|e| format!("path_id varint: {e}"))?;
    if pos >= raw.len() {
        return Err("view_kind byte missing".to_string());
    }
    pos += 1;
    for what in ["view_name", "content", "map"] {
        let n = varint(raw, &mut pos).map_err(|e| format!("{what}_len: {e}"))?;
        if n > (raw.len() - pos) as u64 {
            return Err(format!("{what} truncated"));
        }
        pos += n as usize;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The position space
// ---------------------------------------------------------------------------

/// The line-only resolution of a step position: file bases laid end to end.
#[derive(Default)]
struct PositionSpace {
    bases: Vec<u64>,
    total: u64,
}

impl PositionSpace {
    fn new(sizes: impl IntoIterator<Item = u64>) -> Self {
        let mut bases = Vec::new();
        let mut total = 0u64;
        for s in sizes {
            bases.push(total);
            total += s;
        }
        PositionSpace { bases, total }
    }

    fn resolve(&self, p: u64) -> Result<(u64, u64), String> {
        if p >= self.total {
            if self.bases.is_empty() {
                return Err(format!(
                    "line-only global_position_index {p} cannot be resolved to (file, line): the trace registers no paths"
                ));
            }
            return Err(format!(
                "line-only global_position_index {p} is outside this trace's address space of {} ({} path(s))",
                self.total,
                self.bases.len()
            ));
        }
        let file = self.bases.partition_point(|&b| b <= p) - 1;
        Ok((file as u64, p - self.bases[file] + 1))
    }
}

// ---------------------------------------------------------------------------
// The execution stream
// ---------------------------------------------------------------------------

struct ExecChunk {
    index: usize,
    events: Vec<StepEvent>,
    /// The refusal that stopped the decode, at `events.len()`.
    decode_error: Option<String>,
    positions: Vec<u64>,
    /// The refusal that stopped position resolution, at `positions.len()`.
    position_error: Option<String>,
}

struct ExecReader {
    dat: Vec<u8>,
    stored: bool,
    chunk_size: u64,
    offsets: Vec<u64>,
    allow_reload: bool,
    total: u64,
    cache: Option<ExecChunk>,
}

fn position_refusal(chunk: usize, i: usize, what: &str) -> String {
    format!("steps.dat chunk {chunk} record {i}{what}")
}

impl ExecReader {
    fn open(ctfs: &mut CtfsReader, allow_reload: bool) -> Result<ExecReader, String> {
        let dat = ctfs.read_file("steps.dat").map_err(|e| format!("steps.dat: {e}"))?;
        let idx = ctfs.read_file("steps.idx").map_err(|e| format!("steps.idx: {e}"))?;
        if idx.len() < 4 {
            return Err("index file too small for chunk_size header".to_string());
        }
        let chunk_size = u32::from_le_bytes(idx[0..4].try_into().expect("4 bytes")) as u64;
        if chunk_size == 0 {
            return Err("chunkSize in index is 0".to_string());
        }
        if (idx.len() - 4) % 8 != 0 {
            return Err("index file has trailing bytes in offset region".to_string());
        }
        let offsets: Vec<u64> = idx[4..]
            .chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().expect("8 bytes")))
            .collect();
        let mut r = ExecReader {
            dat,
            stored: ctfs.profile() == Profile::Compact,
            chunk_size,
            offsets,
            allow_reload,
            total: 0,
            cache: None,
        };
        if let Some(last) = r.offsets.len().checked_sub(1) {
            if r.offsets[last] > r.dat.len() as u64 {
                return Err("last chunk offset past end of steps.dat".to_string());
            }
            let chunk = r.decode_chunk(last)?;
            if let Some(e) = &chunk.decode_error {
                return Err(e.clone());
            }
            r.total = last as u64 * chunk_size + chunk.events.len() as u64;
        }
        Ok(r)
    }

    fn decode_chunk(&self, index: usize) -> Result<ExecChunk, String> {
        let start = self.offsets[index];
        let end = if index + 1 < self.offsets.len() {
            self.offsets[index + 1]
        } else {
            self.dat.len() as u64
        };
        if start > end || end > self.dat.len() as u64 {
            return Err(format!("chunk {index} offsets out of range"));
        }
        let frame = &self.dat[start as usize..end as usize];
        let raw = if self.stored {
            frame.to_vec()
        } else {
            codetracer_ctfs::zstd_compat::decode_all(frame).map_err(|e| format!("steps.dat chunk {index}: zstd decode failed: {e}"))?
        };
        let mut events = Vec::new();
        let mut decode_error = None;
        let mut pos = 0;
        while pos < raw.len() {
            match decode_step_event_declared(&raw, &mut pos, self.allow_reload) {
                Ok(ev) => events.push(ev),
                Err(e) => {
                    decode_error = Some(format!("steps.dat chunk {index}: {e}"));
                    break;
                }
            }
        }
        let mut positions = Vec::with_capacity(events.len());
        let mut position_error = None;
        let mut cursor = 0u64;
        let mut anchored = false;
        for (i, ev) in events.iter().enumerate() {
            match ev {
                StepEvent::AbsoluteStep { global_position_index } => {
                    cursor = *global_position_index;
                    anchored = true;
                }
                StepEvent::DeltaStep { delta: d } | StepEvent::DeltaColumn { column_delta: d } => {
                    if !anchored {
                        position_error = Some(position_refusal(
                            index,
                            i,
                            " is a delta before the chunk's first AbsoluteStep, so it has no position to be relative to",
                        ));
                        break;
                    }
                    let p = cursor as i64 + d;
                    if p < 0 {
                        position_error = Some(position_refusal(index, i, " resolves to a negative position"));
                        break;
                    }
                    cursor = p as u64;
                }
                _ => {}
            }
            positions.push(cursor);
        }
        Ok(ExecChunk {
            index,
            events,
            decode_error,
            positions,
            position_error,
        })
    }

    fn chunk(&mut self, index: usize) -> Result<&ExecChunk, String> {
        if index >= self.offsets.len() {
            return Err(format!("chunk index out of range: {index}"));
        }
        if self.cache.as_ref().map(|c| c.index) != Some(index) {
            self.cache = Some(self.decode_chunk(index)?);
        }
        Ok(self.cache.as_ref().expect("cached above"))
    }

    fn event(&mut self, n: u64) -> Result<StepEvent, String> {
        if n >= self.total {
            return Err(format!("step {n} is past the end of the exec stream ({} event(s))", self.total));
        }
        let (c, i) = ((n / self.chunk_size) as usize, (n % self.chunk_size) as usize);
        let chunk = self.chunk(c)?;
        match chunk.events.get(i) {
            Some(ev) => Ok(ev.clone()),
            None => Err(chunk
                .decode_error
                .clone()
                .unwrap_or_else(|| format!("step {n} is past the end of its exec chunk"))),
        }
    }

    fn position(&mut self, n: u64) -> Result<u64, String> {
        if n >= self.total {
            return Err(format!("step {n} is past the end of its exec chunk"));
        }
        let (c, i) = ((n / self.chunk_size) as usize, (n % self.chunk_size) as usize);
        let chunk = self.chunk(c)?;
        if i >= chunk.events.len() {
            return Err(chunk
                .decode_error
                .clone()
                .unwrap_or_else(|| format!("step {n} is past the end of its exec chunk")));
        }
        match chunk.positions.get(i) {
            Some(p) => Ok(*p),
            None => Err(chunk.position_error.clone().unwrap_or_default()),
        }
    }
}

// ---------------------------------------------------------------------------
// A chunked stream of length-framed records
// ---------------------------------------------------------------------------

/// A chunk's records, or the refusal that stopped splitting it.
type SplitChunk = Result<Vec<Vec<u8>>, String>;

/// A `.dat` + `.idx` pair whose chunks hold `[varint length][record]...`.
/// Records are counted by their framing and decoded when read.
struct FramedStream {
    name: &'static str,
    dat: Vec<u8>,
    stored: bool,
    chunk_size: u64,
    offsets: Vec<u64>,
    total: u64,
    cache: Option<(usize, SplitChunk)>,
}

impl FramedStream {
    fn open(ctfs: &mut CtfsReader, name: &'static str) -> Result<FramedStream, String> {
        let dat = ctfs.read_file(&format!("{name}.dat")).map_err(|e| format!("{name}.dat: {e}"))?;
        let idx = ctfs.read_file(&format!("{name}.idx")).map_err(|e| format!("{name}.idx: {e}"))?;
        if idx.len() < 4 {
            return Err(format!("{name}.idx: too short for its header"));
        }
        let chunk_size = u32::from_le_bytes(idx[0..4].try_into().expect("4 bytes")) as u64;
        if chunk_size == 0 {
            return Err(format!("{name}.idx: chunk_size is 0"));
        }
        if (idx.len() - 4) % 8 != 0 {
            return Err(format!("{name}.idx: trailing bytes in the offset region"));
        }
        let offsets: Vec<u64> = idx[4..]
            .chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().expect("8 bytes")))
            .collect();
        let mut s = FramedStream {
            name,
            dat,
            stored: ctfs.profile() == Profile::Compact,
            chunk_size,
            offsets,
            total: 0,
            cache: None,
        };
        if let Some(last) = s.offsets.len().checked_sub(1) {
            let n = s.split(last)?.len() as u64;
            s.total = last as u64 * chunk_size + n;
            s.cache = None;
        }
        Ok(s)
    }

    fn split(&mut self, index: usize) -> Result<&Vec<Vec<u8>>, String> {
        if self.cache.as_ref().map(|c| c.0) != Some(index) {
            let start = self.offsets[index];
            let end = if index + 1 < self.offsets.len() {
                self.offsets[index + 1]
            } else {
                self.dat.len() as u64
            };
            let records = (|| {
                if start > end || end > self.dat.len() as u64 {
                    return Err(format!("{}.dat: chunk {index} offsets out of range", self.name));
                }
                let frame = &self.dat[start as usize..end as usize];
                let raw = if self.stored {
                    frame.to_vec()
                } else {
                    codetracer_ctfs::zstd_compat::decode_all(frame)
                        .map_err(|e| format!("{}.dat chunk {index}: zstd decode failed: {e}", self.name))?
                };
                let mut out = Vec::new();
                let mut pos = 0;
                while pos < raw.len() {
                    let len = varint(&raw, &mut pos).map_err(|e| format!("{}.dat chunk {index}: record length: {e}", self.name))?;
                    if len > (raw.len() - pos) as u64 {
                        return Err(format!("{}.dat chunk {index}: record extends past the chunk", self.name));
                    }
                    out.push(raw[pos..pos + len as usize].to_vec());
                    pos += len as usize;
                }
                Ok(out)
            })();
            self.cache = Some((index, records));
        }
        self.cache.as_ref().expect("cached above").1.as_ref().map_err(Clone::clone)
    }

    fn record(&mut self, n: u64) -> Result<Vec<u8>, String> {
        if n >= self.total {
            return Err(format!("{} record {n} out of range (count {})", self.name, self.total));
        }
        let (c, i) = ((n / self.chunk_size) as usize, (n % self.chunk_size) as usize);
        let name = self.name;
        self.split(c)?
            .get(i)
            .cloned()
            .ok_or_else(|| format!("{name} record {i} missing in chunk {c}"))
    }
}

// ---------------------------------------------------------------------------
// The reader
// ---------------------------------------------------------------------------

/// Per file: the base of each line, and the file's base and size.
type ColumnSpace = (Vec<Vec<u64>>, Vec<u64>, Vec<u64>);

/// The reader behind a `ct_reader_t`.
pub struct TraceReader {
    ctfs: CtfsReader,
    assume_column_aware_paths: bool,
    meta: MetaDat,
    column_aware: bool,
    paths: Table,
    funcs: Table,
    types: Table,
    varnames: Table,
    line_lengths: Vec<Vec<u32>>,
    line_count_payloads: Vec<Vec<u8>>,
    space: PositionSpace,
    /// Per file: the base of each line, and the file's base and size, in the
    /// column-aware position space.
    column_space: Option<ColumnSpace>,
    exec: Option<ExecReader>,
    values: Option<ValueStreamReader>,
    calls: Option<CallStreamReader>,
    events: Option<FramedStream>,
}

fn empty_meta() -> MetaDat {
    MetaDat {
        version: 0,
        flags: 0,
        ext_flags: 0,
        recording_id: String::new(),
        program: String::new(),
        args: Vec::new(),
        workdir: String::new(),
        recorder_id: String::new(),
        blocks: Default::default(),
        trailing: Vec::new(),
    }
}

impl TraceReader {
    fn open(mut ctfs: CtfsReader, assume_column_aware_paths: bool) -> Result<TraceReader, String> {
        let meta = if ctfs.list_files().iter().any(|f| f == "meta.dat") {
            let bytes = ctfs.read_file("meta.dat").map_err(|e| format!("meta.dat: {e}"))?;
            decode_meta_dat(&bytes).map_err(|e| format!("meta.dat present but not readable: {e}"))?
        } else {
            empty_meta()
        };
        let paths = Table::load(&mut ctfs, "paths")?.unwrap_or_default();
        let funcs = Table::load(&mut ctfs, "funcs")?.unwrap_or_default();
        let types = Table::load(&mut ctfs, "types")?.unwrap_or_default();
        let varnames = Table::load(&mut ctfs, "varnames")?.unwrap_or_default();
        let mut column_aware = meta.flags & FLAG_HAS_COLUMN_AWARE_STEPS != 0;
        let mut line_lengths = Vec::new();
        let mut line_count_payloads = Vec::new();
        let mut line_counts = Vec::new();
        if paths.count() > 0 {
            if column_aware || assume_column_aware_paths {
                for i in 0..paths.count() {
                    let raw = paths.read(i).map_err(|e| format!("paths.dat[{i}]: {e}"))?;
                    line_lengths.push(decode_layout_a(raw, false).map_err(|e| format!("paths.dat[{i}]: {e}"))?);
                }
                column_aware = true;
            } else if meta.flags & FLAG_HAS_LINE_COUNT_TABLE != 0 {
                for i in 0..paths.count() {
                    let raw = paths.read(i).map_err(|e| format!("paths.dat[{i}]: {e}"))?;
                    let (payload, count) = decode_line_count_record(raw).map_err(|e| format!("paths.dat[{i}]: {e}"))?;
                    line_count_payloads.push(payload);
                    line_counts.push(count);
                }
            }
        }
        if let Some(views) = Table::load(&mut ctfs, "srcviews").map_err(|e| format!("source_views.dat: {e}"))? {
            for i in 0..views.count() {
                let raw = views.read(i).map_err(|e| format!("source_views.dat[{i}]: {e}"))?;
                decode_source_view(raw).map_err(|e| format!("source_views.dat[{i}]: {e}"))?;
            }
        }
        let file_count = paths.count() as usize;
        let size = |i: usize| -> u64 {
            if column_aware {
                match line_lengths.get(i) {
                    Some(t) if !t.is_empty() => t.iter().map(|&l| u64::from(l)).sum::<u64>().max(1),
                    _ => CONVENTIONAL_FILE_SIZE,
                }
            } else {
                match line_counts.get(i) {
                    Some(&c) if c > 0 => c,
                    _ => DEFAULT_LINES_PER_FILE,
                }
            }
        };
        let space = PositionSpace::new((0..file_count).map(size));
        Ok(TraceReader {
            ctfs,
            assume_column_aware_paths,
            meta,
            column_aware,
            paths,
            funcs,
            types,
            varnames,
            line_lengths,
            line_count_payloads,
            space,
            column_space: None,
            exec: None,
            values: None,
            calls: None,
            events: None,
        })
    }

    fn path(&self, id: u64) -> Result<Vec<u8>, String> {
        if self.paths.count() > 0 && self.column_aware {
            let raw = self.paths.read(id)?;
            let mut pos = 0;
            let len = varint(raw, &mut pos).map_err(|e| format!("paths.dat[{id}]: {e}"))? as usize;
            if pos + len > raw.len() {
                return Err(format!("paths.dat[{id}]: path_bytes truncated"));
            }
            return Ok(raw[pos..pos + len].to_vec());
        }
        if self.paths.count() > 0 && self.meta.flags & FLAG_HAS_LINE_COUNT_TABLE != 0 {
            return self
                .line_count_payloads
                .get(id as usize)
                .cloned()
                .ok_or_else(|| format!("paths.dat[{id}]: out of range ({} record(s))", self.line_count_payloads.len()));
        }
        self.paths.read(id).map(<[u8]>::to_vec)
    }

    fn column_aware_paths_suspected(&self) -> bool {
        self.paths.count() > 0
            && !self.column_aware
            && self.meta.flags & FLAG_HAS_LINE_COUNT_TABLE == 0
            && (0..self.paths.count()).all(|i| self.paths.read(i).is_ok_and(|raw| decode_layout_a(raw, true).is_ok()))
    }

    fn line_length_raw(&self, file: u64, line0: u32) -> Option<u32> {
        let lls = self.line_lengths.get(file as usize)?;
        if lls.is_empty() {
            return (u64::from(line0) < DEFAULT_LINES_PER_FILE).then_some(CONVENTIONAL_LINE_LENGTH);
        }
        lls.get(line0 as usize).copied()
    }

    fn path_table_kind(&self, file: u64) -> i32 {
        if file >= self.paths.count() {
            return -1;
        }
        match self.line_lengths.get(file as usize) {
            Some(t) if t.is_empty() => 3,
            Some(_) => 2,
            None if self.meta.flags & FLAG_HAS_LINE_COUNT_TABLE != 0 => 1,
            None => 0,
        }
    }

    fn decode_column_position(&mut self, p: u64) -> Result<(u64, u64, u64), String> {
        if !self.column_aware {
            return Err("decodeGlobalPositionIndex requires a column-aware trace".to_string());
        }
        if self.column_space.is_none() {
            let mut line_bases = Vec::new();
            let mut file_bases = Vec::new();
            let mut file_sizes = Vec::new();
            let mut running = 0u64;
            for lls in &self.line_lengths {
                let mut sum = 0u64;
                line_bases.push(
                    lls.iter()
                        .map(|&l| {
                            let b = sum;
                            sum += u64::from(l);
                            b
                        })
                        .collect::<Vec<_>>(),
                );
                let size = if lls.is_empty() { CONVENTIONAL_FILE_SIZE } else { sum.max(1) };
                file_bases.push(running);
                file_sizes.push(size);
                running += size;
            }
            self.column_space = Some((line_bases, file_bases, file_sizes));
        }
        let (line_bases, file_bases, file_sizes) = self.column_space.as_ref().expect("built above");
        if file_bases.is_empty() {
            return Err("trace has no paths registered".to_string());
        }
        let n = file_bases.partition_point(|&b| b <= p);
        if n == 0 {
            return Err(format!("global_position_index {p} precedes the first file's base"));
        }
        let fid = n - 1;
        if p >= file_bases[fid] + file_sizes[fid] {
            return Err(format!("global_position_index {p} out of range for file {fid}"));
        }
        let q = p - file_bases[fid];
        let lb = &line_bases[fid];
        if lb.is_empty() {
            let width = u64::from(CONVENTIONAL_LINE_LENGTH);
            return Ok((fid as u64, q / width + 1, q % width + 1));
        }
        let l = lb.partition_point(|&b| b <= q);
        if l == 0 {
            return Err(format!("in-file offset {q} precedes the first line"));
        }
        Ok((fid as u64, l as u64, q - lb[l - 1] + 1))
    }

    fn exec(&mut self) -> Result<&mut ExecReader, String> {
        if self.exec.is_none() {
            let allow = self.meta.ext_flags & FLAG_EXT_HAS_SOURCE_RELOAD != 0;
            self.exec = Some(ExecReader::open(&mut self.ctfs, allow)?);
        }
        Ok(self.exec.as_mut().expect("opened above"))
    }

    fn values(&mut self) -> Result<&mut ValueStreamReader, String> {
        if self.values.is_none() {
            self.values = Some(ValueStreamReader::open(&mut self.ctfs)?.ok_or("values.dat: the container has no value stream")?);
        }
        Ok(self.values.as_mut().expect("opened above"))
    }

    fn calls(&mut self) -> Result<&mut CallStreamReader, String> {
        if self.calls.is_none() {
            self.calls = Some(CallStreamReader::open(&mut self.ctfs)?.ok_or("calls.dat: the container has no call stream")?);
        }
        Ok(self.calls.as_mut().expect("opened above"))
    }

    fn io_events(&mut self) -> Result<&mut FramedStream, String> {
        if self.events.is_none() {
            self.events = Some(FramedStream::open(&mut self.ctfs, "events")?);
        }
        Ok(self.events.as_mut().expect("opened above"))
    }

    fn io_event(&mut self, n: u64) -> Result<IoEventRecord, String> {
        IoEventRecord::decode(&self.io_events()?.record(n)?)
    }

    /// Step `n`'s values: `(varname id, type id, CBOR)`.
    fn step_values(&mut self, n: u64) -> Result<Vec<(u64, u64, Vec<u8>)>, String> {
        let values = self.values()?;
        if n >= values.count() {
            return Err(format!("value step index {n} out of range (count {})", values.count()));
        }
        let entry = values.read(n)?;
        let mut out = Vec::new();
        for ev in entry.events {
            if let ValueStreamEvent::StepValues { values } = ev {
                for (name, data) in values {
                    let tid = cbor_top_level_type_id(&data);
                    out.push((name, tid, data));
                }
            }
        }
        Ok(out)
    }

    fn call(&mut self, key: u64) -> Result<CallStreamRecord, String> {
        self.calls()?.read(key)
    }

    fn call_for_step(&mut self, step: u64) -> Result<CallStreamRecord, String> {
        let total = self.calls()?.count();
        if total == 0 {
            return Err("no call records".to_string());
        }
        let (mut lo, mut hi) = (0u64, total - 1);
        let mut k: Option<u64> = None;
        while lo <= hi {
            let mid = lo + (hi - lo) / 2;
            if self.call(mid)?.first_step_id <= step {
                k = Some(mid);
                lo = mid + 1;
            } else {
                if mid == 0 {
                    break;
                }
                hi = mid - 1;
            }
        }
        let Some(mut i) = k else {
            return Err(format!("step {step} not found in any call"));
        };
        loop {
            let c = self.call(i)?;
            if c.last_step_id >= step {
                return Ok(c);
            }
            if i == 0 {
                break;
            }
            i -= 1;
        }
        Err(format!("step {step} not found in any call"))
    }

    /// Positions of steps `[start, start + count)`, as many as exist.
    fn positions(&mut self, start: u64, count: u64) -> Result<Vec<u64>, String> {
        let exec = self.exec()?;
        if count == 0 || start >= exec.total {
            return Ok(vec![]);
        }
        let end = start.saturating_add(count).min(exec.total);
        (start..end).map(|n| exec.position(n)).collect()
    }
}

/// The `type_id` of a top-level `ValueRecord` CBOR map, or 0.
fn cbor_top_level_type_id(data: &[u8]) -> u64 {
    fn head(data: &[u8], pos: &mut usize) -> Option<(u8, u64)> {
        let b = *data.get(*pos)?;
        *pos += 1;
        let (major, info) = (b >> 5, b & 0x1f);
        let arg = match info {
            0..=23 => u64::from(info),
            24..=27 => {
                let n = 1usize << (info - 24);
                let bytes = data.get(*pos..*pos + n)?;
                *pos += n;
                bytes.iter().fold(0u64, |a, &x| (a << 8) | u64::from(x))
            }
            _ => return None,
        };
        Some((major, arg))
    }
    fn skip(data: &[u8], pos: &mut usize, depth: u32) -> Option<()> {
        if depth > 64 {
            return None;
        }
        let (major, arg) = head(data, pos)?;
        match major {
            0 | 1 | 7 => Some(()),
            2 | 3 => {
                if arg > (data.len() - *pos) as u64 {
                    return None;
                }
                *pos += arg as usize;
                Some(())
            }
            4 => (0..arg).try_for_each(|_| skip(data, pos, depth + 1)),
            5 => (0..arg * 2).try_for_each(|_| skip(data, pos, depth + 1)),
            6 => skip(data, pos, depth + 1),
            _ => None,
        }
    }
    let mut pos = 0;
    let Some((5, entries)) = head(data, &mut pos) else {
        return 0;
    };
    for _ in 0..entries {
        let Some((3, len)) = head(data, &mut pos) else {
            return 0;
        };
        if len > (data.len() - pos) as u64 {
            return 0;
        }
        let is_type_id = &data[pos..pos + len as usize] == b"type_id";
        pos += len as usize;
        if is_type_id {
            return match head(data, &mut pos) {
                Some((0, v)) => v,
                _ => 0,
            };
        }
        if skip(data, &mut pos, 0).is_none() {
            return 0;
        }
    }
    0
}

// ---------------------------------------------------------------------------
// JSON
// ---------------------------------------------------------------------------

fn bytes_json(data: &[u8]) -> String {
    let parts: Vec<String> = data.iter().map(|b| b.to_string()).collect();
    format!("[{}]", parts.join(","))
}

fn step_json(ev: &StepEvent) -> String {
    match ev {
        StepEvent::AbsoluteStep { global_position_index } => format!("{{\"kind\":\"absolute_step\",\"global_line_index\":{global_position_index}}}"),
        StepEvent::DeltaStep { delta } => format!("{{\"kind\":\"delta_step\",\"line_delta\":{delta}}}"),
        StepEvent::Raise { exception_type_id, message } => {
            format!(
                "{{\"kind\":\"raise\",\"exception_type_id\":{exception_type_id},\"message\":{}}}",
                bytes_json(message)
            )
        }
        StepEvent::Catch { exception_type_id } => format!("{{\"kind\":\"catch\",\"exception_type_id\":{exception_type_id}}}"),
        StepEvent::ThreadSwitch { thread_id } => format!("{{\"kind\":\"thread_switch\",\"thread_id\":{thread_id}}}"),
        StepEvent::ThreadStart { thread_id } => format!("{{\"kind\":\"thread_start\",\"thread_id\":{thread_id}}}"),
        StepEvent::ThreadExit { thread_id } => format!("{{\"kind\":\"thread_exit\",\"thread_id\":{thread_id}}}"),
        StepEvent::DeltaColumn { column_delta } => format!("{{\"kind\":\"delta_column\",\"column_delta\":{column_delta}}}"),
        StepEvent::SourceReload {
            reload_ordinal,
            changed,
            in_flight_frames,
        } => {
            let changed: Vec<String> = changed
                .iter()
                .map(|c| {
                    format!(
                        "{{\"old_path_id\":{},\"new_path_id\":{},\"generation\":{}}}",
                        c.old_path_id, c.new_path_id, c.generation
                    )
                })
                .collect();
            format!(
                "{{\"kind\":\"source_reload\",\"reload_ordinal\":{reload_ordinal},\"changed\":[{}],\"in_flight_frames\":{in_flight_frames}}}",
                changed.join(",")
            )
        }
    }
}

fn values_json(values: &[(u64, u64, Vec<u8>)]) -> String {
    let parts: Vec<String> = values
        .iter()
        .map(|(name, tid, data)| format!("{{\"varname_id\":{name},\"type_id\":{tid},\"data\":{}}}", bytes_json(data)))
        .collect();
    format!("[{}]", parts.join(","))
}

fn call_json(c: &CallStreamRecord) -> String {
    let args: Vec<String> = c
        .args
        .iter()
        .map(|a| format!("{{\"varname_id\":{},\"value\":{}}}", a.varname_id, bytes_json(&a.value)))
        .collect();
    let children: Vec<String> = c.children.iter().map(u64::to_string).collect();
    format!(
        "{{\"function_id\":{},\"parent_call_key\":{},\"entry_step\":{},\"exit_step\":{},\"depth\":{},\"args\":[{}],\"return_value\":{},\"exception\":{},\"children\":[{}]}}",
        c.function_id,
        c.parent_key,
        c.first_step_id,
        c.last_step_id,
        c.depth,
        args.join(","),
        bytes_json(&c.return_value),
        bytes_json(&c.raised_exception),
        children.join(",")
    )
}

const EVENT_KIND_NAMES: [&str; 14] = [
    "Write",
    "WriteFile",
    "WriteOther",
    "Read",
    "ReadFile",
    "ReadOther",
    "ReadDir",
    "OpenDir",
    "CloseDir",
    "Socket",
    "Open",
    "Error",
    "TraceLogEvent",
    "EvmEvent",
];

// ---------------------------------------------------------------------------
// Handing results out
// ---------------------------------------------------------------------------

/// A JSON document in a fresh buffer; NULL for an empty one.
unsafe fn json_result(s: String, out_len: *mut usize) -> *mut u8 {
    unsafe { *out_len = s.len() };
    if s.is_empty() {
        return std::ptr::null_mut();
    }
    alloc_buffer(s.as_bytes())
}

/// A name in a fresh buffer, non-NULL even when empty.
unsafe fn string_result(s: &[u8], out_len: *mut usize) -> *mut u8 {
    unsafe { *out_len = s.len() };
    let p = alloc_buffer(s);
    if p.is_null() {
        set_error(&format!("out of memory for {} bytes", s.len()));
    }
    p
}

/// `data` copied into `*out_data`, or NULL with length 0 when empty.
unsafe fn bytes_out(data: &[u8], out_data: *mut *mut u8, out_len: *mut usize) -> i32 {
    unsafe {
        if data.is_empty() {
            *out_data = std::ptr::null_mut();
            *out_len = 0;
            return 0;
        }
        let p = alloc_buffer(data);
        if p.is_null() {
            set_error("allocation failed");
            return 1;
        }
        *out_data = p;
        *out_len = data.len();
    }
    0
}

/// Run `body` on a non-NULL reader.
fn on_reader<R>(name: &str, h: *mut TraceReader, null_value: R, fail: R, body: impl FnOnce(&mut TraceReader) -> R) -> R {
    guarded(name, None, fail, || {
        if h.is_null() {
            return null_value;
        }
        // SAFETY: a non-NULL reader handle is one an open call returned and
        // `ct_reader_close` has not freed (the handle invariant).
        body(unsafe { &mut *h })
    })
}

fn report<T>(r: Result<T, String>) -> Option<T> {
    match r {
        Ok(v) => Some(v),
        Err(e) => {
            set_error(&e);
            None
        }
    }
}

fn open_path(path: *const c_char, assume: bool) -> *mut TraceReader {
    if path.is_null() {
        set_error("NULL path");
        return std::ptr::null_mut();
    }
    let p: PathBuf = path_from_bytes(unsafe { cstr_bytes(path) });
    if !p.is_file() {
        set_error(&format!("file not found: {}", p.display()));
        return std::ptr::null_mut();
    }
    let opened = CtfsReader::open(&p)
        .map_err(|e| e.to_string())
        .and_then(|ctfs| TraceReader::open(ctfs, assume));
    match report(opened) {
        Some(r) => Box::into_raw(Box::new(r)),
        None => std::ptr::null_mut(),
    }
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/// # Safety
/// `path` is NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_open(path: *const c_char) -> *mut TraceReader {
    guarded("ct_reader_open", None, std::ptr::null_mut(), || open_path(path, false))
}

/// # Safety
/// `path` is NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_open_assume_column_aware_paths(path: *const c_char) -> *mut TraceReader {
    guarded("ct_reader_open_assume_column_aware_paths", None, std::ptr::null_mut(), || {
        open_path(path, true)
    })
}

/// # Safety
/// `(data, len)` is readable or NULL/0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_open_bytes(data: *const u8, len: usize) -> *mut TraceReader {
    guarded("ct_reader_open_bytes", None, std::ptr::null_mut(), || {
        if data.is_null() && len > 0 {
            set_error("NULL data with a non-zero length");
            return std::ptr::null_mut();
        }
        let bytes = unsafe { crate::bytes(data, len) }.to_vec();
        let opened = CtfsReader::from_bytes(bytes)
            .map_err(|e| e.to_string())
            .and_then(|ctfs| TraceReader::open(ctfs, false));
        match report(opened) {
            Some(r) => Box::into_raw(Box::new(r)),
            None => std::ptr::null_mut(),
        }
    })
}

/// Re-read the container at `path` into `h`. On failure `h` is unchanged.
///
/// # Safety
/// `h` is NULL or a live reader; `path` is NULL or NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_refresh(h: *mut TraceReader, path: *const c_char) -> i32 {
    guarded("ct_reader_refresh", None, 1, || {
        if h.is_null() {
            set_error("NULL reader handle");
            return 1;
        }
        let r = unsafe { &mut *h };
        let fresh = open_path(path, r.assume_column_aware_paths);
        if fresh.is_null() {
            return 1;
        }
        *r = *unsafe { Box::from_raw(fresh) };
        0
    })
}

/// # Safety
/// `h` is NULL or a live reader, not used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_close(h: *mut TraceReader) {
    guarded("ct_reader_close", None, (), || {
        if !h.is_null() {
            drop(unsafe { Box::from_raw(h) });
        }
    })
}

// ---------------------------------------------------------------------------
// Counts and names
// ---------------------------------------------------------------------------

macro_rules! count_fn {
    ($name:ident, $body:expr) => {
        /// # Safety
        /// `h` is NULL or a live reader.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $name(h: *mut TraceReader) -> u64 {
            on_reader(stringify!($name), h, 0, 0, |r| {
                let f: fn(&mut TraceReader) -> Result<u64, String> = $body;
                report(f(r)).unwrap_or(0)
            })
        }
    };
}

count_fn!(ct_reader_step_count, |r| Ok(r.exec()?.total));
count_fn!(ct_reader_call_count, |r| Ok(r.calls()?.count()));
count_fn!(ct_reader_event_count, |r| Ok(r.io_events()?.total));
count_fn!(ct_reader_path_count, |r| Ok(r.paths.count()));
count_fn!(ct_reader_function_count, |r| Ok(r.funcs.count()));
count_fn!(ct_reader_type_count, |r| Ok(r.types.count()));
count_fn!(ct_reader_varname_count, |r| Ok(r.varnames.count()));

fn name_fn(name: &str, h: *mut TraceReader, out_len: *mut usize, get: impl FnOnce(&mut TraceReader) -> Result<Vec<u8>, String>) -> *mut u8 {
    guarded(name, None, std::ptr::null_mut(), || {
        if h.is_null() || out_len.is_null() {
            set_error("a reader handle and an outLen are required");
            return std::ptr::null_mut();
        }
        match report(get(unsafe { &mut *h })) {
            Some(s) => unsafe { string_result(&s, out_len) },
            None => std::ptr::null_mut(),
        }
    })
}

/// # Safety
/// `h` is NULL or a live reader; `out_len` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_path(h: *mut TraceReader, id: u64, out_len: *mut usize) -> *mut u8 {
    name_fn("ct_reader_path", h, out_len, |r| r.path(id))
}

/// # Safety
/// `h` is NULL or a live reader; `out_len` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_function(h: *mut TraceReader, id: u64, out_len: *mut usize) -> *mut u8 {
    name_fn("ct_reader_function", h, out_len, |r| {
        r.funcs.read(id).and_then(decode_func_record).map(|(_, n)| n)
    })
}

/// # Safety
/// `h` is NULL or a live reader; `out_len` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_type_name(h: *mut TraceReader, id: u64, out_len: *mut usize) -> *mut u8 {
    name_fn("ct_reader_type_name", h, out_len, |r| r.types.read(id).and_then(decode_type_record))
}

/// # Safety
/// `h` is NULL or a live reader; `out_len` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_varname(h: *mut TraceReader, id: u64, out_len: *mut usize) -> *mut u8 {
    name_fn("ct_reader_varname", h, out_len, |r| r.varnames.read(id).map(<[u8]>::to_vec))
}

/// # Safety
/// `h` is NULL or a live reader; `out_len` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_program(h: *mut TraceReader, out_len: *mut usize) -> *mut u8 {
    name_fn("ct_reader_program", h, out_len, |r| Ok(r.meta.program.clone().into_bytes()))
}

/// # Safety
/// `h` is NULL or a live reader; `out_len` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_workdir(h: *mut TraceReader, out_len: *mut usize) -> *mut u8 {
    name_fn("ct_reader_workdir", h, out_len, |r| Ok(r.meta.workdir.clone().into_bytes()))
}

// ---------------------------------------------------------------------------
// Records as JSON
// ---------------------------------------------------------------------------

fn json_fn(name: &str, h: *mut TraceReader, out_len: *mut usize, get: impl FnOnce(&mut TraceReader) -> Result<String, String>) -> *mut u8 {
    guarded(name, None, std::ptr::null_mut(), || {
        if h.is_null() || out_len.is_null() {
            set_error("a reader handle and an outLen are required");
            return std::ptr::null_mut();
        }
        match report(get(unsafe { &mut *h })) {
            Some(s) => unsafe { json_result(s, out_len) },
            None => std::ptr::null_mut(),
        }
    })
}

/// # Safety
/// `h` is NULL or a live reader; `out_len` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_step(h: *mut TraceReader, n: u64, out_len: *mut usize) -> *mut u8 {
    json_fn("ct_reader_step", h, out_len, |r| Ok(step_json(&r.exec()?.event(n)?)))
}

/// # Safety
/// `h` is NULL or a live reader; `out_len` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_values(h: *mut TraceReader, n: u64, out_len: *mut usize) -> *mut u8 {
    json_fn("ct_reader_values", h, out_len, |r| Ok(values_json(&r.step_values(n)?)))
}

/// # Safety
/// `h` is NULL or a live reader; `out_len` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_call(h: *mut TraceReader, key: u64, out_len: *mut usize) -> *mut u8 {
    json_fn("ct_reader_call", h, out_len, |r| Ok(call_json(&r.call(key)?)))
}

/// # Safety
/// `h` is NULL or a live reader; `out_len` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_call_for_step(h: *mut TraceReader, step_id: u64, out_len: *mut usize) -> *mut u8 {
    json_fn("ct_reader_call_for_step", h, out_len, |r| Ok(call_json(&r.call_for_step(step_id)?)))
}

/// # Safety
/// `h` is NULL or a live reader; `out_len` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_event(h: *mut TraceReader, index: u64, out_len: *mut usize) -> *mut u8 {
    json_fn("ct_reader_event", h, out_len, |r| {
        let ev = r.io_event(index)?;
        let kind = EVENT_KIND_NAMES
            .get(ev.kind as usize)
            .ok_or_else(|| format!("event kind {} is not an assigned EventLogKind (0-13)", ev.kind))?;
        Ok(format!(
            "{{\"kind\":\"{kind}\",\"step_id\":{},\"data\":{}}}",
            ev.step_id,
            bytes_json(&ev.content)
        ))
    })
}

// ---------------------------------------------------------------------------
// Step locations
// ---------------------------------------------------------------------------

/// # Safety
/// `h` is NULL or a live reader; the out pointers are NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_step_location(h: *mut TraceReader, n: u64, out_path_id: *mut u64, out_line: *mut u64) -> i32 {
    guarded("ct_reader_step_location", None, 1, || {
        if h.is_null() || out_path_id.is_null() || out_line.is_null() {
            set_error("NULL parameter");
            return 1;
        }
        let r = unsafe { &mut *h };
        let Some(p) = report(r.exec().and_then(|e| e.position(n))) else {
            return 1;
        };
        let Some((file, line)) = report(r.space.resolve(p).map_err(|e| format!("step {n}: {e}"))) else {
            return 1;
        };
        unsafe {
            *out_path_id = file;
            *out_line = line;
        }
        0
    })
}

fn locations(
    name: &str,
    h: *mut TraceReader,
    start: u64,
    count: u64,
    outs: [*mut u64; 3],
    nulls: bool,
    fill: impl FnOnce(&mut TraceReader, &[u64], [*mut u64; 3]) -> Result<(), String>,
) -> u64 {
    guarded(name, None, u64::MAX, || {
        if h.is_null() || nulls {
            set_error("NULL parameter");
            return u64::MAX;
        }
        if count == 0 {
            return 0;
        }
        let r = unsafe { &mut *h };
        let Some(positions) = report(r.positions(start, count)) else {
            return u64::MAX;
        };
        if report(fill(r, &positions, outs)).is_none() {
            return u64::MAX;
        }
        positions.len() as u64
    })
}

/// # Safety
/// `h` is NULL or a live reader; the out buffers hold `count` entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_step_locations(h: *mut TraceReader, start_n: u64, count: u64, out_path_ids: *mut u64, out_lines: *mut u64) -> u64 {
    let nulls = out_path_ids.is_null() || out_lines.is_null();
    locations(
        "ct_reader_step_locations",
        h,
        start_n,
        count,
        [out_path_ids, out_lines, std::ptr::null_mut()],
        nulls,
        |r, positions, [paths, lines, _]| {
            for (i, &p) in positions.iter().enumerate() {
                let (file, line) = r.space.resolve(p).map_err(|e| format!("step {}: {e}", start_n + i as u64))?;
                unsafe {
                    *paths.add(i) = file;
                    *lines.add(i) = line;
                }
            }
            Ok(())
        },
    )
}

/// # Safety
/// `h` is NULL or a live reader; the out buffers hold `count` entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_step_locations_with_columns(
    h: *mut TraceReader,
    start_n: u64,
    count: u64,
    out_path_ids: *mut u64,
    out_lines: *mut u64,
    out_columns: *mut u64,
) -> u64 {
    let nulls = out_path_ids.is_null() || out_lines.is_null() || out_columns.is_null();
    locations(
        "ct_reader_step_locations_with_columns",
        h,
        start_n,
        count,
        [out_path_ids, out_lines, out_columns],
        nulls,
        |r, positions, [paths, lines, columns]| {
            let column_aware = r.meta.flags & FLAG_HAS_COLUMN_AWARE_STEPS != 0;
            for (i, &p) in positions.iter().enumerate() {
                let (file, line, column) = match (column_aware, column_aware.then(|| r.decode_column_position(p))) {
                    (true, Some(Ok(pos))) => pos,
                    (true, _) => {
                        let (f, l) = r.space.resolve(p).map_err(|e| format!("step {}: {e}", start_n + i as u64))?;
                        (f, l, 1)
                    }
                    (false, _) => {
                        let (f, l) = r.space.resolve(p).map_err(|e| format!("step {}: {e}", start_n + i as u64))?;
                        (f, l, 0)
                    }
                };
                unsafe {
                    *paths.add(i) = file;
                    *lines.add(i) = line;
                    *columns.add(i) = column;
                }
            }
            Ok(())
        },
    )
}

/// # Safety
/// `h` is NULL or a live reader; `out_glis` holds `count` entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_step_global_line_indices(h: *mut TraceReader, start_n: u64, count: u64, out_glis: *mut u64) -> u64 {
    locations(
        "ct_reader_step_global_line_indices",
        h,
        start_n,
        count,
        [out_glis, std::ptr::null_mut(), std::ptr::null_mut()],
        out_glis.is_null(),
        |_, positions, [out, _, _]| {
            for (i, &p) in positions.iter().enumerate() {
                unsafe { *out.add(i) = p };
            }
            Ok(())
        },
    )
}

// ---------------------------------------------------------------------------
// paths.dat size tables and meta.dat flags
// ---------------------------------------------------------------------------

/// # Safety
/// `h` is NULL or a live reader; `out_value` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_line_length(h: *mut TraceReader, file_id: u64, line_index0: u32, out_value: *mut u32) -> i32 {
    on_reader("ct_reader_line_length", h, 1, 1, |r| {
        if out_value.is_null() || r.meta.flags & FLAG_HAS_COLUMN_AWARE_STEPS == 0 && !r.column_aware {
            return 1;
        }
        match r.line_length_raw(file_id, line_index0) {
            Some(v) => {
                unsafe { *out_value = v };
                0
            }
            None => 1,
        }
    })
}

/// # Safety
/// `h` is NULL or a live reader; `out_value` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_line_length_raw(h: *mut TraceReader, file_id: u64, line_index0: u32, out_value: *mut u32) -> i32 {
    on_reader("ct_reader_line_length_raw", h, 1, 1, |r| {
        if out_value.is_null() {
            return 1;
        }
        match r.line_length_raw(file_id, line_index0) {
            Some(v) => {
                unsafe { *out_value = v };
                0
            }
            None => 1,
        }
    })
}

/// # Safety
/// `h` is NULL or a live reader.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_line_count_raw(h: *mut TraceReader, file_id: u64) -> u64 {
    on_reader("ct_reader_line_count_raw", h, 0, 0, |r| match r.line_lengths.get(file_id as usize) {
        Some(t) if t.is_empty() => DEFAULT_LINES_PER_FILE,
        Some(t) => t.len() as u64,
        None => 0,
    })
}

/// # Safety
/// `h` is NULL or a live reader.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_path_table_kind(h: *mut TraceReader, file_id: u64) -> i32 {
    on_reader("ct_reader_path_table_kind", h, -1, -1, |r| r.path_table_kind(file_id))
}

fn flag_fn(name: &str, h: *mut TraceReader, get: impl FnOnce(&TraceReader) -> bool) -> i32 {
    on_reader(name, h, -1, -1, |r| i32::from(get(r)))
}

/// # Safety
/// `h` is NULL or a live reader.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_has_column_aware_steps(h: *mut TraceReader) -> i32 {
    flag_fn("ct_reader_has_column_aware_steps", h, |r| r.column_aware)
}

/// # Safety
/// `h` is NULL or a live reader.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_column_aware_paths_suspected(h: *mut TraceReader) -> i32 {
    flag_fn("ct_reader_column_aware_paths_suspected", h, TraceReader::column_aware_paths_suspected)
}

/// # Safety
/// `h` is NULL or a live reader.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_supports_column_breakpoints(h: *mut TraceReader) -> i32 {
    flag_fn("ct_reader_supports_column_breakpoints", h, |r| {
        r.meta.flags & FLAG_SUPPORTS_COLUMN_BREAKPOINTS != 0
    })
}

/// # Safety
/// `h` is NULL or a live reader.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_supports_column_motions(h: *mut TraceReader) -> i32 {
    flag_fn("ct_reader_supports_column_motions", h, |r| {
        r.meta.flags & FLAG_SUPPORTS_COLUMN_MOTIONS != 0
    })
}

// ---------------------------------------------------------------------------
// Structured values, calls and events
// ---------------------------------------------------------------------------

/// # Safety
/// `h` is NULL or a live reader.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_step_value_count(h: *mut TraceReader, n: u64) -> u64 {
    on_reader("ct_reader_step_value_count", h, 0, 0, |r| {
        report(r.step_values(n)).map_or(0, |v| v.len() as u64)
    })
}

/// # Safety
/// `h` is NULL or a live reader; the out pointers are NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_step_value(
    h: *mut TraceReader,
    n: u64,
    value_idx: u64,
    out_varname_id: *mut u64,
    out_type_id: *mut u64,
    out_data: *mut *mut u8,
    out_data_len: *mut usize,
) -> i32 {
    guarded("ct_reader_step_value", None, 1, || {
        if h.is_null() || out_varname_id.is_null() || out_type_id.is_null() || out_data.is_null() || out_data_len.is_null() {
            set_error("NULL parameter");
            return 1;
        }
        let Some(values) = report(unsafe { &mut *h }.step_values(n)) else {
            return 1;
        };
        let Some((name, tid, data)) = values.get(value_idx as usize) else {
            set_error(&format!("value index {value_idx} out of range (count={})", values.len()));
            return 1;
        };
        unsafe {
            *out_varname_id = *name;
            *out_type_id = *tid;
            bytes_out(data, out_data, out_data_len)
        }
    })
}

/// # Safety
/// `h` is NULL or a live reader; the out pointers are NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_call_fields(
    h: *mut TraceReader,
    key: u64,
    out_function_id: *mut u64,
    out_parent_key: *mut i64,
    out_entry_step: *mut u64,
    out_exit_step: *mut u64,
    out_depth: *mut u32,
    out_children_count: *mut u64,
) -> i32 {
    guarded("ct_reader_call_fields", None, 1, || {
        if h.is_null()
            || out_function_id.is_null()
            || out_parent_key.is_null()
            || out_entry_step.is_null()
            || out_exit_step.is_null()
            || out_depth.is_null()
            || out_children_count.is_null()
        {
            set_error("NULL parameter");
            return 1;
        }
        let Some(c) = report(unsafe { &mut *h }.call(key)) else {
            return 1;
        };
        unsafe {
            *out_function_id = c.function_id;
            *out_parent_key = c.parent_key;
            *out_entry_step = c.first_step_id;
            *out_exit_step = c.last_step_id;
            *out_depth = c.depth as u32;
            *out_children_count = c.children.len() as u64;
        }
        0
    })
}

/// # Safety
/// `h` is NULL or a live reader.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_call_child(h: *mut TraceReader, key: u64, child_idx: u64) -> u64 {
    on_reader("ct_reader_call_child", h, u64::MAX, u64::MAX, |r| {
        let Some(c) = report(r.call(key)) else {
            return u64::MAX;
        };
        match c.children.get(child_idx as usize) {
            Some(&k) => k,
            None => {
                set_error(&format!("child index {child_idx} out of range"));
                u64::MAX
            }
        }
    })
}

/// # Safety
/// `h` is NULL or a live reader.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_call_arg_count(h: *mut TraceReader, key: u64) -> u64 {
    guarded("ct_reader_call_arg_count", None, 0, || {
        if h.is_null() {
            set_error("NULL handle");
            return 0;
        }
        report(unsafe { &mut *h }.call(key)).map_or(0, |c| c.args.len() as u64)
    })
}

/// # Safety
/// `h` is NULL or a live reader; the out pointers are NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_call_arg(
    h: *mut TraceReader,
    key: u64,
    arg_idx: u64,
    out_varname_id: *mut u64,
    out_data: *mut *mut u8,
    out_data_len: *mut usize,
) -> i32 {
    guarded("ct_reader_call_arg", None, 1, || {
        if h.is_null() || out_varname_id.is_null() || out_data.is_null() || out_data_len.is_null() {
            set_error("NULL parameter");
            return 1;
        }
        let Some(c) = report(unsafe { &mut *h }.call(key)) else {
            return 1;
        };
        let Some(arg) = c.args.get(arg_idx as usize) else {
            set_error(&format!("arg index {arg_idx} out of range"));
            return 1;
        };
        unsafe {
            *out_varname_id = arg.varname_id;
            bytes_out(&arg.value, out_data, out_data_len)
        }
    })
}

/// # Safety
/// `h` is NULL or a live reader; the out pointers are NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_event_fields(
    h: *mut TraceReader,
    index: u64,
    out_kind: *mut u8,
    out_step_id: *mut u64,
    out_data: *mut *mut u8,
    out_data_len: *mut usize,
) -> i32 {
    guarded("ct_reader_event_fields", None, 1, || {
        if h.is_null() || out_kind.is_null() || out_step_id.is_null() || out_data.is_null() || out_data_len.is_null() {
            set_error("NULL parameter");
            return 1;
        }
        let Some(ev) = report(unsafe { &mut *h }.io_event(index)) else {
            return 1;
        };
        unsafe {
            *out_kind = ev.kind;
            *out_step_id = ev.step_id;
            bytes_out(&ev.content, out_data, out_data_len)
        }
    })
}

/// # Safety
/// `h` is NULL or a live reader; the out pointers are NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_reader_event_metadata(h: *mut TraceReader, index: u64, out_data: *mut *mut u8, out_data_len: *mut usize) -> i32 {
    guarded("ct_reader_event_metadata", None, 1, || {
        if h.is_null() || out_data.is_null() || out_data_len.is_null() {
            set_error("NULL parameter");
            return 1;
        }
        let Some(ev) = report(unsafe { &mut *h }.io_event(index)) else {
            return 1;
        };
        unsafe { bytes_out(&ev.metadata, out_data, out_data_len) }
    })
}
