//! A Chunked Compressed Table written as its records arrive
//! (`ctfs-container.md` §7; `internal-files.md` §"Chunking and compression of
//! the runtime streams").
//!
//! Records are framed by their length (`record_len: varint`, then the record)
//! and gathered into chunks of `chunk_size`; a chunk is sealed — compressed
//! one-shot into a frame that declares its size, and its offset appended to
//! the index — when it reaches `chunk_size` records, or at [`ChunkSink::finish`].
//! The bytes sealed since the last [`ChunkSink::take`] are handed out by it, so
//! a writer can publish each sealed chunk as it seals (§6, "Durability").
//!
//! `calls.dat`, `values.dat` and `events.dat` are all written through this.

/// One `.dat` + `.idx` pair, written as its records arrive.
pub struct ChunkSink {
    name: &'static str,
    chunk_size: usize,
    zstd_level: i32,
    raw: Vec<u8>,
    in_chunk: usize,
    dat_len: u64,
    new_dat: Vec<u8>,
    new_idx: Vec<u8>,
    records: u64,
}

fn put_varint(mut v: u64, out: &mut Vec<u8>) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

impl ChunkSink {
    /// A sink for the stream `name` (`"calls.dat"`, …). The index's
    /// `chunk_size` header is the first thing [`take`](Self::take) returns.
    pub fn new(name: &'static str, chunk_size: usize, zstd_level: i32) -> Self {
        let chunk_size = chunk_size.max(1);
        ChunkSink {
            name,
            chunk_size,
            zstd_level,
            raw: Vec::new(),
            in_chunk: 0,
            dat_len: 0,
            new_dat: Vec::new(),
            new_idx: (chunk_size as u32).to_le_bytes().to_vec(),
            records: 0,
        }
    }

    /// Append one record, sealing the chunk if it is now full.
    pub fn push(&mut self, record: &[u8]) -> Result<(), String> {
        put_varint(record.len() as u64, &mut self.raw);
        self.raw.extend_from_slice(record);
        self.in_chunk += 1;
        self.records += 1;
        if self.in_chunk == self.chunk_size {
            self.seal()?;
        }
        Ok(())
    }

    fn seal(&mut self) -> Result<(), String> {
        if self.in_chunk == 0 {
            return Ok(());
        }
        let frame = codetracer_ctfs::compress_pledged(&self.raw, self.zstd_level, self.name)?;
        self.new_idx.extend_from_slice(&self.dat_len.to_le_bytes());
        self.dat_len += frame.len() as u64;
        self.new_dat.extend_from_slice(&frame);
        self.raw.clear();
        self.in_chunk = 0;
        Ok(())
    }

    /// Seal the trailing partial chunk.
    pub fn finish(&mut self) -> Result<(), String> {
        self.seal()
    }

    /// Whether bytes were sealed since the last [`take`](Self::take).
    pub fn has_new(&self) -> bool {
        !self.new_dat.is_empty() || !self.new_idx.is_empty()
    }

    /// The `.dat` and `.idx` bytes sealed since the last call.
    pub fn take(&mut self) -> (Vec<u8>, Vec<u8>) {
        (std::mem::take(&mut self.new_dat), std::mem::take(&mut self.new_idx))
    }

    /// Records pushed so far.
    pub fn record_count(&self) -> u64 {
        self.records
    }
}

/// Encode `records` as one whole `.dat` + `.idx` pair.
pub fn encode_table<'a>(
    name: &'static str,
    records: impl IntoIterator<Item = &'a [u8]>,
    chunk_size: usize,
    zstd_level: i32,
) -> Result<(Vec<u8>, Vec<u8>), String> {
    let mut sink = ChunkSink::new(name, chunk_size, zstd_level);
    for r in records {
        sink.push(r)?;
    }
    sink.finish()?;
    Ok(sink.take())
}
