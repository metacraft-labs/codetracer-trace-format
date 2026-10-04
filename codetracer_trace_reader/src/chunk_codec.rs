//! The one zstd decode every stream reader in this crate goes through.
//!
//! It is `codetracer_ctfs::zstd_compat`, so the codec follows that crate's
//! `c-zstd` / `pure-rust-zstd` features on every target: the reader decodes
//! with the same library the writer encodes with, natively and on wasm32.
//! Choosing the decoder by target instead made a wasm32 build that wrote with
//! C libzstd read back with `ruzstd`.

/// Inflate one chunk of `stream`, naming the stream in the refusal.
pub(crate) fn inflate(stream: &str, compressed: &[u8]) -> Result<Vec<u8>, String> {
    codetracer_ctfs::zstd_compat::decode_all(compressed).map_err(|e| format!("{stream}: zstd decode failed: {e}"))
}

/// How a stream's chunks are stored in the container a reader was opened on.
///
/// A full-profile container stores each chunk as one zstd frame; a compact
/// one stores it as its content, with every offset that located a frame
/// locating that content instead (`ctfs-container.md` §1f). The profile the
/// container declares decides which: a compact chunk is never inflated, and
/// is not inspected for a zstd magic number, since its content may begin with
/// those four bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ChunkForm {
    /// One zstd frame per chunk: the full profile.
    #[default]
    Framed,
    /// The chunk's content as it is: the compact profile.
    Stored,
}

impl ChunkForm {
    /// The form of every chunk in the container `reader` holds.
    pub fn of(reader: &codetracer_ctfs::CtfsReader) -> ChunkForm {
        match reader.profile() {
            codetracer_ctfs::compact::Profile::Full => ChunkForm::Framed,
            codetracer_ctfs::compact::Profile::Compact => ChunkForm::Stored,
        }
    }

    /// Put the content of `chunk` in `out`, replacing its contents: the
    /// frame's inflation, or the chunk's bytes as they are.
    pub(crate) fn content_into(self, chunk: &[u8], out: &mut Vec<u8>) -> std::io::Result<()> {
        match self {
            ChunkForm::Framed => codetracer_ctfs::zstd_compat::decode_into(chunk, out),
            ChunkForm::Stored => {
                out.clear();
                out.extend_from_slice(chunk);
                Ok(())
            }
        }
    }

    /// The content of `chunk`, borrowed when it is stored as it is.
    pub(crate) fn content<'a>(self, stream: &str, chunk: &'a [u8]) -> Result<std::borrow::Cow<'a, [u8]>, String> {
        match self {
            ChunkForm::Framed => inflate(stream, chunk).map(std::borrow::Cow::Owned),
            ChunkForm::Stored => Ok(std::borrow::Cow::Borrowed(chunk)),
        }
    }
}
