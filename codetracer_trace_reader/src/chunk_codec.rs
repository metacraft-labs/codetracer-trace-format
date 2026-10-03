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
