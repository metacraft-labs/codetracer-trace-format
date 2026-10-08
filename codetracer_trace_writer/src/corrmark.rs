//! `corrmark.ns`, the correlation index: which distributed-trace spans and
//! which boundary crossings a recording carries, answerable by one B-tree
//! lookup (`internal-files.md` §"Correlation Index (`corrmark.ns`)").
//!
//! The member is an `NSB1` namespace keyed by a 64-bit XXH64 of the
//! correlation key, whose 16-byte descriptors address buckets:
//!
//! ```text
//! entry_count u32 LE, then entry_count 60-byte entries:
//!   identity[24]  kind 0: trace_id[16] || span_id[8], wire order
//!                 kind 1: marker_id u64 BE || key_fingerprint u64 BE || 8 zero bytes
//!   wall_time_unix_ns u64 LE   monotonic_time_ns u64 LE
//!   geid u64 LE                thread_id u64 LE
//!   kind u16 LE                flags u16 LE (bit 0: exit)
//! ```
//!
//! A bucket's entries are ordered by identity bytes, then geid, ties in
//! declaration order; buckets by key. Because a key is a hash, a lookup
//! confirms each entry: a kind-0 entry by its full identity, a kind-1 entry by
//! its marker id and the fingerprint of the key value.

use std::collections::HashMap;

use codetracer_ctfs::cow_namespace::{CowNamespace, LeafType, payload_namespace};
use codetracer_ctfs::xxh64::xxh64;

/// The member's name.
pub const CORRMARK_FILE_NAME: &str = "corrmark.ns";
/// A distributed-trace span.
pub const MARKER_KIND_SPAN: u16 = 0;
/// A boundary crossing.
pub const MARKER_KIND_BOUNDARY: u16 = 1;
/// `flags` bit 0: the exit (receive) side.
pub const MARKER_FLAG_EXIT: u16 = 1;
/// The seed of a key value's fingerprint, distinct from the index key's 0.
pub const KEY_FINGERPRINT_SEED: u64 = 2_654_435_761;
/// Bytes per bucket entry.
pub const ENTRY_SIZE: usize = 60;

/// One index entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CorrelationMarker {
    /// The 24 identity bytes, interpreted by `kind`.
    pub identity: [u8; 24],
    pub wall_time_unix_ns: u64,
    pub monotonic_time_ns: u64,
    /// The exec-record index the marker belongs to.
    pub geid: u64,
    pub thread_id: u64,
    pub kind: u16,
    pub flags: u16,
}

/// The index key of a kind-0 entry: XXH64 of `trace_id || span_id`.
pub fn span_key(trace_id: &[u8; 16], span_id: &[u8; 8]) -> u64 {
    let mut buf = [0u8; 24];
    buf[..16].copy_from_slice(trace_id);
    buf[16..].copy_from_slice(span_id);
    xxh64(&buf, 0)
}

/// The index key of a kind-1 entry: XXH64 of `marker_id` (8 bytes big-endian)
/// followed by the key value's bytes.
pub fn boundary_key(marker_id: u64, key_value: &[u8]) -> u64 {
    let mut buf = Vec::with_capacity(8 + key_value.len());
    buf.extend_from_slice(&marker_id.to_be_bytes());
    buf.extend_from_slice(key_value);
    xxh64(&buf, 0)
}

/// The fingerprint a kind-1 entry carries for its key value.
pub fn key_fingerprint(key_value: &[u8]) -> u64 {
    xxh64(key_value, KEY_FINGERPRINT_SEED)
}

impl CorrelationMarker {
    /// A kind-0 entry: the recording covers `(trace_id, span_id)`.
    pub fn span(
        trace_id: &[u8; 16],
        span_id: &[u8; 8],
        wall_time_unix_ns: u64,
        monotonic_time_ns: u64,
        geid: u64,
        thread_id: u64,
        is_exit: bool,
    ) -> Self {
        let mut identity = [0u8; 24];
        identity[..16].copy_from_slice(trace_id);
        identity[16..].copy_from_slice(span_id);
        CorrelationMarker {
            identity,
            wall_time_unix_ns,
            monotonic_time_ns,
            geid,
            thread_id,
            kind: MARKER_KIND_SPAN,
            flags: if is_exit { MARKER_FLAG_EXIT } else { 0 },
        }
    }

    /// A kind-1 entry: a crossing of the boundary labelled `marker_id` with
    /// `key_value`. Its times are zero.
    pub fn boundary(marker_id: u64, key_value: &[u8], is_recv: bool, geid: u64, thread_id: u64) -> Self {
        let mut identity = [0u8; 24];
        identity[..8].copy_from_slice(&marker_id.to_be_bytes());
        identity[8..16].copy_from_slice(&key_fingerprint(key_value).to_be_bytes());
        CorrelationMarker {
            identity,
            geid,
            thread_id,
            kind: MARKER_KIND_BOUNDARY,
            flags: if is_recv { MARKER_FLAG_EXIT } else { 0 },
            ..Default::default()
        }
    }

    /// The interned label id of a kind-1 entry.
    pub fn marker_id(&self) -> u64 {
        u64::from_be_bytes(self.identity[..8].try_into().expect("eight bytes"))
    }

    /// The key-value fingerprint of a kind-1 entry.
    pub fn fingerprint(&self) -> u64 {
        u64::from_be_bytes(self.identity[8..16].try_into().expect("eight bytes"))
    }

    fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.identity);
        out.extend_from_slice(&self.wall_time_unix_ns.to_le_bytes());
        out.extend_from_slice(&self.monotonic_time_ns.to_le_bytes());
        out.extend_from_slice(&self.geid.to_le_bytes());
        out.extend_from_slice(&self.thread_id.to_le_bytes());
        out.extend_from_slice(&self.kind.to_le_bytes());
        out.extend_from_slice(&self.flags.to_le_bytes());
    }

    fn decode(b: &[u8]) -> Self {
        let u64_at = |at: usize| u64::from_le_bytes(b[at..at + 8].try_into().expect("eight bytes"));
        CorrelationMarker {
            identity: b[..24].try_into().expect("24 bytes"),
            wall_time_unix_ns: u64_at(24),
            monotonic_time_ns: u64_at(32),
            geid: u64_at(40),
            thread_id: u64_at(48),
            kind: u16::from_le_bytes([b[56], b[57]]),
            flags: u16::from_le_bytes([b[58], b[59]]),
        }
    }
}

/// The `corrmark.ns` image of `markers`, each paired with its index key.
pub fn serialize_corrmark(markers: &[(u64, CorrelationMarker)]) -> Result<Vec<u8>, String> {
    let mut buckets: HashMap<u64, Vec<CorrelationMarker>> = HashMap::new();
    for (key, m) in markers {
        buckets.entry(*key).or_default().push(*m);
    }
    let mut keys: Vec<u64> = buckets.keys().copied().collect();
    keys.sort_unstable();
    let mut encoded: Vec<(u64, Vec<u8>)> = Vec::with_capacity(keys.len());
    for key in keys {
        let mut bucket = buckets.remove(&key).expect("collected above");
        bucket.sort_by(|a, b| a.identity.cmp(&b.identity).then(a.geid.cmp(&b.geid)));
        let mut bytes = Vec::with_capacity(4 + bucket.len() * ENTRY_SIZE);
        bytes.extend_from_slice(&(bucket.len() as u32).to_le_bytes());
        for m in &bucket {
            m.encode(&mut bytes);
        }
        encoded.push((key, bytes));
    }
    let payloads: Vec<(u64, &[u8])> = encoded.iter().map(|(k, b)| (*k, b.as_slice())).collect();
    payload_namespace(&payloads)
}

/// Parse a hex-rendered identifier (either case) into its wire bytes.
pub fn decode_hex_id(hex: &str, expected_bytes: usize) -> Result<Vec<u8>, String> {
    if hex.len() != expected_bytes * 2 {
        return Err(format!(
            "expected {} hex characters ({expected_bytes} bytes), got {}",
            expected_bytes * 2,
            hex.len()
        ));
    }
    let digit = |c: u8| -> Result<u8, String> {
        match c {
            b'0'..=b'9' => Ok(c - b'0'),
            b'a'..=b'f' => Ok(c - b'a' + 10),
            b'A'..=b'F' => Ok(c - b'A' + 10),
            _ => Err(format!("'{}' is not a hex digit", c as char)),
        }
    };
    hex.as_bytes().chunks(2).map(|p| Ok(digit(p[0])? << 4 | digit(p[1])?)).collect()
}

/// An opened `corrmark.ns` image.
#[derive(Debug, Clone)]
pub struct CorrelationIndex {
    ns: CowNamespace,
}

impl CorrelationIndex {
    /// Open an image read out of its container (see [`CowNamespace::open`]
    /// for what is refused).
    pub fn open(image: Vec<u8>) -> Result<Self, String> {
        Ok(CorrelationIndex {
            ns: CowNamespace::open(image, LeafType::B)?,
        })
    }

    /// The bucket under `key`, empty when there is none. Refused when its
    /// descriptor leaves the image or does not span exactly its count's
    /// entries.
    fn bucket(&self, key: u64) -> Result<Vec<CorrelationMarker>, String> {
        let Some(desc) = self.ns.lookup(key) else {
            return Ok(Vec::new());
        };
        let bytes = self.ns.payload(desc).map_err(|_| "corrmark.ns: descriptor out of range".to_string())?;
        if bytes.len() < 4 {
            return Err(format!("corrmark.ns: bucket of {} bytes is shorter than its count", bytes.len()));
        }
        let count = u64::from(u32::from_le_bytes(bytes[..4].try_into().expect("four bytes")));
        if 4 + count * ENTRY_SIZE as u64 != bytes.len() as u64 {
            return Err(format!(
                "corrmark.ns: bucket of {count} entries does not span its {}-byte descriptor",
                bytes.len()
            ));
        }
        Ok(bytes[4..].chunks(ENTRY_SIZE).map(CorrelationMarker::decode).collect())
    }

    /// The kind-0 entries for `(trace_id, span_id)`; empty when the recording
    /// does not cover the span.
    pub fn lookup_span(&self, trace_id: &[u8; 16], span_id: &[u8; 8]) -> Result<Vec<CorrelationMarker>, String> {
        let mut probe = [0u8; 24];
        probe[..16].copy_from_slice(trace_id);
        probe[16..].copy_from_slice(span_id);
        Ok(self
            .bucket(span_key(trace_id, span_id))?
            .into_iter()
            .filter(|m| m.kind == MARKER_KIND_SPAN && m.identity == probe)
            .collect())
    }

    /// The kind-1 entries for a crossing of `marker_id` with `key_value`.
    pub fn lookup_boundary(&self, marker_id: u64, key_value: &[u8]) -> Result<Vec<CorrelationMarker>, String> {
        let fp = key_fingerprint(key_value);
        Ok(self
            .bucket(boundary_key(marker_id, key_value))?
            .into_iter()
            .filter(|m| m.kind == MARKER_KIND_BOUNDARY && m.marker_id() == marker_id && m.fingerprint() == fp)
            .collect())
    }

    /// Every entry with its bucket's key, in key order and bucket order. For
    /// inspection: answering a lookup never needs it.
    pub fn entries(&self) -> Result<Vec<(u64, CorrelationMarker)>, String> {
        let mut out = Vec::new();
        for key in self.ns.keys() {
            out.extend(self.bucket(key)?.into_iter().map(|m| (key, m)));
        }
        Ok(out)
    }
}
