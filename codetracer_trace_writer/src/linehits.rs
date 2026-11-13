//! `linehits.tc`: for every source position a step executed, the exec-record
//! indices of the steps that executed it, in recording order.
//!
//! The member is an `NSB1` namespace ([`codetracer_ctfs::cow_namespace`]) keyed
//! by the position a step record decodes to — its `global_position_index`, a
//! line address in a line-only trace and a `(line, column)` address in a
//! column-aware one — whose 16-byte descriptors address payloads holding the
//! step indices as unsigned LEB128 varints. A writer records it only when asked
//! to, and writes it at close.

use std::collections::HashMap;

use codetracer_ctfs::cow_namespace::{CowNamespace, LeafType, payload_namespace};

/// The member's name.
pub const LINEHITS_FILE_NAME: &str = "linehits.tc";

fn push_varint(mut v: u64, out: &mut Vec<u8>) {
    loop {
        let byte = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// Collects the hits of a recording.
#[derive(Debug, Default, Clone)]
pub struct LineHitsBuilder {
    lists: HashMap<u64, Vec<u8>>,
}

impl LineHitsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Step `step` executed `position`.
    pub fn record_hit(&mut self, position: u64, step: u64) {
        push_varint(step, self.lists.entry(position).or_default());
    }

    /// The `linehits.tc` image.
    pub fn serialize(&self) -> Result<Vec<u8>, String> {
        let mut keys: Vec<u64> = self.lists.keys().copied().collect();
        keys.sort_unstable();
        let payloads: Vec<(u64, &[u8])> = keys.iter().map(|k| (*k, self.lists[k].as_slice())).collect();
        payload_namespace(&payloads)
    }
}

/// An opened `linehits.tc` image.
#[derive(Debug, Clone)]
pub struct LineHits {
    ns: CowNamespace,
}

fn decode_varint(data: &[u8], pos: &mut usize) -> Result<u64, String> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let Some(&b) = data.get(*pos) else {
            return Err("varint: unexpected end of input".to_string());
        };
        *pos += 1;
        value |= u64::from(b & 0x7F).checked_shl(shift).unwrap_or(0);
        if b & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            return Err("varint: too many bytes (>10)".to_string());
        }
    }
}

impl LineHits {
    /// Open an image read out of its container (see [`CowNamespace::open`]
    /// for what is refused).
    pub fn open(image: Vec<u8>) -> Result<Self, String> {
        Ok(LineHits {
            ns: CowNamespace::open(image, LeafType::B)?,
        })
    }

    /// Number of positions with at least one hit.
    pub fn position_count(&self) -> u64 {
        self.ns.len()
    }

    /// Every position with a hit, ascending.
    pub fn positions(&self) -> Vec<u64> {
        self.ns.keys()
    }

    /// The step indices that executed `position`, in recording order; `None`
    /// when none did. A list whose descriptor leaves the image, or whose last
    /// varint runs past the list's own bytes, is refused.
    pub fn hits(&self, position: u64) -> Result<Option<Vec<u64>>, String> {
        let Some(desc) = self.ns.lookup(position) else {
            return Ok(None);
        };
        let list = self
            .ns
            .payload(desc)
            .map_err(|e| format!("linehits payload for position {position} is out of bounds: {e}"))?;
        let mut steps = Vec::new();
        let mut pos = 0;
        while pos < list.len() {
            steps.push(decode_varint(list, &mut pos).map_err(|e| format!("linehits payload for position {position}: {e}"))?);
        }
        Ok(Some(steps))
    }
}
