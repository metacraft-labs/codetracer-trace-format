//! The compact profile of a finished container: what a writer that writes the
//! full profile throughout and converts it at close does.
//!
//! Spec: `codetracer-trace-format-spec/ctfs-container.md` §1e (the threshold
//! and the converting writer), §1f (how a framed member is stored in a
//! compact container), §1d (the compact body).
//!
//! A compact container is a function of the full container of the same
//! recording (§1f, "Two writers produce the same bytes"): the full
//! container's members, in its own order, under the same names, with every
//! zstd frame of a framed member replaced by its content. So two writers whose
//! full containers are byte-identical produce byte-identical compact ones.

use codetracer_ctfs::CtfsReader;
use codetracer_ctfs::compact::{self, Profile, WholeFileCompression, append_frame_content};

use crate::step_map::{STEP_MAP_CHUNK_ENTRY_SIZE, STEP_MAP_FILE_NAME, STEP_MAP_HEADER_SIZE};

/// 1 MiB of raw member bytes: §1e's RECOMMENDED default, a judgement and not
/// a format rule. No reader depends on it.
pub const DEFAULT_RAW_BYTE_THRESHOLD: u64 = 1 << 20;

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap_or_default())
}

fn u64_at(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap_or_default())
}

/// A chunked compressed table (`ctfs-container.md` §7) with its chunks
/// replaced by their content: `dat` the contents back to back, `idx` the same
/// header and entries with each entry's leading `u64` offset moved to its
/// chunk's content. `header_size`/`entry_size` are 4/8 for the §7 layout and
/// 8/16 for `spans.idx`, whose entries also carry a cumulative record count,
/// kept as it is.
fn inflate_chunked_table(dat: &[u8], idx: &[u8], stem: &str, header_size: usize, entry_size: usize) -> Result<(Vec<u8>, Vec<u8>), String> {
    if idx.len() < header_size || !(idx.len() - header_size).is_multiple_of(entry_size) {
        return Err(format!(
            "{stem}.idx is {} bytes, not a {header_size}-byte header and {entry_size}-byte entries",
            idx.len()
        ));
    }
    let chunks = (idx.len() - header_size) / entry_size;
    let mut new_dat = Vec::new();
    let mut new_idx = idx.to_vec();
    for c in 0..chunks {
        let e = header_size + c * entry_size;
        let start = u64_at(idx, e);
        let stop = if c + 1 < chunks { u64_at(idx, e + entry_size) } else { dat.len() as u64 };
        if start > stop || stop > dat.len() as u64 {
            return Err(format!("{stem}.idx entry {c} spans {start}..{stop} of a {}-byte {stem}.dat", dat.len()));
        }
        new_idx[e..e + 8].copy_from_slice(&(new_dat.len() as u64).to_le_bytes());
        append_frame_content(&dat[start as usize..stop as usize], &format!("{stem}.dat chunk {c}"), &mut new_dat)?;
    }
    Ok((new_dat, new_idx))
}

/// `step-map.ns` with every chunk's frame replaced by its content and the
/// chunk table's `frame_offset`s moved to it (`internal-files.md`
/// §"`step-map.ns`"; `ctfs-container.md` §1f).
fn inflate_step_map(m: &[u8]) -> Result<Vec<u8>, String> {
    if m.len() < STEP_MAP_HEADER_SIZE {
        return Err(format!("{STEP_MAP_FILE_NAME} is {} bytes, shorter than its header", m.len()));
    }
    let n = u32_at(m, 6) as usize;
    let table_end = n
        .checked_mul(STEP_MAP_CHUNK_ENTRY_SIZE)
        .and_then(|t| t.checked_add(STEP_MAP_HEADER_SIZE))
        .filter(|end| *end <= m.len())
        .ok_or_else(|| format!("{STEP_MAP_FILE_NAME}: its chunk table runs past the member"))?;
    let mut out = m[..table_end].to_vec();
    for c in 0..n {
        let e = STEP_MAP_HEADER_SIZE + c * STEP_MAP_CHUNK_ENTRY_SIZE;
        let at = |entry: usize| usize::try_from(u64_at(m, entry)).ok().and_then(|o| o.checked_add(table_end));
        let start = at(e);
        let stop = if c + 1 < n { at(e + STEP_MAP_CHUNK_ENTRY_SIZE) } else { Some(m.len()) };
        let (start, stop) = match (start, stop) {
            (Some(start), Some(stop)) if start <= stop && stop <= m.len() => (start, stop),
            _ => return Err(format!("{STEP_MAP_FILE_NAME}: chunk {c}'s frame is out of range")),
        };
        let content_offset = (out.len() - table_end) as u64;
        out[e..e + 8].copy_from_slice(&content_offset.to_le_bytes());
        append_frame_content(&m[start..stop], &format!("{STEP_MAP_FILE_NAME} chunk {c}"), &mut out)?;
    }
    Ok(out)
}

/// The members a compact container of the full container `reader` holds
/// carries: the full container's members in its own order, under the same
/// names, with every framed member's frames replaced by their content
/// (`ctfs-container.md` §1f).
///
/// Framed members are recognised by the formats the split-stream writers
/// emit: a `.dat` with an `.idx` companion is a chunked compressed table
/// (`spans` in its own index layout), and `step-map.ns`. A seekable-zstd
/// `events.log` is refused rather than copied, since copying its frames would
/// break §1e's raw-member property; so is a container that is not in the full
/// profile.
pub fn compact_members_of_reader(reader: &mut CtfsReader) -> Result<Vec<(String, Vec<u8>)>, String> {
    if reader.profile() != Profile::Full {
        return Err("the container is not in the full profile, so there is nothing to convert".to_string());
    }
    let mut members = reader.members().map_err(|e| format!("reading the full container's members: {e:?}"))?;
    let position = |members: &[(String, Vec<u8>)], name: &str| members.iter().position(|(n, _)| n == name);
    for i in 0..members.len() {
        let name = members[i].0.clone();
        if name == "events.log" {
            return Err(
                "events.log is a seekable-zstd stream this conversion does not inflate; a compact container \
                        must not carry its frames (ctfs-container.md §1e)"
                    .to_string(),
            );
        }
        if name == STEP_MAP_FILE_NAME {
            members[i].1 = inflate_step_map(&members[i].1)?;
        } else if let Some(stem) = name.strip_suffix(".dat")
            && let Some(j) = position(&members, &format!("{stem}.idx"))
        {
            let (header_size, entry_size) = if stem == "spans" { (8, 16) } else { (4, 8) };
            let (dat, idx) = inflate_chunked_table(&members[i].1, &members[j].1, stem, header_size, entry_size)?;
            members[i].1 = dat;
            members[j].1 = idx;
        }
    }
    Ok(members)
}

/// [`compact_members_of_reader`] over a full container's bytes.
pub fn compact_members_of(full: &[u8]) -> Result<Vec<(String, Vec<u8>)>, String> {
    let mut reader = CtfsReader::from_bytes(full.to_vec()).map_err(|e| format!("opening the full container: {e:?}"))?;
    compact_members_of_reader(&mut reader)
}

/// §1e's measured quantity: the total length of the compact members.
pub fn raw_member_bytes(members: &[(String, Vec<u8>)]) -> u64 {
    members.iter().map(|(_, payload)| payload.len() as u64).sum()
}

/// The compact container image laid out from `members`, stored as-is.
pub fn encode_compact(members: &[(String, Vec<u8>)]) -> Result<Vec<u8>, String> {
    let borrowed: Vec<(&str, &[u8])> = members.iter().map(|(n, p)| (n.as_str(), p.as_slice())).collect();
    compact::encode_compact_container(&borrowed, WholeFileCompression::None).map_err(|e| format!("laying out the compact container: {e:?}"))
}

/// The container a converting writer emits for the full container `full`:
/// the compact container when the compact members total fewer than
/// `threshold` raw bytes, `full` itself otherwise (`ctfs-container.md` §1e).
/// Returns the profile chosen, the container's bytes and the raw total.
pub fn select_profile(full: Vec<u8>, threshold: u64) -> Result<(Profile, Vec<u8>, u64), String> {
    let members = compact_members_of(&full)?;
    let raw = raw_member_bytes(&members);
    if raw < threshold {
        Ok((Profile::Compact, encode_compact(&members)?, raw))
    } else {
        Ok((Profile::Full, full, raw))
    }
}
