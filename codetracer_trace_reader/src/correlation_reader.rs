//! The members a recording may add beside its streams to answer lookups
//! without scanning them: `linehits.tc` (which steps executed a position),
//! `corrmark.ns` (which spans and boundary crossings it carries) and
//! `markers.dat` / `markers.off` (the boundary labels).
//!
//! Each opener returns `Ok(None)` when the container has no such member —
//! for the correlation index, "not indexed", which a consumer must not
//! report as "covers nothing" — and an error when the member is present but
//! malformed.

use codetracer_ctfs::CtfsReader;
pub use codetracer_trace_writer::corrmark::{CORRMARK_FILE_NAME, CorrelationIndex, CorrelationMarker, MARKER_KIND_BOUNDARY, MARKER_KIND_SPAN};
pub use codetracer_trace_writer::linehits::{LINEHITS_FILE_NAME, LineHits};

fn member(reader: &mut CtfsReader, name: &str) -> Result<Option<Vec<u8>>, String> {
    if !reader.list_files().iter().any(|f| f == name) {
        return Ok(None);
    }
    reader.read_file(name).map(Some).map_err(|e| format!("{name}: {e:?}"))
}

/// The container's line-hit index.
pub fn open_line_hits(reader: &mut CtfsReader) -> Result<Option<LineHits>, String> {
    member(reader, LINEHITS_FILE_NAME)?
        .map(|image| LineHits::open(image).map_err(|e| format!("{LINEHITS_FILE_NAME}: {e}")))
        .transpose()
}

/// The container's correlation index.
pub fn open_correlation_index(reader: &mut CtfsReader) -> Result<Option<CorrelationIndex>, String> {
    member(reader, CORRMARK_FILE_NAME)?
        .map(|image| CorrelationIndex::open(image).map_err(|e| format!("{CORRMARK_FILE_NAME}: {e}")))
        .transpose()
}

/// The correlation-marker labels in id order. `markers.off` holds `u64`
/// end offsets after a leading 0, non-decreasing and ending at
/// `markers.dat`'s length.
pub fn marker_labels(reader: &mut CtfsReader) -> Result<Option<Vec<Vec<u8>>>, String> {
    let Some(dat) = member(reader, "markers.dat")? else {
        return Ok(None);
    };
    let off = member(reader, "markers.off")?.ok_or("markers.dat has no markers.off")?;
    if off.len() % 8 != 0 || off.is_empty() {
        return Err(format!("markers.off is {} bytes", off.len()));
    }
    let at = |i: usize| u64::from_le_bytes(off[i * 8..i * 8 + 8].try_into().expect("eight bytes"));
    if at(0) != 0 {
        return Err("markers.off does not start at 0".to_string());
    }
    let mut labels = Vec::new();
    for i in 1..off.len() / 8 {
        let (a, b) = (at(i - 1), at(i));
        if b < a || b > dat.len() as u64 {
            return Err(format!("markers.off: offset {i} is out of order or past the data"));
        }
        labels.push(dat[a as usize..b as usize].to_vec());
    }
    if at(off.len() / 8 - 1) != dat.len() as u64 {
        return Err("markers.off: the offsets do not end at the data's length".to_string());
    }
    Ok(Some(labels))
}
