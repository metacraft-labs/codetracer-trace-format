//! Path-based entry points over a container's split streams.
//!
//! The events of a `.ct` container are assembled from `steps.dat`,
//! `values.dat`, `calls.dat`, `events.dat` and the interning tables by
//! [`crate::split_stream_reader`]; these functions open the container from a
//! path and hand it over.

use codetracer_ctfs::CtfsReader;
use codetracer_trace_types::TraceLowLevelEvent;

/// Read every event of the `.ct` container at `path`.
pub fn read_trace_from_ctfs(path: &std::path::Path) -> Result<Vec<TraceLowLevelEvent>, Box<dyn std::error::Error>> {
    let mut reader = CtfsReader::open(path)?;
    if reader.list_files().iter().any(|name| name == "entry.dat") {
        reader = reader.snapshot_owned()?;
        if reader.list_files().iter().filter(|name| *name == "entry.dat").count() != 1 {
            return Err("entry.dat: member changed during held-image capture".into());
        }
    }
    crate::recorded_entry_reader::RecordedEntryReader::recorded_entry(&mut reader)?;

    crate::split_stream_reader::read_trace_from_split_streams(&mut reader).map_err(|e| e.into())
}

/// Read the events of `count` steps starting at step `target_event`.
///
/// A split-stream container has no global event ordinal -- its streams are
/// indexed by step, by call key and by record -- so `target_event` is a STEP
/// index, as at [`crate::split_stream_reader::read_window`].
pub fn seek_events_in_ctfs(path: &std::path::Path, target_event: usize, count: usize) -> Result<Vec<TraceLowLevelEvent>, Box<dyn std::error::Error>> {
    let mut reader = CtfsReader::open(path)?;
    crate::split_stream_reader::read_window(&mut reader, target_event as u64, count as u64).map_err(|e| e.into())
}
