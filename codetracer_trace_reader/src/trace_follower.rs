//! A reader of a container that is still being written.
//!
//! [`TraceFollower`] holds a container opened from its path and its stream
//! readers. [`TraceFollower::refresh`] makes what the writer has published
//! since readable (`ctfs-container.md` §6, "Live progress: per-stream
//! following"): it re-reads the root directory, extends each stream reader by
//! the chunks published since, and opens the streams that have appeared. After
//! a refresh every answer is what a fresh open of the container at that moment
//! gives.

use std::path::Path;

use codetracer_ctfs::CtfsReader;

use crate::call_stream_reader::CallStreamReader;
use crate::interning_tables_reader::InterningTablesReader;
use crate::io_event_stream_reader::IoEventStreamReader;
use crate::span_stream_reader::SpanStreamReader;
use crate::step_map_reader::StepMapReader;
use crate::step_stream_reader::StepStreamReader;
use crate::value_stream_reader::ValueStreamReader;

/// A container read while it is written. See the module documentation.
pub struct TraceFollower {
    ctfs: CtfsReader,
    steps: Option<StepStreamReader>,
    values: Option<ValueStreamReader>,
    calls: Option<CallStreamReader>,
    events: Option<IoEventStreamReader>,
    spans: Option<SpanStreamReader>,
}

impl TraceFollower {
    /// Open the container at `path` to follow it.
    pub fn open(path: &Path) -> Result<TraceFollower, String> {
        let mut ctfs = CtfsReader::open(path).map_err(|e| format!("failed to open {}: {e}", path.display()))?;
        crate::retired_streams::refuse_retired_members(&ctfs)?;
        Ok(TraceFollower {
            steps: StepStreamReader::open(&mut ctfs)?,
            values: ValueStreamReader::open(&mut ctfs)?,
            calls: CallStreamReader::open(&mut ctfs)?,
            events: IoEventStreamReader::open(&mut ctfs)?,
            spans: SpanStreamReader::open(&mut ctfs)?,
            ctfs,
        })
    }

    /// Make what the writer has published since the last open or refresh
    /// readable. A container that changed in a way a writer never changes one
    /// -- a member that shrank, a published chunk that moved -- is refused,
    /// naming the member, and nothing this follower read before is replaced.
    pub fn refresh(&mut self) -> Result<(), String> {
        self.ctfs.refresh().map_err(|e| e.to_string())?;
        crate::retired_streams::refuse_retired_members(&self.ctfs)?;
        let ctfs = &mut self.ctfs;
        match &mut self.steps {
            Some(r) => r.refresh(ctfs)?,
            None => self.steps = StepStreamReader::open(ctfs)?,
        }
        match &mut self.values {
            Some(r) => r.refresh(ctfs)?,
            None => self.values = ValueStreamReader::open(ctfs)?,
        }
        match &mut self.calls {
            Some(r) => r.refresh(ctfs)?,
            None => self.calls = CallStreamReader::open(ctfs)?,
        }
        match &mut self.events {
            Some(r) => r.refresh(ctfs)?,
            None => self.events = IoEventStreamReader::open(ctfs)?,
        }
        match &mut self.spans {
            Some(r) => r.refresh(ctfs)?,
            None => self.spans = SpanStreamReader::open(ctfs)?,
        }
        Ok(())
    }

    /// The container, at the root directory of the last open or refresh.
    pub fn container(&mut self) -> &mut CtfsReader {
        &mut self.ctfs
    }

    /// The step stream, once the container has one.
    pub fn steps(&mut self) -> Option<&mut StepStreamReader> {
        self.steps.as_mut()
    }

    /// The value stream, once the container has one.
    pub fn values(&mut self) -> Option<&mut ValueStreamReader> {
        self.values.as_mut()
    }

    /// The call stream, once the container has one.
    pub fn calls(&mut self) -> Option<&mut CallStreamReader> {
        self.calls.as_mut()
    }

    /// The I/O event stream, once the container has one.
    pub fn events(&mut self) -> Option<&mut IoEventStreamReader> {
        self.events.as_mut()
    }

    /// The span stream, once the container has one (a writer creates it with
    /// its first span).
    pub fn spans(&mut self) -> Option<&mut SpanStreamReader> {
        self.spans.as_mut()
    }

    /// The interning tables as published now. They are small and read whole.
    pub fn interning_tables(&mut self) -> Result<Option<InterningTablesReader>, String> {
        InterningTablesReader::open(&mut self.ctfs)
    }

    /// `step-map.ns`, which a writer adds at close; `None` before then.
    pub fn step_map(&mut self) -> Result<Option<StepMapReader>, String> {
        StepMapReader::open(&mut self.ctfs)
    }
}
