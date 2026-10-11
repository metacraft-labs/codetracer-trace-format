//! Optional entry identity from the caller's already-owned CTFS image.
//! Absence preserves legacy behavior; present corrupt members never become absence.
use codetracer_ctfs::CtfsReader;
use codetracer_trace_writer::recorded_entry_identity::{RecordedEntryIdentity, decode_entry_identity};
use crate::{call_stream_reader::CallStreamReader, step_stream_reader::StepStreamReader,
    interning_tables_reader::InterningTablesReader};

pub trait RecordedEntryReader {
    fn recorded_entry(&mut self) -> Result<Option<RecordedEntryIdentity>, String>;
}
impl RecordedEntryReader for CtfsReader {
    fn recorded_entry(&mut self) -> Result<Option<RecordedEntryIdentity>, String> {
        let count = self.list_files().iter().filter(|name| *name == "entry.dat").count();
        if count == 0 { return Ok(None); }
        if count != 1 { return Err("entry.dat: duplicate member".into()); }
        if !self.is_owned_image() {
            let captured = self.snapshot_owned().map_err(|e| format!("entry.dat: {e}"))?;
            if captured.list_files().iter().filter(|name| *name == "entry.dat").count() != 1 {
                return Err("entry.dat: member changed during held-image capture".into());
            }
            *self = captured;
            return self.recorded_entry();
        }
        let bytes = self.read_file("entry.dat").map_err(|e| format!("entry.dat: {e}"))?;
        let entry = decode_entry_identity(&bytes)?;
        let mut calls = CallStreamReader::open(self)?.ok_or("entry.dat: missing call stream")?;
        let records = calls.read_all()?;
        let steps = StepStreamReader::open(self)?.ok_or("entry.dat: missing step stream")?;
        let tables = InterningTablesReader::open(self)?.ok_or("entry.dat: missing function table")?;
        let mut names = std::collections::BTreeMap::new();
        // Validate against the complete declared same-image function table.
        for index in 0..tables.func_count() {
            let id = u64::try_from(index).map_err(|_| "entry.dat: function table exceeds wire domain")?;
            names.insert(id, tables.func(id)?.name);
        }
        for record in &records {
            // The wire function domain is u64, while the table's indexing API
            // uses usize. Check before its cast, including on wasm32.
            let index = usize::try_from(record.function_id).map_err(|_| "entry.dat: function index exceeds platform domain")?;
            if index >= tables.func_count() { return Err("entry.dat: function index is outside the same-image table".into()); }
            names.insert(record.function_id, tables.func(record.function_id)?.name);
        }
        codetracer_trace_writer::recorded_entry_identity::validate_entry_image(entry, &records, steps.count(), &names)?;
        Ok(Some(entry))
    }
}
