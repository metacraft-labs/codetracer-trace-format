//! Helper for generating trace events from a running program or interpreter.

use crate::non_streaming_trace_writer::NonStreamingTraceWriter;
use crate::trace_readers::{BinaryTraceReader, JsonTraceReader, TraceReader};
use crate::types::{
    FunctionId, TypeId,
    ValueRecord,
};
use crate::TraceWriter;

#[derive(Debug, Clone, Copy)]
pub enum TraceEventsFileFormat {
    Json,
    BinaryV0,
    Binary
}

// we ensure in start they are registered with those id-s

// pub const EXAMPLE_INT_TYPE_ID: TypeId = TypeId(0);
// pub const EXAMPLE_FLOAT_TYPE_ID: TypeId = TypeId(1);
// pub const EXAMPLE_BOOL_TYPE_ID: TypeId = TypeId(2);
// pub const EXAMPLE_STRING_TYPE_ID: TypeId = TypeId(3);
pub const NONE_TYPE_ID: TypeId = TypeId(0);
pub const NONE_VALUE: ValueRecord = ValueRecord::None { type_id: NONE_TYPE_ID };

pub const TOP_LEVEL_FUNCTION_ID: FunctionId = FunctionId(0);


pub fn create_trace_reader(format: TraceEventsFileFormat) -> Box<dyn TraceReader> {
    match format {
        TraceEventsFileFormat::Json => Box::new(JsonTraceReader {}),
        TraceEventsFileFormat::BinaryV0 |
        TraceEventsFileFormat::Binary => Box::new(BinaryTraceReader {}),
    }
}

pub fn create_trace_writer(program: &str, args: &[String], format: TraceEventsFileFormat) -> Box<dyn TraceWriter> {
    match format {
        TraceEventsFileFormat::Json |
        TraceEventsFileFormat::BinaryV0 => {
            let mut result = Box::new(NonStreamingTraceWriter::new(program, args));
            result.set_format(format);
            result
        }
        TraceEventsFileFormat::Binary => {
            Box::new(crate::cbor_zstd_writer::CborZstdTraceWriter::new(program, args))
        }
    }
}
