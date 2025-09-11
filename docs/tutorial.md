# Tutorial: Recording a Trace with `runtime_tracing`

This guide shows how to capture execution data from a program and write it in the
`runtime_tracing` format.

## Add the crate

Add the library to your `Cargo.toml`:

```toml
runtime_tracing = "0.14.1"
```

## Basic usage

Create a `NonStreamingTraceWriter`, record a few events and store them as JSON.

```rust
use runtime_tracing::{
    NonStreamingTraceWriter, TraceEventsFileFormat, TraceWriter, Line,
    TypeKind, ValueRecord, NONE_VALUE,
};
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Prepare the tracer
    let mut tracer = NonStreamingTraceWriter::new("example_program", &[]);
    tracer.set_format(TraceEventsFileFormat::Json);

    // Record some events
    let src = Path::new("example.rs");
    tracer.start(src, Line(1));
    tracer.register_step(src, Line(1));

    let value = ValueRecord::Int {
        i: 42,
        type_id: tracer.ensure_type_id(TypeKind::Int, "i32"),
    };
    tracer.register_variable_with_full_value("answer", value);
    tracer.register_return(NONE_VALUE);

    // Write the trace files
    tracer.begin_writing_trace_metadata(Path::new("trace_metadata.json"))?;
    tracer.begin_writing_trace_paths(Path::new("trace_paths.json"))?;
    tracer.begin_writing_trace_events(Path::new("trace.json"))?;
    tracer.finish_writing_trace_events()?;
    tracer.finish_writing_trace_metadata()?;
    tracer.finish_writing_trace_paths()?;
    Ok(())
}
```

This minimal example generates the three JSON files expected by the
CodeTracer debugger.

For more complex scenarios, such as streaming traces or binary formats,
see the APIs in `tracer.rs` and the tests in `src/lib.rs`.
