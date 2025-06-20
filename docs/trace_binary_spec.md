# CodeTracer Trace Binary Format

This document describes the binary format stored in `trace.bin` files produced by the `runtime_tracing` crate. The binary format contains the same information as the JSON trace but encoded using [Cap'n Proto](https://capnproto.org/) for efficiency.

## File Layout

A binary trace begins with an 8‑byte header:

```
C0 DE 72 AC E2 00 00 00
```

* The first five bytes (`C0 DE 72 AC E2`) identify the file as a CodeTracer trace.
* The remaining three bytes are reserved for versioning. They are zero for the initial version. Non‑zero values indicate an incompatible future format.

After the header comes a Cap'n Proto message serialized with the packed encoding. The schema for this message is defined in [`runtime_tracing/src/trace.capnp`](../runtime_tracing/src/trace.capnp). The root object is `Trace`, which contains an array of `TraceLowLevelEvent` values.

The mapping between the Rust data structures and the Cap'n Proto schema is implemented in `capnptrace.rs`. Helper functions `write_trace` and `read_trace` write and read the binary format.

## Usage

To write a binary trace:

```rust
use runtime_tracing::{Tracer, TraceEventsFileFormat};
# let mut tracer = Tracer::new("prog", &[]);
# // record events
tracer.store_trace_events(Path::new("trace.bin"), TraceEventsFileFormat::Binary)?;
```

To read it back:

```rust
let mut tracer = Tracer::new("prog", &[]);
tracer.load_trace_events(Path::new("trace.bin"), TraceEventsFileFormat::Binary)?;
```

## Summary

`trace.bin` provides a compact representation of the same event stream described in [Trace JSON Format](trace_json_spec.md). It starts with the 8‑byte magic header followed by a packed Cap'n Proto `Trace` message.
