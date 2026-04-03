# CodeTracer Trace Format

Read @AGENTS.md

Rust workspace containing crates for trace types, reading, and writing. All CodeTracer recording backends depend on these crates.

Key crates:
- `codetracer_trace_types` — event types, published to crates.io
- `codetracer_trace_reader` — reads CBOR+Zstd and Cap'n Proto traces
- `codetracer_trace_writer` — writes CBOR+Zstd traces with seekable Zstd (`zeekstd`)
- `codetracer_trace_writer_ffi` — C FFI bindings for Nim consumers

The planned `codetracer_ctfs` crate (CTFS binary container format) will be added here. See `~/metacraft/codetracer-specs/Trace-Files/CTFS-Binary-Format.status.org` for milestones.
