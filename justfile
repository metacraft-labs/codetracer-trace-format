# Run all tests
test:
  cargo test --verbose

# Run clippy lint checks
lint:
  cargo clippy

# Build all crates
build:
  cargo build --verbose

# Run all checks (lint + test)
check: lint test

# Run FFI crate tests only
test-ffi:
  cargo test -p codetracer_trace_writer_ffi --verbose

# Run trace writer tests only
test-writer:
  cargo test -p codetracer_trace_writer --verbose

# Run binary format roundtrip tests
test-roundtrip:
  cargo test -p codetracer_trace_util --verbose
