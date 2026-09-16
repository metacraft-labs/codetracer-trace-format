# Run all tests
test:
  cargo test --verbose

# Run clippy lint checks.
#
# THIS IS THE LINT GATE. CI runs this recipe and so does the pre-commit hook,
# so there is one definition of what "lint" means here and the two cannot
# disagree about it.
#
# `--workspace --all-targets` because the bare `cargo clippy` this replaced
# linted only the default members' libraries: tests, benches and the other
# crates were never looked at. `-D warnings` because a warning nobody is
# obliged to fix is a warning that accumulates — the backlog this turns on
# against stood at 159 across the workspace, and one of them was a loop that
# indexed one past the end of every BigInt it wrote.
lint:
  cargo clippy --workspace --all-targets -- -D warnings

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
