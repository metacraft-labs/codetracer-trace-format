# Run all tests
test:
  cargo test --verbose

# Run the formatting and clippy lint checks.
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
#
# `cargo fmt --all --check` runs first because nothing else checks formatting:
# sixteen files had drifted from rustfmt unnoticed, so any contributor who ran
# `cargo fmt` produced a diff mixing their change with unrelated reformatting.
lint:
  cargo fmt --all --check
  cargo clippy --workspace --all-targets -- -D warnings

# Type-check the reader for the browser (wasm32-unknown-unknown).
#
# The browser replay engine links `codetracer_trace_reader` for wasm32, where
# libzstd is replaced by `ruzstd` and the file-backed readers are compiled out.
# Native builds never compile the wasm32 arms, so a module that is gated out on
# wasm32 while an ungated one still imports it builds and tests green here and
# fails only in the browser build. This recipe is what notices.
#
# Needs the `wasm32-unknown-unknown` Rust target and a clang that can emit wasm
# (zstd-sys's build script compiles C for the target even under `cargo check`).
check-wasm32:
  CC_wasm32_unknown_unknown="${CC_wasm32_unknown_unknown:-clang}" cargo check -p codetracer_trace_reader --target wasm32-unknown-unknown

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
