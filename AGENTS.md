# Development Environment

The development environment is managed with a Nix flake. Enter the dev shell with `repro allow` and the Repro shell hook, or use `repro exec` to run commands:

```
repro exec ~/metacraft/codetracer-trace-format -- cargo build --workspace
repro exec ~/metacraft/codetracer-trace-format -- cargo test --workspace
```

## Testing

Run the full test suite:
```
cargo test --workspace
bash codetracer_trace_writer_nim/tests/nimcache_environment.sh
```

The second check runs the real Cargo/Nim build with two short temporary-directory
overrides and an overlong one (`TMPDIR` on POSIX; `TMP`/`TEMP`/`SYSTEMTEMP` on
Windows) to verify environment invalidation and the short-cache fallback.
`just test` (also run by CI) includes it after the Rust tests.

### Cross-repo checks that run elsewhere

- **This implementation against the Nim one.** Every test that compares the
  two implementations lives in
  [`codetracer-trace-format-conformance`](https://github.com/metacraft-labs/codetracer-trace-format-conformance),
  which depends on both: the writer and reader comparisons (including the
  adapter-driven `*_cross_writer`/`*_cross_reader` tests and
  `writer_differential`), the C ABI parity check, the Nim-adjudicated
  cross-reads, the Nim-written-bundle cross-reads, and the genuine-producer
  entry gate with its runtime constructor. The `conformance` workflow runs that
  suite against every change here; locally, check it out beside this
  repository and run `just test` in its dev shell. Changing a writer, a
  reader, the C ABI, the trace-storage fixtures or the format can break it.
  The `codetracer_trace_writer_nim` adapter crate stays here with the tests of
  its own behaviour that need no Rust reader (they read back through the Nim
  reader, or not at all); its build script still compiles the Nim checkout.
  An adapter test that reads the Nim writer's output with a Rust reader is a
  cross-read and belongs in the conformance repository.
- **The recorders against the trace-storage contract.** Nothing in this
  repository reads a recorder checkout: each recorder checks its own sources
  by calling `codetracer_ctfs::recorder_conformance` from its own test suite
  (the token list is `codetracer_ctfs/src/recorder_conformance_tokens.txt`),
  and the `recorder-conformance` workflow runs those tests against a change
  here.

## Working on zeekstd

The `zeekstd` seekable Zstd crate lives at `~/metacraft/zeekstd/`. Its own flake does not include a Rust toolchain, so use this repo's dev shell:
```
repro exec ~/metacraft/codetracer-trace-format -- bash -c 'cd ~/metacraft/zeekstd && cargo test'
```

## Managing dependencies

Rust packages are managed with Cargo. The Rust toolchain is pinned via `nix-codetracer-toolchains`.
After adding dependencies, verify `cargo build --workspace` still succeeds.
