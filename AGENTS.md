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

The second check runs the real Cargo/Nim build with two short `TMPDIR` values
and an overlong one to verify environment invalidation and the short-cache
fallback. `just test` (also run by CI) includes it after the Rust tests.

## Working on zeekstd

The `zeekstd` seekable Zstd crate lives at `~/metacraft/zeekstd/`. Its own flake does not include a Rust toolchain, so use this repo's dev shell:
```
repro exec ~/metacraft/codetracer-trace-format -- bash -c 'cd ~/metacraft/zeekstd && cargo test'
```

## Managing dependencies

Rust packages are managed with Cargo. The Rust toolchain is pinned via `nix-codetracer-toolchains`.
After adding dependencies, verify `cargo build --workspace` still succeeds.
