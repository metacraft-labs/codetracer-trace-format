# Development Environment

The development environment is managed with a Nix flake. Enter the dev shell with `direnv allow` or use `direnv exec` to run commands:

```
direnv exec ~/metacraft/codetracer-trace-format cargo build --workspace
direnv exec ~/metacraft/codetracer-trace-format cargo test --workspace
```

## Testing

Run the full test suite:
```
cargo test --workspace
```

## Working on zeekstd

The `zeekstd` seekable Zstd crate lives at `~/metacraft/zeekstd/`. Its own flake does not include a Rust toolchain, so use this repo's dev shell:
```
direnv exec ~/metacraft/codetracer-trace-format bash -c 'cd ~/metacraft/zeekstd && cargo test'
```

## Managing dependencies

Rust packages are managed with Cargo. The Rust toolchain is pinned via `nix-codetracer-toolchains`.
After adding dependencies, verify `cargo build --workspace` still succeeds.
