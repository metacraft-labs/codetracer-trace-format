{
  description = "CodeTracer Trace Format - Rust crates for trace types, reading, and writing";

  inputs = {
    codetracer-toolchains.url = "github:metacraft-labs/nix-codetracer-toolchains";
    nixpkgs.follows = "codetracer-toolchains/nixpkgs";

    flake-parts = {
      url = "github:hercules-ci/flake-parts";
      inputs.nixpkgs-lib.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{
      nixpkgs,
      flake-parts,
      ...
    }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      perSystem =
        { pkgs, system, ... }:
        let
          toolchainsPkgs = inputs."codetracer-toolchains".packages.${system};

          # VERIFY(linux): rust-stable is a combined toolchain derivation;
          # makeRustPlatform expects it to expose `cargo` + `rustc` on PATH.
          # If the attr split differs, pass `toolchainsPkgs.rust-stable` as
          # both (works when it bundles both) or use pkgs.rustPlatform.
          rustPlatform = pkgs.makeRustPlatform {
            cargo = toolchainsPkgs.rust-stable;
            rustc = toolchainsPkgs.rust-stable;
          };

          # `codetracer-managed-upload` lives in the `codetracer_ctfs` crate.
          # Its dependency closure is crates.io + `zstd` only — NO in-repo
          # path deps — so building just this bin does NOT trigger the nim
          # staticlib (codetracer_trace_writer_nim/build.rs) or capnpc
          # (codetracer_trace_format_capnp/build.rs). The build is therefore
          # self-contained: rust + libzstd + pkg-config.
          codetracer-managed-upload = rustPlatform.buildRustPackage {
            pname = "codetracer-managed-upload";
            version = "0.1.0";
            src = ./.;
            cargoLock.lockFile = ./Cargo.lock; # all registry deps (no git+ → no outputHashes needed)
            # Restrict the whole virtual workspace to just this one binary so
            # the nim/capnp `build.rs` crates never enter the build graph.
            cargoBuildFlags = [
              "-p"
              "codetracer_ctfs"
              "--bin"
              "codetracer-managed-upload"
            ];
            doCheck = false; # unit tests need live fixtures; keep the package lean
            nativeBuildInputs = [ pkgs.pkg-config ];
            buildInputs = [ pkgs.zstd ];
            PKG_CONFIG_PATH = "${pkgs.zstd.dev}/lib/pkgconfig";
            meta.mainProgram = "codetracer-managed-upload";
          };
        in
        {
          packages = {
            inherit codetracer-managed-upload;
            default = codetracer-managed-upload;
          };

          devShells.default = pkgs.mkShell {
            packages = [
              # Rust toolchain
              toolchainsPkgs.rust-stable
              toolchainsPkgs.nim-2_2
              toolchainsPkgs.nimble

              # Native dependencies for crates
              pkgs.capnproto # capnpc for codetracer_trace_format_capnp
              pkgs.pkg-config
              pkgs.zstd # libzstd for zeekstd/zstd-sys

              # Development tools
              pkgs.cargo-edit
            ];

            # For zstd-sys to find libzstd
            PKG_CONFIG_PATH = "${pkgs.zstd.dev}/lib/pkgconfig";
          };
        };
    };
}
