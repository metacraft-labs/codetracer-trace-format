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
          fenixPkgs = inputs."codetracer-toolchains".inputs.fenix.packages.${system};
          # Match the pinned native compiler for the declared wasm32 gate.
          rustWithWasm = fenixPkgs.combine [
            toolchainsPkgs.rust-stable
            fenixPkgs.targets.wasm32-unknown-unknown.stable.rust-std
          ];
        in
        {
          devShells.default = pkgs.mkShell {
            packages = [
              # Rust toolchain
              rustWithWasm
              toolchainsPkgs.nim-2_2
              toolchainsPkgs.nimble

              # Native dependencies for crates
              pkgs.clang # native compiler includes the pinned platform headers
              pkgs.capnproto # capnpc for codetracer_trace_format_capnp
              pkgs.pkg-config
              pkgs.zstd # libzstd for zeekstd/zstd-sys

              # Monitored launches require a non-SIP shell on macOS.
              pkgs.bash

              # Development tools
              pkgs.cargo-edit
            ];

            # For zstd-sys to find libzstd
            CC_wasm32_unknown_unknown = "${pkgs.llvmPackages.clang-unwrapped}/bin/clang";
            PKG_CONFIG_PATH = "${pkgs.zstd.dev}/lib/pkgconfig";
          };
        };
    };
}
