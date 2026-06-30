{
  description = "CodeTracer Trace Format - Rust crates for trace types, reading, and writing";

  nixConfig = {
    extra-substituters = [ "https://cache.metacraft-labs.com/metacraft-codetracer" ];
    extra-trusted-public-keys = [
      "metacraft-codetracer:9OV9wCDX560bt5/MrD4dlqnPpCitAEjpoqhNfQpWY3U="
    ];
  };

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
        in
        {
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
