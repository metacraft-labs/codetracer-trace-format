# Source-owned runtime shared by CI acquisition and typed reader provisioning.
{ pkgs }:
let
  # Direnv evaluates the locked peer's unchanged .envrc with this exact module.
  # Per-user Direnv configuration is neither edited nor an implicit supplier.
  overridePlugin = pkgs.fetchurl {
    url = "https://direnv-flake-overrides.blocksense.network/plugin";
    hash = "sha256-ixCgoLDPJelkuT5gukgffNxg9edSZEfVOK5QciktI74=";
  };
  runtimeConfig = pkgs.writeTextDir "direnvrc" ''
    source ${pkgs.nix-direnv}/share/nix-direnv/direnvrc
  '';
in
{
  direnv = pkgs.symlinkJoin {
    name = "trace-cross-reader-direnv";
    paths = [ pkgs.direnv ];
    passthru = {
      backend = pkgs.direnv;
      inherit runtimeConfig;
      nixDirenv = pkgs.nix-direnv;
      inherit overridePlugin;
    };
    nativeBuildInputs = [ pkgs.makeWrapper ];
    postBuild = ''
      mkdir -p "$out/share/trace-cross-reader"
      ln -s ${overridePlugin} "$out/share/trace-cross-reader/flake-overrides.bash"
      ln -s ${runtimeConfig} "$out/share/trace-cross-reader/config"
      ln -s ${pkgs.nix-direnv}/share/nix-direnv/direnvrc "$out/share/trace-cross-reader/nix-direnvrc"
      wrapProgram "$out/bin/direnv" \
        --set DIRENV_CONFIG ${runtimeConfig} \
        --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.nix pkgs.coreutils pkgs.bash pkgs.git pkgs.curl pkgs.jq ]}
    '';
  };
}
