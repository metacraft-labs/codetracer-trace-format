# Source-owned runtime shared by CI acquisition and typed reader provisioning.
{ pkgs }:
let
  # Direnv evaluates the locked peer's unchanged .envrc with this exact module.
  # Per-user Direnv configuration is neither edited nor an implicit supplier.
  #
  # The URL names an upstream commit, not the moving `stable` branch that
  # https://direnv-flake-overrides.blocksense.network/plugin redirects to: the
  # bytes behind that redirect changed on 2026-11-04 and broke the fixed-output
  # fetch. Reviewed against 1bd8dca (the previous content): 630546d adds
  # NIX_FLAKE_OVERRIDE_EXCLUDE and stops falling back to path: for submodule
  # checkouts. Moving the plugin is a deliberate change of this rev and hash.
  overridePlugin = pkgs.fetchurl {
    url = "https://raw.githubusercontent.com/blocksense-network/direnv-nix-flake-overrides/630546dc5b9a75587bc37bd89752c3f6a01571f5/plugin/flake-overrides.bash";
    hash = "sha256-CMc2XpOI2l3pi9j+GKjjInH0AG9lcw4Pzapi0iM1sNI=";
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
