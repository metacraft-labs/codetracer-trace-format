# Same constructor shared by CI acquisition and typed direnv provisioning.
{ pkgs }:
{
  direnv = pkgs.symlinkJoin {
    name = "trace-cross-reader-direnv";
    paths = [ pkgs.direnv ];
    passthru.backend = pkgs.direnv;
    nativeBuildInputs = [ pkgs.makeWrapper ];
    postBuild = ''
      wrapProgram "$out/bin/direnv" \
        --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.nix pkgs.coreutils pkgs.bash pkgs.git pkgs.curl ]}
    '';
  };
}
