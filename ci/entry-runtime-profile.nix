# Exact owning lock supplier; no canonical ambient nixpkgs selector.
let
  lock = builtins.fromJSON (builtins.readFile ../flake.lock);
  fail = reason: throw ("trace native compiler lock refused: " + reason);
  resolve = node: names: seen:
    if names == [] then node else
    let key = node + ":" + builtins.concatStringsSep "/" names;
        current = if builtins.hasAttr node lock.nodes then lock.nodes.${node} else fail "unknown node";
        name = builtins.head names;
        edge = if current ? inputs && builtins.hasAttr name current.inputs then current.inputs.${name} else fail "unknown input";
        next = if builtins.elem key seen then fail "cycle"
          else if builtins.isString edge then edge
          else if builtins.isList edge && edge != [] then resolve lock.root edge (seen ++ [ key ])
          else fail "invalid follows edge";
    in resolve next (builtins.tail names) (seen ++ [ key ]);
  root = if lock.version == 7 && lock ? root && lock ? nodes then lock.root else fail "unsupported schema";
  name = resolve root [ "nixpkgs" ] [];
  supplier = lock.nodes.${name}.locked or (fail "missing locked supplier");
  source = if supplier ? type && supplier ? narHash
    && builtins.elem supplier.type [ "github" "tarball" "git" ]
    then builtins.fetchTree supplier else fail "unbound supplier";
  pkgs = import source.outPath { system = builtins.currentSystem; config = {}; overlays = []; };
  runtime = import ./cross-reader-profiles.nix { inherit pkgs; };
  peer = import ./locked-nim-library-profile.nix {};
  peerGit = peer.peerGit;
  peerNimbleSource = peer.peerNimbleSource;
in {
  entryRuntime = pkgs.writeShellScriptBin "prepare-entry-runtime" ''
    exec ${pkgs.python3}/bin/python3 -B ${./prepare-entry-runtime.py} \
      --python-profile ${pkgs.python3} --direnv-profile ${runtime.direnv} \
      --just-profile ${pkgs.just} --git ${peerGit}/bin/git --nix ${pkgs.nix}/bin/nix \
      --nimble-source ${peerNimbleSource} "$@"
  '';
}
