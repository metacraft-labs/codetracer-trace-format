# Real published Nim FFI constructor selected only by the generated workspace lock.
{ system ? builtins.currentSystem }:
let
  lock = builtins.fromTOML (builtins.readFile ../repro.lock);
  refuse = why: throw ("locked Nim library profile refused: " + why);
  # v3 (written by repro 0.2.x) drops each entry's `path`: a sibling is at
  # `../<name>` by the workspace convention, which v2 recorded explicitly.
  deps = if builtins.elem lock.schema [ "reprobuild.solved-graph-lock.v2" "reprobuild.solved-graph-lock.v3" ] && lock ? lock && lock.lock ? deps && builtins.isList lock.lock.deps
    then lock.lock.deps else refuse "unsupported generated lock";
  matches = builtins.filter (dep: dep.name == "codetracer-trace-format-nim") deps;
  dep = if builtins.length matches == 1 then builtins.head matches else refuse "nonunique source supplier";
  revision = if dep.coord_kind == "vcs"
    && dep.url == "https://github.com/metacraft-labs/codetracer-trace-format-nim"
    && (dep.path or "../codetracer-trace-format-nim") == "../codetracer-trace-format-nim"
    && builtins.isString dep.revision && builtins.match "[0-9a-f]{40}" dep.revision != null
    && dep.integrity == "git-sha1:" + dep.revision
    then dep.revision else refuse "foreign source supplier";
  supplier = builtins.getFlake ("github:metacraft-labs/codetracer-trace-format-nim/" + revision);
  packages = supplier.packages.${system} or (refuse "unsupported native system");
in {
  # The selected supplier flake itself, for the peer runtime role profiles
  # (ci/peer-runtime-role-profiles.nix). Not a derivation: building this file
  # does not realize it.
  inherit supplier;
  peerGit = packages.ci-checkout-git or (refuse "declared checkout Git constructor absent");
  peerNimble = packages.ci-nimble or (refuse "declared Nimble runtime constructor absent");
  peerNimbleSource = packages.ci-nimble-source or (refuse "declared Nimble source constructor absent");
  traceWriterFfi = packages.trace-writer-ffi or (refuse "declared static archive constructor absent");
}
