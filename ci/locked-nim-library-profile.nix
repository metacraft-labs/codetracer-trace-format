# Real published Nim FFI constructor selected only by the generated workspace lock.
{ system ? builtins.currentSystem }:
let
  lock = builtins.fromTOML (builtins.readFile ../repro.lock);
  refuse = why: throw ("locked Nim library profile refused: " + why);
  deps = if lock.schema == "reprobuild.solved-graph-lock.v2" && lock ? lock && lock.lock ? deps && builtins.isList lock.lock.deps
    then lock.lock.deps else refuse "unsupported generated lock";
  matches = builtins.filter (dep: dep.name == "codetracer-trace-format-nim") deps;
  dep = if builtins.length matches == 1 then builtins.head matches else refuse "nonunique source supplier";
  revision = if dep.coord_kind == "vcs"
    && dep.url == "https://github.com/metacraft-labs/codetracer-trace-format-nim"
    && dep.path == "../codetracer-trace-format-nim"
    && builtins.isString dep.revision && builtins.match "[0-9a-f]{40}" dep.revision != null
    && dep.integrity == "git-sha1:" + dep.revision
    then dep.revision else refuse "foreign source supplier";
  supplier = builtins.getFlake ("github:metacraft-labs/codetracer-trace-format-nim/" + revision);
  packages = supplier.packages.${system} or (refuse "unsupported native system");
in {
  traceWriterFfi = packages.trace-writer-ffi or (refuse "declared static archive constructor absent");
}
