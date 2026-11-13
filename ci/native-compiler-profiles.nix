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
in {
  nativeEnv = pkgs.coreutils.out;
  nativeClang = pkgs.clang.out;
  nativeGcc = pkgs.stdenv.cc.out;
  # The archiver Nim's `clang` C compiler runs: on Linux Nim hard-codes
  # `llvm-ar rcs` for `--app:staticlib` (compiler/extccomp.nim, `llvmGcc`,
  # which `clang` inherits; no config key overrides it), while the clang
  # wrapper above carries only GNU binutils' `ar`. The C ABI parity test
  # builds the Nim archive with `--cc:clang` when `cc` is that clang. Only the
  # one executable, from the LLVM release the wrapped clang is built from,
  # so the edge receives neither LLVM's other tools nor its search paths.
  nativeLlvmAr =
    let llvm = pkgs.llvmPackages.llvm;
    in if llvm.version != pkgs.clang.version then fail "llvm-ar is not from the wrapped clang's LLVM"
    else pkgs.runCommandLocal "trace-native-llvm-ar-${llvm.version}" {} ''
      mkdir -p $out/bin
      ln -s ${llvm}/bin/llvm-ar $out/bin/llvm-ar
    '';
  nativeDirenv = (import ./cross-reader-profiles.nix { inherit pkgs; }).direnv;
}
