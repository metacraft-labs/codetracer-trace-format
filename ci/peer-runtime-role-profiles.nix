# The coupled runtime's tool roles, as the locked supplier's own dev shell
# declares them, for the typed test edge that runs the Nim cross-implementation
# tests (repro.nim, `codetracer-trace-format.cargo-test-run`).
#
# ci/prepare-cross-reader-runtime.py records, for each role (nim, nimble, git,
# gcc, bash), the image that shell's PATH selects: the runtime authority's
# expectedSelectedImages. The tests accept the declared runtime only when the
# edge's PATH selects exactly those images. Each package here must be one of
# the shell's own packages, so this cannot pick a different constructor than
# the shell does; which of them the shell's PATH reaches first is checked by
# the tests themselves against the authority.
#
# A separate file from locked-nim-library-profile.nix because Reprobuild
# realizes an expression file whole and gives every tool declared from it the
# search paths of all its outputs: the edges that only consume the FFI archive
# must not receive these, and this edge must not receive the archive's.
{ system ? builtins.currentSystem }:
let
  refuse = why: throw ("peer runtime role profile refused: " + why);
  supplier = (import ./locked-nim-library-profile.nix { inherit system; }).supplier;
  packages = supplier.packages.${system} or (refuse "unsupported native system");
  shell = supplier.devShells.${system}.default or (refuse "declared dev shell constructor absent");
  supplierPkgs = supplier.inputs.nixpkgs.legacyPackages.${system} or (refuse "declared supplier package set absent");
  toolchains = supplier.inputs.codetracer-toolchains.packages.${system} or (refuse "declared supplier toolchain set absent");
  # mkShell lists each package by its development output; the role image is
  # in the package's own output, which that development output propagates.
  shellRole = role: package:
    if builtins.any (input: input.outPath == (supplierPkgs.lib.getDev package).outPath) shell.nativeBuildInputs
    then package else refuse ("declared " + role + " role is not a dev shell package");
in {
  peerRoleNim = shellRole "nim" (toolchains.nim-2_2 or (refuse "declared Nim constructor absent"));
  peerRoleNimble = shellRole "nimble" (packages.ci-nimble or (refuse "declared Nimble runtime constructor absent"));
  peerRoleGit = shellRole "git" (packages.ci-checkout-git or (refuse "declared checkout Git constructor absent"));
  peerRoleGcc = shellRole "gcc" supplierPkgs.gcc;
  peerRoleBash = shellRole "bash" supplierPkgs.bash;
  # The shell's libzstd, which the Nim reader links (`-lzstd`) and the shell
  # supplies through its compiler search flags. Joined with its headers and
  # executable, so the edge's header and library search paths reach the same
  # files.
  peerZstdLibrary =
    let zstd = shellRole "zstd" supplierPkgs.zstd;
    in supplierPkgs.symlinkJoin {
      name = "trace-peer-zstd-library";
      paths = [ zstd.bin zstd.dev zstd.out ];
    };
}
