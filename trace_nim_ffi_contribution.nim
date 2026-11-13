## Add provisioning to the genuine sibling interface; do not replace its source.
import blake3
import repro_project_dsl
const sourceBytes = staticRead("ci/locked-nim-library-profile.nix") & "\0" & staticRead("repro.lock")
let profileIdentity = "blake3:" & blake3.toHex(blake3.digest(sourceBytes))
when defined(linux) or defined(macosx):
  provisioningFor "codetracer_trace_format_nim":
    developInterface
    contributor "traceformat-locked-nim-ffi"
    # SDK766 admits .a as a nonexecuted data anchor. Cargo still compiles the
    # actual source sibling through its unchanged build.rs and declared tools.
    nixPackage "traceWriterFfi", expressionFile = "ci/locked-nim-library-profile.nix",
      executablePath = "lib/libcodetracer_trace_writer.a", lockIdentity = profileIdentity

const direnvBytes = staticRead("ci/cross-reader-profiles.nix") & "\0" &
  staticRead("ci/native-compiler-profiles.nix") & "\0" & staticRead("flake.lock")
let direnvIdentity = "blake3:" & blake3.toHex(blake3.digest(direnvBytes))
when defined(linux) or defined(macosx):
  # SDK766 has no canonical Direnv package module to contribute to.
  # This owning tool declaration binds the actual unchanged Nix constructor.
  package direnv:
    provisioning:
      nixPackage "nativeDirenv", expressionFile = "ci/native-compiler-profiles.nix",
        executablePath = "bin/direnv", lockIdentity = direnvIdentity
