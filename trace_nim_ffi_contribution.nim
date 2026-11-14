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
