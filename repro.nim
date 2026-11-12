import repro_dsl_stdlib/foreign_env
## Reprobuild dev env + build recipe for codetracer-trace-format.
##
## Mirrors the dev shell declared in ``flake.nix`` (Linux/macOS).
## ``repro build`` / ``repro test`` reproduce the same artefacts and the
## same test set that ``just build`` / ``just test`` (``cargo build
## --verbose`` / ``cargo test --verbose``) produce today.
##
## Per ``codetracer-specs/Repo-Requirements.md`` §2.8 the recipe
## expresses build and test execution NATIVELY through typed-tool edges
## (``cargo.build``, ``cargo.test``). It does NOT delegate to
## ``shell(command = "bash scripts/...")`` wrappers — delegation defeats
## the engine's incremental-build, action-cache, per-test invalidation,
## and the CI sharding the engine grows into per
## ``reprobuild-specs/CI-Sharding.md``.
##
## The Rust virtual workspace keeps its in-repo Cargo members and the complete
## original whole-workspace build/test commands. Its required cross-language
## fixture producer and Nim FFI now declare the published sibling
## ``codetracer_trace_format_nim`` library as a source dependency. The owning
## generated lock binds that supplier; required producer artifacts feed the
## original Cargo test edge through explicit inputs and fixture environment.
## The FFI build.rs still compiles the actual sibling FFI entry point with the
## declared Nim/Nimble/zstd toolchain; the required fixture gate also runs both
## genuine language producers and readers. Compiler tools alone do not declare
## this source supplier.
##
## **Per-test platform gating.** ``just test`` is ``cargo test
## --verbose`` — one whole-workspace cargo run. No test FILE in this
## repo carries a per-file host gate; the only ``cfg(target_os = …)`` /
## ``cfg(unix)`` conditionals live in library ``src/`` (conditional
## compilation of platform code, not test selection), and no test is
## ``#[ignore]``d. So the corpus runs identically on every host cargo
## supports, and the single whole-workspace ``cargo.test`` execute edge
## below matches the repo's own ``just test`` one-for-one — there is no
## per-OS partition to model.
##
## **Tool provisioning.** ``defaultToolProvisioning "path"`` matches the
## canonical Rust-recorder recipes: the nix dev shell puts ``cargo`` /
## ``rustc`` / ``nim`` / ``nimble`` / ``capnp`` / ``zstd`` on ``PATH``
## (and ``PKG_CONFIG_PATH`` for libzstd), so the weak-local PATH
## resolver is the right default. Without it ``repro build`` refuses to
## run with "typed tool provisioning is required for uses declarations".

import repro_project_dsl
import std/[os, strutils]
when defined(linux) or defined(macosx):
  import trace_nim_ffi_contribution
import nimcrypto/sha2 as nativeCompilerHash

when defined(linux):
  const nativeCompilerLockIdentity =
    "trace-native-c-owning-lock-sha256:" & $nativeCompilerHash.sha256.digest(staticRead("flake.lock"))

package codetracer_trace_format:
  devEnv:
    when not defined(windows):
      useFlakeDevShell()

  defaultToolProvisioning "path"

  uses:
    when defined(linux):
      "traceformat_native_clang"
      "traceformat_native_gcc"
      "traceformat_native_llvm_ar"

    # Rust toolchain — declared by version so the tarball-direct
    # provisioning entries in repro_dsl_stdlib/packages/cargo.nim /
    # rustc.nim resolve on Windows. On Linux/macOS the nix flake
    # supplies the same versions. The floor matches the workspace
    # (edition 2024 crates need a recent stable) and the downstream
    # recorder recipes.
    "rustc >=1.85"
    "cargo >=1.85"

    # Nim toolchain — ``codetracer_trace_writer_nim``'s build.rs
    # compiles the sibling ``codetracer-trace-format-nim`` FFI entry
    # point into a static library at cargo build time via ``nim c``.
    # ``nimble`` is invoked by the same build.rs (``nimble install
    # --depsOnly``) to resolve that FFI's ``stew`` / ``results`` nimble
    # requirements.
    "nim >=2.2 <3.0"
    "nimble"
    # Real required cross-language producer source, resolved from its owning library.
    "codetracer_trace_format_nim"
    "python3 >=3.11"
    "git"
    when defined(windows):
      # ci/native-entry-provider.py and the coupled producer require it.
      "traceformat_entry_python"
    when not defined(windows):
      "direnv"
      "sh"
      "traceformat_native_env"
      "traceformat_entry_runtime"
      # The coupled runtime's tool roles; see the cargo-test-run edge.
      "traceformat_peer_nim"
      "traceformat_peer_nimble"
      "traceformat_peer_git"
      "traceformat_peer_gcc"
      "traceformat_peer_bash"
      "traceformat_peer_zstd"

    # Cap'n Proto schema compiler — ``codetracer_trace_format_capnp``'s
    # build.rs runs ``capnpc`` over ``src/trace.capnp``.
    "capnp"

    # libzstd headers + library — the CBOR+Zstd writer/reader crates
    # link libzstd (via ``zstd`` / ``zeekstd`` / ``zstd-sys``), and the
    # Nim FFI's C output ``#include``s ``zstd.h`` (build.rs threads the
    # zstd include dir onto the Nim C compile via ``DEP_ZSTD_ROOT``).
    "zstd"

    # pkg-config — ``zstd-sys`` / the zstd link step consult pkg-config
    # to find libzstd on Linux/macOS (the flake sets ``PKG_CONFIG_PATH``
    # to zstd's pkgconfig dir). Not on the Windows floor (zstd-sys
    # builds libzstd from source there).
    when not defined(windows):
      "pkg-config"

  # Library declaration — this repo is a fan-out of independent Rust
  # library crates (trace types, reader, writer, FFI, CTFS container,
  # mapping tools). Consumers depend on the individual crates by their
  # Cargo package names; the umbrella ``library`` records the workspace
  # as a consumable unit.
  library codetracer_trace_format

  build:
    # ---- Primary build edge (the `default` collection) ----------------
    #
    # Native whole-workspace cargo build (the root ``Cargo.toml`` is a
    # virtual manifest with no ``[package]``, so bare ``cargo build``
    # builds every member — exactly what ``just build`` → ``cargo build
    # --verbose`` does). Enrolled into the conventional ``default``
    # collection per reprobuild-specs/Build-Graph-Collections.md
    # §"`default`" so ``repro build`` (no positional target)
    # materialises this edge's closure.
    #
    # ``locked = true`` because the root ``Cargo.lock`` IS committed
    # (``git ls-files`` tracks it): the build must fail rather than
    # silently regenerate the lock if a member's ``Cargo.toml`` drifts
    # from the pinned resolution.
    #
    # The union of every member's source root + build.rs + the two
    # schema/header inputs is declared as ``extraInputs`` so the engine
    # tracks the whole workspace tree as the build edge's input set
    # (cargo's own ``.d`` depfiles under ``target/*/deps`` refine this
    # per-crate at action-end via the makeDepfile dependency policy the
    # cargo package declares).
    let workspaceInputs = @[
      "Cargo.toml", "Cargo.lock", "repro.lock",
      "trace_nim_ffi_contribution.nim", "ci/locked-nim-library-profile.nix",
      "ci/native-compiler-profiles.nix", "ci/cross-reader-profiles.nix", "flake.lock",
      "ci/peer-runtime-role-profiles.nix",
      "codetracer_trace_types",
      "codetracer_trace_format_capnp",
      "codetracer_trace_format_cbor_zstd",
      "codetracer_trace_reader",
      "codetracer_trace_writer",
      "codetracer_trace_writer_ffi",
      "codetracer_trace_writer_nim",
      "codetracer_trace_util",
      "trace_formatter",
      "codetracer_ctfs",
      "codetracer_trace_filter",
      "origin_patterns",
      "origin_pattern_discovery",
      "sourcemap-translate",
      "ct-mapping-tools",
      "mapping-catalog",
    ]

    let workspaceBuild = cargo.build(
      locked = true,
      actionId = "codetracer-trace-format.cargo-build",
      extraInputs = workspaceInputs)
    discard collect("default", @[workspaceBuild])

    # ---- Test-binary build + run edges (the `test` collection) -------
    #
    # Two-stage shape per Repo-Requirements.md §2.8: ``cargo.test(noRun =
    # true)`` builds every workspace test binary into
    # ``target/debug/deps/<crate>-<hash>`` (the engine tracks the deps
    # directory as the build edge's effect set because the hashed
    # filename floats with input content); the second ``cargo.test``
    # (``noRun`` defaulting to false) then runs the binaries in one
    # cargo invocation — the same whole-workspace pass ``just test`` →
    # ``cargo test --verbose`` performs. The execute edge depends on the
    # build edge so the engine only re-runs tests when an input changed
    # since the last successful execution.
    #
    # Per-test execute edges fall out automatically once the
    # ct-test-runner cargo adapter lands per
    # reprobuild-specs/Test-Edges-And-Parallel-Runner.milestones.org
    # §M4 — the whole-binary edge becomes a fan-out point without
    # changing this recipe.

    let testsBuild = cargo.test(
      locked = true,
      noRun = true,
      actionId = "codetracer-trace-format.cargo-test-build",
      after = @[workspaceBuild],
      extraInputs = workspaceInputs,
      extraOutputs = @["target/debug/deps"])

    # Real coupled artifacts are an explicit required test dependency.
    # The original cargo execute edge consumes them; no missing-fixture skip.
    let entryFixtureRoot = getCurrentDir() / "target" / "entry-fixtures"
    var entryProducerArgv = @["python3", "-B", "tests/run_entry_coupled.py",
      "--nim-environment-root", getCurrentDir().parentDir / "codetracer-trace-format-nim",
      "--prepare-only", "--fixture-output", entryFixtureRoot]
    when defined(windows):
      # The Windows counterpart of the Unix `entry-runtime` edge below: the
      # native Nim/Clang/Zstd frontend the coupled producer runs, built by
      # ci/native-entry-provider.py for the supplier revision repro.lock pins
      # (the script reads it with ci/select-entry-nim-supplier.py, the
      # selector windows-tests.yml uses). Its output directory is opaque
      # (`removeBeforeRun`): the script refuses an existing one, so the
      # engine empties it before every run. The producer must run under the
      # very Python the provider recorded, so both edges take the declared
      # `traceformat_entry_python` ahead of every other `python3`.
      let windowsProviderRoot = ".repro/entry-native-provider"
      let windowsProvider = buildAction(
        id = "codetracer-trace-format.entry-native-provider",
        call = inlineExecCall(@["python3", "-I", "-B", "ci/native-entry-provider.py",
          "--output", windowsProviderRoot,
          "--nim-supplier", "../codetracer-trace-format-nim",
          "--repro-lock", "repro.lock"]),
        deps = @[workspaceBuild.id],
        inputs = @["repro.lock", "ci/native-entry-provider.py",
          "ci/select-entry-nim-supplier.py", "../codetracer-trace-format-nim"],
        outputs = @[windowsProviderRoot],
        removeBeforeRun = @[windowsProviderRoot],
        toolIdentityRefs = @["traceformat_entry_python", "git"])
      entryProducerArgv.add(["--windows-provider",
        windowsProviderRoot / "frontend-component.json"])
    else:
      entryProducerArgv.add(["--direnv-from-unix-authority"])
    let unixRuntimeRoot = ".repro/build/entry-runtime"
    when not defined(windows):
      let runtimePreparation = buildAction(
        id = "codetracer-trace-format.entry-runtime",
        call = inlineExecCall(@["prepare-entry-runtime", "--root", ".",
          "--output", unixRuntimeRoot]),
        deps = @[workspaceBuild.id],
        inputs = @["repro.lock", "flake.lock", "ci/prepare-entry-runtime.py",
          "ci/prepare-cross-reader-runtime.py", "ci/generated-runtime-namespace.py",
          "ci/entry-runtime-profile.nix", "ci/cross-reader-profiles.nix",
          "ci/locked-nim-library-profile.nix", "../codetracer-trace-format-nim"],
        outputs = @[unixRuntimeRoot],
        toolIdentityRefs = @["traceformat_entry_runtime"])
      entryProducerArgv.add(["--unix-runtime-authority",
        unixRuntimeRoot / "runtime/peer-runtime-proof.json"])
      # The source-owned runtime producer creates the exact locked peer.
      let peerArg = entryProducerArgv.find("--nim-environment-root")
      if peerArg < 0: raise newException(ValueError, "Declared Nim environment argument absent")
      entryProducerArgv[peerArg + 1] = unixRuntimeRoot / "codetracer-trace-format-nim"
    let entryFixtures = buildAction(
      id = "codetracer-trace-format.entry-fixtures",
      call = inlineExecCall(entryProducerArgv),
      deps = (when defined(windows): @[workspaceBuild.id, windowsProvider.id]
        else: @[workspaceBuild.id, runtimePreparation.id]),
      inputs = workspaceInputs & @["repro.lock", "tests/entry_coupled.nim",
        "tests/run_entry_coupled.py", "tests/publish_entry_fixtures.py",
        "../codetracer-trace-format-nim"] & (when defined(windows): @[windowsProviderRoot]
        else: @[unixRuntimeRoot / "runtime/peer-runtime-proof.json"]),
      outputs = @[entryFixtureRoot / "marked.ct", entryFixtureRoot / "unmarked.ct",
        entryFixtureRoot / ".producer-owner"],
      toolIdentityRefs = (when defined(windows):
        @["traceformat_entry_python", "git", "nim", "nimble", "cargo", "rustc"]
      else:
        @["python3", "git", "nim", "nimble", "cargo", "rustc", "direnv"]))

    # The reload cross-writer and BigInt tests run the production Nim reader.
    # The entry-runtime constructor already built `ct-print` in the locked
    # peer, so the tests are pointed at it (`just test` reaches the same reader
    # through tests/run_entry_coupled.py). Windows has no such peer yet.
    #
    # The Nim cross-implementation tests (`nim_adjudicator` and its users)
    # build and run the production Nim reader with the coupled runtime's own
    # toolchain. They take it when `ENTRY_NIM_RUNTIME_AUTHORITY` names the
    # constructor's complete authority and `PATH` selects, for each of its
    # roles (nim, nimble, git, gcc, bash), exactly the image the authority
    # records; `just test` provides both by running cargo inside the
    # activated peer. Here the edge declares them: the authority is an input,
    # and the `traceformat_peer_*` tools below are the packages the locked
    # peer's dev shell puts on its PATH, appended to this action's tool refs
    # ahead of every other directory holding an executable of a role's name.
    # The peer's dev shell also supplies the libzstd the Nim reader links;
    # `traceformat_peer_zstd` gives the edge that library's search paths. Its
    # Nim package requirements are the constructor's prepared seed in the
    # peer's `.nimble` (the authority's expectedSeedInstalledPackages), which
    # `NIMBLE_DIR` names exactly as the peer's shell does for `just test`.
    var testRunInputs = workspaceInputs & @["target/debug/deps", entryFixtureRoot]
    var testRunEnv = @[("ENTRY_NIM_FIXTURE_ROOT", entryFixtureRoot)]
    when not defined(windows):
      let peerCtPrint = getCurrentDir() / unixRuntimeRoot / "codetracer-trace-format-nim" / "ct-print"
      testRunInputs.add(peerCtPrint)
      testRunEnv.add(("CODETRACER_TRACE_FORMAT_NIM_CT_PRINT", peerCtPrint))
      let peerRuntimeAuthority = getCurrentDir() / unixRuntimeRoot / "runtime/peer-runtime-proof.json"
      testRunInputs.add(peerRuntimeAuthority)
      testRunEnv.add(("ENTRY_NIM_RUNTIME_AUTHORITY", peerRuntimeAuthority))
      let peerNimbleDir = getCurrentDir() / unixRuntimeRoot / "codetracer-trace-format-nim" / ".nimble"
      testRunInputs.add(peerNimbleDir)
      testRunEnv.add(("NIMBLE_DIR", peerNimbleDir))
    let testsRun = cargo.test(
      locked = true,
      actionId = "codetracer-trace-format.cargo-test-run",
      after = @[testsBuild.action, entryFixtures],
      extraInputs = testRunInputs,
      extraEnv = testRunEnv)

    when not defined(windows):
      # PATH is searched in tool-ref order, so the role images must precede
      # the `traceformat_native_gcc`, `nim`, `nimble`, `git` and `sh` (a bash)
      # directories. The native clang directory stays first: it holds `cc`,
      # which the peer's gcc directory also holds, and no role name.
      when defined(linux):
        appendRegisteredActionToolIdentityRefs(testsRun.action.id,
          ["traceformat_native_clang"])
        # The C ABI parity test (codetracer_trace_writer_ffi/tests/
        # c_abi_parity.rs) has Nim build its archive with the family of this
        # edge's `cc`, the native clang, and Nim's `clang` archives with
        # `llvm-ar` on Linux, which that clang's wrapper does not carry.
        appendRegisteredActionToolIdentityRefs(testsRun.action.id,
          ["traceformat_native_llvm_ar"])
      appendRegisteredActionToolIdentityRefs(testsRun.action.id,
        ["traceformat_peer_nim", "traceformat_peer_nimble", "traceformat_peer_git",
         "traceformat_peer_gcc", "traceformat_peer_bash", "traceformat_peer_zstd"])

    when defined(linux):
      for action in [workspaceBuild, testsBuild.action, testsRun.action]:
        appendRegisteredActionToolIdentityRefs(action.id,
          ["traceformat_native_clang", "traceformat_native_gcc",
           "nim", "nimble", "git", "capnp", "zstd", "pkg-config"])

    when not defined(windows):
      # Original cross-read and durability tests spawn these declared tools.
      appendRegisteredActionToolIdentityRefs(testsRun.action.id,
        ["direnv", "sh", "traceformat_native_env"])

    discard collect("test", @[testsRun.action])

when defined(linux):
  package traceformat_native_clang:
    provisioning:
      nixPackage "nativeClang", executablePath = "bin/clang",
        expressionFile = "ci/native-compiler-profiles.nix",
        lockIdentity = nativeCompilerLockIdentity & ":clang"
  package traceformat_native_gcc:
    provisioning:
      nixPackage "nativeGcc", executablePath = "bin/gcc",
        expressionFile = "ci/native-compiler-profiles.nix",
        lockIdentity = nativeCompilerLockIdentity & ":gcc"
  package traceformat_native_llvm_ar:
    provisioning:
      nixPackage "nativeLlvmAr", executablePath = "bin/llvm-ar",
        expressionFile = "ci/native-compiler-profiles.nix",
        lockIdentity = nativeCompilerLockIdentity & ":llvm-ar"

when defined(windows):
  # The Python the Windows coupled-entry gate requires: ci/native-entry-provider.py
  # refuses any other than 3.13.16 and records the image, and
  # tests/run_entry_coupled.py then refuses to run under a different one.
  # windows-tests.yml installs the same release with actions/setup-python.
  # python.org's embeddable builds; the sha256 values are the ones python.org
  # publishes for these files (www.python.org/api/v2/downloads/release_file/).
  package traceformat_entry_python:
    provisioning:
      tarball url = "https://www.python.org/ftp/python/3.13.16/python-3.13.16-embed-amd64.zip",
        sha256 = "97dae5274cc54867065e8d5a3226e48c35017ed332a0fdb0e27d5b5821961297",
        archiveType = "zip",
        executablePath = "python.exe",
        executableAlias = "python3.exe",
        packageId = "python@3.13.16",
        cpu = "x86_64",
        os = "windows",
        lockIdentity = "tarball:python@3.13.16:windows-x86_64:sha256:97dae5274cc54867065e8d5a3226e48c35017ed332a0fdb0e27d5b5821961297",
        version = "3.13.16"
      tarball url = "https://www.python.org/ftp/python/3.13.16/python-3.13.16-embed-arm64.zip",
        sha256 = "790d097697a2020477549a6764d89d22194c88d6e42caebcb2db63791e004c94",
        archiveType = "zip",
        executablePath = "python.exe",
        executableAlias = "python3.exe",
        packageId = "python@3.13.16",
        cpu = "aarch64",
        os = "windows",
        lockIdentity = "tarball:python@3.13.16:windows-aarch64:sha256:790d097697a2020477549a6764d89d22194c88d6e42caebcb2db63791e004c94",
        version = "3.13.16"

when not defined(windows):
  const crossReaderProcessLockIdentity =
    "trace-cross-reader-process-owning-lock-sha256:" & $nativeCompilerHash.sha256.digest(staticRead("flake.lock"))
  package traceformat_native_env:
    provisioning:
      nixPackage "nativeEnv", expressionFile = "ci/native-compiler-profiles.nix",
        executablePath = "bin/env", lockIdentity = crossReaderProcessLockIdentity

when not defined(windows):
  # The coupled runtime's role images, selected from the locked peer's own
  # dev shell (ci/peer-runtime-role-profiles.nix, keyed by it, the profile
  # that selects the peer, and the lock that pins it).
  const peerRuntimeRoleIdentity = "trace-peer-runtime-role-sha256:" &
    $nativeCompilerHash.sha256.digest(staticRead("ci/peer-runtime-role-profiles.nix") &
      staticRead("ci/locked-nim-library-profile.nix") & staticRead("repro.lock"))
  package traceformat_peer_nim:
    provisioning:
      nixPackage "peerRoleNim", expressionFile = "ci/peer-runtime-role-profiles.nix",
        executablePath = "bin/nim", lockIdentity = peerRuntimeRoleIdentity & ":nim"
  package traceformat_peer_nimble:
    provisioning:
      nixPackage "peerRoleNimble", expressionFile = "ci/peer-runtime-role-profiles.nix",
        executablePath = "bin/nimble", lockIdentity = peerRuntimeRoleIdentity & ":nimble"
  package traceformat_peer_git:
    provisioning:
      nixPackage "peerRoleGit", expressionFile = "ci/peer-runtime-role-profiles.nix",
        executablePath = "bin/git", lockIdentity = peerRuntimeRoleIdentity & ":git"
  package traceformat_peer_gcc:
    provisioning:
      nixPackage "peerRoleGcc", expressionFile = "ci/peer-runtime-role-profiles.nix",
        executablePath = "bin/gcc", lockIdentity = peerRuntimeRoleIdentity & ":gcc"
  package traceformat_peer_bash:
    provisioning:
      nixPackage "peerRoleBash", expressionFile = "ci/peer-runtime-role-profiles.nix",
        executablePath = "bin/bash", lockIdentity = peerRuntimeRoleIdentity & ":bash"
  package traceformat_peer_zstd:
    provisioning:
      nixPackage "peerZstdLibrary", expressionFile = "ci/peer-runtime-role-profiles.nix",
        executablePath = "bin/zstd", lockIdentity = peerRuntimeRoleIdentity & ":zstd"

when not defined(windows):
  const entryRuntimeSourceIdentity = "trace-entry-runtime-source-sha256:" &
    $nativeCompilerHash.sha256.digest(staticRead("ci/entry-runtime-profile.nix") &
      staticRead("ci/prepare-entry-runtime.py") &
      staticRead("ci/prepare-cross-reader-runtime.py") &
      staticRead("ci/generated-runtime-namespace.py") &
      staticRead("ci/cross-reader-profiles.nix") &
      staticRead("ci/locked-nim-library-profile.nix") & staticRead("flake.lock") &
      staticRead("repro.lock"))
  package traceformat_entry_runtime:
    provisioning:
      nixPackage "entryRuntime", expressionFile = "ci/entry-runtime-profile.nix",
        executablePath = "bin/prepare-entry-runtime",
        lockIdentity = entryRuntimeSourceIdentity
