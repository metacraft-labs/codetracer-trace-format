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
## **This repo is a LEAF (Rust virtual workspace).** Every member crate
## in the root ``Cargo.toml`` ``[workspace] members`` list resolves
## against another member of the same workspace (``path = "..."`` deps,
## all in-repo) — there are NO ``../..`` sibling ``path`` deps and no
## ``git`` deps, so there is no from-source ``uses: "<sibling>"`` edge.
## The ``uses:`` block is the toolchain floor only. (See the note on
## ``codetracer_trace_writer_nim`` below for the ONE cross-repo input,
## which is consumed inside a cargo build.rs, not via reprobuild's
## library-threading mechanism.)
##
## **Cross-repo note — the Nim FFI static library.** The
## ``codetracer_trace_writer_nim`` member crate's ``build.rs`` compiles
## the sibling ``codetracer-trace-format-nim`` repo's FFI entry point
## (``src/codetracer_trace_writer_ffi.nim``) into a native static
## library via a direct ``nim c --app:staticlib`` call, and resolves
## that repo at ``../../codetracer-trace-format-nim`` (override:
## ``CODETRACER_TRACE_FORMAT_NIM_DIR``). Because that ``nim c`` runs
## INSIDE cargo's build.rs — out of reprobuild's reach — it is NOT a
## reprobuild ``uses:``-library consumption (the SC-11 develop-mode
## src-threading only applies to reprobuild's OWN ``nim.c`` edges, and
## the target isn't in the AVAILABLE landed-sibling set anyway). The
## build.rs handles its own Nim toolchain needs: it runs ``nimble
## install --depsOnly`` to fetch the ``.nimble``'s ``stew`` / ``results``
## requirements into the global nimble store, then invokes ``nim c``.
## So the toolchain floor for THIS repo mirrors exactly what the
## downstream recorder repos (evm, cairo, …) already declare for the
## identical ``codetracer_trace_writer_nim`` dependency: ``nim`` +
## ``nimble`` + ``capnp`` + ``zstd`` on top of the Rust toolchain.
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

package codetracer_trace_format:
  defaultToolProvisioning "path"

  uses:
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
      "Cargo.toml", "Cargo.lock",
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

    let testsRun = cargo.test(
      locked = true,
      actionId = "codetracer-trace-format.cargo-test-run",
      after = @[testsBuild.action],
      extraInputs = workspaceInputs & @["target/debug/deps"])

    discard collect("test", @[testsRun.action])
