# Writer alignment log

Running log. Appended after every step. Measurements only — a claim without a
command behind it does not belong here.

## Step 0 — the operative spec revision

`codetracer-trace-format-spec` has **two divergent lineages** sharing merge-base
`cb3e460`:

| ref | tip | column format present? |
|---|---|---|
| `origin/latest` (repo default HEAD) | `f786ab2` | **no** |
| `origin/dev` == `origin/main` | `ff95fe4` | **yes** |
| working tree / `blocktracer` | `b04fcc6` (on the `latest` lineage) | **no** |

Measured:

```
$ git grep -c 'global_position\|line_lengths\|Layout A' <rev>
b04fcc6        0 files      origin/latest  0 files
origin/dev     2 files      origin/main    2 files
```

`origin/latest..origin/dev` = 6 commits, `origin/dev..origin/latest` = 8. They
have genuinely forked; neither contains the other.

**Operative revision for this work: `ff95fe4`** (tip of `origin/dev` and
`origin/main`). It is the only revision that defines the column format the two
writers implement. The checked-out `b04fcc6` predates it and has zero hits for
`global_position`, `line_lengths` or `Layout A`, exactly as the brief said.

Column-format commits, all on the `dev`/`main` lineage only:

```
f4f1510 P6.1: extend trace-events spec with column-aware addressing
f9a3542 P6.3: lock DeltaColumn (tag 0x05) as the column encoding
d9f3588 P6.3 correction: shift DeltaColumn from 0x05 to 0x07
d633e94 spec(internal-files): document FLAG bit 4 + 5 + source_views.dat
23f4e37 spec: align srcviews.dat filename with base40 12-char limit
ff95fe4 spec: document column-aware navigation campaign deliverables
```

`b04fcc6 -> ff95fe4` touches 3 files: `internal-files.md` (+230/-…),
`trace-events.md` (+467/-…), `fixtures/README.md`.

## Step 1 — baseline

Toolchain: neither `cargo-nextest` nor `capnp` is on PATH; the workspace does
not build without the nix devshell (`codetracer_trace_format_capnp`'s build
script shells out to `capnp`). All runs below use
`nix develop --command …`. `cargo-nextest` is **not** in the devshell, so the
suite is run with `cargo test`.

```
$ nix develop --command cargo test --workspace --no-fail-fast
passed=353 failed=0 ignored=0   across 64 test targets
```

353/0 matches the brief's figure exactly. The brief's "63 binaries" is nextest's
count, which excludes the doc-test target; 63 binaries + 1 doc-test target = the
64 `test result:` lines seen here. Baseline reconciled.

## Step 2 — scoping discovery: the Nim writer is in another repo

`codetracer_trace_writer_nim/build.rs` compiles the Nim static library from the
**sibling repo** `/home/zahary/m/blocktracer/codetracer-trace-format-nim`
(overridable with `CODETRACER_TRACE_FORMAT_NIM_DIR`). There is exactly one
`.nim` file in `codetracer-trace-format` itself (`repro.nim`, a build script).

So "the two writers" do not both live in the repo I was pointed at:

| writer | source of truth | repo |
|---|---|---|
| Rust (Path A) | `codetracer_trace_writer/src/*.rs` | `codetracer-trace-format` (authorised) |
| Nim (reference) | `src/codetracer_trace_writer/*.nim` | `codetracer-trace-format-nim` (**not** mentioned in the brief) |

`codetracer-trace-format-nim` is on branch `blocktracer`, clean, tip `7296a09`.
Consequence recorded here before any change: every divergence whose correct
resolution is "the Nim writer is wrong" lands **outside** the repo I was
authorised to commit to. Verdicts below therefore carry a "which repo" column.

## Step 3 — the divergence list is longer than the brief's four reasons

The brief describes four enumerated reasons for excluding whole-container
equality. The file's `KNOWN_DIVERGENCES` constant actually carries **six**
entries, and three of them are real encoder disagreements rather than file-set
bookkeeping:

| file | brief's framing | what it actually is |
|---|---|---|
| `meta.dat` | reason 2 (UUIDv7) | partly inherent, partly a real field-set difference |
| `calls.dat` | not mentioned | **real**: `first_step_id` off by one |
| `funcs.dat` | not mentioned | **real**: record shape |
| `funcs.off` | not mentioned | follows `funcs.dat` |
| `types.dat` | not mentioned | **real**: Nim auto-registers `type_0`, Rust does not |
| `types.off` | not mentioned | follows `types.dat` |

Measured record shapes for `funcs.dat`:

* Rust — `codetracer_trace_writer/src/interning_tables.rs:92`
  `encode_func_record` = `varint(global_line_index) ++ varint(name_len) ++ name`.
* Nim — `src/codetracer_trace_writer/interning_table.nim:42` `ensureId` appends
  **bare name bytes**, and it is one generic `InterningTableWriter` shared by
  paths / funcs / types / varnames alike.
  `multi_stream_writer.nim:529` `registerFunction(w, name: string)` takes only a
  name — it has no `(path_id, line)` to pack, so the Nim table *cannot*
  represent the Rust record without an API change.

That asymmetry (Rust: four per-table record shapes; Nim: one generic bare-bytes
table) is the root of the `funcs`/`types` rows. Which side is wrong is a
question for the spec at `ff95fe4`, pending.

## Step 4 — `events.log` is not a spare wheel

`ctfs_writer.rs:888` adds `events.log` as the **first** file of every container
and it is the primary sink of `add_event`. The Rust reader path
`codetracer_trace_reader::create_trace_reader(Ctfs).load_trace_events()` reads
it — used, among others, by `ctfs_writer.rs`'s own tests at lines 1220 and 1285.

So "delete the legacy surface" is not a one-line removal of a redundant sidecar:
`events.log` currently carries the only full event stream the Rust writer emits,
and the split streams (`steps.dat` etc.) are additive on top of it. Deleting it
means the split streams must first be shown to carry everything it carries.
Consumer census pending.

## Step 5 — the probe: measuring every divergence instead of quoting it

Temporary test `codetracer_trace_writer_nim/tests/probe_meta.rs` drives the
census fixture through both writers with the **same program name** (`"same"`) in
two different directories, then dumps the bytes. The existing differential uses
*different* program names (`census_nim` / `census_rust`), which is itself one of
the reasons `meta.dat` "differs".

### File sets (the whole census, measured)

```
NIM  (17): calls.dat calls.idx events.dat events.idx funcs.dat funcs.off
           meta.dat paths.dat paths.off steps.dat steps.idx types.dat
           types.off values.dat values.idx varnames.dat varnames.off
RUST (21): the same 17, plus events.fmt events.log meta.json paths.json
```

Two corrections to the test file's header:

* **Reason 3 is false.** "The Nim writer emits files of its own
  (`step-map.ns`, the span/linehit tables, …)" — on this fixture the Nim
  container emits **no file the Rust one lacks**. `NIM_ONLY` is empty and the
  measurement says it is genuinely empty, not merely unpopulated. Those Nim
  files exist only when a recorder opts into spans / linehits / step-map, which
  neither writer does here.
* **Reason 4 therefore dissolves entirely** once the four Rust-only files go:
  the file sets become identical, so CTFS block allocation has nothing left to
  differ about.

### `calls.dat` — one byte

```
NIM  28 b5 2f fd 20 0b 59 00 00 0a 00 01 03 0b 00 00 01 ff 00 00
RUST 28 b5 2f fd 20 0b 59 00 00 0a 00 01 02 0b 00 00 01 ff 00 00
                                        ^^ ^^
```

Byte 12 only: Nim `03`, Rust `02` — `first_step_id`. Everything else, including
the Zstd frame header, is identical. The declared reason ("NOT a framing
difference") is correct and the divergence is exactly the off-by-one.

`call_stream.rs:326` `current_step_id()` returns `step_index - 1`, i.e. the index
of the last step **already emitted**. The census registers the call after step 2,
so Rust records 2 and Nim records 3. Read plainly, `first_step_id` is the first
step *of* the call — the next one to arrive, i.e. 3. Nim looks right; pending the
spec.

### `funcs.dat` — record shape, and the Nim table cannot express it

```
NIM  66                          = "f"                      (bare name bytes)
RUST 81 80 80 80 10 01 66        = varint(gli) varint(1) "f"
```

`81 80 80 80 10` decodes to `0x1_0000_0001` = `pack_global_line_index(path_id=1,
line=1)`. So Rust writes the spec layout its module docstring quotes and Nim
writes bare bytes.

Root cause, measured: Nim has **one generic** `InterningTableWriter`
(`interning_table.nim:42`) shared by paths / funcs / types / varnames, and it
appends bare bytes. The information is not lost at the FFI boundary though —
`codetracer_trace_writer_ffi.nim:653` `trace_writer_ensure_function_id` **does**
receive `(name, path, line)` and stores them in a `FunctionEntry`, then throws
them away at the interning call:

```nim
  # Key on name only so the FFI ID-space agrees with the multi-stream
  # writer's `registerFunction`, which interns by name.
  discard handle.msWriter.registerFunction(n)   # name only
```

So aligning `funcs.dat` is possible but lands in `codetracer-trace-format-nim`.

### `types.dat` — an invention, and it is in the authorised repo

```
NIM  74 79 70 65 5f 30 = "type_0"
RUST (empty)
```

Source: **`codetracer_trace_writer_nim/src/lib.rs:1609`**, the Rust *wrapper*
around the Nim FFI, synthesises a name for a value that carries only a
`TypeId`:

```rust
let type_name = str_to_cstring(&format!("type_{}", type_id.0));
```

The fixture's `ValueRecord::Int { type_id: TypeId(0) }` references a type nobody
registered. The pure-Rust writer leaves `types.dat` empty; the wrapper invents
`type_0` and the Nim backend interns it. This one is inside the repo I may
change.

### `meta.dat` — three causes, and the declared reason is wrong

```
NIM  CTMD 03 00 | flags 0f10 | 24 <uuid> | 04 "same" | 00 args | 00 workdir | 00 recorder | 03 <paths>
RUST CTMD 03 00 | flags 1f10 | 24 <uuid> | 04 "same" | 00 args | 4e "/home/zahary/m/blocktracer/…" | …
```

`KNOWN_DIVERGENCES` says *"the Rust header carries program/args fields the Nim
one does not"*. **That is false.** Both encoders write the identical field
sequence — magic, u16 version, u16 flags, recording_id, program, args count +
args, workdir, recorder_id, paths count + paths — and on this probe both wrote
`04 "same"` for program and `00` for args. Verified against
`meta_dat.rs:173` `encode_meta_dat` and `meta_dat.nim:440-509` `writeMetaDat`
side by side.

The three actual causes:

1. **`recording_id`** — a fresh UUIDv7 each run. Genuinely inherent; needs
   seeding.
2. **`workdir`** — Nim writes `""`, Rust writes the real cwd. Also makes Rust's
   `meta.dat` machine-dependent.
3. **flags `0x0f10` vs `0x1f10`**, differing in `0x1000` = **bit 12 =
   `FLAG_HAS_INTERNING_TABLES`**. The bit constants are identical in both
   (`meta_dat.rs:104`, `meta_dat.nim:186`). The Nim writer **emits all four
   interning tables and then fails to stamp the flag**:
   `multi_stream_writer.nim:1298` passes `hasCallStream/hasStepStream/
   hasValueStream/hasIoEventStream/hasSpanStream` and simply never passes
   `hasInterningTables`, so it defaults to `false`. A container that has the
   tables declares it does not. That is a Nim writer bug, not an inherent
   difference.

So of `meta.dat`'s three causes only one is inherent; the brief's hypothesis
(seed the id and `meta.dat` becomes comparable) is right, but two real defects
have to be fixed first.

## Step 6 — what the spec at `ff95fe4` actually requires

Read at `ff95fe4` (whole tree is 7 files: `README.md`, `Trace-Filters.md`,
`ctfs-container.md`, `fixtures/README.md`, `internal-files.md`,
`seekable-zstd.md`, `trace-events.md`).

### 6.1 The legacy surface is spec-confirmed legacy

`events.log`, `events.fmt`, `meta.json`, `paths.json` appear **only** in
`fixtures/README.md:36-39`, describing the shipped fixture
`fixtures/minimal_trace.ct` — which is a pre-redesign 4-file container holding
exactly those four files and none of `meta.dat` / `steps.dat` / `paths.dat`.
They appear nowhere in `internal-files.md`, `trace-events.md`,
`ctfs-container.md` or `seekable-zstd.md`.

`meta.json` is affirmatively historical — `internal-files.md:437` puts it in the
**removed** v1 window — and `ctfs-container.md:14` states the container property

> "Self-contained | All metadata in binary format within the container; **no
> external files or JSON**."

So the four files are not merely unrequired, they are contrary to a stated
container property. **Verdict: delete from the Rust writer.** (Blocked on the
consumer census, still running.)

### 6.2 `funcs.dat` — Rust is right, Nim is wrong

`internal-files.md:46`:

> | Functions | `funcs.dat` | `funcs.off` | global_line_index: varint, name_len: varint, name: bytes |

That is exactly `encode_func_record` in `interning_tables.rs:92`. The Nim table
writes bare name bytes and no `global_line_index` at all — it matches neither
this nor the looser `trace-events.md:16` phrasing ("global_line_index (varint) +
name (bytes)"), which still requires the `global_line_index`.

**Verdict: `funcs.dat`/`funcs.off` — the Nim writer is out of spec.** Fix lands
in `codetracer-trace-format-nim`.

(Spec defect to report upstream: `internal-files.md:46` and
`trace-events.md:16,195` disagree on whether `name_len` is present. Rust follows
the former. The field is redundant given `funcs.off`, but it is what the more
specific document says.)

### 6.3 `types.dat` — Rust is right, and the `type_0` name is an invention

`internal-files.md:45`:

> | Types | `types.dat` | `types.off` | kind: u8, lang_type_len: varint, lang_type: bytes, specific_info: binary |

`encode_type_record` (`interning_tables.rs:101`) matches. Nim writes the bare
string `type_0` — not a `kind` byte, not a length prefix, not a `specific_info`
blob.

On auto-registration the spec is **silent**: no rule that a writer must invent a
type when a value carries a `type_id` nobody registered, no reserved type ids,
no defined behaviour for a dangling `type_id`. The recorder-side pseudocode at
`trace-events.md:383` has the *caller* call `ensure_type_id(writer, "Int")`
explicitly.

**Verdict: `types.dat` — the `format!("type_{}", type_id.0)` synthesis at
`codetracer_trace_writer_nim/src/lib.rs:1609` is one writer's invention with no
spec basis. Delete it.** That file is in the authorised repo.

### 6.4 `calls.dat` `first_step_id` — the spec does not say

The entire normative content is `trace-events.md:161`:

> | `first_step_id` | varint | **First step in this call** |

plus the closed-interval notation `[first_step_id, last_step_id]` used by the
interpolation-search recipe (`trace-events.md:45`, `internal-files.md:112`).

The spec never states whether a call registered after step N records N (the last
step before entry) or N+1 (the first step inside). It also never states range
inclusivity, the nested-range tie-break, or the zero-step case.

The one adjacent passage, `trace-events.md:1032`, is a **bug report** rather than
a rule — `ct_print` drops a `call_entry` whose step index exceeds `stepCount`.

**Verdict: inherent-to-the-spec ambiguity, not an inherent writer difference.**
It must be decided, written down, and made identical in both writers; the plain
reading of "first step in this call" is N+1, which is what Nim emits.

### 6.5 The two findings that indict *both* writers

**`values.dat` / `values.idx` are not in the spec at all.**

```
$ git grep -n 'values\.dat\|values\.idx' ff95fe4    # zero hits
```

The spec puts the value stream **inside `steps.dat`** — `trace-events.md:49`
heads the section "#### 2. Value Stream (`steps.dat`)", and
`internal-files.md:85` describes `steps.dat` as the "Combined execution + values
stream (steps with embedded variable values)". Both writers instead emit a
separate `values.dat` + `values.idx` pair, and the differential byte-compares
them happily. They agree with each other and neither agrees with the spec.

(The spec's own account of the combined file is not implementable as written: the
execution and value tag spaces both start at 0 inside one file and
`trace-events.md:66` defines the correspondence circularly. So the writers'
split is the sane engineering choice — but it is an undocumented extension, and
"both must implement the same spec" cannot be satisfied by a stream the spec does
not contain.)

**The delta window is out of spec in both writers.** `trace-events.md:656`
encoding rule 4:

> "All other steps use DeltaStep if the delta fits in 3 varint bytes
> (**±1048575**), otherwise AbsoluteStep"

Both writers promote to `AbsoluteStep` at ±63/±64, and the differential fixture
deliberately pins that window (`fixture_ops`'s ±63/±64/±65 rows). Output stays
*decodable* — an `AbsoluteStep` where a `DeltaStep` was allowed resolves to the
same position — so this is a compression-efficiency divergence, not a
correctness one. But it means the fixture pins agreement with the *other writer*,
not with the spec.

### 6.6 Pledged zstd content size is an implementation constraint, not a spec rule

```
$ git grep -ni 'pledge\|content.size\|ZSTD_c_\|skippable' ff95fe4   # zero hits
```

The spec says only "each chunk independently compressed with Zstd", level 3,
4096 records (`seekable-zstd.md:5-17,86`). It says nothing about frame headers or
pledged size. The requirement is real but it comes from the **Nim reader**, which
calls `ZSTD_getFrameContentSize` and fails on `UNKNOWN`. The negative control
`the_streaming_zstd_framing_fails_the_differential` is therefore pinning a
reader contract, and its docstring already says so correctly. Keep it, and keep
its justification reader-based rather than spec-based.

### 6.7 Verdict table

| file | verdict | which writer moves | repo |
|---|---|---|---|
| `events.log` `events.fmt` `meta.json` `paths.json` | legacy, spec-contrary | delete from Rust | authorised |
| `steps.dat` `steps.idx` | in spec, already byte-identical | — | — |
| `paths.dat` `paths.off` | in spec (Layout A), already byte-identical | — | — |
| `values.dat` `values.idx` | **not in the spec**; both writers invent them, identically | neither; needs a spec PR | spec repo |
| `calls.dat` | in spec; `first_step_id` **undefined**, writers differ | align Rust to Nim's N+1 reading | authorised |
| `funcs.dat` `funcs.off` | in spec; **Nim out of spec** | Nim | **not authorised** |
| `types.dat` `types.off` | in spec; **Nim out of spec**, and `type_0` is invented | delete invention (authorised); shape fix is Nim | mixed |
| `meta.dat` bit 12 | Nim emits the tables, never stamps the flag | Nim | **not authorised** |
| `meta.dat` workdir | Nim writes `""`, Rust writes real cwd | Nim | **not authorised** |
| `meta.dat` recording_id | genuinely inherent (UUIDv7) | seed it for the differential | authorised |
| `varnames.dat` `varnames.off` | in spec ("raw bytes"), already byte-identical | — | — |
| `events.dat` `events.idx` | in spec, already byte-identical | — | — |

## Step 7 — consumer census: `events.log` is NOT deletable

The brief said to measure before deleting. Measured, and the answer is no.

`events.log` is used as a **format discriminator by its absence** in at least
two shipping readers. Both verified by reading the source, not by report:

```rust
// codetracer/src/db-backend/src/ctfs_trace_reader/mod.rs:684
fn is_new_format(ctfs: &CtfsReader) -> bool {
    ctfs.has_file("steps.dat") && !ctfs.has_file("events.log")
}
```

```nim
# codetracer-trace-format-nim/src/codetracer_trace_reader.nim:198
  # Detect v4 (multi-stream) by absence of events.log.  v3 traces always
  # contain events.log; v4 traces never do (they use per-kind streams).
  let isV4 = findInternalFileEntry(data, "events.log", maxEntries).size == 0 and …
```

Deleting `events.log` from the Rust writer flips both predicates for every
container it produces and routes them to a decoder built for the *other*
writer's index layout. The db-backend's own doc comment states the
consequence: *"Routing such a Rust-writer bundle through the Nim reader yields
zero steps/calls."* That is a trace that opens successfully and reports zero
steps — the exact silently-empty failure this campaign exists to catch.

`events.fmt` is coupled: `codetracer_trace_reader/src/ctfs_reader.rs:19` and
the db-backend both **default to CBOR when it is absent**, so deleting it
alone silently misdecodes every split-binary container. Delete both or
neither.

Other real readers of `events.log`: `codetracer_trace_reader/src/ctfs_reader.rs`
(hard `?`, reached from the `codetracer_trace_util convert` CLI),
`codetracer_ct_print.nim:1702`, and `codetracer-beam-recorder/src/main.rs:1647`
(appends to existing traces; tolerates absence but then renumbers `VariableId`s
wrongly).

**Per the brief's rule — "if your change would break one, say so with the
measurement and stop" — `events.log` and `events.fmt` stay.** Retiring them
needs a positive marker (a `meta.dat` capability bit or writer-identity field)
to replace the absence test, landed in `codetracer` and
`codetracer-trace-format-nim` first. That is a cross-repo change this brief
does not authorise.

`aztec-avm-runtime` is insulated for now: its `pins.json` `trace_format` anchor
is `592fa42cbf` on branch `wasm/ctfs-writer`, and

```
$ git merge-base --is-ancestor 592fa42cbf HEAD  →  not an ancestor
```

so it will not pick up anything here until someone re-pins. Its `ct-print` is
built from `codetracer-trace-format-nim` at pin `baea074019`, and its build
script confirms the brief's warning: *"`ct-print` diverts any container carrying
`events.log` to the LEGACY combined-stream reader … NEITHER binary ever touches
`steps.dat`, `values.dat`, `calls.dat` or `events.dat`."* Several Aztec
verification scripts parse that legacy `doc["events"]` shape.

### `meta.json` / `paths.json`

Both are safe from a *reader* standpoint in the default writer configuration —
every consumer prefers `meta.dat` / `paths.dat` and only falls back. But:

* `paths.json` is the **oracle** for this repo's own interning-table tests
  (`codetracer_trace_reader/tests/interning_tables_tests.rs:109,123,128,273`),
  which validate `paths.dat` by comparing it against `paths.json`. Deleting it
  means rewriting those tests to be self-describing.
* `meta.json` is the only metadata in the splits-disabled legacy mode, which
  has no `meta.dat` at all and is pinned by
  `default_split_streams_tests.rs:164` `disabled_bundle_is_events_log_only_legacy`.

Neither deletion unlocks whole-container equality while `events.log` stays, so
both are deferred rather than done, and recorded here as scoped follow-ups.

## Step 8 — what was actually changed

Three commits on `blocktracer` in `codetracer-trace-format`:

**`6fac0ca` fix(writer): attribute a call entry to the callee's first step**

`calls.dat` differed in exactly one byte. Rust recorded `first_step_id` as the
last step already emitted; the reference writer records the first step of the
callee's body and documents it (`multi_stream_writer.nim:784`, the "CTFS-M
entry_step convention"), including a leaf-call clamp for a call that returns
without emitting a body step. Ported both. The spec is **silent** on this
(`trace-events.md:161` says only "First step in this call"), so the tie was
broken toward the reference writer, which is what five existing readers are
built against.

Red → green → mutation: removing `calls.dat` from `KNOWN_DIVERGENCES` first
reddened the census with `calls.dat differs (nim 20 B, rust 20 B) and is not in
KNOWN_DIVERGENCES`; after the fix it is byte-identical and asserted so by name.
Re-introducing a `+1` reddens exactly that one test again, and no other.

**`9d6724a` feat(meta.dat): decode the header so metadata can be compared by field**

Added `decode_meta_dat`, the missing inverse of `encode_meta_dat`, plus a
round-trip test and a truncation test. Replaced the differential's blanket
`meta.dat` exclusion — which rested on a false claim — with a field-by-field
comparison. `recording_id` is now asserted to differ (inherent); `version`,
`program`, `args`, `recorder_id`, `paths` and the trailing extension bytes are
asserted equal; and the two real defects are pinned individually so that fixing
either turns the test red:

* the Nim writer stores an empty `workdir`;
* the Nim writer never stamps bit 12 `FLAG_HAS_INTERNING_TABLES`, and the
  flags word is asserted to differ in that bit **and nothing else**.

Mutations: making the Rust writer emit an empty workdir reddens it; making it
drop bit 12 reddens it with `the Rust writer stamps bit 12 on a container that
has the interning tables`.

**`f10c54a` test(differential): state the measured reasons, not the remembered ones**

Header rewritten to the measurement; `RUST_ONLY`/`NIM_ONLY` now enforced in
both directions (mutation: adding a `gone.json` entry fails with *"is listed in
RUST_ONLY but is no longer Rust-only"*); `funcs.dat`/`types.dat` divergence
reasons given their spec citations; the two writers-agree-but-spec-disagrees
findings recorded.

Differential: **12 tests → 13**, all passing. Compared set grew from 6 files +
a masked flags word to 11 files + every `meta.dat` field but one.
