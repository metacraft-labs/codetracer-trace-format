#!/usr/bin/env bash
# Reject a CI reference that names a branch which does not exist, or one that
# names a legacy `main`/`master` a repository has moved off.
#
# WHY THIS EXISTS
#
# The `main` -> `dev` migration across this repo family has now produced the
# same defect five separate times, in three different shapes, and none of them
# was caught by anything:
#
#   1. `uses: owner/repo/action@main` where the action repo has no `main`.
#      Every step using it fails to load. Eight of these were live on this
#      repo's `dev`.
#   2. `on: push: branches: [main]` where `main` was deleted. The workflow
#      simply never runs again, and a never-run workflow reports nothing at
#      all -- so the mainline looked clean because it was unchecked.
#   3. `with: ref: main` on a clone step. Five siblings had no `main` (hard
#      failure); one still had a frozen `main` a month behind its `dev`, so CI
#      quietly built a stale tree and reported it as current.
#
# THE TRAP THIS IS BUILT AROUND
#
# `git ls-remote --heads <url> main` exits 0 and prints NOTHING when the ref
# is absent. Any check written around its exit status therefore passes. This
# script tests the OUTPUT, never the status. That single distinction is what
# eight dead references survived.
#
# ASYMMETRY, DELIBERATELY ENCODED
#
# The three shapes do not fail alike, so they are not reported alike. A gate
# that cries wolf on the harmless case gets disabled by whoever hits it first.
#
#   * A `uses:` or `ref:` naming an absent branch is FATAL -- the job breaks.
#   * A `uses:` or `ref:` naming a `main`/`master` that resolves but is NOT
#     that repo's default is FATAL -- this is the quiet regime that builds a
#     frozen tree and reports it as current. It is the one a resolve-only
#     check misses.
#   * A push-trigger branch filter listing a branch that does not exist is
#     only a WARNING: a filter entry that matches nothing is inert.
#   * ...UNLESS no entry in the filter exists, in which case the workflow can
#     never trigger on push at all, and that is FATAL.
#
# Version tags (`@v4`, `@v1.2.3`) and 40-hex SHAs are not branch names and are
# left alone.
#
# USAGE
#   ci/check-ci-refs.sh              scan .github/workflows against the network
#   ci/check-ci-refs.sh --self-test  run the hermetic self-tests (no network)
#
# NETWORK
#   Unreachable network is a FAILURE when $CI is set and a SKIP otherwise, so
#   the gate cannot be quietly satisfied by an offline runner.

set -uo pipefail

WORKFLOW_DIR="${WORKFLOW_DIR:-.github/workflows}"
SELF_REPO_URL="${SELF_REPO_URL:-}"

errors=0
warnings=0
refs_checked=0

err()  { printf 'ERROR: %s\n'   "$*" >&2; errors=$((errors + 1)); }
warn() { printf 'WARNING: %s\n' "$*" >&2; warnings=$((warnings + 1)); }
info() { printf '%s\n' "$*"; }

# --------------------------------------------------------------------------
# Resolution
#
# `CI_REFS_FAKE_RESOLVER` points at a table so the self-tests can exercise
# every branch of the logic hermetically. Lines are:
#     <url><TAB><ref><TAB>present|absent
#     <url><TAB>@default<TAB><branch>
# --------------------------------------------------------------------------

resolve_ref() {
  # $1 = repo url, $2 = ref. Prints "present" or "absent"; returns 2 if the
  # network could not be reached at all.
  local url="$1" ref="$2"
  if [ -n "${CI_REFS_FAKE_RESOLVER:-}" ]; then
    local hit
    hit=$(awk -F'\t' -v u="$url" -v r="$ref" '$1==u && $2==r {print $3}' \
          "$CI_REFS_FAKE_RESOLVER")
    [ "$hit" = "unreachable" ] && return 2
    [ "$hit" = "present" ] && { echo present; return 0; }
    echo absent; return 0
  fi

  local out rc
  # Query heads AND tags: a `uses:` ref is legitimately either.
  out=$(git ls-remote "$url" "refs/heads/$ref" "refs/tags/$ref" 2>/dev/null)
  rc=$?
  if [ $rc -ne 0 ]; then
    return 2                      # could not reach the remote
  fi
  # THE POINT: judge the OUTPUT, not $rc. ls-remote exits 0 for a missing ref.
  if [ -n "$out" ]; then echo present; else echo absent; fi
}

default_branch() {
  # $1 = repo url. Prints the default branch name, empty if unknown.
  local url="$1"
  if [ -n "${CI_REFS_FAKE_RESOLVER:-}" ]; then
    awk -F'\t' -v u="$url" '$1==u && $2=="@default" {print $3}' \
      "$CI_REFS_FAKE_RESOLVER"
    return 0
  fi
  git ls-remote --symref "$url" HEAD 2>/dev/null \
    | awk '/^ref:/{sub("refs/heads/","",$2); print $2; exit}'
}

network_unreachable() {
  if [ -n "${CI:-}" ]; then
    err "cannot reach the network to resolve refs, and \$CI is set. A gate that cannot resolve is not a gate that passes."
  else
    info "SKIP: network unreachable and \$CI is unset; not resolving refs."
  fi
}

# --------------------------------------------------------------------------
# Checks
# --------------------------------------------------------------------------

check_pinned_ref() {
  # $1 = repo url, $2 = ref, $3 = human location
  local url="$1" ref="$2" where="$3" state deflt
  case "$ref" in
    v[0-9]*|[0-9]*.[0-9]*) return 0 ;;                 # version tag
    *[!0-9a-f]*) : ;;                                  # not a bare sha
    *) [ ${#ref} -eq 40 ] && return 0 ;;               # pinned sha
  esac

  refs_checked=$((refs_checked + 1))
  state=$(resolve_ref "$url" "$ref"); local rc=$?
  if [ $rc -eq 2 ]; then network_unreachable; return 1; fi

  if [ "$state" = "absent" ]; then
    err "$where: '$ref' does not exist in $url — this reference is broken, not merely stale."
    return 0
  fi

  case "$ref" in
    main|master)
      deflt=$(default_branch "$url")
      if [ -n "$deflt" ] && [ "$deflt" != "$ref" ]; then
        err "$where: '$ref' resolves in $url but that repo's default is '$deflt'. A frozen legacy branch resolves cleanly and silently supplies a stale tree; pin '$deflt' or a SHA."
      fi
      ;;
  esac
}

scan_uses_and_pins() {
  local f base line url ref repo
  for f in "$WORKFLOW_DIR"/*.y*ml; do
    [ -e "$f" ] || continue
    base=$(basename "$f")

    # ---- axis 1: `uses: owner/repo[/path]@ref`
    while IFS= read -r line; do
      ref=${line##*@}
      repo=${line%@*}
      repo=${repo#*uses:}
      repo=$(printf '%s' "$repo" | tr -d " '\"")
      # owner/repo are the first two path components
      url="https://github.com/$(printf '%s' "$repo" | cut -d/ -f1-2)"
      check_pinned_ref "$url" "$ref" "$base: uses: $repo@$ref"
    done < <(tr -d '\r' < "$f" | grep -oE "uses: *[A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+@[A-Za-z0-9_.-]+")

    # ---- axis 3: a `repo:`/`ref:` pair in a clone step
    while IFS=$'\t' read -r repo ref; do
      [ -z "$repo" ] && continue
      url="https://github.com/$repo"
      check_pinned_ref "$url" "$ref" "$base: clones $repo at ref: $ref"
    done < <(tr -d '\r' < "$f" | awk '
      /^[[:space:]]*repo:[[:space:]]*[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+[[:space:]]*$/ {
        r=$2; have=1; next
      }
      have && /^[[:space:]]*ref:[[:space:]]*[A-Za-z0-9_.\/-]+[[:space:]]*$/ {
        print r "\t" $2; have=0; next
      }
      /^[[:space:]]*-[[:space:]]*(name|uses):/ { have=0 }
    ')
  done
}

scan_push_filters() {
  # axis 2: `on: push: branches: [...]` — inert per-entry, fatal if ALL dead.
  local f base list b state live total self
  self="$SELF_REPO_URL"
  if [ -z "$self" ]; then
    self=$(git config --get remote.origin.url 2>/dev/null)
  fi
  [ -z "$self" ] && { warn "no origin remote; skipping push-filter checks"; return 0; }

  for f in "$WORKFLOW_DIR"/*.y*ml; do
    [ -e "$f" ] || continue
    base=$(basename "$f")
    list=$(tr -d '\r' < "$f" | awk '
      /^on:/          { inon=1; next }
      /^[^[:space:]]/ { inon=0 }
      inon && /^[[:space:]]*push:/          { inpush=1; next }
      inon && /^[[:space:]]{2}[a-z_]+:/     { inpush=0 }
      inpush && /branches:/ {
        line=$0; sub(/.*branches:[[:space:]]*/,"",line);
        gsub(/[][",]/," ",line); print line
      }
    ')
    [ -z "$list" ] && continue

    live=0; total=0
    for b in $list; do
      [ -z "$b" ] && continue
      total=$((total + 1))
      state=$(resolve_ref "$self" "$b"); local rc=$?
      if [ $rc -eq 2 ]; then network_unreachable; return 1; fi
      refs_checked=$((refs_checked + 1))
      if [ "$state" = "present" ]; then
        live=$((live + 1))
      else
        warn "$base: push filter names '$b', which does not exist here. Inert rather than harmful — a filter entry that matches nothing simply never fires — but it is misleading and should go."
      fi
    done

    if [ "$total" -gt 0 ] && [ "$live" -eq 0 ]; then
      err "$base: NOT ONE of its push-trigger branches ($list) exists. This workflow can never run on a push, so it reports nothing and its silence looks like success."
    fi
  done
}

# --------------------------------------------------------------------------
# Self-tests — the mutation controls
# --------------------------------------------------------------------------

self_test() {
  local tmp rc out pass=0 fail=0
  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' RETURN

  cat > "$tmp/resolver" <<'TBL'
https://github.com/acme/actions	main	absent
https://github.com/acme/actions	dev	present
https://github.com/acme/actions	@default	dev
https://github.com/acme/frozen	main	present
https://github.com/acme/frozen	dev	present
https://github.com/acme/frozen	@default	dev
https://github.com/acme/self	dev	present
https://github.com/acme/self	main	absent
https://github.com/acme/self	stable	present
https://github.com/acme/offline	main	unreachable
TBL

  run_case() { # $1 desc, $2 dir, $3 expect(pass|fail), $4.. env
    local desc="$1" dir="$2" expect="$3"; shift 3
    out=$(env CI_REFS_FAKE_RESOLVER="$tmp/resolver" \
              SELF_REPO_URL="https://github.com/acme/self" \
              WORKFLOW_DIR="$dir" "$0" 2>&1); rc=$?
    if { [ "$expect" = pass ] && [ $rc -eq 0 ]; } || \
       { [ "$expect" = fail ] && [ $rc -ne 0 ]; }; then
      printf '  ok   %s\n' "$desc"; pass=$((pass + 1))
    else
      printf '  FAIL %s (rc=%d, expected %s)\n%s\n' "$desc" "$rc" "$expect" "$out"
      fail=$((fail + 1))
    fi
  }

  mk() { mkdir -p "$tmp/$1"; cat > "$tmp/$1/w.yml"; }

  # A: uses: an absent branch -> fatal. Proves the scanner SEES the needle.
  mk a <<'Y'
on:
  push:
    branches: [dev]
jobs:
  j:
    steps:
      - uses: acme/actions/setup@main
Y
  run_case "uses: @main that does not exist is fatal" "$tmp/a" fail

  # B: the same file corrected -> clean. Proves A failed for its stated
  # reason and not because the fixture is malformed.
  mk b <<'Y'
on:
  push:
    branches: [dev]
jobs:
  j:
    steps:
      - uses: acme/actions/setup@dev
Y
  run_case "uses: @dev resolves and passes" "$tmp/b" pass

  # C: the quiet regime -- resolves, but is not the default.
  mk c <<'Y'
on:
  push:
    branches: [dev]
jobs:
  j:
    steps:
      - uses: acme/frozen/act@main
Y
  run_case "uses: a resolving but non-default @main is fatal" "$tmp/c" fail

  # D: every push-filter branch dead -> fatal (workflow can never run).
  mk d <<'Y'
on:
  push:
    branches: [main, master]
jobs:
  j:
    steps:
      - uses: acme/actions/setup@dev
Y
  run_case "push filter with no live branch is fatal" "$tmp/d" fail

  # E: one dead + one live -> warn only. The harmless case must NOT fail,
  # or the gate gets switched off by the first person it annoys.
  mk e <<'Y'
on:
  push:
    branches: [main, dev]
jobs:
  j:
    steps:
      - uses: acme/actions/setup@dev
Y
  run_case "push filter with a dead entry but a live one only warns" "$tmp/e" pass

  # F: clone pin at an absent ref -> fatal.
  mk f <<'Y'
on:
  push:
    branches: [dev]
jobs:
  j:
    steps:
      - uses: acme/actions/clone@dev
        with:
          repo: acme/actions
          ref: main
Y
  run_case "clone-step ref: main that does not exist is fatal" "$tmp/f" fail

  # G: version tags and SHAs are not branches and must be ignored.
  mk g <<'Y'
on:
  push:
    branches: [dev]
jobs:
  j:
    steps:
      - uses: actions/checkout@v4
      - uses: acme/actions/setup@dev
Y
  run_case "version tags are left alone" "$tmp/g" pass

  # H: unreachable network fails under CI...
  mk h <<'Y'
on:
  push:
    branches: [dev]
jobs:
  j:
    steps:
      - uses: acme/offline/act@main
Y
  out=$(env CI=1 CI_REFS_FAKE_RESOLVER="$tmp/resolver" \
            SELF_REPO_URL="https://github.com/acme/self" \
            WORKFLOW_DIR="$tmp/h" "$0" 2>&1); rc=$?
  if [ $rc -ne 0 ]; then printf '  ok   unreachable network fails under $CI\n'; pass=$((pass+1))
  else printf '  FAIL unreachable network should fail under $CI\n'; fail=$((fail+1)); fi

  # ...and skips without it.
  out=$(env -u CI CI_REFS_FAKE_RESOLVER="$tmp/resolver" \
            SELF_REPO_URL="https://github.com/acme/self" \
            WORKFLOW_DIR="$tmp/h" "$0" 2>&1); rc=$?
  if [ $rc -eq 0 ]; then printf '  ok   unreachable network skips without $CI\n'; pass=$((pass+1))
  else printf '  FAIL unreachable network should skip without $CI\n%s\n' "$out"; fail=$((fail+1)); fi

  # I: NON-VACUITY. A scanner that silently matched nothing would pass every
  # case above. Assert it actually examined refs in a known-populated tree.
  out=$(env CI_REFS_FAKE_RESOLVER="$tmp/resolver" \
            SELF_REPO_URL="https://github.com/acme/self" \
            WORKFLOW_DIR="$tmp/b" CI_REFS_REPORT_COUNT=1 "$0" 2>&1)
  if printf '%s' "$out" | grep -q 'refs checked: [1-9]'; then
    printf '  ok   the scanner reports a non-zero ref count (not vacuous)\n'; pass=$((pass+1))
  else
    printf '  FAIL scanner examined zero refs; every case above would pass vacuously\n%s\n' "$out"
    fail=$((fail+1))
  fi

  printf 'self-test: %d passed, %d failed\n' "$pass" "$fail"
  [ "$fail" -eq 0 ]
}

# --------------------------------------------------------------------------

if [ "${1:-}" = "--self-test" ]; then
  self_test; exit $?
fi

scan_uses_and_pins
scan_push_filters

if [ -n "${CI_REFS_REPORT_COUNT:-}" ] || [ -z "${CI_REFS_FAKE_RESOLVER:-}" ]; then
  info "refs checked: $refs_checked"
fi

if [ "$errors" -gt 0 ]; then
  printf '\n%d error(s), %d warning(s).\n' "$errors" "$warnings" >&2
  exit 1
fi
printf '%d warning(s); no broken references.\n' "$warnings"
exit 0
