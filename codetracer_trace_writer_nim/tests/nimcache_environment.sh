#!/usr/bin/env bash
# Run inside the repository dev shell. This exercises Cargo's real build-script
# fingerprint and the real Nim compiler; no fake compiler or prebuilt archive
# can prove that changing TMPDIR actually moves generated C scratch.
set -euo pipefail
cd "$(dirname "$0")/../.."
unset CODETRACER_TRACE_WRITER_NIM_PREBUILT
scratch=$(mktemp -d /tmp/ctnw-env.XXXXXX)
trap 'rm -rf "$scratch"' EXIT
mkdir "$scratch/one" "$scratch/two"
long="$scratch/$(printf '%0120d' 0)"
mkdir "$long"
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2}

check_scratch() {
  local temp=$1 log=$2
  TMPDIR="$temp" cargo check -p codetracer_trace_writer_nim -vv >"$log" 2>&1 || {
    cat "$log" >&2
    return 1
  }
}

check_scratch "$scratch/one" "$scratch/one.log"
grep -F -- "--nimcache:$scratch/one/ctnw/" "$scratch/one.log"
# No source or Cargo option changes: only TMPDIR can invalidate this build.
check_scratch "$scratch/two" "$scratch/two.log"
grep -F -- "--nimcache:$scratch/two/ctnw/" "$scratch/two.log"
check_scratch "$long" "$scratch/long.log"
grep -F -- 'exceeds the 100-unit nimcache path budget; using /tmp/ctnw/' "$scratch/long.log"
grep -F -- '--nimcache:/tmp/ctnw/' "$scratch/long.log"
if grep -F -- "--nimcache:$long/ctnw/" "$scratch/long.log"; then
  echo 'overlong TMPDIR reached the Nim compiler' >&2
  exit 1
fi
echo 'Cargo TMPDIR invalidation and real Nim scratch routing passed'
