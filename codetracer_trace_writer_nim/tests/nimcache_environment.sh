#!/usr/bin/env bash
# Run inside the repository dev shell. This exercises Cargo's real build-script
# fingerprint and the real Nim compiler; no fake compiler or prebuilt archive
# can prove that changing the host's temp environment moves generated C scratch.
set -euo pipefail
cd "$(dirname "$0")/../.."
unset CODETRACER_TRACE_WRITER_NIM_PREBUILT
windows=false
temp_base=/tmp
fallback=/tmp
case "$(uname -s)" in
CYGWIN* | MINGW* | MSYS*)
  windows=true
  # Use the short writable system temp directory for the fixtures too. The
  # ordinary Git Bash /tmp can expand to a long user-profile directory and
  # would then exercise the fallback even in the positive controls.
  system_root=${SystemRoot:-${SYSTEMROOT:?Windows requires SystemRoot}}
  temp_base=$(cygpath -u "$system_root/Temp")
  fallback=$(cygpath -m "$temp_base")
  ;;
esac
scratch=$(mktemp -d "$temp_base/ctnw-env.XXXXXX")
cleanup() {
  local status=$?
  if ((status != 0)); then
    for log in "$scratch"/*.log.raw; do
      [[ -f $log ]] || continue
      tail -80 "$log" >&2
    done
  fi
  rm -rf "$scratch"
  exit "$status"
}
trap cleanup EXIT
mkdir "$scratch/one" "$scratch/two"
long="$scratch/$(printf '%0120d' 0)"
mkdir "$long"
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2}

check_scratch() {
  local temp=$1 log=$2
  if "$windows"; then
    temp=$(cygpath -m "$temp")
    # Windows GetTempPath/GetTempPath2 use TMP/TEMP, or SYSTEMTEMP for a
    # system account, rather than POSIX TMPDIR. Exercise both host APIs.
    TMP="$temp" TEMP="$temp" SYSTEMTEMP="$temp" cargo check -p codetracer_trace_writer_nim -vv >"$log.raw" 2>&1 || {
      cat "$log.raw" >&2
      return 1
    }
  else
    TMPDIR="$temp" cargo check -p codetracer_trace_writer_nim -vv >"$log.raw" 2>&1 || {
      cat "$log.raw" >&2
      return 1
    }
  fi
  # Native Windows paths can contain backslashes even when cygpath supplied
  # forward slashes. Compare the actual compiler output in one spelling.
  tr '\134' '/' <"$log.raw" >"$log"
}

native_path() {
  if "$windows"; then
    cygpath -m "$1"
  else
    printf '%s\n' "$1"
  fi
}

preferred=$(native_path "$scratch")
if ((${#preferred} + 4 + 22 > 100)); then
  echo "test scratch directory $preferred is too long for a positive control" >&2
  exit 1
fi

check_scratch "$scratch/one" "$scratch/one.log"
grep -F -- "--nimcache:$preferred/one/ctnw/" "$scratch/one.log"
# No source or Cargo option changes: only the temp environment can invalidate
# this build. A missing rerun-if-env-changed makes this second control fail.
check_scratch "$scratch/two" "$scratch/two.log"
grep -F -- "--nimcache:$preferred/two/ctnw/" "$scratch/two.log"
check_scratch "$long" "$scratch/long.log"
grep -F -- "exceeds the 100-unit nimcache path budget; using $fallback/ctnw/" "$scratch/long.log"
grep -F -- "--nimcache:$fallback/ctnw/" "$scratch/long.log"
if grep -F -- "--nimcache:$(native_path "$long")/ctnw/" "$scratch/long.log"; then
  echo 'overlong temporary directory reached the Nim compiler' >&2
  exit 1
fi
echo 'Cargo temp-environment invalidation and real Nim scratch routing passed'
