#!/usr/bin/env bash
# Real locked native C profile activation; Linux x64 only. No ambient Rust fallback.
set -euo pipefail
[[ ${RUNNER_OS:?} == Linux && $(uname -s) == Linux && $(uname -m) == x86_64 ]] || { echo 'Native Linux x64 required' >&2; exit 1; }
root=$(pwd -P)
git=$(command -v git); nix=$(command -v nix)
[[ $git == /* && $nix == /* && -x $git && -x $nix ]] || { echo 'Missing explicit Git/Nix images' >&2; exit 1; }
git_sha=$(sha256sum "$(readlink -f "$git")" | cut -d' ' -f1)
nix_sha=$(sha256sum "$(readlink -f "$nix")" | cut -d' ' -f1)
metadata_git() (
  for name in ${!GIT_@}; do unset "$name"; done
  exec "$git" -C "$root" "$@"
)
# Metadata admission precedes exclusive receipt creation; no mutable source executes.
[[ $(metadata_git rev-parse --show-toplevel) == "$root" && $(metadata_git rev-parse HEAD) == "${GITHUB_SHA:?}" ]] || { echo 'Foreign source root/event' >&2; exit 1; }
[[ ${RUNNER_TEMP:?} == /* && ! -L $RUNNER_TEMP && $(cd "$RUNNER_TEMP" && pwd -P) == "$RUNNER_TEMP" && ${GITHUB_RUN_ID:?} =~ ^[0-9]+$ && ${GITHUB_RUN_ATTEMPT:?} =~ ^[0-9]+$ && ${GITHUB_JOB:?} =~ ^[A-Za-z0-9_-]+$ ]] || { echo 'Foreign receipt namespace' >&2; exit 1; }
receipt_root=$RUNNER_TEMP/trace-format-native-c-profile-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT-$GITHUB_JOB
[[ ! -e $receipt_root && ! -L $receipt_root ]] || { echo 'Occupied receipt path' >&2; exit 1; }
mkdir -- "$receipt_root"
receipt_identity=$(stat -Lc '%d:%i' "$receipt_root")
guard_count=0
source_guard() {
  [[ $(metadata_git rev-parse --show-toplevel) == "$root" ]] || { echo 'Foreign source root' >&2; return 1; }
  [[ $(metadata_git rev-parse HEAD) == "${GITHUB_SHA:?}" ]] || { echo 'Foreign source event' >&2; return 1; }
  metadata_git diff --cached --quiet HEAD -- || { echo 'Foreign source index' >&2; return 1; }
  ((guard_count+=1))
  tree_file=$receipt_root/source-tree-$guard_count.gitraw
  [[ ! -e $tree_file && ! -L $tree_file ]] || { echo 'Occupied source inventory' >&2; return 1; }
  (set -o noclobber; : > "$tree_file") || { echo 'Occupied source inventory' >&2; return 1; }
  [[ -f $tree_file && ! -L $tree_file && $(stat -Lc '%h' "$tree_file") == 1 ]] || return 1
  tree_identity=$(stat -Lc '%d:%i' "$tree_file")
  exec {tree_fd}<>"$tree_file"
  held_tree=/proc/$$/fd/$tree_fd
  [[ $(stat -Lc '%d:%i' "$held_tree") == "$tree_identity" ]] || return 1
  # The real producer must terminate successfully before any inventory is read.
  metadata_git ls-tree -rz HEAD >&"$tree_fd" || { echo 'Source inventory command failed' >&2; exec {tree_fd}>&-; return 1; }
  [[ -s $held_tree ]] || { echo 'Empty source inventory refused' >&2; exec {tree_fd}>&-; return 1; }
  while IFS=$'\t' read -r -d '' entry path; do
    IFS=' ' read -r mode kind oid <<< "$entry"
    [[ $path != /* && $path != *$'\n'* && $path != *$'\r'* && $path != ../* && $path != */../* ]] || return 1
    case $mode in
      100644|100755)
        [[ -f "$root/$path" && ! -L "$root/$path" ]] || { echo 'Foreign regular source' >&2; return 1; }
        if [[ $mode == 100755 ]]; then [[ -x "$root/$path" ]] || return 1; else [[ ! -x "$root/$path" ]] || return 1; fi
        observed=$(metadata_git hash-object --no-filters -- "$root/$path") ;;
      120000)
        [[ -L "$root/$path" ]] || { echo 'Foreign link source' >&2; return 1; }
        observed=$(readlink -n -- "$root/$path" | metadata_git hash-object --stdin) ;;
      *) echo 'Unsupported source type' >&2; return 1 ;;
    esac
    [[ $observed == "$oid" ]] || { echo 'Changed physical source bytes' >&2; return 1; }
  done < "$held_tree"
  [[ ! -L $tree_file && $(stat -Lc '%d:%i' "$tree_file") == "$tree_identity" && $(stat -Lc '%d:%i' "$held_tree") == "$tree_identity" ]] || { echo 'Changed source inventory inode' >&2; exec {tree_fd}>&-; return 1; }
  exec {tree_fd}>&-
  [[ $(sha256sum "$(readlink -f "$git")" | cut -d' ' -f1) == "$git_sha" && $(sha256sum "$(readlink -f "$nix")" | cut -d' ' -f1) == "$nix_sha" ]] || { echo 'Changed Git/Nix image' >&2; return 1; }
}
source_guard
root_identity=$(stat -Lc '%d:%i' "$root")
activation=${GITHUB_PATH:?}
[[ $activation == /* && -f $activation && ! -L $activation && $(stat -Lc '%h' "$activation") == 1 ]] || { echo 'Foreign activation file' >&2; exit 1; }
activation_identity=$(stat -Lc '%d:%i' "$activation")
activation_sha=$(sha256sum "$activation" | cut -d' ' -f1)
exec {activation_fd}>>"$activation"
held_activation="/proc/$$/fd/$activation_fd"
activation_guard() {
  [[ -f $activation && ! -L $activation && $(stat -Lc '%d:%i' "$activation") == "$activation_identity" && $(stat -Lc '%d:%i' "$held_activation") == "$activation_identity" && $(stat -Lc '%h' "$held_activation") == 1 && $(sha256sum "$held_activation" | cut -d' ' -f1) == "$activation_sha" ]] || { echo 'Changed activation file authority' >&2; return 1; }
}
activation_guard
flake="git+file://$root?rev=${GITHUB_SHA}&shallow=1"
: > "$receipt_root/executable-identities.sha256"
profiles=()
for role in gcc clang; do
  profile=$("$nix" build --no-link --print-out-paths --no-update-lock-file "$flake#ci-native-$role.out")
  [[ $profile == /nix/store/* && $profile != *$'\n'* && -d $profile ]] || { echo 'Foreign native compiler profile' >&2; exit 1; }
  expected_drv=$("$nix" eval --raw --no-update-lock-file "$flake#ci-native-$role.drvPath")
  actual_drv=$("$nix" path-info --derivation "$profile")
  [[ $expected_drv == "$actual_drv" ]] || { echo 'Foreign native compiler deriver' >&2; exit 1; }
  profiles+=("$profile")
  linker=$(readlink -f "$profile/bin/ld")
  [[ $linker == /nix/store/*/bin/ld && -f $linker && -x $linker ]] || { echo 'Missing declared bintools linker' >&2; exit 1; }
  bintools=${linker%/bin/ld}
  [[ -d $bintools/nix-support ]] || { echo 'Missing bintools support authority' >&2; exit 1; }
  printf '%s\n' "$linker" > "$receipt_root/$role-linker.txt"
  for metadata_role in orig-cc orig-libc orig-libc-dev; do
    declared=$(cat "$profile/nix-support/$metadata_role")
    [[ $declared == /nix/store/* && -d $declared ]] || { echo 'Foreign compiler metadata dependency' >&2; exit 1; }
    printf '%s\t%s\n' "$metadata_role" "$declared" >> "$receipt_root/$role-support-roles.txt"
  done
  for support in "$bintools"/nix-support/*; do
    resolved=$(readlink -f "$support")
    [[ $resolved == /nix/store/* && -f $resolved ]] || { echo 'Foreign bintools support member' >&2; exit 1; }
    sha256sum "$resolved" >> "$receipt_root/executable-identities.sha256"
  done
  printf '%s\n' "$profile" > "$receipt_root/$role-output.txt"
  printf '%s\n' "$actual_drv" > "$receipt_root/$role-derivation.txt"
  "$nix" hash path --type sha256 --sri "$profile" > "$receipt_root/$role-nar.txt"
  # Bind the complete store closure and wrapper support metadata; no bin aliases.
  "$nix" path-info --recursive --json "$profile" > "$receipt_root/$role-runtime-closure.json"
  closure=$("$nix" path-info --recursive "$profile")
  [[ -n $closure ]] || { echo 'Empty compiler runtime closure' >&2; exit 1; }
  : > "$receipt_root/$role-runtime-nars.txt"
  while IFS= read -r dependency; do
    [[ $dependency == /nix/store/* && $dependency != *' '* && -e $dependency ]] || { echo 'Foreign compiler dependency' >&2; exit 1; }
    printf '%s\t' "$dependency" >> "$receipt_root/$role-runtime-nars.txt"
    "$nix" hash path --type sha256 --sri "$dependency" >> "$receipt_root/$role-runtime-nars.txt"
  done <<< "$closure"
  [[ -s $receipt_root/$role-runtime-closure.json && -d $profile/nix-support ]] || { echo 'Missing compiler support closure' >&2; exit 1; }
  for member in "$profile"/nix-support/*; do
    resolved=$(readlink -f "$member")
    [[ $resolved == /nix/store/* && -f $resolved ]] || { echo 'Foreign compiler support member' >&2; exit 1; }
    sha256sum "$resolved" >> "$receipt_root/executable-identities.sha256"
  done
  [[ -s $profile/nix-support/libc-cflags && -s $profile/nix-support/orig-libc && -s $profile/nix-support/orig-libc-dev && -s $profile/nix-support/dynamic-linker ]] || { echo 'Incomplete compiler header/linker authority' >&2; exit 1; }
  for tool in "$role" cc ld; do
    image=$(readlink -f "$profile/bin/$tool")
    [[ $image == /nix/store/* && -f $image && -x $image ]] || { echo 'Missing compiler image' >&2; exit 1; }
    sha256sum "$image" >> "$receipt_root/executable-identities.sha256"
  done
  original_compiler=$(cat "$profile/nix-support/orig-cc")
  [[ $original_compiler == /nix/store/* && -d $original_compiler ]] || { echo 'Foreign wrapped compiler image' >&2; exit 1; }
  actual_image=$(readlink -f "$original_compiler/bin/$role")
  [[ $actual_image == /nix/store/* && -f $actual_image && -x $actual_image ]] || { echo 'Missing actual compiler image' >&2; exit 1; }
  sha256sum "$actual_image" >> "$receipt_root/executable-identities.sha256"
  "$profile/bin/$role" --version > "$receipt_root/$role-version.txt"
  "$profile/bin/$role" -dumpmachine > "$receipt_root/$role-target.txt"
  [[ $(cat "$receipt_root/$role-target.txt") == x86_64-*linux* ]] || { echo 'Foreign native compiler target' >&2; exit 1; }
  cat > "$receipt_root/$role-probe.c" <<'C'
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
int main(void) { pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER; if(pthread_mutex_lock(&m))return 2; const char *s="native-c-header-linker-ok"; if(strlen(s)!=25 || sizeof(uint64_t)!=8 || getpid()<=0)return 3; puts(s); return pthread_mutex_unlock(&m); }
C
  "$profile/bin/$role" -pthread "$receipt_root/$role-probe.c" -o "$receipt_root/$role-probe" > "$receipt_root/$role-compile.stdout" 2> "$receipt_root/$role-compile.stderr"
  sha256sum "$receipt_root/$role-probe.c" "$receipt_root/$role-probe" >> "$receipt_root/executable-identities.sha256"
  "$receipt_root/$role-probe" > "$receipt_root/$role-run.stdout" 2> "$receipt_root/$role-run.stderr"
  [[ $(cat "$receipt_root/$role-run.stdout") == native-c-header-linker-ok ]] || { echo 'Native compiler roundtrip refused' >&2; exit 1; }
done
sha256sum --check --status "$receipt_root/executable-identities.sha256"
for role in gcc clang; do
  profile=$(cat "$receipt_root/$role-output.txt")
  [[ $("$nix" path-info --derivation "$profile") == $(cat "$receipt_root/$role-derivation.txt") ]] || { echo 'Changed compiler deriver' >&2; exit 1; }
  "$nix" path-info --recursive --json "$profile" > "$receipt_root/$role-runtime-closure-after.json"
  cmp -- "$receipt_root/$role-runtime-closure.json" "$receipt_root/$role-runtime-closure-after.json" || { echo 'Changed compiler runtime closure metadata' >&2; exit 1; }
  [[ $(readlink -f "$profile/bin/ld") == $(cat "$receipt_root/$role-linker.txt") ]] || { echo 'Changed compiler linker authority' >&2; exit 1; }
  while IFS=$'\t' read -r metadata_role declared; do
    [[ $(cat "$profile/nix-support/$metadata_role") == "$declared" ]] || { echo 'Changed compiler support role' >&2; exit 1; }
  done < "$receipt_root/$role-support-roles.txt"
  while IFS=$'\t' read -r dependency expected_nar; do
    [[ $("$nix" hash path --type sha256 --sri "$dependency") == "$expected_nar" ]] || { echo 'Changed compiler runtime/header closure' >&2; exit 1; }
  done < "$receipt_root/$role-runtime-nars.txt"
done
source_guard
[[ $(stat -Lc '%d:%i' "$root") == "$root_identity" && ! -L $receipt_root && $(stat -Lc '%d:%i' "$receipt_root") == "$receipt_identity" ]] || { echo 'Changed source/receipt directory authority' >&2; exit 1; }
activation_guard
# Append through the exact held original inode; never follow a replaced name.
expected_activation_sha=$({ cat "$held_activation"; printf '%s\n' "${profiles[0]}/bin" "${profiles[1]}/bin"; } | sha256sum | cut -d' ' -f1)
activation_guard
printf '%s\n' "${profiles[0]}/bin" "${profiles[1]}/bin" >&"$activation_fd"
[[ $(sha256sum "$held_activation" | cut -d' ' -f1) == "$expected_activation_sha" ]] || { echo 'Changed activation content after append' >&2; exit 1; }
[[ $(stat -Lc '%d:%i' "$activation") == "$activation_identity" && $(stat -Lc '%d:%i' "$held_activation") == "$activation_identity" ]] || { echo 'Changed activation inode' >&2; exit 1; }
source_guard
printf '%s\n' 'Declared native C profile activation completed; original test bodies unexecuted' > "$receipt_root/activation-completed.txt"
exec {activation_fd}>&-
