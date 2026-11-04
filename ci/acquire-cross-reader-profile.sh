#!/usr/bin/env bash
# Source-owned coupled-reader runtime activation; native Linux x64 only.
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
receipt_root=$RUNNER_TEMP/trace-format-cross-reader-profile-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT-$GITHUB_JOB
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
for role in python direnv just; do
  case $role in
    python) selector=ci-cross-reader-python; executable=python3 ;;
    just) selector=ci-cross-reader-just; executable=just ;;
    direnv) selector=ci-cross-reader-direnv; executable=direnv ;;
    *) echo 'Unknown source-owned runtime role' >&2; exit 1 ;;
  esac
  output=$("$nix" build --no-link --print-out-paths --no-update-lock-file "$flake#$selector")
  [[ $output == /nix/store/* && $output != *$'\n'* && -d $output && -x $output/bin/$executable ]] || { echo 'Foreign runtime output' >&2; exit 1; }
  "$nix" store verify --recursive --no-trust "$output"
  "$nix" derivation show --json --offline --option allow-import-from-derivation false "$flake#$selector" > "$receipt_root/$role-derivation.json"
  printf '%s\n' "$output" > "$receipt_root/$role-output.txt"
  "$nix" hash path --type sha256 --sri "$output" > "$receipt_root/$role-nar.txt"
  image=$(readlink -f "$output/bin/$executable")
  [[ $image == /nix/store/* && -f $image ]] || { echo 'Foreign runtime executable' >&2; exit 1; }
  sha256sum "$image" >> "$receipt_root/runtime-executable-identities.sha256"
  case $role in python) python_profile=$output ;; direnv) direnv_profile=$output ;; just) just_profile=$output ;; esac
  source_guard
 done
# The selected actual Python validates all three constructor outputs and the exact
# locked peer before admitting its tracked .envrc. No ambient Python/profile.
"$python_profile/bin/python3" -B ci/prepare-cross-reader-runtime.py \
  "$root" "$receipt_root" "$python_profile" "$direnv_profile" "$just_profile" "$git" "$nix"
source_guard
sha256sum --check --status "$receipt_root/runtime-executable-identities.sha256"
activation_guard
expected_activation_sha=$({ cat "$held_activation"; printf '%s\n' "$python_profile/bin" "$direnv_profile/bin" "$just_profile/bin"; } | sha256sum | cut -d' ' -f1)
printf '%s\n' "$python_profile/bin" "$direnv_profile/bin" "$just_profile/bin" >&"$activation_fd"
[[ $(sha256sum "$held_activation" | cut -d' ' -f1) == "$expected_activation_sha" && $(stat -Lc '%d:%i' "$activation") == "$activation_identity" && $(stat -Lc '%d:%i' "$held_activation") == "$activation_identity" ]] || { echo 'Changed runtime activation authority' >&2; exit 1; }
source_guard
printf 'ENTRY_NIM_RUNTIME_AUTHORITY=%s\n' "$receipt_root/peer-runtime-proof.json" >> "$GITHUB_ENV"
printf '%s\n' 'Declared coupled-reader runtime admitted; original test body unexecuted' > "$receipt_root/activation-completed.txt"
exec {activation_fd}>&-
