#!/usr/bin/env bash
# Real locked Rust profile activation; Linux x64 only. No ambient Rust fallback.
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
receipt_root=$RUNNER_TEMP/trace-format-rust-profile-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT-$GITHUB_JOB
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
profile=$("$nix" build --no-link --print-out-paths --no-update-lock-file "$flake#ci-rust-profile")
[[ $profile == /nix/store/* && $profile != *$'\n'* && -d $profile ]] || { echo 'Foreign Rust profile' >&2; exit 1; }
expected_drv=$("$nix" eval --raw --no-update-lock-file "$flake#ci-rust-profile.drvPath")
actual_drv=$("$nix" path-info --derivation "$profile")
[[ $expected_drv == "$actual_drv" ]] || { echo 'Foreign Rust profile deriver' >&2; exit 1; }
printf '%s\n' "$profile" > "$receipt_root/profile-output.txt"
printf '%s\n' "$actual_drv" > "$receipt_root/profile-derivation.txt"
"$nix" hash path --type sha256 --sri "$profile" > "$receipt_root/profile-nar.txt"
: > "$receipt_root/executable-identities.sha256"
for tool in cargo rustc rustfmt cargo-clippy clippy-driver; do
  [[ -x $profile/bin/$tool ]] || { echo 'Incomplete declared Rust profile' >&2; exit 1; }
  image=$(readlink -f "$profile/bin/$tool")
  [[ $image == /nix/store/* && -f $image ]] || { echo 'Foreign Rust image' >&2; exit 1; }
  sha256sum "$image" >> "$receipt_root/executable-identities.sha256"
done
"$profile/bin/rustc" -Vv > "$receipt_root/rustc-version.txt"
"$profile/bin/cargo" --version > "$receipt_root/cargo-version.txt"
"$profile/bin/rustfmt" --version > "$receipt_root/rustfmt-version.txt"
"$profile/bin/cargo-clippy" --version > "$receipt_root/clippy-version.txt"
wasm_lib=$("$profile/bin/rustc" --print target-libdir --target wasm32-unknown-unknown)
[[ $wasm_lib == /nix/store/* && -d $wasm_lib ]] || { echo 'Missing declared wasm target' >&2; exit 1; }
compgen -G "$wasm_lib/libcore-*.rlib" >/dev/null && compgen -G "$wasm_lib/libstd-*.rlib" >/dev/null || { echo 'Incomplete declared wasm stdlib' >&2; exit 1; }
printf '%s\n' "$wasm_lib" > "$receipt_root/wasm-target-libdir.txt"
wasm_members=("$wasm_lib"/*)
[[ ${#wasm_members[@]} -gt 0 && -e ${wasm_members[0]} ]] || { echo 'Empty wasm library census' >&2; exit 1; }
: > "$receipt_root/wasm-target-identities.sha256"
: > "$receipt_root/wasm-target-links.txt"
for member in "${wasm_members[@]}"; do
  resolved=$(readlink -f "$member")
  [[ $resolved == /nix/store/* && -f $resolved && $member != *$'\n'* && $resolved != *$'\n'* ]] || { echo 'Foreign wasm library member' >&2; exit 1; }
  printf '%s\t%s\n' "$member" "$resolved" >> "$receipt_root/wasm-target-links.txt"
  sha256sum "$resolved" >> "$receipt_root/wasm-target-identities.sha256"
done
[[ -s $receipt_root/wasm-target-identities.sha256 ]] || { echo 'Empty wasm library census' >&2; exit 1; }
sha256sum --check --status "$receipt_root/wasm-target-identities.sha256"
while IFS=$'\t' read -r member resolved; do
  [[ $(readlink -f "$member") == "$resolved" && -f $resolved ]] || { echo 'Changed wasm library authority' >&2; exit 1; }
done < "$receipt_root/wasm-target-links.txt"
[[ ${#wasm_members[@]} == $(printf '%s\n' "$wasm_lib"/* | wc -l) ]] || { echo 'Changed wasm library membership' >&2; exit 1; }
sha256sum --check --status "$receipt_root/executable-identities.sha256"
source_guard
[[ $(stat -Lc '%d:%i' "$root") == "$root_identity" && ! -L $receipt_root && $(stat -Lc '%d:%i' "$receipt_root") == "$receipt_identity" ]] || { echo 'Changed source/receipt directory authority' >&2; exit 1; }
activation_guard
# Append through the exact held original inode; never follow a replaced name.
expected_activation_sha=$({ cat "$held_activation"; printf '%s\n' "$profile/bin"; } | sha256sum | cut -d' ' -f1)
activation_guard
printf '%s\n' "$profile/bin" >&"$activation_fd"
[[ $(sha256sum "$held_activation" | cut -d' ' -f1) == "$expected_activation_sha" ]] || { echo 'Changed activation content after append' >&2; exit 1; }
[[ $(stat -Lc '%d:%i' "$activation") == "$activation_identity" && $(stat -Lc '%d:%i' "$held_activation") == "$activation_identity" ]] || { echo 'Changed activation inode' >&2; exit 1; }
source_guard
printf '%s\n' 'Declared Rust profile activation completed; original test bodies unexecuted' > "$receipt_root/activation-completed.txt"
exec {activation_fd}>&-
