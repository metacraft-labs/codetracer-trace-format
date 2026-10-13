"""Exact constructor projection for the locked peer's CI-generated runtime files."""
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import shutil

def verify_generated_outputs(peer, expected_rc, expected_profile, expected_sources,
                             seed, seed_before, module, plugin_body, selected,
                             activated, run_child, namespace, receipts, mask, metadata_slots=None):
    # Every generated member has a named constructor. Unknown members cannot be
    # made admissible merely by placing them under .direnv or .nimble.
    layout = peer / '.direnv'
    suffix = '-' + hashlib.sha1(b'.\n').hexdigest()
    profile = 'flake-profile' + suffix
    expected_names = {'.', 'bin', 'bin/nix-direnv-reload', profile, profile + '.rc',
                      'flake-inputs', 'local-flake-overrides',
                      'local-flake-overrides/bin', 'local-flake-overrides/.gitignore'}
    plugin_text = plugin_body.decode()
    expected_bodies = {'local-flake-overrides/.gitignore': b'*\n!.gitignore\n'}
    for name in ['flake-override-args-quoted', 'with-local-flake-overrides', 'collect-flake-override-args']:
        marker = 'cat > "$bindir/' + name + '" <<EOF\n'
        if plugin_text.count(marker) != 1:
            raise RuntimeError('Pinned plugin output template ambiguity')
        template = plugin_text.split(marker, 1)[1].split('\nEOF\n', 1)[0]
        # Expand the original heredoc with the verified no-override CI context.
        # Only the authenticated template runs; generated consumer scripts do not.
        command = [selected['bash']['lexical'], '--noprofile', '--norc', '-c',
                   "argsq=''; cat <<EOF\n" + template + '\nEOF\n']
        body, _ = run_child(command, receipts, 'project-pinned-plugin-' + name,
                            {'PATH': activated['PATH']})
        role = 'local-flake-overrides/bin/' + name
        expected_names.add(role)
        expected_bodies[role] = body
    module_text = module.read_text()
    marker = '<<-EOF\n'
    if module_text.count(marker) != 1:
        raise RuntimeError('Actual nix-direnv reload template ambiguity')
    template = module_text.split(marker, 1)[1].split('\nEOF\n', 1)[0] + '\n'
    expected_bodies['bin/nix-direnv-reload'] = template.replace('$PWD', str(peer)).replace('${layout_dir}', str(layout)).encode()
    expected_bodies[profile + '.rc'] = expected_rc
    source_links = {'flake-inputs/' + Path(source).name: source for source in expected_sources}
    expected_names.update(source_links)
    actual_names = {row[0] for row in namespace(layout)}
    if actual_names != expected_names:
        raise RuntimeError('Unknown or missing generated Direnv member')
    for name in expected_names:
        path = layout if name == '.' else layout / name
        st = path.lstat()
        if st.st_uid != os.getuid():
            raise RuntimeError('Foreign generated runtime owner')
        if name in expected_bodies:
            if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1 or path.read_bytes() != expected_bodies[name]:
                raise RuntimeError('Generated runtime body differs from actual constructor')
            executable = name == 'bin/nix-direnv-reload' or name.startswith('local-flake-overrides/bin/')
            expected_mode = ((0o666 & ~mask) | (0o111 & ~mask)) if executable else (0o666 & ~mask)
            if stat.S_IMODE(st.st_mode) != expected_mode:
                raise RuntimeError('Generated runtime mode differs from actual constructor')
        elif name == profile or name in source_links:
            target = str(expected_profile) if name == profile else source_links[name]
            if not stat.S_ISLNK(st.st_mode) or str(path.resolve(strict=True)) != target:
                raise RuntimeError('Generated runtime source/profile link differs from constructor')
        elif not stat.S_ISDIR(st.st_mode) or stat.S_IMODE(st.st_mode) != (0o777 & ~mask):
            raise RuntimeError('Generated runtime directory differs from constructor')
    # The writable Nimble source cache must be precisely the actual locked seed,
    # not an arbitrary previously installed package set or mutable exemption.
    if namespace(seed) != seed_before:
        raise RuntimeError('Actual immutable Nimble seed changed')
    expected_seed = {row[0]: row for row in seed_before}
    cache = peer / '.nimble'
    actual_cache = {row[0]: row for row in namespace(cache)}
    if metadata_slots is not None:
        if set(metadata_slots) != {'nimbledata2.json', 'official-nim-releases.json'}:
            raise RuntimeError('Incomplete actual Nimble constructor output inventory')
        for name, expected in metadata_slots.items():
            path = cache / name
            value = path.lstat()
            if not stat.S_ISREG(value.st_mode) or value.st_nlink != 1 or value.st_uid != os.getuid() or stat.S_IMODE(value.st_mode) != (0o666 & ~mask):
                raise RuntimeError('Foreign or nonregular Nimble metadata output')
            if hashlib.sha256(path.read_bytes()).hexdigest() != expected['sha256']:
                raise RuntimeError('Actual Nimble metadata writer output changed')
            if json.loads(path.read_bytes()) != expected['value']:
                raise RuntimeError('Actual Nimble metadata writer schema/body changed')
            del actual_cache[name]
    if set(actual_cache) != set(expected_seed):
        raise RuntimeError('Nimble source namespace differs from actual locked seed')
    for name, row in expected_seed.items():
        actual = actual_cache[name]
        if actual[4] != os.getuid() or actual[5:] != row[5:]:
            raise RuntimeError('Nimble source body/kind differs from actual locked seed')
        expected_mode = (0o777 & ~mask) if name == '.' else ((row[3] & ~mask) | stat.S_IWUSR)
        if actual[3] != expected_mode:
            raise RuntimeError('Nimble source mode differs from actual seed copy constructor')
    return True

def apply_activation(delta, diagnostics, base_environment, expected_roles, image):
    if b'falling back' in diagnostics.lower() or b'using previous environment' in diagnostics.lower():
        raise RuntimeError('Declared peer activation fallback refused')
    if not isinstance(delta, dict) or any(not isinstance(k, str) or (v is not None and not isinstance(v, str)) for k, v in delta.items()):
        raise RuntimeError('Invalid declared activation delta')
    activated = dict(base_environment)
    for key, value in delta.items():
        if value is None:
            activated.pop(key, None)
        else:
            activated[key] = value
    if activated.get('NIX_DIRENV_DID_FALLBACK'):
        raise RuntimeError('Declared peer activation fallback signal refused')
    selected = {}
    for name in ['nim', 'nimble', 'git', 'gcc', 'bash']:
        lexical = shutil.which(name, path=activated.get('PATH', ''))
        if not lexical or not Path(lexical).resolve(strict=True).is_relative_to('/nix/store'):
            raise RuntimeError('Declared activation runtime role absent or foreign')
        selected[name] = image(Path(lexical))
        if selected[name] != expected_roles[name]:
            raise RuntimeError('Activated role differs from actual locked peer constructor')
    return activated, selected
