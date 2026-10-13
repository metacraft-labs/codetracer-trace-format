#!/usr/bin/env python3
"""Admit the declared reader runtime and the exact locked peer's tracked envrc."""
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tomllib
import time
import shutil
import re
import shlex
import importlib.util
import ast

sys.dont_write_bytecode = True
helper_path = Path(__file__).with_name('generated-runtime-namespace.py')
spec = importlib.util.spec_from_file_location('generated_runtime_namespace', helper_path)
helper_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper_module)
verify_generated_outputs = helper_module.verify_generated_outputs

arguments = sys.argv[1:]
peer_override = None
if len(arguments) == 9 and arguments[-2] == '--peer-root':
    peer_override = Path(arguments[-1]).resolve(strict=True)
    arguments = arguments[:-2]
root, receipts, python_profile, direnv_profile, just_profile, git, nix = map(Path, arguments)
root = root.resolve(strict=True)
receipts = receipts.resolve(strict=True)
proof = {'finished': False, 'success': False, 'scope': 'Runtime prerequisite only; original tests unexecuted', 'stages': []}
diagnostic_source_peer = None

receipt_fd = os.open(receipts / 'peer-runtime-proof.json', os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600)
receipt_stat = os.fstat(receipt_fd)
def save():
    current = (receipts / 'peer-runtime-proof.json').lstat()
    if (current.st_dev, current.st_ino, current.st_mode, current.st_nlink) != (receipt_stat.st_dev, receipt_stat.st_ino, receipt_stat.st_mode, 1):
        raise RuntimeError('Changed receipt authority')
    body = (json.dumps(proof, indent=2) + '\n').encode()
    os.lseek(receipt_fd, 0, os.SEEK_SET)
    os.ftruncate(receipt_fd, 0)
    os.write(receipt_fd, body)
    os.fsync(receipt_fd)

def image(path):
    lexical = path.absolute()
    resolved = lexical.resolve(strict=True)
    st = resolved.stat()
    if not stat.S_ISREG(st.st_mode):
        raise RuntimeError('Nonregular selected image')
    return {'lexical': str(lexical), 'resolved': str(resolved), 'dev': st.st_dev, 'ino': st.st_ino,
            'mode': stat.S_IMODE(st.st_mode), 'sha256': hashlib.sha256(resolved.read_bytes()).hexdigest()}

def namespace(path):
    result = []
    def walk(current, relative):
        st = current.lstat()
        mode = stat.S_IMODE(st.st_mode)
        common = [relative, st.st_dev, st.st_ino, mode, st.st_uid]
        if stat.S_ISLNK(st.st_mode):
            result.append(common + ['link', os.readlink(current)])
        elif stat.S_ISREG(st.st_mode):
            result.append(common + ['file', hashlib.sha256(current.read_bytes()).hexdigest()])
        elif stat.S_ISDIR(st.st_mode):
            result.append(common + ['dir'])
            for child in sorted(current.iterdir()):
                # Git metadata has its own HEAD/index/config authority. No source
                # or activation-code member is omitted from this physical census.
                if relative == '.' and child.name == '.git':
                    continue
                walk(child, child.name if relative == '.' else relative + '/' + child.name)
        else:
            raise RuntimeError('Special peer namespace member')
    walk(path, '.')
    return result

def metadata(path, *args):
    env = {k: v for k, v in os.environ.items() if not k.startswith('GIT_')}
    return subprocess.check_output([str(git), '-C', str(path), *args], env=env)

def tracked(path):
    rows = metadata(path, 'ls-tree', '-rz', 'HEAD').split(b'\0')
    result = []
    for row in rows:
        if not row:
            continue
        header, name = row.split(b'\t', 1)
        mode, kind, oid = header.split()
        p = path / os.fsdecode(name)
        st = p.lstat()
        if mode == b'120000':
            if not stat.S_ISLNK(st.st_mode):
                raise RuntimeError('Changed tracked link kind')
            body = os.fsencode(os.readlink(p))
        elif mode in (b'100644', b'100755') and stat.S_ISREG(st.st_mode):
            if bool(st.st_mode & 0o111) != (mode == b'100755'):
                raise RuntimeError('Changed tracked execution parity')
            body = p.read_bytes()
        else:
            raise RuntimeError('Unsupported tracked member')
        actual = hashlib.sha1(b'blob ' + str(len(body)).encode() + b'\0' + body).hexdigest().encode()
        if actual != oid:
            raise RuntimeError('Changed tracked member bytes')
        result.append([os.fsdecode(name), mode.decode(), oid.decode()])
    if metadata(path, 'diff', '--cached', '--name-only', 'HEAD'):
        raise RuntimeError('Changed peer index')
    return result

hook_transition = None
hook_directory_bindings = []
def hook_directory_identity(path, fd=None):
    st = path.lstat() if fd is None else os.fstat(fd)
    if not stat.S_ISDIR(st.st_mode) or path.resolve(strict=True) != path:
        raise RuntimeError('Native hook directory ownership differs')
    return [st.st_dev, st.st_ino, st.st_mode, st.st_uid]

def declared_peer_config_continuity(peer, before):
    if any(hook_directory_identity(path) != identity or hook_directory_identity(path, fd) != identity for path, fd, identity in hook_directory_bindings):
        return False
    current = metadata(peer, 'config', '--null', '--show-origin', '--list')
    if current == before:
        return hook_transition is None or namespace(Path(hook_transition['path'])) == hook_transition['namespace']
    if hook_transition is None:
        return False
    def split(data):
        fields = data.split(b'\0')
        if fields[-1] != b'' or (len(fields) - 1) % 2:
            raise RuntimeError('Unsupported actual Git config inventory schema')
        keep, hooks = [], []
        for origin, entry in zip(fields[::2], fields[1::2]):
            key, sep, value = entry.partition(b'\n')
            if not sep:
                raise RuntimeError('Unsupported actual Git config entry schema')
            if key == b'core.hookspath':
                hooks.append((origin, value))
            else:
                keep.append((origin, entry))
        return keep, hooks
    old_rest, old_hooks = split(before)
    new_rest, new_hooks = split(current)
    expected = (b'file:.git/config', os.fsencode(hook_transition['path']))
    if old_rest != new_rest or old_hooks not in ([], [expected]) or new_hooks != [expected]:
        return False
    if metadata(peer, 'config', '--local', '--get', 'core.hooksPath').rstrip(b'\n') != expected[1]:
        return False
    common = Path(os.fsdecode(metadata(peer, 'rev-parse', '--path-format=absolute', '--git-common-dir').rstrip(b'\n')))
    hooks = common / 'hooks'
    return common == peer / '.git' and str(hooks) == hook_transition['path'] and namespace(hooks) == hook_transition['namespace']

def run_child(command, peer, role, child_env=None):
    child = subprocess.Popen(command, cwd=peer, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             start_new_session=True, env=child_env)
    fields = Path('/proc/' + str(child.pid) + '/stat').read_text().rsplit(')', 1)[1].split()
    birth = int(fields[19])
    sid = os.getsid(child.pid)
    row = {'role': role, 'pid': child.pid, 'birth': birth,
           'sid': sid, 'exitCode': None, 'remaining': [], 'unknown': []}
    proof['stages'].append(row)
    save()
    stdout, stderr = child.communicate()
    row['exitCode'] = child.returncode
    # Direnv diagnostics can contain configuration values. Retain only nonsecret
    # outcome metadata, never the raw activation/configuration payload.
    row['stdoutBytes'] = len(stdout)
    row['stderrBytes'] = len(stderr)
    if child.returncode:
        # Diagnostics stay in memory. Emit only fixed classifications and
        # literal refusal prefixes from the already authenticated peer scripts.
        text = stderr.decode('utf-8', errors='replace')
        prefixes = []
        if diagnostic_source_peer is not None:
            for name in ['nix/hook-ownership-guard.py', 'nix/hook-transaction.py']:
                source_tree = ast.parse((diagnostic_source_peer / name).read_text())
                for node in ast.walk(source_tree):
                    if not (isinstance(node, ast.Raise) and isinstance(node.exc, ast.Call)
                            and isinstance(node.exc.func, ast.Name) and node.exc.func.id == 'RuntimeError'
                            and node.exc.args):
                        continue
                    value = node.exc.args[0]
                    if isinstance(value, ast.BinOp) and isinstance(value.op, ast.Add):
                        value = value.left
                    if isinstance(value, ast.Constant) and isinstance(value.value, str) and len(value.value) > 10:
                        if value.value in text:
                            prefixes.append({'source': name, 'literalRefusalPrefix': value.value})
        commands = {'bash', 'python3', 'git', 'nix', 'direnv', 'jq', 'curl', 'dirname',
                    'basename', 'readlink', 'sort', 'sha256sum', 'cut', 'head', 'sed',
                    'grep', 'tr', 'mkdir', 'cp', 'chmod', 'touch', 'printf', 'env'}
        missing = sorted({name for name in re.findall(r'([A-Za-z_][A-Za-z0-9_-]*): command not found', text)
                          if name in commands})
        row['nonsecretFailureClassification'] = {
            'sourceRefusalPrefixes': prefixes,
            'knownMissingCommands': missing,
            'classes': {name: marker in text.lower() for name, marker in {
                'permissionDenied': 'permission denied',
                'readOnlyFilesystem': 'read-only file system',
                'shellSyntaxError': 'syntax error',
                'commandNotFound': 'command not found',
                'unboundVariable': 'unbound variable',
                'cannotExecute': 'cannot execute',
                'blockedEnvrc': 'is blocked',
                'hashMismatch': 'hash mismatch',
                'fallback': 'falling back',
            }.items()},
            'scope': 'Classifier metadata only; no diagnostic/config/export bytes or hashes retained',
        }
    while True:
        remaining, unknown = [], []
        for proc in Path('/proc').iterdir():
            if not proc.name.isdecimal():
                continue
            try:
                if proc.stat().st_uid != os.getuid():
                    continue
                fields = (proc / 'stat').read_text().rsplit(')', 1)[1].split()
                if int(fields[3]) == sid and fields[0] != 'Z':
                    remaining.append({'pid': int(proc.name), 'birth': int(fields[19])})
            except FileNotFoundError:
                continue
            except (PermissionError, OSError, ValueError) as error:
                unknown.append({'pid': int(proc.name), 'errorType': type(error).__name__})
        row['remaining'], row['unknown'] = remaining, unknown
        save()
        if unknown:
            raise RuntimeError('Unknown authorization session census')
        if not remaining:
            break
        time.sleep(.1)
    if child.returncode:
        raise RuntimeError('Declared peer runtime operation refused')
    return stdout, stderr

held = None
metadata_runtime_trees = {}
generated_verified = False
generated_after = None
try:
    deps = tomllib.loads((root / 'repro.lock').read_text())['lock']['deps']
    matches = [d for d in deps if d['name'] == 'codetracer-trace-format-nim']
    if len(matches) != 1:
        raise RuntimeError('Missing or duplicate locked Nim peer')
    dep = matches[0]
    revision = dep['revision']
    if dep['path'] != '../codetracer-trace-format-nim' or dep['url'] != 'https://github.com/metacraft-labs/codetracer-trace-format-nim' or dep['integrity'] != 'git-sha1:' + revision:
        raise RuntimeError('Foreign locked Nim peer authority')
    peer = peer_override or root.parent / 'codetracer-trace-format-nim'
    if peer.is_symlink() or peer.resolve(strict=True) != peer:
        raise RuntimeError('Foreign peer namespace')
    if metadata(peer, 'rev-parse', 'HEAD').decode().strip() != revision:
        raise RuntimeError('Foreign peer revision')
    module = direnv_profile / 'share/trace-cross-reader/nix-direnvrc'
    config_file = direnv_profile / 'share/trace-cross-reader/config/direnvrc'
    expected_config = 'source ' + str(module.resolve(strict=True)) + '\n'
    if config_file.read_text() != expected_config:
        raise RuntimeError('Foreign source-owned Direnv configuration')
    if not module.resolve(strict=True).is_relative_to('/nix/store') or not config_file.resolve(strict=True).is_relative_to('/nix/store'):
        raise RuntimeError('Foreign immutable Direnv runtime constructor')
    images = [image(p) for p in (Path(sys.executable), git, nix, direnv_profile / 'bin/direnv', module, config_file, helper_path, direnv_profile / 'share/trace-cross-reader/flake-overrides.bash', just_profile / 'bin/just')]
    for role, output in [('python', python_profile), ('direnv', direnv_profile), ('just', just_profile)]:
        drv = json.loads((receipts / (role + '-derivation.json')).read_text())
        paths = [v['path'] for d in drv.values() for v in d['outputs'].values() if 'path' in v]
        normalized = [p if p.startswith('/nix/store/') else '/nix/store/' + p for p in paths]
        if normalized.count(str(output)) != 1:
            raise RuntimeError('Selected runtime output differs from actual constructor')
    envrc = peer / '.envrc'
    if not stat.S_ISREG(envrc.lstat().st_mode) or envrc.is_symlink():
        raise RuntimeError('Foreign peer envrc')
    source = tracked(peer)
    diagnostic_source_peer = peer
    config = metadata(peer, 'config', '--null', '--show-origin', '--list')
    # Reject every foreign/duplicate initial hook config before constructors or
    # activation. Preserve all configuration bytes only in task memory.
    fields = config.split(b'\0')
    if fields[-1] != b'' or (len(fields) - 1) % 2:
        raise RuntimeError('Unsupported actual Git config inventory schema')
    initial_hook_entries = [(origin, entry.partition(b'\n')[2])
            for origin, entry in zip(fields[::2], fields[1::2])
            if entry.partition(b'\n')[0] == b'core.hookspath']
    if initial_hook_entries not in ([], [(b'file:.git/config', os.fsencode(peer / '.git/hooks'))]):
        raise RuntimeError('Preexisting foreign native hook configuration refused')
    # Fresh CI admission is deliberately exclusive. Existing activation/cache
    # code must not execute merely because its namespace was inventoried.
    for generated in ['.direnv', '.nimble', '.pre-commit-config.yaml']:
        if (peer / generated).exists() or (peer / generated).is_symlink():
            raise RuntimeError('Preexisting CI generated runtime root refused')
    peer_namespace = namespace(peer)
    input_lock = json.loads((peer / 'flake.lock').read_text())
    for name in input_lock['nodes'][input_lock['root']]['inputs']:
        candidates = [peer.parent / name]
        if name.endswith('-src'):
            candidates.append(peer.parent / name[:-4])
        if any(x.exists() or x.is_symlink() for x in candidates):
            raise RuntimeError('CI runtime requires the exact locked inputs, not sibling overrides')
    for key in ['NIX_FLAKE_OVERRIDE_INPUTS', 'NIX_FLAKE_OVERRIDE_FLAKES', 'NIX_FLAKE_OVERRIDE_SIBLINGS', 'NIX_FLAKE_OVERRIDE_SIBLINGS_ROOT']:
        if os.environ.get(key):
            raise RuntimeError('Foreign CI flake override selector')
    if (peer / '.env').exists() or (peer / '.env').is_symlink():
        raise RuntimeError('Undeclared CI dotenv activation input')
    if (peer.parent / 'direnv-nix-flake-overrides/plugin/flake-overrides.bash').exists():
        raise RuntimeError('CI runtime requires the envrc pinned published plugin')
    generated_roots = {'.direnv', '.nimble', '.pre-commit-config.yaml'}
    source_namespace = [row for row in peer_namespace if row[0].split('/')[0] not in generated_roots]
    proof['prospectiveGeneratedRoots'] = sorted(generated_roots)
    proof['ordinarySourceNamespace'] = source_namespace
    proof['generatedInputBefore'] = [row for row in peer_namespace if row[0].split('/')[0] in generated_roots]
    root_source = tracked(root)
    root_config = metadata(root, 'config', '--null', '--show-origin', '--list')
    environment_before = dict(os.environ)
    held = (peer, source, config, images, peer_namespace, root_source, root_config, environment_before)
    # Independent same-source print-dev-env supplies the exact expected cache
    # body and seed. It uses no private environment replacement or copied envrc.
    expected_profile_slot = receipts / 'expected-nim-env-profile'
    if expected_profile_slot.exists() or expected_profile_slot.is_symlink():
        raise RuntimeError('Occupied expected runtime profile output')
    expected_rc, _ = run_child([str(nix), 'print-dev-env', '--profile', str(expected_profile_slot), '--offline', '--option', 'allow-import-from-derivation', 'false', '--no-write-lock-file', '.'], peer, 'actual-locked-peer-cache-constructor')
    expected_rc = expected_rc.rstrip(b'\n') + b'\n'
    expected_json, _ = run_child([str(nix), 'print-dev-env', '--json', '--offline', '--option', 'allow-import-from-derivation', 'false', '--no-write-lock-file', '.'], peer, 'actual-locked-peer-runtime-role-constructor')
    expected_environment = json.loads(expected_json)
    variables = expected_environment['variables']
    if not isinstance(variables, dict) or variables['PATH']['type'] != 'exported' or not isinstance(variables['PATH']['value'], str):
        raise RuntimeError('Unsupported actual print-dev-env PATH schema')
    expected_roles = {}
    for role in ['nim', 'nimble', 'git', 'gcc', 'bash']:
        lexical = shutil.which(role, path=variables['PATH']['value'])
        if not lexical or not Path(lexical).resolve(strict=True).is_relative_to('/nix/store'):
            raise RuntimeError('Independently constructed locked runtime role absent')
        expected_roles[role] = image(Path(lexical))
    proof['expectedSelectedImages'] = expected_roles
    shell_hook = variables['shellHook']['value']
    if not isinstance(shell_hook, str):
        raise RuntimeError('Unsupported actual shellHook constructor schema')
    transactions = [shlex.split(line) for line in shell_hook.splitlines() if 'hook-transaction' in line]
    if len(transactions) != 1:
        raise RuntimeError('Actual native-hook config constructor absent or ambiguous')
    tokens = transactions[0]
    script_positions = [i for i, token in enumerate(tokens)
                        if re.fullmatch(r'/nix/store/[a-z0-9]{32}-hook-transaction\.py', token)]
    if len(script_positions) != 1:
        raise RuntimeError('Actual native-hook transaction script absent or ambiguous')
    script_position = script_positions[0]
    if script_position == 0 or not re.fullmatch(r'/nix/store/[a-z0-9]{32}-python3-[^/]+/bin/python3', tokens[script_position - 1]):
        raise RuntimeError('Actual native-hook Python constructor absent')
    transaction_python = image(Path(tokens[script_position - 1]))
    transaction_script = Path(tokens[script_position])
    transaction_image = image(transaction_script)
    if transaction_script.read_bytes() != (peer / 'nix/hook-transaction.py').read_bytes():
        raise RuntimeError('Native-hook transaction differs from the tracked peer constructor')
    # shellHook wraps the command in a subshell. Its whole-line token offset is
    # not an argv offset; derive the role from the held script's actual parser.
    parser = ast.parse(transaction_script.read_text())
    config_roles = [node.value for node in parser.body if isinstance(node, ast.Assign)
                    and any(isinstance(target, ast.Name) and target.id == 'config'
                            for target in node.targets)]
    if len(config_roles) != 1:
        raise RuntimeError('Actual native-hook config parser absent or ambiguous')
    role = config_roles[0]
    if not (isinstance(role, ast.Subscript) and isinstance(role.value, ast.Attribute)
            and isinstance(role.value.value, ast.Name) and role.value.value.id == 'sys'
            and role.value.attr == 'argv' and isinstance(role.slice, ast.Constant)
            and type(role.slice.value) is int and role.slice.value == 6):
        raise RuntimeError('Unsupported actual native-hook config parser')
    config_position = script_position + role.slice.value
    if config_position >= len(tokens):
        raise RuntimeError('Actual native-hook config argument absent')
    expected_hook_config = Path(tokens[config_position])
    if not re.fullmatch(r'/nix/store/[a-z0-9]{32}-pre-commit-config\.json', str(expected_hook_config)):
        raise RuntimeError('Actual native-hook config constructor role differs')
    expected_hook_config_before = image(expected_hook_config)
    images.append(transaction_image)
    images.append(transaction_python)
    proof['nativeHookConstructor'] = {'transaction': transaction_image,
                                     'python': transaction_python,
                                     'configArgvPosition': role.slice.value,
                                     'config': expected_hook_config_before}
    expected_profile = expected_profile_slot.resolve(strict=True)
    archive_bytes, _ = run_child([str(nix), 'flake', 'archive', '--json', '--offline', '--no-write-lock-file', '.'], peer, 'actual-locked-peer-source-constructor')
    archive = json.loads(archive_bytes)
    expected_sources = set()
    def source_paths(node):
        if not isinstance(node, dict) or not isinstance(node.get('path'), str) or not node['path'].startswith('/nix/store/'):
            raise RuntimeError('Invalid actual source archive schema')
        expected_sources.add(node['path'])
        for child in node.get('inputs', {}).values():
            source_paths(child)
    source_paths(archive)
    mask = os.umask(0)
    os.umask(mask)
    seed_matches = set(re.findall(rb'/nix/store/[a-z0-9]{32}-codetracer-trace-format-nim-nimble-seed', expected_rc))
    if len(seed_matches) != 1:
        raise RuntimeError('Actual locked Nimble seed constructor is absent or ambiguous')
    seed = Path(os.fsdecode(seed_matches.pop()))
    seed_before = namespace(seed)
    module_before = image(module)
    plugin = direnv_profile / 'share/trace-cross-reader/flake-overrides.bash'
    plugin_body = plugin.read_bytes()
    if hashlib.sha256(plugin_body).hexdigest() != '8b10a0a0b0cf25e964b93e60ba481f7cdc60f5e7526447d538ae5072292d23be':
        raise RuntimeError('Plugin differs from the actual envrc pinned body')
    root_source = tracked(root)
    root_config = metadata(root, 'config', '--null', '--show-origin', '--list')
    environment_before = dict(os.environ)
    held = (peer, source, config, images, peer_namespace, root_source, root_config, environment_before)
    proof['schema'] = 'codetracer-trace-format.unix-runtime-authority.v1'
    proof['constructorSourceSha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    proof['flakeLockSha256'] = hashlib.sha256((peer / 'flake.lock').read_bytes()).hexdigest()
    proof['selectedDirenvImage'] = image(direnv_profile / 'bin/direnv')
    proof['selectedNixDirenvModuleImage'] = image(module)
    proof['peerRevision'] = revision
    proof['envrcSha256'] = hashlib.sha256(envrc.read_bytes()).hexdigest()
    common_before = Path(os.fsdecode(metadata(peer, 'rev-parse', '--path-format=absolute', '--git-common-dir').rstrip(b'\n')))
    if common_before != peer / '.git':
        raise RuntimeError('Unsupported primary native hook common directory')
    for path in (common_before, common_before / 'hooks'):
        identity = hook_directory_identity(path)
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        if hook_directory_identity(path, descriptor) != identity:
            os.close(descriptor)
            raise RuntimeError('Native hook held directory differs')
        hook_directory_bindings.append((path, descriptor, identity))
    # Authenticate the same tracked native constructor's complete canonical
    # hook census before accepting its one declared local config output.
    hook_roles = {'guard': 2, 'native': 3, 'templates': 4, 'legacy': 5,
                  'legacy_config': 7, 'prior_native': 11, 'prior_config': 12}
    for role_name, role_index in hook_roles.items():
        assignments = [node.value for node in parser.body if isinstance(node, ast.Assign)
                       and any(isinstance(t, ast.Name) and t.id == role_name for t in node.targets)]
        if len(assignments) != 1 or not (isinstance(assignments[0], ast.Subscript)
                and isinstance(assignments[0].value, ast.Attribute)
                and isinstance(assignments[0].value.value, ast.Name)
                and assignments[0].value.value.id == 'sys' and assignments[0].value.attr == 'argv'
                and isinstance(assignments[0].slice, ast.Constant)
                and assignments[0].slice.value == role_index):
            raise RuntimeError('Unsupported actual native hook role parser')
    hook_guard = Path(tokens[script_position + hook_roles['guard']])
    if hook_guard.read_bytes() != (peer / 'nix/hook-ownership-guard.py').read_bytes():
        raise RuntimeError('Native hook census differs from tracked constructor')
    images.append(image(hook_guard))
    role_values = {name: tokens[script_position + index] for name, index in hook_roles.items()}
    hook_env = dict(os.environ)
    hook_env['PATH'] = str(Path(expected_roles['git']['lexical']).parent) + os.pathsep + hook_env.get('PATH', '')
    census_bytes, _ = run_child([transaction_python['lexical'], str(hook_guard), str(peer),
            role_values['native'], 'snapshot', 'reserved', role_values['templates'],
            role_values['legacy'], str(expected_hook_config), role_values['legacy_config'],
            role_values['prior_native'], role_values['prior_config']], peer,
            'verify-initial-native-hook-authority', hook_env)
    proof['initialHookAuthorityVerified'] = True
    # Authorize only this already-authenticated tracked file, never a global allow.
    command = [str(direnv_profile / 'bin/direnv'), 'allow', str(envrc)]
    run_child(command, peer, 'authorize-exact-tracked-envrc')
    exported, diagnostics = run_child([str(direnv_profile / 'bin/direnv'), 'export', 'json'], peer, 'activate-exact-locked-peer')
    # Successful export is a delta. An already active environment can return {}.
    # Neither absence of PATH in that delta nor frontend zero proves a fresh shell.
    delta = json.loads(exported)
    activated, selected = helper_module.apply_activation(delta, diagnostics, os.environ, expected_roles, image)
    if activated.get('NIMBLE_DIR') != str(peer / '.nimble'):
        raise RuntimeError('Declared Nimble source cache selector differs from owned seed output')
    config_slot = peer / '.pre-commit-config.yaml'
    if not stat.S_ISLNK(config_slot.lstat().st_mode) or str(config_slot.resolve(strict=True)) != str(expected_hook_config):
        raise RuntimeError('Native-hook generated config link differs from actual constructor')
    census_bytes, _ = run_child([transaction_python['lexical'], str(hook_guard), str(peer),
            role_values['native'], 'snapshot', 'reserved', role_values['templates'],
            role_values['legacy'], str(expected_hook_config), role_values['legacy_config'],
            role_values['prior_native'], role_values['prior_config']], peer,
            'verify-actual-generated-native-hooks', hook_env)
    hook_census = json.loads(census_bytes)
    for name in ('pre-commit', 'pre-push'):
        native_file = Path(role_values['native']) / name
        if not stat.S_ISREG(native_file.lstat().st_mode):
            raise RuntimeError('Native hook source is not an ordinary file')
        if hook_census['entries'].get(name + '.repro-local') != [0o755, hashlib.sha256(native_file.read_bytes()).hexdigest()]:
            raise RuntimeError('Actual native local hook differs from constructor')
        images.append(image(native_file))
    common = Path(os.fsdecode(metadata(peer, 'rev-parse', '--path-format=absolute', '--git-common-dir').rstrip(b'\n')))
    if common != peer / '.git' or common.is_symlink() or (common / 'hooks').is_symlink():
        raise RuntimeError('Unsupported local hook config output ownership')
    hook_transition = {'path': str(common / 'hooks'), 'namespace': namespace(common / 'hooks')}
    if not declared_peer_config_continuity(peer, config):
        raise RuntimeError('Native hook constructor changed undeclared Git configuration')
    proof['declaredHookConfigTransition'] = {'key': 'core.hooksPath', 'scope': 'local',
            'origin': 'file:.git/config', 'path': hook_transition['path'],
            'constructor': transaction_image, 'hookCensus': hook_census,
            'scopeLimit': 'All other effective configuration remains exact in memory'}
    if activated.get('DIRENV_DIR') != '-' + str(peer):
        raise RuntimeError('Actual locked peer loaded-RC association differs')
    proof['loadedRcDirectory'] = activated['DIRENV_DIR']
    proof['activatedSelectedImages'] = selected
    generated_verified = verify_generated_outputs(peer, expected_rc, expected_profile, expected_sources, seed, seed_before, module, plugin_body, selected, activated, run_child, namespace, receipts, mask)
    # The first census refuses any preexisting non-seed metadata slot.
    metadata_bytes, _ = run_child([str(nix), 'eval', '--raw', '--offline',
        '--option', 'allow-import-from-derivation', 'false', '--no-write-lock-file',
        '.#ci-nimble-runtime-metadata.outPath'], peer, 'actual-nimble-metadata-constructor')
    metadata_root = Path(metadata_bytes.decode().strip())
    if not re.fullmatch(r'/nix/store/[a-z0-9]{32}-[^/]+', str(metadata_root)):
        raise RuntimeError('Declared metadata writer store output schema differs')
    drv_bytes, _ = run_child([str(nix), 'eval', '--raw', '--offline',
        '--option', 'allow-import-from-derivation', 'false', '--no-write-lock-file',
        '.#ci-nimble-runtime-metadata.drvPath'], peer, 'actual-metadata-writer-derivation')
    drv = drv_bytes.decode().strip()
    drv_json, _ = run_child([str(nix), 'derivation', 'show', drv], peer,
        'actual-metadata-writer-derivation-attributes')
    descriptions = json.loads(drv_json)
    descriptions = descriptions.get('derivations', descriptions)
    if set(descriptions) != {Path(drv).name}:
        raise RuntimeError('Actual writer derivation inventory differs')
    attributes = descriptions[Path(drv).name]
    source_bytes, _ = run_child([str(nix), 'eval', '--raw', '--offline',
        '--option', 'allow-import-from-derivation', 'false', '--no-write-lock-file',
        '.#ci-nimble-source.outPath'], peer, 'actual-metadata-selected-nimble-source')
    source_root = source_bytes.decode().strip()
    writer_source = peer / 'nix/construct-runtime-metadata.nim'
    source_matches = re.findall(r'/nix/store/[a-z0-9]{32}-construct-runtime-metadata\.nim', attributes['env']['buildPhase'])
    if len(source_matches) != 1 or Path(source_matches[0]).read_bytes() != writer_source.read_bytes() or attributes['env']['src'] != source_root or attributes['env']['out'] != str(metadata_root):
        raise RuntimeError('Writer source/output differs from actual held constructor')
    images.append(image(writer_source)); images.append(image(Path(source_matches[0])))
    closure_json, _ = run_child([str(nix), 'path-info', '--json', '--recursive', str(metadata_root)], peer,
        'actual-metadata-writer-registered-closure')
    closure = json.loads(closure_json)
    if not isinstance(closure, dict) or str(metadata_root) not in closure or any(not isinstance(v, dict) or not isinstance(v.get('narHash'), str) or not isinstance(v.get('references'), list) or not set(v['references']).issubset(closure) for v in closure.values()):
        raise RuntimeError('Actual writer recursive registered closure incomplete')
    run_child([str(nix), 'store', 'verify', '--recursive', '--no-trust', str(metadata_root)],
        peer, 'actual-nimble-metadata-runtime-content')
    metadata_runtime_trees = {path: namespace(Path(path)) for path in closure}
    proof['actualMetadataRuntimeRegisteredClosure'] = closure
    writer = metadata_root / 'bin/construct-runtime-metadata'
    writer_before = image(writer)
    images.append(writer_before)
    writer_result, _ = run_child([str(writer), str(peer / '.nimble')], peer,
        'genuine-nimble-two-metadata-writers', activated)
    producer_result = json.loads(writer_result)
    if set(producer_result) != {'installedPackages', 'releaseCount'} or not isinstance(producer_result['installedPackages'], list) or not isinstance(producer_result['releaseCount'], int) or producer_result['releaseCount'] <= 0:
        raise RuntimeError('Genuine metadata writer result differs')
    expected_installed = []
    for directory in sorted((seed / 'pkgs2').iterdir()):
        if not directory.is_dir() or directory.is_symlink():
            raise RuntimeError('Unsupported actual seed package member')
        match = re.fullmatch(r'([A-Za-z0-9_]+)-([0-9]+(?:\.[0-9]+){1,3})-([a-f0-9]{40})', directory.name)
        if match is None or not (directory / (match.group(1) + '.nimble')).is_file():
            raise RuntimeError('Unsupported selected seed package constructor')
        expected_installed.append({'name': match.group(1), 'version': match.group(2), 'checksum': match.group(3)})
    installed = producer_result['installedPackages']
    if any(not isinstance(v, dict) or set(v) != {'name', 'version', 'checksum'} or any(not isinstance(item, str) for item in v.values()) for v in installed):
        raise RuntimeError('Actual installed package inventory schema differs')
    if sorted(installed, key=lambda v: (v['name'], v['version'], v['checksum'])) != sorted(expected_installed, key=lambda v: (v['name'], v['version'], v['checksum'])):
        raise RuntimeError('Actual metadata writer inventory differs from independently held complete seed')
    proof['expectedSeedInstalledPackages'] = expected_installed
    # The original reader task registers dependency reverse edges and produces
    # its named worktree-local cache before the final runtime authority is sealed.
    reader_before = namespace(peer)
    expected_data_bytes, _ = run_child([str(writer), str(peer / '.nimble'),
        '--expected-reader-data', str(peer)], peer, 'actual-reader-dependency-constructor', activated)
    expected_reader_data = json.loads(expected_data_bytes)
    if namespace(peer) != reader_before:
        raise RuntimeError('Read-only reader dependency constructor changed the peer')
    reader = peer / 'ct-print'
    cache_root = '.nimcache/src/codetracer_ct_print_r'
    if reader.exists() or reader.is_symlink() or (peer / '.nimcache').exists() or (peer / '.nimcache').is_symlink():
        raise RuntimeError('Preexisting reader/compiler outputs refused')
    run_child([proof['expectedSelectedImages']['nimble']['lexical'], 'buildCtPrint'],
        peer, 'actual-source-declared-reader-build', activated)
    reader_after = namespace(peer)
    def reader_member(name):
        return name == 'ct-print' or name in {'.nimcache', '.nimcache/src', cache_root} or name.startswith(cache_root + '/')
    prior = {row[0]: row for row in reader_before}
    current = {row[0]: row for row in reader_after}
    metadata_name = '.nimble/nimbledata2.json'
    if any(current.get(name) != row for name, row in prior.items() if name != metadata_name) or any(not reader_member(name) for name in current.keys() - prior.keys()):
        raise RuntimeError('Declared reader changed another execution member')
    if current[metadata_name][:-1] != prior[metadata_name][:-1]:
        raise RuntimeError('Declared reader changed metadata physical ownership')
    # ReverseDependency arrays have set semantics in the actual addRevDep API.
    def canonical_reverse_data(value):
        if not isinstance(value, dict) or set(value) != {'version', 'reverseDeps'} or value['version'] != 1 or not isinstance(value['reverseDeps'], dict):
            raise RuntimeError('Unsupported reader reverse-dependency schema')
        return {name: {version: {checksum: sorted(json.dumps(row, sort_keys=True) for row in rows)
                    for checksum, rows in checksums.items()} for version, checksums in versions.items()}
                    for name, versions in value['reverseDeps'].items()}
    actual_reader_data = json.loads((peer / metadata_name).read_bytes())
    if canonical_reverse_data(actual_reader_data) != canonical_reverse_data(expected_reader_data):
        raise RuntimeError('Reader dependency metadata differs from actual selected Nimble constructor')
    if not {'ct-print', '.nimcache', '.nimcache/src', cache_root}.issubset(current):
        raise RuntimeError('Declared reader cache constructor output incomplete')
    reader_rows = [row for row in reader_after if reader_member(row[0])]
    if not any(row[0] == 'ct-print' and row[5] == 'file' and row[3] & 0o111 for row in reader_rows):
        raise RuntimeError('Genuine reader output absent or nonexecutable')
    for row in reader_rows:
        path = peer / row[0]; physical = path.lstat()
        if row[4] != os.getuid() or row[5] not in {'dir', 'file'} or (row[5] == 'file' and physical.st_nlink != 1):
            raise RuntimeError('Foreign or nonregular reader/compiler output')
    proof['declaredReaderConstructor'] = {'image': image(reader), 'outputs': reader_rows,
        'expectedReverseDependencies': expected_reader_data}
    images.append(image(reader))
    source_namespace = [row for row in reader_after if row[0].split('/')[0] not in generated_roots]
    proof['ordinarySourceNamespace'] = source_namespace
    metadata_slots = {}
    for name in ['nimbledata2.json', 'official-nim-releases.json']:
        path = peer / '.nimble' / name
        value = path.lstat()
        if not stat.S_ISREG(value.st_mode) or value.st_nlink != 1 or value.st_uid != os.getuid():
            raise RuntimeError('Genuine metadata constructor output ownership differs')
        body = path.read_bytes()
        metadata_slots[name] = {'sha256': hashlib.sha256(body).hexdigest(), 'value': json.loads(body)}
    data = metadata_slots['nimbledata2.json']['value']
    versions = metadata_slots['official-nim-releases.json']['value']
    if not isinstance(data, dict) or set(data) != {'version', 'reverseDeps'} or data['version'] != 1 or not isinstance(data['reverseDeps'], dict):
        raise RuntimeError('Actual selected Nimble data schema differs')
    if not isinstance(versions, list) or len(versions) != producer_result['releaseCount'] or any(not isinstance(v, dict) or set(v) != {'version'} or not isinstance(v['version'], str) for v in versions):
        raise RuntimeError('Actual selected release data schema differs')
    proof['actualNimbleMetadataConstructor'] = {'image': writer_before, 'result': producer_result, 'slots': metadata_slots}
    if image(writer) != writer_before:
        raise RuntimeError('Actual metadata writer changed during execution')
    generated_verified = verify_generated_outputs(peer, expected_rc, expected_profile, expected_sources, seed, seed_before, module, plugin_body, selected, activated, run_child, namespace, receipts, mask, metadata_slots)
    generated_after = [row for row in namespace(peer) if row[0].split('/')[0] in generated_roots]
    proof['generatedOutputAfter'] = generated_after
    proof['mandatoryNimbleMetadataSlots'] = metadata_slots
    proof['success'] = True
except BaseException as error:
    proof['failureType'] = type(error).__name__
    # Our RuntimeError predicates have constant messages. Other exceptions may
    # carry configuration/export payloads and are represented only by type.
    if isinstance(error, RuntimeError):
        proof['failure'] = str(error)
        print(str(error), file=sys.stderr)
finally:
    proof['authorityGuards'] = {}
    if held:
        checks = {
            'peerTracked': lambda: tracked(held[0]) == held[1],
            'peerConfigMemory': lambda: declared_peer_config_continuity(held[0], held[2]),
            'selectedImages': lambda: [image(Path(p['lexical'])) for p in held[3]] == held[3],
            'peerOrdinaryNamespace': lambda: [row for row in namespace(peer) if row[0].split('/')[0] not in generated_roots] == source_namespace,
            'rootTracked': lambda: tracked(root) == held[5],
            'rootConfigMemory': lambda: metadata(root, 'config', '--null', '--show-origin', '--list') == held[6],
            'constructorEnvironmentMemory': lambda: dict(os.environ) == held[7],
        }
        for name, check in checks.items():
            try:
                proof['authorityGuards'][name] = bool(check())
            except BaseException as error:
                proof['authorityGuards'][name] = False
                proof.setdefault('guardErrors', {})[name] = type(error).__name__
    try:
        proof['finalGuards'] = bool(all(namespace(Path(p)) == rows for p, rows in metadata_runtime_trees.items()) and held and tracked(held[0]) == held[1] and declared_peer_config_continuity(held[0], held[2]) and [image(Path(p['lexical'])) for p in held[3]] == held[3] and [row for row in namespace(peer) if row[0].split('/')[0] not in generated_roots] == source_namespace and generated_verified and [row for row in namespace(peer) if row[0].split('/')[0] in generated_roots] == generated_after and namespace(seed) == seed_before and image(module) == module_before and image(expected_hook_config) == expected_hook_config_before and tracked(root) == held[5] and metadata(root, 'config', '--null', '--show-origin', '--list') == held[6] and dict(os.environ) == held[7] and all(image(Path(v['lexical'])) == v for v in proof.get('activatedSelectedImages', {}).values()) and all(image(Path(v['lexical'])) == v for v in proof.get('expectedSelectedImages', {}).values()))
    except Exception:
        proof['finalGuards'] = False
    proof['success'] = bool(proof['success'] and proof['finalGuards'])
    proof['finished'] = True
    save()
    for _, descriptor, _ in hook_directory_bindings:
        os.close(descriptor)
    if not proof['finalGuards'] and 'failureType' not in proof:
        print('Runtime prerequisite final authority changed', file=sys.stderr)
if not proof['success']:
    raise SystemExit(1)
