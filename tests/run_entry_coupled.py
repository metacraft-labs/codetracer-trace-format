"""Required genuine Nim/Rust entry artifact gate; no mocks or absence skips.

The Nim environment root is the declared supplier checkout. A separately bound
private source override is permitted for pre-adoption qualification only; its
copied .envrc is never activated. Both real producers and readers must execute.
"""
from pathlib import Path
import argparse, ctypes, hashlib, json, os, platform, shutil, stat, struct, subprocess, sys, tempfile, tomllib
from contextlib import ExitStack

# Dynamic producer helpers must not write build cache into tracked source roots.
sys.dont_write_bytecode = True

parser = argparse.ArgumentParser()
parser.add_argument('--nim-environment-root', required=True, type=Path)
parser.add_argument('--qualification-source', type=Path)
parser.add_argument('--direnv')
parser.add_argument('--unix-runtime-authority', type=Path)
parser.add_argument('--windows-provider', type=Path)
parser.add_argument('--work-root', type=Path)
parser.add_argument('--prepare-only', action='store_true')
parser.add_argument('--fixture-output', type=Path)
args = parser.parse_args()
root = Path(__file__).resolve().parent.parent
environment = args.nim_environment_root.resolve(strict=True)
source = (args.qualification_source or environment).resolve(strict=True)
if args.qualification_source and os.environ.get('CI') == 'true':
    raise RuntimeError('Private source override is not a normal CI supplier')
if not (environment / '.git').exists():
    raise RuntimeError('Required genuine Nim environment checkout is absent')
if not (source / 'src/codetracer_trace_writer/entry_identity.nim').is_file():
    raise RuntimeError('Required declared Nim entry API is absent')

lock = tomllib.loads((root / 'repro.lock').read_text())
suppliers = [row for row in lock['lock']['deps'] if row['name'] == 'codetracer-trace-format-nim']
if len(suppliers) != 1:
    raise RuntimeError('Required declared Nim supplier is ambiguous or absent')
head = subprocess.check_output(['git', '-C', str(environment), 'rev-parse', 'HEAD'], text=True).strip()
if head != suppliers[0]['revision']:
    raise RuntimeError('Nim environment differs from the declared supplier revision')
image_holds = None
unix_authority_guard = None
try:
    native_flags = []
    if os.name == 'nt':
        if platform.machine().upper() not in ('AMD64', 'ARM64'):
            raise RuntimeError('Unsupported actual Windows host')
        if not args.windows_provider:
            raise RuntimeError('Required native Windows coupled provider absent')
        profile = json.loads(args.windows_provider.read_text())
        provider_root = args.windows_provider.resolve(strict=True).parent
        receipt_path = provider_root / 'receipt.json'
        constructor_receipt = json.loads(receipt_path.read_text())
        if constructor_receipt.get('success') is not True or constructor_receipt.get('directChildTerminal') is not True:
            raise RuntimeError('Native constructor did not finish successfully')
        constructor_receipt_bytes = receipt_path.read_bytes()
        for name, expected in constructor_receipt['raw'].items():
            if Path(name).name != name or hashlib.sha256((provider_root / name).read_bytes()).hexdigest() != expected:
                raise RuntimeError('Changed constructor raw receipt')
        if profile['schema'] != 'entry-native-frontend.v1' or profile['supplierRevision'] != head or profile['nativeMachine'] != platform.machine().upper():
            raise RuntimeError('Foreign native provider source/platform')
        expected_cpu = 0xaa64 if platform.machine().upper() == 'ARM64' else 0x8664
        image_holds = ExitStack()
        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel.CreateFileW.argtypes = [ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p, ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p]
        kernel.CreateFileW.restype = ctypes.c_void_p
        kernel.CloseHandle.argtypes = [ctypes.c_void_p]
        kernel.CloseHandle.restype = ctypes.c_int
        def hold_image(path):
            handle = kernel.CreateFileW(str(path), 0x80000000, 1, None, 3, 0x00200000, None)
            if handle in (None, ctypes.c_void_p(-1).value):
                raise OSError(ctypes.get_last_error(), 'Cannot hold declared native image')
            image_holds.callback(kernel.CloseHandle, handle)
        def check_image(row):
            if row['machine'] != expected_cpu:
                raise RuntimeError('Declared native image targets foreign host')
            path = Path(row['path']).resolve(strict=True)
            if hashlib.sha256(path.read_bytes()).hexdigest() != row['sha256']:
                raise RuntimeError('Changed declared native executable')
            with path.open('rb') as file:
                if file.read(2) != b'MZ':
                    raise RuntimeError('Native executable lacks PE header')
                file.seek(0x3c); offset = struct.unpack('<I', file.read(4))[0]
                file.seek(offset)
                if file.read(4) != b'PE\0\0' or struct.unpack('<H', file.read(2))[0] != row['machine']:
                    raise RuntimeError('Native executable machine differs')
            hold_image(path)
            return str(path)
        native_profile_bytes = args.windows_provider.read_bytes()
        def checked_tree(directory, expected):
            import stat
            rows = {'.': {'kind': 'directory', 'mode': stat.S_IMODE(directory.lstat().st_mode)}}
            for path in sorted(directory.rglob('*')):
                name = path.relative_to(directory).as_posix()
                mode = stat.S_IMODE(path.lstat().st_mode)
                if path.is_symlink(): rows[name] = {'kind': 'link', 'mode': mode, 'link': os.readlink(path)}
                elif path.is_dir(): rows[name] = {'kind': 'directory', 'mode': mode}
                elif path.is_file(): rows[name] = {'kind': 'file', 'mode': mode, 'size': path.stat().st_size, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
                else: raise RuntimeError('Foreign native provider payload kind')
            if rows != expected: raise RuntimeError('Changed complete declared provider payload')
        def native_payloads():
            for package in profile['inputPackages']:
                if hashlib.sha256(Path(package['archive']).read_bytes()).hexdigest() != package['archiveSHA256']:
                    raise RuntimeError('Changed actual provider source archive')
                checked_tree(Path(package['root']), package['members'])
            checked_tree(Path(profile['pythonRoot']), profile['pythonRuntime'])
            checked_tree(Path(profile['nim']['path']).parent.parent, profile['nativeRuntime'])
            checked_tree(Path(profile['zstdRoot']), profile['zstdSDK'])
            for dependency in profile['dependencies']:
                directory = Path(dependency['path'])
                if subprocess.check_output(['git', '-C', str(directory), 'rev-parse', 'HEAD'], text=True).strip() != dependency['revision']:
                    raise RuntimeError('Changed declared Nim dependency revision')
                listing = subprocess.check_output(['git', '-C', str(directory), 'ls-tree', '-rz', '--full-tree', dependency['revision']])
                expected_members = {row[0]: row[1] for row in dependency['source']}
                actual_members = {}
                for record in listing.split(b'\0'):
                    if not record: continue
                    metadata, name = record.split(b'\t', 1)
                    mode, kind, oid = metadata.split()
                    if kind != b'blob': raise RuntimeError('Foreign dependency source kind')
                    actual_members[os.fsdecode(name)] = mode.decode()
                    blob_oid = oid.decode()
                    # Physical bodies below are checked against this actual Git tree.
                if actual_members != expected_members:
                    raise RuntimeError('Changed complete dependency mode/membership')
                index = subprocess.check_output(['git', '-C', str(directory), 'ls-files', '--stage', '-z'])
                index_members = {}
                tree_index = {record.split(b'\t',1)[1]: tuple(record.split(b'\t',1)[0].split()[i] for i in (0,2)) for record in listing.split(b'\0') if record}
                for record in index.split(b'\0'):
                    if not record: continue
                    metadata, name = record.split(b'\t', 1)
                    mode, oid, stage = metadata.split()
                    if stage != b'0': raise RuntimeError('Unmerged dependency index')
                    if (mode, oid) != tree_index.get(name):
                        raise RuntimeError('Dependency index differs from declared tree')
                    index_members[os.fsdecode(name)] = mode.decode()
                if subprocess.check_output(['git', '-C', str(directory), 'status', '--porcelain']):
                    raise RuntimeError('Changed complete dependency worktree membership')
                if index_members != expected_members:
                    raise RuntimeError('Changed dependency index mode/membership')
                for name, mode, sha in dependency['source']:
                    path = directory / name
                    body = os.fsencode(os.readlink(path)) if path.is_symlink() else path.read_bytes()
                    if hashlib.sha256(body).hexdigest() != sha:
                        raise RuntimeError('Changed declared Nim dependency body')
        if Path(profile['supplierRoot']).resolve(strict=True) != environment:
            raise RuntimeError('Native profile supplier root differs')
        expected_script = root / 'ci/native-entry-provider.py'
        if hashlib.sha256(expected_script.read_bytes()).hexdigest() != profile['providerScriptSHA256']:
            raise RuntimeError('Native profile constructor differs from owning source')
        def actual_supplier_source():
            rows = []
            listing = subprocess.check_output(['git', '-C', str(environment), 'ls-tree', '-rz', '--full-tree', head])
            for record in listing.split(b'\0'):
                if not record: continue
                metadata, name = record.split(b'\t', 1)
                mode, kind, oid = metadata.split()
                if kind != b'blob': raise RuntimeError('Unknown supplier Git source kind')
                path = environment / os.fsdecode(name)
                body = os.fsencode(os.readlink(path)) if path.is_symlink() else path.read_bytes()
                blob = subprocess.check_output(['git', '-C', str(environment), 'cat-file', 'blob', oid.decode()])
                if body != blob: raise RuntimeError('Changed actual supplier source body')
                rows.append([os.fsdecode(name), mode.decode(), hashlib.sha256(body).hexdigest()])
            if subprocess.check_output(['git', '-C', str(environment), 'status', '--porcelain']):
                raise RuntimeError('Changed actual supplier source namespace')
            return rows
        if actual_supplier_source() != profile['supplierSource']:
            raise RuntimeError('Native profile actual source differs')
        native_payloads()
        nim = check_image(profile['nim']); clang = check_image(profile['clang'])
        rust_images_before = {}
        for role in ('cargo', 'rustc'):
            selected = shutil.which(role)
            if not selected: raise RuntimeError('Declared native Rust tool absent')
            row = {'path': str(Path(selected).resolve(strict=True)), 'machine': expected_cpu, 'sha256': hashlib.sha256(Path(selected).read_bytes()).hexdigest()}
            check_image(row); rust_images_before[role] = row
        if Path(sys.executable).resolve(strict=True) != Path(profile['python']['path']).resolve(strict=True):
            raise RuntimeError('Gate interpreter differs from declared native provider')
        check_image(profile['python'])
        nim_env = os.environ.copy()
        sdk = Path(profile['zstdRoot']).resolve(strict=True)
        rust_target = 'aarch64-pc-windows-gnullvm' if expected_cpu == 0xaa64 else 'x86_64-pc-windows-gnullvm'
        rust_key = rust_target.upper().replace('-', '_')
        linker_key = 'CARGO_TARGET_' + rust_key + '_LINKER'
        cc_key = 'CC_' + rust_target.replace('-', '_')
        for key in (linker_key, cc_key):
            if os.environ.get(key) and Path(os.environ[key]).resolve() != Path(clang):
                raise RuntimeError('Foreign declared native Rust C-link role')
        rust_env = os.environ.copy()
        rust_env[linker_key] = clang
        rust_env[cc_key] = clang
        rust_env['AR_' + rust_target.replace('-', '_')] = check_image(profile['archiveTool'])
        native_flags = ['--skipUserCfg', '--skipParentCfg', '--cc:clang',
            '--lib:' + str(Path(nim).parent.parent / 'lib'), '--clang.exe:' + clang, '--clang.linkerexe:' + clang,
            '-d:msys2MingwInc=' + str(sdk / 'include'),
            '-d:msys2MingwLib=' + str(sdk / 'lib')]
        for dependency in profile['dependencies']:
            native_flags.append('-p:' + dependency['path'])
    else:
        if not args.direnv:
            raise RuntimeError('Required declared Unix direnv selector absent')
        # Capture activation directly and fail closed. direnv exec alone can execute the
        # requested command after a failed activation; no ambient Nim is admitted here.
        if not args.unix_runtime_authority:
            raise RuntimeError('Required genuine Unix runtime constructor authority absent')
        authority_path = args.unix_runtime_authority.absolute()
        image_holds = ExitStack()
        authority_parents = []
        for parent in reversed(authority_path.parents):
            before = parent.lstat()
            if not stat.S_ISDIR(before.st_mode) or parent.resolve(strict=True) != parent:
                raise RuntimeError('Unix authority parent is not its physical directory')
            descriptor = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            image_holds.callback(os.close, descriptor)
            held = os.fstat(descriptor)
            if (before.st_dev, before.st_ino, before.st_mode) != (held.st_dev, held.st_ino, held.st_mode):
                raise RuntimeError('Unix authority parent changed at open')
            authority_parents.append((parent, descriptor, before))
        descriptor = os.open(authority_path, os.O_RDONLY | os.O_NOFOLLOW)
        image_holds.callback(os.close, descriptor)
        authority_stat = os.fstat(descriptor)
        if not stat.S_ISREG(authority_stat.st_mode) or authority_stat.st_uid != os.getuid() or authority_stat.st_nlink != 1:
            raise RuntimeError('Unix runtime authority physical owner or kind differs')
        with os.fdopen(os.dup(descriptor), 'rb') as authority_stream:
            authority_bytes = authority_stream.read()
        def guard_unix_authority():
            for parent, held_descriptor, before in authority_parents:
                now = parent.lstat(); held = os.fstat(held_descriptor)
                if parent.resolve(strict=True) != parent or (now.st_dev, now.st_ino, now.st_mode) != (before.st_dev, before.st_ino, before.st_mode) or (held.st_dev, held.st_ino, held.st_mode) != (before.st_dev, before.st_ino, before.st_mode):
                    raise RuntimeError('Held Unix authority parent changed')
            now = authority_path.lstat()
            if (now.st_dev, now.st_ino, now.st_mode, now.st_uid, now.st_nlink) != (authority_stat.st_dev, authority_stat.st_ino, authority_stat.st_mode, authority_stat.st_uid, 1) or authority_path.read_bytes() != authority_bytes:
                raise RuntimeError('Held Unix runtime authority changed')
        unix_authority_guard = guard_unix_authority
        unix_authority_guard()
        if args.unix_runtime_authority.is_symlink() or not stat.S_ISREG(authority_path.lstat().st_mode):
            raise RuntimeError('Unix runtime authority is not an ordinary source-owned file')
        def unique_authority(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise RuntimeError('Duplicate Unix runtime authority key')
                result[key] = value
            return result
        authority = json.loads(authority_bytes, object_pairs_hook=unique_authority)
        if authority.get('schema') != 'codetracer-trace-format.unix-runtime-authority.v1' or set(authority.get('expectedSelectedImages', {})) != {'nim', 'nimble', 'git', 'gcc', 'bash'}:
            raise RuntimeError('Unix runtime authority mandatory role schema differs')
        image_fields = {'lexical', 'resolved', 'dev', 'ino', 'mode', 'sha256'}
        role_rows = [authority['selectedDirenvImage'], *authority['expectedSelectedImages'].values()]
        if any(not isinstance(row, dict) or set(row) != image_fields or not all(isinstance(row[key], str) for key in ['lexical', 'resolved', 'sha256']) or not all(isinstance(row[key], int) and not isinstance(row[key], bool) for key in ['dev', 'ino', 'mode']) for row in role_rows):
            raise RuntimeError('Unix runtime producer and consumer image field shape differs')
        if not all(authority.get(key) is True for key in ['finished', 'success', 'finalGuards']):
            raise RuntimeError('Unix runtime constructor authority incomplete')
        if authority.get('peerRevision') != head or authority.get('envrcSha256') != hashlib.sha256((environment / '.envrc').read_bytes()).hexdigest() or authority.get('flakeLockSha256') != hashlib.sha256((environment / 'flake.lock').read_bytes()).hexdigest():
            raise RuntimeError('Unix runtime authority differs from current locked supplier')
        if authority.get('constructorSourceSha256') != hashlib.sha256((root / 'ci/prepare-cross-reader-runtime.py').read_bytes()).hexdigest():
            raise RuntimeError('Unix runtime authority producer source differs')
        def current_role(path):
            lexical = Path(path); resolved = lexical.resolve(strict=True); physical = resolved.lstat()
            if not stat.S_ISREG(physical.st_mode):
                raise RuntimeError('Selected Unix runtime role is not regular')
            return {'lexical': str(lexical), 'resolved': str(resolved), 'dev': physical.st_dev,
                    'ino': physical.st_ino, 'mode': stat.S_IMODE(physical.st_mode),
                    'sha256': hashlib.sha256(resolved.read_bytes()).hexdigest()}
        if current_role(args.direnv) != authority.get('selectedDirenvImage'):
            raise RuntimeError('Unix runtime authority selected foreign Direnv')
        def current_peer_namespace():
            result = []
            def walk(path, relative):
                value = path.lstat()
                common = [relative, value.st_dev, value.st_ino, stat.S_IMODE(value.st_mode), value.st_uid]
                if stat.S_ISLNK(value.st_mode):
                    result.append(common + ['link', os.readlink(path)])
                elif stat.S_ISREG(value.st_mode):
                    result.append(common + ['file', hashlib.sha256(path.read_bytes()).hexdigest()])
                elif stat.S_ISDIR(value.st_mode):
                    result.append(common + ['dir'])
                    for child in sorted(path.iterdir()):
                        if relative == '.' and child.name == '.git': continue
                        walk(child, child.name if relative == '.' else relative + '/' + child.name)
                else: raise RuntimeError('Special current peer namespace member')
            walk(environment, '.')
            return result
        declared_generated = {'.direnv', '.nimble', '.pre-commit-config.yaml'}
        if authority.get('prospectiveGeneratedRoots') != sorted(declared_generated) or not isinstance(authority.get('ordinarySourceNamespace'), list) or not isinstance(authority.get('generatedOutputAfter'), list):
            raise RuntimeError('Unix runtime authority namespace schema differs')
        expected_generated = authority['generatedOutputAfter']
        expected_ordinary = authority['ordinarySourceNamespace']
        reload_holds = []
        def guard_peer_execution_namespace():
            for path, descriptor, expected in reload_holds:
                now = path.lstat(); held = os.fstat(descriptor)
                if (now.st_dev, now.st_ino, now.st_mode, now.st_uid, now.st_nlink) != expected or (held.st_dev, held.st_ino, held.st_mode, held.st_uid, held.st_nlink) != expected:
                    raise RuntimeError('Selected reload output physical identity changed')
            rows = current_peer_namespace()
            if [row for row in rows if row[0].split('/')[0] in declared_generated] != expected_generated or [row for row in rows if row[0].split('/')[0] not in declared_generated] != expected_ordinary:
                raise RuntimeError('Current peer source or generated execution namespace differs')
        guard_peer_execution_namespace()
        module_path = Path(args.direnv).parents[1] / 'share/trace-cross-reader/nix-direnvrc'
        expected_module = authority.get('selectedNixDirenvModuleImage')
        if current_role(module_path) != expected_module:
            raise RuntimeError('Selected nix-direnv output constructor differs')
        reload_before = (environment / '.direnv/bin/nix-direnv-reload').lstat()
        if not stat.S_ISREG(reload_before.st_mode) or reload_before.st_uid != os.getuid() or reload_before.st_nlink != 1:
            raise RuntimeError('Initial reload output ownership differs')
        # Verify source authority before export can execute its admitted RC/cache.
        unix_authority_guard()
        activation = subprocess.run([args.direnv, 'export', 'json'], cwd=environment,
                                    capture_output=True, text=True)
        if activation.returncode:
            raise RuntimeError('Required declared Nim environment activation failed')
        # Direnv2.37.1 returns no output when its loaded RC needs no update.
        # The same empty output also occurs without any RC, so empty export
        # requires the actual constructor's current loaded-RC association.
        exports = json.loads(activation.stdout) if activation.stdout.strip() else {}
        if not activation.stdout.strip():
            if os.environ.get('DIRENV_DIR') != authority.get('loadedRcDirectory') or authority.get('loadedRcDirectory') != '-' + str(environment):
                raise RuntimeError('Empty Unix activation lacks its actual current loaded RC')
        if not isinstance(exports, dict):
            raise RuntimeError('Declared Nim activation did not supply its tool PATH')
        nim_env = os.environ.copy()
        for key, value in exports.items():
            if value is None:
                nim_env.pop(key, None)
            else:
                nim_env[key] = value
        def current_loaded_rc():
            # Exact Direnv2.37.1 cmd_status.go schema; this operation does not source RC.
            status_result = subprocess.run([args.direnv, 'status', '--json'], cwd=environment,
                                           env=nim_env, capture_output=True, text=True)
            if status_result.returncode:
                raise RuntimeError('Selected Direnv current RC inspection refused')
            status = json.loads(status_result.stdout, object_pairs_hook=unique_authority)
            expected = str(environment / '.envrc')
            for field in ['loadedRC', 'foundRC']:
                row = status.get('state', {}).get(field)
                if not isinstance(row, dict) or set(row) != {'path', 'allowed'} or row['path'] != expected or type(row['allowed']) is not int or row['allowed'] != 0:
                    raise RuntimeError('Selected Direnv lacks current allowed supplier RC')
            if nim_env.get('DIRENV_FILE') != expected or nim_env.get('DIRENV_DIR') != '-' + str(environment):
                raise RuntimeError('Selected Direnv RC environment association differs')
        current_loaded_rc()
        nim = shutil.which('nim', path=nim_env['PATH'])
        if not nim or not Path(nim).resolve(strict=True).is_relative_to('/nix/store'):
            raise RuntimeError('Declared Nim activation did not select its immutable compiler')
        for role, expected in authority['expectedSelectedImages'].items():
            actual = shutil.which(role, path=nim_env['PATH'])
            if not actual or current_role(actual) != expected:
                raise RuntimeError('Unix activation differs from independently selected constructor role')
        # A successful selected activation may recreate this exact nix-direnv output.
        # Its source/body/mode and every other namespace member remain mandatory.
        module_path = Path(args.direnv).parents[1] / 'share/trace-cross-reader/nix-direnvrc'
        expected_module = authority.get('selectedNixDirenvModuleImage')
        if current_role(module_path) != expected_module:
            raise RuntimeError('Selected nix-direnv output constructor differs')
        rows = current_peer_namespace()
        generated_now = [row for row in rows if row[0].split('/')[0] in declared_generated]
        if [row for row in rows if row[0].split('/')[0] not in declared_generated] != expected_ordinary:
            raise RuntimeError('Selected activation changed ordinary peer source')
        prior = {row[0]: row for row in expected_generated}
        current = {row[0]: row for row in generated_now}
        reload_name = '.direnv/bin/nix-direnv-reload'
        if set(prior) != set(current) or any(prior[name] != current[name] for name in prior if name != reload_name):
            raise RuntimeError('Selected activation changed another generated member')
        before = prior[reload_name]; after = current[reload_name]
        if before[:2] + before[3:] != after[:2] + after[3:] or after[5] != 'file' or after[4] != os.getuid():
            raise RuntimeError('Selected reload constructor body or mode changed')
        reload_path = environment / reload_name
        descriptor = os.open(reload_path, os.O_RDONLY | os.O_NOFOLLOW)
        image_holds.callback(os.close, descriptor)
        held = os.fstat(descriptor)
        expected = (held.st_dev, held.st_ino, held.st_mode, held.st_uid, held.st_nlink)
        if not stat.S_ISREG(held.st_mode) or held.st_nlink != 1 or held.st_uid != os.getuid() or [held.st_dev, held.st_ino, stat.S_IMODE(held.st_mode), held.st_uid] != after[1:5]:
            raise RuntimeError('Selected reload output ownership changed at open')
        reload_holds.append((reload_path, descriptor, expected))
        expected_generated = generated_now
        receipt_guard = unix_authority_guard
        def guard_current_unix_runtime():
            receipt_guard()
            current_loaded_rc()
            if current_role(module_path) != expected_module:
                raise RuntimeError('Held nix-direnv output constructor changed')
            guard_peer_execution_namespace()
            if hashlib.sha256((environment / '.envrc').read_bytes()).hexdigest() != authority['envrcSha256'] or hashlib.sha256((environment / 'flake.lock').read_bytes()).hexdigest() != authority['flakeLockSha256']:
                raise RuntimeError('Unix runtime source authority changed')
            actual_head = subprocess.check_output([authority['expectedSelectedImages']['git']['lexical'], '-C', str(environment), 'rev-parse', 'HEAD'], text=True).strip()
            if actual_head != authority['peerRevision'] or current_role(args.direnv) != authority['selectedDirenvImage']:
                raise RuntimeError('Unix runtime source or Direnv role changed')
            for role, expected in authority['expectedSelectedImages'].items():
                actual = shutil.which(role, path=nim_env['PATH'])
                if not actual or current_role(actual) != expected:
                    raise RuntimeError('Held Unix selected runtime role changed')
        unix_authority_guard = guard_current_unix_runtime
        unix_authority_guard()

    def nim_run(argv):
        if unix_authority_guard is not None:
            unix_authority_guard()
        if argv[0] != 'nim':
            raise RuntimeError('Unsupported declared Nim invocation')
        subprocess.run([nim, argv[1], *native_flags, *argv[2:]], cwd=environment, env=nim_env, check=True)

    # Retain real artifacts for source-bound qualification and diagnosis.
    if args.work_root:
        work = args.work_root.resolve(); work.mkdir(exist_ok=False)
    else:
        work = Path(tempfile.mkdtemp(prefix='entry-coupled-', dir=root.parent))
    print(work, flush=True)
    nim_images = work / 'nim-images'
    rust_images = work / 'rust-images'
    nim_images.mkdir()
    rust_images.mkdir()
    binary = work / ('entry-coupled.exe' if os.name == 'nt' else 'entry-coupled')
    nim_run(['nim', 'c', '-d:release', '--mm:arc', '-p:' + str(source / 'src'),
             '--nimcache:' + str(work / 'nimcache'), '-o:' + str(binary),
             str(root / 'tests/entry_coupled.nim')])
    if os.name == 'nt':
        producer_row = {'path': str(binary), 'machine': expected_cpu, 'sha256': hashlib.sha256(binary.read_bytes()).hexdigest()}
        check_image(producer_row)
    subprocess.run([str(binary), 'produce', str(nim_images)], check=True)
    if os.name == 'nt':
        subprocess.run(['cargo', 'build', '--locked', '--offline', '-p',
            'codetracer_trace_reader', '--example', 'entry_cross_fixture', '--target', rust_target], cwd=root, env=rust_env, check=True)
        cargo_target = Path(os.environ.get('CARGO_TARGET_DIR', root / 'target'))
        if not cargo_target.is_absolute(): cargo_target = root / cargo_target
        rust_producer = cargo_target / rust_target / 'debug/examples/entry_cross_fixture.exe'
        rust_producer_row = {'path': str(rust_producer), 'machine': expected_cpu,
            'sha256': hashlib.sha256(rust_producer.read_bytes()).hexdigest()}
        check_image(rust_producer_row)
        subprocess.run([str(rust_producer), str(rust_images)], cwd=root, check=True)
    else:
        subprocess.run(['cargo', 'run', '--locked', '--offline', '-p',
                        'codetracer_trace_reader', '--example', 'entry_cross_fixture',
                        '--', str(rust_images)], cwd=root, check=True)
    subprocess.run([str(binary), 'read-rust', str(rust_images)], check=True)
    env = rust_env.copy() if os.name == 'nt' else nim_env.copy()
    if os.name != 'nt':
        env['ENTRY_NIM_RUNTIME_AUTHORITY'] = str(authority_path)
        # The sealed producer already resolved and compiled the real requirements.
        # Use build.rs's prepared-dependency interface without mutating that seed.
        installed = authority.get('expectedSeedInstalledPackages')
        if not isinstance(installed, list) or not installed:
            raise RuntimeError('Prepared Nim dependency inventory is missing')
        package_paths = []
        for package in installed:
            if not isinstance(package, dict) or set(package) != {'name', 'version', 'checksum'}:
                raise RuntimeError('Prepared Nim dependency inventory differs')
            path = environment / '.nimble' / 'pkgs2' / (package['name'] + '-' + package['version'] + '-' + package['checksum'])
            if not path.is_dir() or path.is_symlink():
                raise RuntimeError('Prepared Nim dependency source is missing')
            package_paths.append(str(path))
        guard_peer_execution_namespace()
        env['CODETRACER_TRACE_FORMAT_NIM_SKIP_NIMBLE_INSTALL'] = '1'
        env['CODETRACER_TRACE_FORMAT_NIM_EXTRA_PATHS'] = os.pathsep.join(package_paths)
    if args.prepare_only and args.fixture_output is None:
        raise RuntimeError('Declared fixture output is required')
    published_root = args.fixture_output if args.prepare_only else root / 'target' / 'entry-fixtures'
    (root / 'target').mkdir(mode=0o700, exist_ok=True)
    import importlib.util
    helper = root / 'tests/publish_entry_fixtures.py'
    spec = importlib.util.spec_from_file_location('publish_entry_fixtures', helper)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    module.publish(root, nim_images, published_root)
    env['ENTRY_NIM_FIXTURE_ROOT'] = str(published_root)
    # Preserve every original consumer command. All branches consume the same
    # committed three-member producer unit rather than the raw two images.
    if args.prepare_only:
        pass
    elif os.name == 'nt':
        # Additive required entry gate; original Windows workspace Test argv stays intact.
        subprocess.run(['cargo', 'test', '--locked', '-p', 'codetracer_trace_reader',
            '--test', 'recorded_entry_identity', '--test', 'recorded_entry_sealed', '--target', rust_target], cwd=root, env=env, check=True)
    else:
        subprocess.run(['cargo', 'test', '--verbose'], cwd=root, env=env, check=True)

    if os.name == 'nt':
        native_payloads()
        if actual_supplier_source() != profile['supplierSource']:
            raise RuntimeError('Changed supplier source during required gate')
        if receipt_path.read_bytes() != constructor_receipt_bytes:
            raise RuntimeError('Changed constructor receipt during gate')
        if args.windows_provider.read_bytes() != native_profile_bytes:
            raise RuntimeError('Changed native profile during required gate')
        check_image(profile['nim']); check_image(profile['clang']); check_image(producer_row); check_image(rust_producer_row)
        for row in rust_images_before.values(): check_image(row)
        # Required producer artifact edge for the subsequent ORIGINAL Windows Test.
        import importlib.util
        helper = root / 'ci/append-entry-fixture-environment.py'
        spec = importlib.util.spec_from_file_location('entry_fixture_environment', helper)
        module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
        if not args.prepare_only:
            module.append_fixture_root(published_root)

finally:
    try:
        if unix_authority_guard is not None:
            unix_authority_guard()
    finally:
        if image_holds is not None:
            image_holds.close()
