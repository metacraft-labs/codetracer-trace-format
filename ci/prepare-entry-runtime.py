"""Construct an exclusive locked Unix peer and its genuine runtime authority."""
from pathlib import Path
import argparse, ctypes, errno, hashlib, json, os, stat, subprocess, sys, time, tomllib
sys.dont_write_bytecode = True
class Once(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        if getattr(namespace, self.dest, None) is not None:
            parser.error('duplicate runtime constructor argument: ' + option_string)
        setattr(namespace, self.dest, value)

p = argparse.ArgumentParser()
for name in ['root', 'output', 'python-profile', 'direnv-profile', 'just-profile', 'git', 'nix', 'nimble-source']:
    p.add_argument('--' + name, required=True, type=Path, action=Once)
p.add_argument('--nix-version', required=True, action=Once)
a = p.parse_args()
root = a.root.resolve(strict=True)
out = a.output.absolute()
proof = {'finished': False, 'success': False, 'scope': 'Declared runtime constructor only; original tests unexecuted', 'stages': []}
active = None
receipt = None
receipt_fd = None
held_dirs = []
held_images = []
held_evidence = []
config = None
source = None
supplier_namespace = None
supplier_index = None
selected_images = None
nimble_source_before = None
profile_trees = None
reused = False

def digest(path): return hashlib.sha256(path.read_bytes()).hexdigest()

def namespace(path):
    result = {}
    for item in [path, *sorted(path.rglob('*'))]:
        value = item.lstat()
        kind = stat.S_IFMT(value.st_mode)
        if kind == stat.S_IFREG:
            body = digest(item)
        elif kind == stat.S_IFLNK:
            body = os.readlink(item)
        elif kind == stat.S_IFDIR:
            body = None
        else:
            raise RuntimeError('Unsupported runtime source member')
        result[str(item.relative_to(path))] = [value.st_dev, value.st_ino, value.st_mode, value.st_uid, kind, body]
    return result

def image(path):
    lexical = path.absolute()
    actual = lexical.resolve(strict=True)
    value = actual.stat()
    if not stat.S_ISREG(value.st_mode):
        raise RuntimeError('Runtime tool is not a regular image')
    return [str(lexical), str(actual), value.st_dev, value.st_ino, value.st_mode, digest(actual)]


def hold_directory(path):
    path = path.absolute()
    descriptor = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    held_dirs.append((Path(path.anchor), descriptor))
    cursor = Path(path.anchor)
    for name in path.parts[1:]:
        descriptor = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
        cursor = cursor / name
        held_dirs.append((cursor, descriptor))
    return descriptor

def hold_image(path):
    expected = image(path)
    descriptor = os.open(expected[1], os.O_RDONLY | os.O_NOFOLLOW)
    current = os.fstat(descriptor)
    if [current.st_dev, current.st_ino, current.st_mode] != expected[2:5]:
        os.close(descriptor)
        raise RuntimeError('Runtime image changed at hold')
    held_images.append((expected, descriptor))
    return expected

def hold_evidence(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    value = os.fstat(descriptor)
    if not stat.S_ISREG(value.st_mode) or value.st_uid != os.getuid() or value.st_nlink != 1:
        os.close(descriptor); raise RuntimeError('Cached runtime evidence owner or kind differs')
    with os.fdopen(os.dup(descriptor), 'rb') as stream: body = stream.read()
    held_evidence.append((path, descriptor, value, body))
    return body

def boundary():
    for path, descriptor, initial, body in held_evidence:
        lexical = path.lstat(); held = os.fstat(descriptor)
        if (lexical.st_dev, lexical.st_ino, lexical.st_mode, lexical.st_uid, lexical.st_nlink) != (initial.st_dev, initial.st_ino, initial.st_mode, os.getuid(), 1) or (held.st_dev, held.st_ino, held.st_mode) != (initial.st_dev, initial.st_ino, initial.st_mode) or path.read_bytes() != body:
            raise RuntimeError('Held cached runtime evidence changed')
    if receipt_fd is not None:
        lexical = receipt.lstat(); held = os.fstat(receipt_fd)
        if not stat.S_ISREG(lexical.st_mode) or lexical.st_nlink != 1 or (lexical.st_dev, lexical.st_ino, lexical.st_mode, lexical.st_uid) != (held.st_dev, held.st_ino, held.st_mode, os.getuid()):
            raise RuntimeError('Held runtime receipt identity changed')
    for path, descriptor in held_dirs:
        lexical = path.lstat(); held = os.fstat(descriptor)
        if not stat.S_ISDIR(lexical.st_mode) or (lexical.st_dev, lexical.st_ino, lexical.st_mode) != (held.st_dev, held.st_ino, held.st_mode):
            raise RuntimeError('Held runtime directory identity changed')
    for expected, descriptor in held_images:
        held = os.fstat(descriptor)
        if image(Path(expected[0])) != expected or [held.st_dev, held.st_ino, held.st_mode] != expected[2:5]:
            raise RuntimeError('Held runtime executable identity changed')
    if profile_trees is not None and any(namespace(Path(path)) != before for path, before in profile_trees.items()):
        raise RuntimeError('Selected runtime recursive closure changed')
    if nimble_source_before is not None and namespace(a.nimble_source) != nimble_source_before:
        raise RuntimeError('Selected Nimble source constructor changed')
    if source is not None and any(digest(root / name) != value for name, value in source.items()):
        raise RuntimeError('Runtime constructor source changed')
    if supplier_namespace is not None and namespace(original) != supplier_namespace:
        raise RuntimeError('Original supplier physical namespace changed')
    if supplier_index is not None and (original / '.git').is_dir() and image(original / '.git/index') != supplier_index:
        raise RuntimeError('Original supplier index changed')
    if selected_images is not None and any(image(Path(row[0])) != row for row in selected_images):
        raise RuntimeError('Selected runtime image changed')

# Darwin ABI: Apple xnu bsd/sys/proc_info.h, PROC_PIDTBSDINFO=3.
# Native qualification is required; Linux syntax checks do not qualify this seam.
class BsdInfo(ctypes.Structure):
    _fields_ = [(name, ctypes.c_uint32) for name in [
        'flags', 'status', 'xstatus', 'pid', 'ppid', 'uid', 'gid',
        'ruid', 'rgid', 'svuid', 'svgid', 'reserved']] + [
        ('comm', ctypes.c_char * 16), ('name', ctypes.c_char * 32)] + [
        (name, ctypes.c_uint32) for name in ['nfiles', 'pgid', 'jobc', 'tdev', 'tpgid']] + [
        ('nice', ctypes.c_int32), ('start_sec', ctypes.c_uint64), ('start_usec', ctypes.c_uint64)]
libproc = None
if sys.platform == 'darwin':
    if ctypes.sizeof(BsdInfo) != 136 or BsdInfo.start_sec.offset != 120:
        raise RuntimeError('Darwin process ABI differs')
    libproc = ctypes.CDLL('/usr/lib/libproc.dylib', use_errno=True)
    libproc.proc_listpids.argtypes = [ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p, ctypes.c_int]
    libproc.proc_listpids.restype = ctypes.c_int
    libproc.proc_pidinfo.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_uint64, ctypes.c_void_p, ctypes.c_int]
    libproc.proc_pidinfo.restype = ctypes.c_int

def process_identity(pid):
    if libproc is not None:
        info = BsdInfo()
        size = libproc.proc_pidinfo(pid, 3, 0, ctypes.byref(info), ctypes.sizeof(info))
        if size != ctypes.sizeof(info):
            raise OSError(ctypes.get_errno(), 'Darwin process identity unavailable')
        if info.pid != pid or info.start_usec >= 1000000:
            raise RuntimeError('Darwin process identity malformed')
        if info.status not in [1, 2, 3, 4, 5]:
            raise RuntimeError('Darwin process state unknown')
        return {'pid': pid, 'birth': [info.start_sec, info.start_usec], 'sid': os.getsid(pid), 'uid': info.uid, 'state': info.status}
    fields = (Path('/proc') / str(pid) / 'stat').read_text().rsplit(')', 1)[1].split()
    return {'pid': pid, 'birth': int(fields[19]), 'sid': int(fields[3])}

def census(sid):
    alive = []; unknown = []
    if libproc is not None:
        required = libproc.proc_listpids(1, 0, None, 0)
        if required <= 0: raise RuntimeError('Darwin process census unavailable')
        slots = (ctypes.c_int * (required // ctypes.sizeof(ctypes.c_int) + 1024))()
        count = libproc.proc_listpids(1, 0, slots, ctypes.sizeof(slots))
        if count <= 0 or count >= ctypes.sizeof(slots) or count % ctypes.sizeof(ctypes.c_int):
            raise RuntimeError('Darwin process census truncated or malformed')
        for pid in slots[:count // ctypes.sizeof(ctypes.c_int)]:
            if pid <= 0: continue
            try:
                identity = process_identity(pid)
                if identity['sid'] == sid and identity['state'] != 5: alive.append([pid, identity['birth']])
            except OSError as error:
                if error.errno not in [errno.ESRCH, errno.ENOENT]: unknown.append([pid, type(error).__name__])
            except RuntimeError as error: unknown.append([pid, type(error).__name__])
        return {'alive': alive, 'unknown': unknown}
    for entry in Path('/proc').iterdir():
        if not entry.name.isdecimal(): continue
        try:
            fields = (entry / 'stat').read_text().rsplit(')', 1)[1].split()
            if int(fields[3]) == sid and fields[0] != 'Z': alive.append([int(entry.name), int(fields[19])])
        except (FileNotFoundError, ProcessLookupError): pass
        except (OSError, ValueError, IndexError) as error: unknown.append([int(entry.name), type(error).__name__])
    return {'alive': alive, 'unknown': unknown}
def save():
    if receipt_fd is not None:
        body = (json.dumps(proof, indent=2) + '\n').encode()
        os.lseek(receipt_fd, 0, os.SEEK_SET)
        pending = memoryview(body)
        while pending:
            pending = pending[os.write(receipt_fd, pending):]
        os.ftruncate(receipt_fd, len(body)); os.fsync(receipt_fd)
def operation(name, argv, cwd):
    global active
    boundary()
    row = {'name': name, 'argv': list(map(str, argv)), 'cwd': str(cwd)}
    proof['stages'].append(row); save()
    active = subprocess.Popen(argv, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True, umask=0o022)
    row['leader'] = process_identity(active.pid); save()
    stdout, stderr = active.communicate(); row['exit'] = active.returncode
    row['stdoutBytes'] = len(stdout); row['stderrBytes'] = len(stderr)
    while True:
        row['drain'] = census(row['leader']['sid']); save()
        if row['drain']['unknown']: raise RuntimeError('Unknown runtime constructor session census')
        if not row['drain']['alive']: break
        time.sleep(.1)
    row['terminal'] = True; active = None; boundary(); save()
    if row['exit']: raise RuntimeError('Declared runtime constructor child refused: ' + name)
    return stdout
try:
    if os.name != 'posix' or (libproc is None and not Path('/proc').is_dir()):
        raise RuntimeError('Unsupported native process identity constructor')
    if out != root / '.repro/build/entry-runtime':
        raise RuntimeError('Undeclared runtime output root')
    root_fd = hold_directory(root)
    if out.exists() or out.is_symlink():
        if out.is_symlink() or not out.is_dir() or out.stat().st_uid != os.getuid():
            raise RuntimeError('Foreign existing runtime output refused')
        hold_directory(out)
        hold_directory(out / 'runtime')
        owner = out / 'runtime-owner.json'
        if owner.is_symlink() or not owner.is_file() or owner.stat().st_uid != os.getuid() or owner.stat().st_nlink != 1:
            raise RuntimeError('Existing runtime output has no owned manifest')
        def unique(rows):
            result = {}
            for key, value in rows:
                if key in result: raise RuntimeError('Duplicate runtime owner field')
                result[key] = value
            return result
        recorded = json.loads(hold_evidence(owner), object_pairs_hook=unique)
        current = namespace(out); current.pop('runtime-owner.json', None)
        expected_names = {'repro.lock', 'flake.lock', 'ci/prepare-entry-runtime.py', 'ci/prepare-cross-reader-runtime.py', 'ci/generated-runtime-namespace.py', 'ci/entry-runtime-profile.nix', 'ci/locked-nim-library-profile.nix', 'ci/cross-reader-profiles.nix'}
        if not isinstance(recorded.get('source'), dict) or set(recorded['source']) != expected_names:
            raise RuntimeError('Existing runtime owner source membership differs')
        expected_source = {name: digest(root / name) for name in expected_names}
        if recorded.get('schema') != 'codetracer-trace-format.entry-runtime-owner.v1' or recorded.get('root') != str(root) or recorded.get('output') != str(out) or recorded.get('namespace') != current or recorded.get('source') != expected_source:
            raise RuntimeError('Existing runtime output ownership correspondence refused')
        expected_images = [image(path) for path in [a.git, a.python_profile / 'bin/python3', a.direnv_profile / 'bin/direnv', a.just_profile / 'bin/just', a.nix]]
        if recorded.get('selectedImages') != expected_images:
            raise RuntimeError('Existing runtime owner selected images differ')
        authority = json.loads(hold_evidence(out / 'runtime/peer-runtime-proof.json'), object_pairs_hook=unique)
        if not all(authority.get(key) is True for key in ['finished', 'success', 'finalGuards']):
            raise RuntimeError('Existing runtime authority incomplete')
        boundary()
        reused = True
        proof['success'] = True
        # Reuse returns without executing cached code or changing the original receipt.
        raise SystemExit(0)
    for parent in out.parents:
        if parent.exists() and (parent.is_symlink() or not parent.is_dir()):
            raise RuntimeError('Foreign runtime output ancestor')
    parent_fd = root_fd
    parent_path = root
    for name in ['.repro', 'build']:
        try: os.mkdir(name, mode=0o755, dir_fd=parent_fd)
        except FileExistsError: pass
        parent_fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
        parent_path = parent_path / name; held_dirs.append((parent_path, parent_fd))
    os.mkdir('entry-runtime', mode=0o755, dir_fd=parent_fd)
    output_fd = os.open('entry-runtime', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
    held_dirs.append((out, output_fd))
    os.mkdir('runtime', mode=0o755, dir_fd=output_fd)
    receipts = out / 'runtime'
    receipts_fd = os.open('runtime', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=output_fd)
    held_dirs.append((receipts, receipts_fd))
    receipt = receipts / 'constructor.json'
    receipt_fd = os.open('constructor.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=receipts_fd)
    save()
    lock = tomllib.loads((root / 'repro.lock').read_text())
    candidates = [row for row in lock['lock']['deps'] if row['name'] == 'codetracer-trace-format-nim']
    if len(candidates) != 1: raise RuntimeError('Nonunique locked runtime supplier')
    supplier = candidates[0]
    if supplier['coord_kind'] != 'vcs' or supplier['url'] != 'https://github.com/metacraft-labs/codetracer-trace-format-nim' or supplier['path'] != '../codetracer-trace-format-nim' or supplier['integrity'] != 'git-sha1:' + supplier['revision']:
        raise RuntimeError('Foreign locked runtime supplier')
    original = (root / supplier['path']).resolve(strict=True)
    git = a.git.resolve(strict=True); template = git.parent.parent / 'share/git-core/templates'
    if not template.is_dir() or template.is_symlink(): raise RuntimeError('Selected source Git template absent')
    source = {name: digest(root / name) for name in ['repro.lock', 'flake.lock', 'ci/prepare-entry-runtime.py', 'ci/prepare-cross-reader-runtime.py', 'ci/generated-runtime-namespace.py', 'ci/entry-runtime-profile.nix', 'ci/locked-nim-library-profile.nix', 'ci/cross-reader-profiles.nix']}
    supplier_namespace = namespace(original)
    selected_images = [hold_image(path) for path in [git, a.python_profile / 'bin/python3', a.direnv_profile / 'bin/direnv', a.just_profile / 'bin/just', a.nix]]
    runtime_roots = sorted({str(Path(row[1]).parents[1]) for row in selected_images} | {str(a.nimble_source)})
    if any(not value.startswith('/nix/store/') for value in runtime_roots):
        raise RuntimeError('Selected runtime role is outside declared store closure')
    if operation('selected-runtime-nix-version', [a.nix, '--version'], root).decode().strip() != 'nix (Nix) ' + a.nix_version:
        raise RuntimeError('Selected runtime Nix constructor version differs')
    closure = json.loads(operation('selected-runtime-path-info', [a.nix, 'path-info', '--offline', '--recursive', '--json', *runtime_roots], root))
    if not isinstance(closure, dict) or not closure:
        raise RuntimeError('Selected Nix path-info schema differs')
    paths = list(closure)
    if any(not isinstance(path, str) or not path.startswith('/nix/store/') or len(Path(path).parts) != 4 for path in paths):
        raise RuntimeError('Selected runtime closure path set malformed')
    if not set(runtime_roots).issubset(paths) or any(not isinstance(row, dict) or not isinstance(row.get('narSize'), int) or isinstance(row['narSize'], bool) or row['narSize'] < 0 or not isinstance(row.get('narHash'), str) or not row['narHash'].startswith('sha256-') or not isinstance(row.get('references'), list) or not set(row['references']).issubset(paths) for row in closure.values()):
        raise RuntimeError('Selected runtime recursive closure incomplete')
    operation('selected-runtime-content', [a.nix, 'store', 'verify', '--offline', '--no-trust', '--recursive', *runtime_roots], root)
    profile_trees = {path: namespace(Path(path)) for path in paths}
    proof['selectedRuntimeRegisteredClosure'] = closure
    nimble_source_before = namespace(a.nimble_source)
    boundary()
    if operation('source-head', [git, '-C', original, 'rev-parse', 'HEAD'], root).decode().strip() != supplier['revision']:
        raise RuntimeError('Actual supplier differs from owning generated lock')
    config = operation('source-config', [git, '-C', original, 'config', '--null', '--show-origin', '--list'], root)
    supplier_namespace = namespace(original)
    if (original / '.git').is_dir(): supplier_index = image(original / '.git/index')
    selected_images = [hold_image(path) for path in [git, a.python_profile / 'bin/python3', a.direnv_profile / 'bin/direnv', a.just_profile / 'bin/just', a.nix]]
    nimble_source_before = namespace(a.nimble_source)
    proof['selectedNimbleSource'] = {'path': str(a.nimble_source), 'physicalNamespace': nimble_source_before}
    boundary()
    peer = out / 'codetracer-trace-format-nim'
    operation('clone-locked-peer', [git, '-c', 'init.templateDir=' + str(template), 'clone', '--no-local', '--no-checkout', '--template=' + str(template), str(original), str(peer)], root)
    operation('checkout-locked-peer', [git, '-C', peer, 'checkout', '--detach', supplier['revision']], root)
    operation('prepare-real-runtime', [a.python_profile / 'bin/python3', '-B', root / 'ci/prepare-cross-reader-runtime.py', root, receipts, a.python_profile, a.direnv_profile, a.just_profile, git, a.nix, '--peer-root', peer], root)
    authority = json.loads((receipts / 'peer-runtime-proof.json').read_text())
    if not all(authority.get(key) is True for key in ['finished', 'success', 'finalGuards']) or authority.get('peerRevision') != supplier['revision']:
        raise RuntimeError('Actual locked runtime authority incomplete')
    proof['success'] = True
except SystemExit as error:
    if not reused: raise
except BaseException as error:
    proof['failureType'] = type(error).__name__
    proof['failure'] = str(error) if isinstance(error, RuntimeError) else 'Nonconstant details suppressed'
finally:
    try:
        try:
            if active is not None: raise RuntimeError('Runtime constructor child still active')
            boundary()
            if config is not None and subprocess.check_output([a.git, '-C', original, 'config', '--null', '--show-origin', '--list']) != config: raise RuntimeError('Memory-only supplier configuration changed')
            proof['finalGuards'] = True
        except BaseException as error:
            proof['finalGuards'] = False; proof['guardErrorType'] = type(error).__name__
        proof['finished'] = True; proof['success'] = bool(proof['success'] and proof['finalGuards']); save()
        if proof['success'] and not reused:
            owner_record = {'schema': 'codetracer-trace-format.entry-runtime-owner.v1', 'root': str(root), 'output': str(out), 'source': source, 'selectedImages': selected_images, 'namespace': namespace(out)}
            boundary()
            owner_fd = os.open('runtime-owner.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=output_fd)
            with os.fdopen(owner_fd, 'w') as owner_stream:
                owner_stream.write(json.dumps(owner_record, sort_keys=True) + '\n')
                owner_stream.flush(); os.fsync(owner_stream.fileno())
    except BaseException as error:
        proof['finished'] = True; proof['success'] = False; proof['finalGuards'] = False
        proof['publicationErrorType'] = type(error).__name__
        save()
    finally:
        if receipt_fd is not None: os.close(receipt_fd)
        for _, descriptor, _, _ in reversed(held_evidence): os.close(descriptor)
        for _, descriptor in reversed(held_images): os.close(descriptor)
        for _, descriptor in reversed(held_dirs): os.close(descriptor)
sys.exit(0 if proof['success'] else 1)
