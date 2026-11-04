"""Construct the real Windows Nim/Clang frontend component for the entry gate.

This proposal is not a native qualification receipt. It uses the declared
architecture archives, then executes the resulting native compiler and probe.
The complete source/archive trees are retained and checked after construction.
"""
from pathlib import Path, PurePosixPath
import argparse, ctypes, hashlib, json, os, platform, shutil, stat, struct, subprocess, sys
import tarfile, urllib.request, zipfile

parser = argparse.ArgumentParser()
parser.add_argument('--output', required=True, type=Path)
parser.add_argument('--nim-supplier', required=True, type=Path)
parser.add_argument('--nim-revision', required=True)
args = parser.parse_args()
if os.name != 'nt':
    raise RuntimeError('Actual Windows provider host required')
machine = platform.machine().upper()
if machine not in ('AMD64', 'ARM64'):
    raise RuntimeError('Unsupported native Windows machine')
cpu = 0xAA64 if machine == 'ARM64' else 0x8664
root = args.output.resolve()
root.mkdir(exist_ok=False)
supplier = args.nim_supplier.resolve(strict=True)
git = shutil.which('git')
if not git or subprocess.check_output([git, '-C', str(supplier), 'rev-parse', 'HEAD'], text=True).strip() != args.nim_revision:
    raise RuntimeError('Declared real Nim source revision required')
lock_before = (supplier / 'flake.lock').read_bytes()
config_before = subprocess.check_output([git, '-C', str(supplier), 'config', '--null', '--show-origin', '--show-scope', '--list'])


def digest(path):
    with path.open('rb') as file:
        return hashlib.file_digest(file, 'sha256').hexdigest()

def census(path):
    rows = {'.': {'kind': 'directory', 'mode': stat.S_IMODE(path.lstat().st_mode)}}
    for member in sorted(path.rglob('*')):
        if member.is_symlink():
            rows[member.relative_to(path).as_posix()] = {'kind': 'link', 'mode': stat.S_IMODE(member.lstat().st_mode), 'link': os.readlink(member)}
        elif member.is_file():
            rows[member.relative_to(path).as_posix()] = {'kind': 'file', 'mode': stat.S_IMODE(member.stat().st_mode), 'size': member.stat().st_size, 'sha256': digest(member)}
        elif member.is_dir():
            rows[member.relative_to(path).as_posix()] = {'kind': 'directory', 'mode': stat.S_IMODE(member.lstat().st_mode)}
        else:
            raise RuntimeError('Unsupported provider payload kind')
    return rows

def pe(path, expected):
    with path.open('rb') as image:
        if image.read(2) != b'MZ':
            raise RuntimeError('Declared executable is not PE')
        image.seek(0x3C)
        offset = struct.unpack('<I', image.read(4))[0]
        image.seek(offset)
        if image.read(4) != b'PE\0\0' or struct.unpack('<H', image.read(2))[0] != expected:
            raise RuntimeError('Foreign executable machine')
    return {'path': str(path), 'machine': expected, 'sha256': digest(path)}

# Windows opens deny new writes/deletion while selected images are in use.
handles = []
kernel = ctypes.WinDLL("kernel32", use_last_error=True)
kernel.CreateFileW.argtypes = [ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p, ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p]
kernel.CreateFileW.restype = ctypes.c_void_p
def hold(path):
    path = Path(path).resolve(strict=True)
    handle = kernel.CreateFileW(str(path), 0x80000000, 1, None, 3, 0x00200000, None)
    if handle in (None, ctypes.c_void_p(-1).value):
        raise OSError(ctypes.get_last_error(), "Cannot hold selected principal")
    handles.append(handle)
    return {"path": str(path), "sha256": digest(path)}
active_child = None
receipt = {'success': False, 'scope': 'Direct-child Windows constructor component; no native test corpus or descendant acceptance'}
try:
    principals = {"git": hold(git), "python": hold(sys.executable)}
    python_image = pe(Path(sys.executable), cpu)
    python_root = Path(sys.base_prefix).resolve(strict=True)
    python_runtime_before = census(python_root)
    if sys.version_info[:3] != (3, 13, 16):
        raise RuntimeError('Declared native Python version mismatch')

    def supplier_state(directory=supplier, revision=args.nim_revision):
        if subprocess.check_output([git, "-C", str(directory), "status", "--porcelain"]):
            raise RuntimeError("Declared supplier is dirty")
        listing = subprocess.check_output([git, "-C", str(directory), "ls-tree", "-rz", "--full-tree", revision])
        rows = []
        for record in listing.split(b"\0"):
            if not record:
                continue
            metadata, name = record.split(b"\t", 1)
            mode, kind, oid = metadata.split()
            if kind != b"blob":
                raise RuntimeError("Unsupported supplier Git membership")
            path = directory / os.fsdecode(name)
            blob = subprocess.check_output([git, "-C", str(directory), "cat-file", "blob", oid.decode()])
            body = os.fsencode(os.readlink(path)) if path.is_symlink() else path.read_bytes()
            if body != blob:
                raise RuntimeError("Supplier physical source differs from declared Git body")
            rows.append((os.fsdecode(name), mode.decode(), hashlib.sha256(body).hexdigest()))
        return rows
    supplier_before = supplier_state()

    stages = []
    active_child = None
    def run(name, argv):
        global active_child
        with (root / (name + '.stdout')).open('xb') as out, (root / (name + '.stderr')).open('xb') as err:
            child = subprocess.Popen([str(v) for v in argv], cwd=root, stdout=out, stderr=err)
            active_child = child
            status = child.wait()
            active_child = None
        stages.append({'name': name, 'argv': [str(v) for v in argv], 'pid': child.pid, 'exit': status})
        if status:
            raise RuntimeError('Real provider stage failed: ' + name)

    inputs = []
    def acquire(name, url, sha):
        archive = root / (name + '.zip')
        urllib.request.urlretrieve(url, archive)
        if digest(archive) != sha:
            raise RuntimeError('Declared archive digest mismatch')
        output = root / name
        output.mkdir()
        with zipfile.ZipFile(archive) as zipped:
            expected = {'.': ('directory', None, None)}
            declared = set()
            for member in zipped.infolist():
                relative = PurePosixPath(member.filename)
                parts = relative.parts
                if (relative.is_absolute() or not parts or '..' in parts
                        or any('\\' in part or ':' in part or part.endswith((' ', '.')) for part in parts)):
                    raise RuntimeError('Foreign archive member path')
                key = relative.as_posix().rstrip('/')
                folded = key.casefold()
                if folded in declared:
                    raise RuntimeError('Duplicate Windows archive member')
                declared.add(folded)
                kind = (member.external_attr >> 16) & 0o170000
                if kind not in (0, 0o040000, 0o100000):
                    raise RuntimeError('Unsupported executable archive member kind')
                for parent in relative.parents:
                    if parent.as_posix() != '.':
                        expected.setdefault(parent.as_posix(), ('directory', None, None))
                if member.is_dir():
                    value = ('directory', None, None)
                else:
                    with zipped.open(member) as body:
                        value = ('file', member.file_size, hashlib.file_digest(body, 'sha256').hexdigest())
                if key in expected and expected[key] != value:
                    raise RuntimeError('Conflicting archive file/directory membership')
                expected[key] = value
            if len({name.casefold() for name in expected}) != len(expected):
                raise RuntimeError('Case-colliding Windows archive membership')
            zipped.extractall(output)
        actual = census(output)
        if set(actual) != set(expected):
            raise RuntimeError('Extracted archive membership mismatch')
        for name, (kind, size, sha256) in expected.items():
            row = actual[name]
            if row['kind'] != kind or (kind == 'file' and (row['size'], row['sha256']) != (size, sha256)):
                raise RuntimeError('Extracted archive payload mismatch')
        inputs.append((archive, sha, output, census(output)))
        return output

    llvm_sha = {'ARM64': '9a835d5179c9f3a5a783c11a7f14062a249b4765dd11add64a66786864e82ab2',
                'AMD64': '317492c456aa27ee607a5919f1d2d38dcdc1112516a24d0bf4b00d078f52d17a'}[machine]
    arch = 'aarch64' if machine == 'ARM64' else 'x86_64'
    llvm_name = 'llvm-mingw-20261006-ucrt-' + arch
    llvm = acquire('llvm', 'https://github.com/mstorsjo/llvm-mingw/releases/download/20261006/' + llvm_name + '.zip', llvm_sha) / llvm_name
    nim_source = acquire('nim-source', 'https://nim-lang.org/download/nim-2.2.10_x64.zip',
                         'fe0686a9b298e5b13d0a983df37e002a8c6320f8b16cc45a51d15cf4046a109f') / 'nim-2.2.10'
    clang = llvm / 'bin/clang.exe'
    archive_tool = llvm / 'bin/llvm-ar.exe'
    principals['clang'] = hold(clang)
    clang_image = pe(clang, cpu)
    # Native image architecture and actual emitted C target are separate roles.

    principals['archiveTool'] = hold(archive_tool)
    archive_image = pe(archive_tool, cpu)
    run('clang-target', [clang, '-dumpmachine'])
    emitted_target = (root / 'clang-target.stdout').read_text().strip()
    expected_arch = 'aarch64-' if machine == 'ARM64' else 'x86_64-'
    if not emitted_target.startswith(expected_arch) or not emitted_target.endswith(('mingw32', 'windows-gnu')):
        raise RuntimeError('Foreign native C target')
    run('clang-version', [clang, '--version'])
    bootstrap = nim_source / 'bin/nim.exe'
    principals['bootstrap'] = hold(bootstrap)
    pe(bootstrap, 0x8664)
    native = root / 'native-nim'
    native.mkdir()
    (native / 'bin').mkdir()
    for part in ('lib', 'config'):
        shutil.copytree(nim_source / part, native / part)
        if census(nim_source / part) != census(native / part):
            raise RuntimeError('Copied Nim runtime differs from declared source')
    nim = native / 'bin/nim.exe'
    target_cpu = 'arm64' if machine == 'ARM64' else 'amd64'
    run('native-nim-build', [bootstrap, 'c', '--skipUserCfg', '--skipParentCfg', '--os:windows', '--cpu:' + target_cpu,
        '--cc:clang', '--clang.exe:' + str(clang), '--clang.linkerexe:' + str(clang),
        '--lib:' + str(nim_source / 'lib'), '--nimcache:' + str(root / 'compiler-cache'),
        '--out:' + str(nim), nim_source / 'compiler/nim.nim'])
    principals['nim'] = hold(nim)
    nim_image = pe(nim, cpu)
    run('native-nim-version', [nim, '--version'])
    version = (root / 'native-nim-version.stdout').read_text()
    if 'Version 2.2.10' not in version or ('Windows: ' + target_cpu) not in version:
        raise RuntimeError('Declared native Nim version/host mismatch')

    # Build the same declared Zstd version as the genuine Nim supplier shell.
    zstd_archive = root / "zstd-1.5.7.tar.gz"
    urllib.request.urlretrieve("https://github.com/facebook/zstd/releases/download/v1.5.7/zstd-1.5.7.tar.gz", zstd_archive)
    zstd_sha = "eb33e51f49a15e023950cd7825ca74a4a2b43db8354825ac24fc1b7ee09e6fa3"
    if digest(zstd_archive) != zstd_sha:
        raise RuntimeError("Foreign Zstd source archive")
    zstd_source = root / "zstd-source"
    zstd_source.mkdir()
    with tarfile.open(zstd_archive) as archive:
        seen = set()
        for member in archive.getmembers():
            name = PurePosixPath(member.name)
            if name.is_absolute() or ".." in name.parts or member.name in seen:
                raise RuntimeError("Foreign Zstd source membership")
            seen.add(member.name)
            if not (member.isfile() or member.isdir() or member.issym()):
                raise RuntimeError("Unsupported Zstd source member")
        archive.extractall(zstd_source, filter="data")
    inputs.append((zstd_archive, zstd_sha, zstd_source, census(zstd_source)))
    library_source = zstd_source / "zstd-1.5.7/lib"
    sdk = root / "zstd-native"
    (sdk / "include").mkdir(parents=True)
    (sdk / "lib").mkdir()
    for name in ("zstd.h", "zdict.h", "zstd_errors.h"):
        shutil.copyfile(library_source / name, sdk / "include" / name)
    sources = sorted(path for group in ("common", "compress", "decompress", "dictBuilder")
                     for path in (library_source / group).glob("*.c"))
    if not sources:
        raise RuntimeError("Missing complete native Zstd sources")
    objects = []
    for index, source in enumerate(sources):
        obj = root / ("zstd-object-" + str(index) + ".o")
        run("zstd-compile-" + str(index), [clang, "-O2", "-DZSTD_DISABLE_ASM", "-I" + str(library_source), "-c", source, "-o", obj])
        with obj.open("rb") as file:
            if struct.unpack("<H", file.read(2))[0] != cpu:
                raise RuntimeError("Foreign Zstd object machine")
        objects.append(obj)
    library = sdk / "lib/libzstd.a"
    run("zstd-archive", [archive_tool, "rcs", library, *objects])
    run("zstd-archive-members", [archive_tool, "t", library])
    if (root / "zstd-archive-members.stdout").read_text().splitlines() != [obj.name for obj in objects]:
        raise RuntimeError('Native Zstd archive membership differs')
    probe_source = root / "zstd-roundtrip.c"
    probe_source.write_text("#include <zstd.h>\n#include <string.h>\n#include <stdio.h>\nint main(void){if(ZSTD_versionNumber()!=10507)return 3;char c[128],d[128];const char*s=\"entry-native-zstd\";size_t n=ZSTD_compress(c,sizeof(c),s,strlen(s),1);if(ZSTD_isError(n))return 1;size_t m=ZSTD_decompress(d,sizeof(d),c,n);if(ZSTD_isError(m)||m!=strlen(s)||memcmp(s,d,m))return 2;puts(\"entry-native-zstd-ok\");return 0;}\n")
    probe = root / "zstd-roundtrip.exe"
    run("zstd-roundtrip-build", [clang, "-I" + str(sdk / "include"), probe_source, library, "-o", probe])
    zstd_probe_image = pe(probe, cpu)
    run("zstd-roundtrip-run", [probe])
    if (root / "zstd-roundtrip-run.stdout").read_text().strip() != "entry-native-zstd-ok":
        raise RuntimeError("Real native Zstd roundtrip failed")

    # Materialize exactly the three dependency commits declared by this supplier.
    flake_lock = json.loads(lock_before)
    dependencies = []
    for name in ("nim-results", "nim-stew", "nim-unittest2"):
        locked = flake_lock["nodes"][name]["locked"]
        if locked.get("type") != "github" or len(locked.get("rev", "")) != 40:
            raise RuntimeError("Unsupported declared Nim dependency source")
        directory = root / name
        run(name + "-init", [git, "-c", "init.templateDir=", "init", directory])
        url = "https://github.com/" + locked["owner"] + "/" + locked["repo"] + ".git"
        run(name + "-fetch", [git, "-C", directory, "fetch", "--depth=1", url, locked["rev"]])
        run(name + "-checkout", [git, "-C", directory, "checkout", "--detach", locked["rev"]])
        dependencies.append({"name": name, "path": str(directory), "revision": locked["rev"],
                             "declaredNarHash": locked["narHash"], "source": supplier_state(directory, locked["rev"])})

    for part in ('lib', 'config'):
        if census(nim_source / part) != census(native / part):
            raise RuntimeError('Native stdlib/config changed from declared source')
    # Source and archive authority must remain unchanged even after genuine builds.
    if any(digest(archive) != sha or census(source) != before for archive, sha, source, before in inputs):
        raise RuntimeError('Acquired provider input changed')
    if (supplier / 'flake.lock').read_bytes() != lock_before:
        raise RuntimeError('Declared supplier dependency lock changed')
    component = {'schema': 'entry-native-frontend.v1',
        'python': python_image, 'pythonRoot': str(python_root), 'pythonRuntime': python_runtime_before, 'nativeMachine': machine, 'supplierRevision': args.nim_revision, 'supplierRoot': str(supplier), 'supplierSource': supplier_before, 'providerScriptSHA256': digest(Path(__file__).resolve()), 'nim': nim_image,
        'clang': clang_image, 'archiveTool': archive_image, 'nativeRuntime': census(native),
        'inputPackages': [{'archive': str(archive), 'archiveSHA256': sha, 'root': str(source), 'members': before} for archive, sha, source, before in inputs], 'dependencies': dependencies, 'zstdSDK': census(sdk), 'zstdRoot': str(sdk), 'zstdNativeProbe': zstd_probe_image,
        'stages': stages, 'scope': 'Native provider construction only; portable continuous gate still pending'}

    if any(supplier_state(Path(row['path']), row['revision']) != row['source'] for row in dependencies):
        raise RuntimeError('Declared dependency source changed')
    if census(python_root) != python_runtime_before:
        raise RuntimeError('Declared native Python runtime changed')
    if supplier_state() != supplier_before or subprocess.check_output([git, '-C', str(supplier), 'config', '--null', '--show-origin', '--show-scope', '--list']) != config_before or any(digest(Path(row['path'])) != row['sha256'] for row in principals.values()):
        raise RuntimeError('Provider source/config/principals changed')

except BaseException as error:
    receipt['failureType'] = type(error).__name__
    raise
finally:
    receipt['directChildTerminal'] = active_child is None or active_child.poll() is not None
    receipt['raw'] = {p.name: digest(p) for p in root.glob('*.stdout')}
    receipt['raw'].update({p.name: digest(p) for p in root.glob('*.stderr')})
    receipt['success'] = 'failureType' not in receipt and receipt['directChildTerminal']
    if receipt['success']:
        (root / 'frontend-component.json').write_text(json.dumps(component, indent=2))
    (root / 'receipt.json').write_text(json.dumps(receipt, indent=2))
    kernel.CloseHandle.argtypes = [ctypes.c_void_p]
    kernel.CloseHandle.restype = ctypes.c_int
    for handle in reversed(handles):
        kernel.CloseHandle(handle)
