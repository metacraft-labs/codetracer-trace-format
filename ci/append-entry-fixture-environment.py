"""Bind the real producer artifact to subsequent unchanged Windows tests."""
from pathlib import Path
import ctypes, msvcrt, os, stat

def append_fixture_root(root: Path):
    value = str(root.resolve(strict=True))
    if '\r' in value or '\n' in value:
        raise RuntimeError('Malformed produced fixture path')
    for name in ('marked.ct', 'unmarked.ct'):
        if not (root / name).is_file():
            raise RuntimeError('Required actual Nim artifact is absent')
    path = Path(os.environ['GITHUB_ENV'])
    original_stat = path.lstat()
    if not stat.S_ISREG(original_stat.st_mode) or path.is_symlink():
        raise RuntimeError('Foreign environment command-file authority')
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.CreateFileW.argtypes = [ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p, ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p]
    kernel.CreateFileW.restype = ctypes.c_void_p
    # FILE_READ_DATA | FILE_APPEND_DATA, FILE_SHARE_READ, OPEN_EXISTING,
    # FILE_FLAG_OPEN_REPARSE_POINT. Other writers cannot alter the held file.
    handle = kernel.CreateFileW(str(path), 5, 1, None, 3, 0x00200000, None)
    if handle in (None, ctypes.c_void_p(-1).value):
        raise OSError(ctypes.get_last_error(), 'Cannot hold environment command file')
    kernel.CloseHandle.argtypes = [ctypes.c_void_p]
    kernel.CloseHandle.restype = ctypes.c_int
    try:
        descriptor = msvcrt.open_osfhandle(handle, os.O_RDWR | os.O_APPEND | os.O_BINARY)
    except BaseException:
        kernel.CloseHandle(handle)
        raise
    try:
        file = os.fdopen(descriptor, 'r+b')
    except BaseException:
        os.close(descriptor)
        raise
    with file:
        held = os.fstat(file.fileno())
        if (held.st_dev, held.st_ino) != (original_stat.st_dev, original_stat.st_ino):
            raise RuntimeError('Changed environment command-file identity')
        original = file.read()
        if original and not original.endswith(b'\n'):
            raise RuntimeError('Malformed existing environment record')
        addition = ('ENTRY_NIM_FIXTURE_ROOT=' + value + '\n').encode()
        file.write(addition)
        file.flush()
        file.seek(0)
        final_stat = path.lstat()
        if file.read() != original + addition or (held.st_dev, held.st_ino) != (final_stat.st_dev, final_stat.st_ino):
            raise RuntimeError('Changed environment command-file bytes or identity')
