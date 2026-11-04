"""Export the sole declared supplier revision through a held command file."""
from pathlib import Path
import os, stat, tomllib

root = Path(__file__).resolve().parent.parent
lock = tomllib.loads((root / 'repro.lock').read_text())
rows = [row for row in lock['lock']['deps'] if row['name'] == 'codetracer-trace-format-nim']
if len(rows) != 1:
    raise RuntimeError('Declared Nim supplier absent or ambiguous')
revision = rows[0]['revision']
if len(revision) != 40 or any(char not in '0123456789abcdef' for char in revision):
    raise RuntimeError('Declared Nim supplier revision is malformed')
path = Path(os.environ['GITHUB_OUTPUT'])
before = path.lstat()
if not stat.S_ISREG(before.st_mode) or path.is_symlink():
    raise RuntimeError('Foreign command-file authority')
with path.open('a+b') as file:
    held = os.fstat(file.fileno())
    if (held.st_dev, held.st_ino) != (before.st_dev, before.st_ino):
        raise RuntimeError('Changed command-file identity')
    file.seek(0)
    original = file.read()
    if original and not original.endswith(b'\n'):
        raise RuntimeError('Malformed existing command-file record')
    addition = ('revision=' + revision + '\n').encode()
    file.write(addition)
    file.flush()
    file.seek(0)
    after = path.lstat()
    if file.read() != original + addition or (after.st_dev, after.st_ino) != (held.st_dev, held.st_ino):
        raise RuntimeError('Changed command-file body or identity')
