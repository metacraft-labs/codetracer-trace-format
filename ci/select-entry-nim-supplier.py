"""The sole declared Nim supplier revision, from this repository's repro.lock.

Run as a script, it exports the revision through the held GitHub Actions
command file (windows-tests.yml). `declared_revision` is the same selection
for a caller that needs the value itself: ci/native-entry-provider.py loads
this file by path when the repro entry-native-provider edge passes
`--repro-lock`, so there is one reading of the lock, not two.
"""
from pathlib import Path
import os, stat, tomllib


def declared_revision(lock_path):
    """The `codetracer-trace-format-nim` revision the lock at `lock_path` pins."""
    lock = tomllib.loads(Path(lock_path).read_text())
    rows = [row for row in lock['lock']['deps'] if row['name'] == 'codetracer-trace-format-nim']
    if len(rows) != 1:
        raise RuntimeError('Declared Nim supplier absent or ambiguous')
    revision = rows[0]['revision']
    if len(revision) != 40 or any(char not in '0123456789abcdef' for char in revision):
        raise RuntimeError('Declared Nim supplier revision is malformed')
    return revision


def export_to_command_file(revision):
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


if __name__ == '__main__':
    export_to_command_file(declared_revision(Path(__file__).resolve().parent.parent / 'repro.lock'))
