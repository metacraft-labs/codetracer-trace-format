"""Every per-crate test recipe must be declared as a subset of `test`.

`test-ffi`, `test-writer` and `test-roundtrip` each run `cargo test -p <crate>`
for one workspace member. No workflow runs them, so the CI test-coverage audit
(`ci-test-audit`) reported them as test recipes CI never reaches. They are
developer shortcuts: `just test` runs `cargo test` at the workspace root, which
tests every member because `[workspace]` declares no `default-members`, and the
Rust workflow runs `just test`. The audit accepts such a recipe once a
`# ci-test-audit: covered-by <recipe>` comment precedes it; this test keeps the
declaration and the claim behind it true:

- every recipe whose body is a single `cargo test -p <crate>` carries
  `# ci-test-audit: covered-by test` in the comment block above it;
- `<crate>` is a member of the Cargo workspace;
- `test` still runs a plain workspace-root `cargo test` (no `-p`, no
  `--exclude`), and the workspace has no `default-members`, so the subset
  claim holds.

The audit reads comments the way this test does: a `# ci-test-audit:` line
anywhere in the comment block directly above a recipe header applies to that
recipe. The annotation is placed above the recipe's doc comment so that
`just --list` keeps showing the human description.

No mocks: this reads the real justfile and Cargo.toml.
"""
import re
import tomllib
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
JUSTFILE = ROOT / "justfile"
CARGO = ROOT / "Cargo.toml"
HEADER = re.compile(r"^@?([\w-]+)\b[^:=\n]*:(?!=)")
PER_CRATE = re.compile(r"^cargo test -p ([\w-]+)(?: --verbose)?$")
COVERED_BY_TEST = re.compile(r"^#\s*ci-test-audit:\s*covered-by\s+test\s*$")


def recipes(text):
    """Map recipe name -> (comment lines directly above it, body lines)."""
    out, comments, cur = {}, [], None
    for line in text.splitlines():
        if line.startswith((" ", "\t")):
            if cur is not None and line.strip():
                out[cur][1].append(line.strip())
            continue
        if line.startswith("#"):
            comments.append(line)
            cur = None
            continue
        m = HEADER.match(line)
        if m:
            cur = m[1]
            out[cur] = (comments, [])
        else:
            cur = None
        comments = []
    return out


class PerCrateRecipesDeclared(unittest.TestCase):
    def setUp(self):
        self.recipes = recipes(JUSTFILE.read_text())
        self.workspace = tomllib.loads(CARGO.read_text())["workspace"]

    def per_crate(self):
        found = {}
        for name, (_, body) in self.recipes.items():
            if name != "test" and len(body) == 1 and (m := PER_CRATE.match(body[0])):
                found[name] = m[1]
        return found

    def test_per_crate_recipes_exist(self):
        # Guards the other assertions against passing vacuously if the
        # recipe shape changes.
        self.assertEqual(
            set(self.per_crate()), {"test-ffi", "test-writer", "test-roundtrip"})

    def test_each_is_declared_covered_by_test(self):
        for name in self.per_crate():
            with self.subTest(recipe=name):
                comments = self.recipes[name][0]
                self.assertTrue(
                    any(COVERED_BY_TEST.match(c) for c in comments),
                    f"`{name}` runs one crate and no workflow runs it: declare "
                    "it with `# ci-test-audit: covered-by test` above it",
                )

    def test_each_crate_is_a_workspace_member(self):
        members = set(self.workspace["members"])
        for name, crate in self.per_crate().items():
            with self.subTest(recipe=name):
                self.assertIn(crate, members)

    def test_test_runs_every_member(self):
        self.assertNotIn(
            "default-members", self.workspace,
            "default-members narrows a root `cargo test`; `covered-by test` "
            "would no longer hold for the per-crate recipes",
        )
        body = self.recipes["test"][1]
        cargo = [b for b in body if b.startswith("cargo test")]
        self.assertEqual(len(cargo), 1, body)
        self.assertNotRegex(cargo[0], r"(^|\s)(-p|--package|--exclude)\b")


if __name__ == "__main__":
    unittest.main()
