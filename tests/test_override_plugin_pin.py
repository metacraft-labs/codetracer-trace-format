"""The direnv overrides plugin must be fetched from an immutable URL.

ci/cross-reader-profiles.nix builds the plugin with a fixed-output `fetchurl`
that the flake dev shell depends on. The URL it used,
https://direnv-flake-overrides.blocksense.network/plugin, redirects to the
upstream repository's moving `stable` branch, so a re-release changed the bytes
under the pinned hash (2026-11-04) and `repro exec` failed with a hash mismatch
in plugin.drv. A hash alone only turns the next upstream edit into the next
outage; the URL must name a commit.

No mocks: this reads the real Nix source the dev shell evaluates.
"""
import re
import unittest
from pathlib import Path

PROFILES = Path(__file__).resolve().parent.parent / "ci" / "cross-reader-profiles.nix"
IMMUTABLE = re.compile(
    r"^https://raw\.githubusercontent\.com/blocksense-network/"
    r"direnv-nix-flake-overrides/[0-9a-f]{40}/plugin/flake-overrides\.bash$"
)


class OverridePluginPin(unittest.TestCase):
    def test_fetch_url_names_a_commit(self):
        text = PROFILES.read_text()
        block = re.search(r"overridePlugin\s*=\s*pkgs\.fetchurl\s*\{(.*?)\};", text, re.S)
        self.assertIsNotNone(block, "overridePlugin is no longer a pkgs.fetchurl")
        url = re.search(r'url\s*=\s*"([^"]+)"', block.group(1))
        self.assertIsNotNone(url, "overridePlugin has no url")
        self.assertRegex(
            url.group(1), IMMUTABLE,
            "overridePlugin must fetch a commit-pinned URL: a mutable or "
            "redirecting URL lets upstream change the shell under the hash",
        )
        self.assertRegex(block.group(1), r'hash\s*=\s*"sha256-[A-Za-z0-9+/]{43}="')


if __name__ == "__main__":
    unittest.main()
