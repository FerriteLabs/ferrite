import io
import json
import subprocess
import sys
import tomllib
import unittest
from pathlib import Path
from urllib.error import HTTPError

sys.path.insert(0, str(Path(__file__).resolve().parent))

from check_crates_io_ownership import CRATES_API, inspect_crate
from check_release_metadata import validate_release_metadata


ROOT = Path(__file__).resolve().parent.parent
RELEASE_SCRIPT = ROOT / "scripts" / "release.sh"
PUBLISH_WORKFLOW = ROOT / ".github" / "workflows" / "publish.yml"


class ReleaseScriptTests(unittest.TestCase):
    def test_release_script_has_valid_shell_syntax(self) -> None:
        subprocess.run(["bash", "-n", str(RELEASE_SCRIPT)], check=True, cwd=ROOT)

    def test_release_script_uses_synchronized_portable_versioning(self) -> None:
        script = RELEASE_SCRIPT.read_text(encoding="utf-8")
        self.assertNotIn("sed -i", script)
        self.assertIn('cargo release version "$VERSION" --workspace --execute --no-confirm', script)
        self.assertIn('python3 scripts/check_release_metadata.py "$VERSION"', script)
        self.assertIn("grep -c '^warning:' || true", script)
        self.assertIn('if [[ "$CURRENT_VERSION" == "$VERSION" ]]', script)
        self.assertIn('remote_tag_status=$?', script)
        self.assertIn('Non-dry-run releases must run from main.', script)

    def test_release_script_uses_protected_publication_workflow(self) -> None:
        script = RELEASE_SCRIPT.read_text(encoding="utf-8")
        self.assertNotIn("cargo publish -p", script)
        self.assertIn("gh workflow run publish.yml", script)
        self.assertIn("dry_run=true", script)
        self.assertIn("dry_run=false", script)

    def test_current_release_metadata_is_synchronized(self) -> None:
        manifest = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
        version = manifest["workspace"]["package"]["version"]
        self.assertEqual(version, "0.5.0")
        validate_release_metadata(ROOT, version)

    def test_real_publication_is_tag_bound_and_resumable(self) -> None:
        workflow = PUBLISH_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("Verify tagged publication", workflow)
        self.assertIn('"$GITHUB_REF" != "refs/tags/v${VERSION}"', workflow)
        self.assertIn("expected_version is required", workflow)
        self.assertIn("start_at", workflow)
        self.assertIn("Resume crate is not in the publication list", workflow)
        self.assertEqual(
            workflow.count('cargo info --registry crates-io "${crate}@${VERSION}"'),
            2,
        )
        self.assertIn("Cannot resume at ${RESUME_AT}", workflow)

    def test_legacy_full_release_workflow_is_removed(self) -> None:
        self.assertFalse((ROOT / ".github" / "workflows" / "release-full.yml").exists())

    def test_h2_advisory_is_scoped_to_the_legacy_optional_stack(self) -> None:
        lockfile = tomllib.loads((ROOT / "Cargo.lock").read_text(encoding="utf-8"))
        h2_versions = {
            package["version"]
            for package in lockfile["package"]
            if package["name"] == "h2"
        }
        self.assertIn("0.3.27", h2_versions)
        self.assertIn("0.4.16", h2_versions)
        self.assertNotIn("0.4.12", h2_versions)

        for path in (ROOT / "deny.toml", ROOT / ".cargo" / "audit.toml"):
            policy = path.read_text(encoding="utf-8")
            self.assertIn("RUSTSEC-2026-0258", policy)
            self.assertIn("OpenTelemetry 0.22 / tonic 0.11", policy)


class FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


def fake_opener(routes: dict[str, dict | None]):
    def open_request(request, timeout=0):
        del timeout
        value = routes.get(request.full_url)
        if value is None:
            raise HTTPError(request.full_url, 404, "not found", {}, None)
        return FakeResponse(json.dumps(value).encode())

    return open_request


class CratesIoOwnershipTests(unittest.TestCase):
    def test_unclaimed_name_is_available_for_first_publish(self) -> None:
        opener = fake_opener({f"{CRATES_API}/crates/ferrite-new": None})
        self.assertEqual(
            inspect_crate("ferrite-new", opener),
            "ferrite-new: unclaimed",
        )

    def test_existing_ferrite_repository_is_accepted(self) -> None:
        opener = fake_opener(
            {
                f"{CRATES_API}/crates/ferrite-owned": {
                    "crate": {
                        "repository": "https://github.com/FerriteLabs/ferrite.git"
                    }
                },
                f"{CRATES_API}/crates/ferrite-owned/owners": {
                    "users": [{"login": "release-owner"}]
                },
            }
        )
        self.assertIn("release-owner", inspect_crate("ferrite-owned", opener))

    def test_unrelated_existing_crate_is_rejected(self) -> None:
        opener = fake_opener(
            {
                f"{CRATES_API}/crates/ferrite-core": {
                    "crate": {
                        "repository": "https://github.com/master-of-zen/ferrite"
                    }
                },
                f"{CRATES_API}/crates/ferrite-core/owners": {
                    "users": [{"login": "master-of-zen"}]
                },
            }
        )
        with self.assertRaisesRegex(ValueError, "unrelated project"):
            inspect_crate("ferrite-core", opener)

    def test_verified_transfer_can_bootstrap_legacy_repository_metadata(self) -> None:
        opener = fake_opener(
            {
                f"{CRATES_API}/crates/ferrite-core": {
                    "crate": {
                        "repository": "https://github.com/master-of-zen/ferrite"
                    }
                },
                f"{CRATES_API}/crates/ferrite-core/owners": {
                    "users": [{"login": "master-of-zen"}],
                    "teams": [{"login": "ferritelabs-release"}],
                },
            }
        )
        result = inspect_crate(
            "ferrite-core",
            opener,
            frozenset({"ferritelabs-release"}),
        )
        self.assertIn("ownership transfer bootstrap approved", result)


if __name__ == "__main__":
    unittest.main()
