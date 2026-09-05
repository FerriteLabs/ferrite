import json
import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DOCS_URL = "https://github.com/ferritelabs/ferrite-docs"
DISCUSSIONS_URL = "https://github.com/ferritelabs/ferrite/discussions"
PRIVATE_REPORT_URL = "https://github.com/ferritelabs/ferrite/security/advisories/new"
UNVERIFIED_DOMAINS = ("ferrite" + ".dev", "ferrite" + ".rs", "ferritelabs" + ".dev")


class ReleaseLinkTests(unittest.TestCase):
    def test_high_visibility_surfaces_do_not_advertise_unverified_domains(self) -> None:
        paths = [
            "SUPPORT.md",
            "README.ja-JP.md",
            "README.ko-KR.md",
            "README.zh-CN.md",
            "examples/github-actions/README.md",
            "docs/templates/take-home-distributed.md",
            "docs/templates/take-home-rust.md",
            "docs/adrs/README.md",
            "docs/adrs/adr-008-interactive-playground.md",
            "scripts/deploy-playground.sh",
            "src/main.rs",
            "src/commands/handlers/admin.rs",
            "src/commands/executor/server_ops.rs",
            "crates/ferrite-plugins/src/marketplace/client.rs",
            "crates/ferrite-plugins/src/marketplace/mod.rs",
            "crates/ferrite-plugins/src/marketplace/registry.rs",
            "CODE_OF_CONDUCT.md",
            "sdk/python/pyproject.toml",
            "sdk/nodejs/package.json",
            "sdk/typescript/package.json",
        ]
        for relative_path in paths:
            with self.subTest(path=relative_path):
                content = (ROOT / relative_path).read_text(encoding="utf-8")
                for domain in UNVERIFIED_DOMAINS:
                    self.assertNotIn(domain, content)

    def test_documentation_surfaces_use_github_fallback(self) -> None:
        paths = [
            "SUPPORT.md",
            "README.ja-JP.md",
            "README.ko-KR.md",
            "README.zh-CN.md",
            "examples/github-actions/README.md",
            "src/main.rs",
            "src/commands/handlers/admin.rs",
            "src/commands/executor/server_ops.rs",
        ]
        for relative_path in paths:
            with self.subTest(path=relative_path):
                content = (ROOT / relative_path).read_text(encoding="utf-8")
                self.assertIn(DOCS_URL, content)

    def test_marketplace_defaults_to_explicit_offline_state(self) -> None:
        marketplace = (ROOT / "crates/ferrite-plugins/src/marketplace/mod.rs").read_text(encoding="utf-8")
        registry = (ROOT / "crates/ferrite-plugins/src/marketplace/registry.rs").read_text(encoding="utf-8")
        client = (ROOT / "crates/ferrite-plugins/src/marketplace/client.rs").read_text(encoding="utf-8")

        self.assertIn("registry_url: String::new()", marketplace)
        self.assertIn("registry_url: String::new()", registry)
        self.assertIn('MarketplaceClient::new("https://registry.example.com/api/v1")', client)
        self.assertIn("let offline = registry_url.is_empty();", client)

    def test_support_and_take_home_contacts_use_verified_github_channels(self) -> None:
        for relative_path in [
            "SUPPORT.md",
            "docs/templates/take-home-distributed.md",
            "docs/templates/take-home-rust.md",
        ]:
            with self.subTest(path=relative_path):
                content = (ROOT / relative_path).read_text(encoding="utf-8")
                self.assertIn(DISCUSSIONS_URL, content)
                self.assertIn(PRIVATE_REPORT_URL, content)
                for domain in UNVERIFIED_DOMAINS:
                    self.assertNotIn("@" + domain, content)

    def test_code_of_conduct_uses_honest_github_contact_guidance(self) -> None:
        content = (ROOT / "CODE_OF_CONDUCT.md").read_text(encoding="utf-8")
        self.assertIn(DISCUSSIONS_URL, content)
        self.assertIn(PRIVATE_REPORT_URL, content)
        self.assertIn("Do not include complaint details", content)
        for domain in UNVERIFIED_DOMAINS:
            self.assertNotIn(domain, content)

    def test_sdk_metadata_retains_organization_without_unverified_email(self) -> None:
        python_metadata = tomllib.loads((ROOT / "sdk/python/pyproject.toml").read_text(encoding="utf-8"))
        self.assertEqual(python_metadata["project"]["authors"], [{"name": "Ferrite Labs"}])
        self.assertEqual(python_metadata["project"]["urls"]["Documentation"], DOCS_URL)

        for relative_path in ["sdk/nodejs/package.json", "sdk/typescript/package.json"]:
            with self.subTest(path=relative_path):
                package = json.loads((ROOT / relative_path).read_text(encoding="utf-8"))
                self.assertEqual(package["author"], "Ferrite Labs")
                self.assertEqual(package["homepage"], DOCS_URL)

    def test_release_docs_record_owned_domain_gate(self) -> None:
        changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
        checklist = (ROOT / "RELEASE_CHECKLIST.md").read_text(encoding="utf-8")
        self.assertIn("After live verification on 2026-09-02", changelog)
        self.assertIn("After live verification on 2026-09-02 found that `ferritelabs.dev` did not resolve", changelog)
        self.assertIn("verified FerriteLabs ownership", checklist)
        self.assertIn("Verified contact blocker (2026-09-02)", checklist)


if __name__ == "__main__":
    unittest.main()
