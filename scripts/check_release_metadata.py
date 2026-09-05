#!/usr/bin/env python3

import argparse
import json
import subprocess
import tomllib
from pathlib import Path


def validate_release_metadata(root: Path, expected_version: str) -> None:
    manifest = tomllib.loads((root / "Cargo.toml").read_text(encoding="utf-8"))
    workspace_version = manifest["workspace"]["package"]["version"]
    if workspace_version != expected_version:
        raise ValueError(f"workspace version is {workspace_version}, expected {expected_version}")

    for name, dependency in manifest["workspace"]["dependencies"].items():
        if name.startswith("ferrite-") and isinstance(dependency, dict) and dependency.get("path"):
            if dependency.get("version") != expected_version:
                raise ValueError(f"workspace dependency {name} is not pinned to {expected_version}")

    metadata = json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"],
            cwd=root,
            text=True,
        )
    )
    packages = [package for package in metadata["packages"] if package["source"] is None and package["name"].startswith("ferrite")]
    for package in packages:
        if package["version"] != expected_version:
            raise ValueError(f"{package['name']} is {package['version']}, expected {expected_version}")
        for dependency in package["dependencies"]:
            if dependency["name"].startswith("ferrite-") and dependency.get("path") and dependency["req"] == "*":
                raise ValueError(f"{package['name']} has an unversioned path dependency on {dependency['name']}")

    lockfile = tomllib.loads((root / "Cargo.lock").read_text(encoding="utf-8"))
    locked_packages = [package for package in lockfile["package"] if package["name"].startswith("ferrite") and "source" not in package]
    for package in locked_packages:
        if package["version"] != expected_version:
            raise ValueError(f"Cargo.lock contains {package['name']} {package['version']}, expected {expected_version}")

    if {package["name"] for package in packages} != {package["name"] for package in locked_packages}:
        raise ValueError("Cargo.lock Ferrite package set does not match cargo metadata")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate synchronized Ferrite release metadata")
    parser.add_argument("version", help="Expected workspace release version")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    validate_release_metadata(root, args.version)
    print(f"Validated synchronized Ferrite release metadata at {args.version}")


if __name__ == "__main__":
    main()
