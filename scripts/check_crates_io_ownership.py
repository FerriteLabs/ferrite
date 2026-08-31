#!/usr/bin/env python3

import argparse
import json
import sys
from pathlib import Path
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


CRATES_API = "https://crates.io/api/v1"
EXPECTED_REPOSITORY = "https://github.com/ferritelabs/ferrite"
USER_AGENT = "FerriteLabs-release-preflight/1.0"


def normalize_repository(value: str | None) -> str:
    return (value or "").lower().removesuffix(".git").rstrip("/")


def fetch_json(
    url: str,
    opener: Callable = urlopen,
) -> dict | None:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with opener(request, timeout=20) as response:
            return json.load(response)
    except HTTPError as error:
        if error.code == 404:
            error.close()
            return None
        raise RuntimeError(f"crates.io returned HTTP {error.code} for {url}") from error
    except URLError as error:
        raise RuntimeError(f"could not reach crates.io for {url}: {error.reason}") from error


def inspect_crate(
    name: str,
    opener: Callable = urlopen,
    trusted_owners: frozenset[str] = frozenset(),
) -> str:
    metadata = fetch_json(f"{CRATES_API}/crates/{name}", opener)
    if metadata is None:
        return f"{name}: unclaimed"

    crate = metadata.get("crate", {})
    repository = crate.get("repository")
    owners = fetch_json(f"{CRATES_API}/crates/{name}/owners", opener)
    owner_names = [
        owner.get("login") or owner.get("name")
        for owner in (owners or {}).get("users", [])
        if owner.get("login") or owner.get("name")
    ]
    owner_names.extend(
        owner.get("login") or owner.get("name")
        for owner in (owners or {}).get("teams", [])
        if owner.get("login") or owner.get("name")
    )
    if not owner_names:
        raise ValueError(f"{name} is published but crates.io returned no user owners")
    if normalize_repository(repository) != normalize_repository(EXPECTED_REPOSITORY):
        transferred_owners = trusted_owners.intersection(owner_names)
        if transferred_owners:
            return (
                f"{name}: ownership transfer bootstrap approved for "
                f"{', '.join(sorted(transferred_owners))}; legacy repository metadata "
                "must change on the first FerriteLabs publication"
            )
        raise ValueError(
            f"{name} is already published by an unrelated project "
            f"(repository={repository!r}, owners={owner_names!r}); obtain a verified "
            "ownership transfer and configure CRATES_IO_TRUSTED_OWNER, or rename the "
            "crate before release"
        )
    return f"{name}: existing Ferrite crate owned by {', '.join(owner_names)}"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reject crates.io names owned by unrelated projects"
    )
    parser.add_argument(
        "--list",
        type=Path,
        default=Path(".github/publish-crates.txt"),
        help="newline-delimited crate publication order",
    )
    parser.add_argument(
        "--trusted-owner",
        action="append",
        default=[],
        help="crates.io owner login approved for a one-release ownership-transfer bootstrap",
    )
    args = parser.parse_args()
    trusted_owners = frozenset(args.trusted_owner)

    errors = []
    for name in args.list.read_text(encoding="utf-8").splitlines():
        name = name.strip()
        if not name:
            continue
        try:
            print(inspect_crate(name, trusted_owners=trusted_owners))
        except (RuntimeError, ValueError) as error:
            errors.append(str(error))

    for error in errors:
        print(f"error: {error}", file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
