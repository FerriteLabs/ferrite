#!/usr/bin/env python3
"""Convert Criterion's bencher output into github-action-benchmark JSON."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Iterable

TEST_LINE = re.compile(r"^test (.+?) \.\.\.")
BENCH_LINE = re.compile(
    r"^bench:\s+([0-9,]+)\s+ns/iter\s+\(\+/-\s+([0-9,]+)\)"
)


def parse_bencher_output(output: str, source: str) -> list[dict[str, int | str]]:
    """Extract named benchmark measurements from one bencher output."""
    results: list[dict[str, int | str]] = []
    current_name: str | None = None

    for line in output.splitlines():
        if match := TEST_LINE.match(line):
            current_name = match.group(1)
            continue

        if match := BENCH_LINE.match(line):
            if current_name is None:
                raise ValueError(f"{source}: benchmark result has no preceding test name")
            results.append(
                {
                    "name": current_name,
                    "unit": "ns/iter",
                    "value": int(match.group(1).replace(",", "")),
                    "range": match.group(2).replace(",", ""),
                    "extra": source,
                }
            )
            current_name = None

    return results


def convert_files(paths: Iterable[Path]) -> list[dict[str, int | str]]:
    """Convert files while rejecting empty or ambiguous benchmark data."""
    results: list[dict[str, int | str]] = []
    names: set[str] = set()

    for path in paths:
        parsed = parse_bencher_output(path.read_text(encoding="utf-8"), path.name)
        for result in parsed:
            name = str(result["name"])
            if name in names:
                raise ValueError(f"duplicate benchmark name: {name}")
            names.add(name)
            results.append(result)

    if not results:
        raise ValueError("no benchmark results found")

    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    try:
        results = convert_files(args.inputs)
    except (OSError, ValueError) as error:
        parser.error(str(error))

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(results, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Converted {len(results)} benchmark results to {args.output}")


if __name__ == "__main__":
    main()
