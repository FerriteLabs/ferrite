import tempfile
import unittest
from pathlib import Path

from criterion_bencher_to_json import convert_files, parse_bencher_output


class CriterionBencherToJsonTests(unittest.TestCase):
    def test_parses_criterion_bencher_output_with_diagnostic(self) -> None:
        output = """\
test group/operation ... Criterion.rs ERROR: missing baseline
bench:       12,345 ns/iter (+/- 678)
"""

        self.assertEqual(
            parse_bencher_output(output, "throughput-output.txt"),
            [
                {
                    "name": "group/operation",
                    "unit": "ns/iter",
                    "value": 12345,
                    "range": "678",
                    "extra": "throughput-output.txt",
                }
            ],
        )

    def test_rejects_duplicate_names_across_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            first = Path(directory, "first.txt")
            second = Path(directory, "second.txt")
            output = "test duplicate ...\nbench: 10 ns/iter (+/- 1)\n"
            first.write_text(output, encoding="utf-8")
            second.write_text(output, encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "duplicate benchmark name"):
                convert_files([first, second])

    def test_rejects_output_without_results(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory, "empty.txt")
            output.write_text("no benchmarks here\n", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "no benchmark results found"):
                convert_files([output])


if __name__ == "__main__":
    unittest.main()
