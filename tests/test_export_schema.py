"""export_schema.py must describe the CSVs the parsers really write, column
for column and in order: the Splunk index-time transforms, the search-time
REPORT regexes and the Cribl pipeline all address columns by position
(splunk/build_field_config.py), so a column added to block_parser.py
without updating export_schema.py would silently shift every field after it.

Also replays the generated Splunk regexes (Python re, after translating
PCRE's (?<name>) to (?P<name>)) against real CSV rows, so a wrong position
fails here rather than in Splunk.

    python -m unittest discover -s tests      (or: pytest tests)
"""

from __future__ import annotations

import configparser
import csv
import json
import io
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from btc_parser_app.export_schema import (
    BLOCK_COLUMNS,
    INPUT_COLUMNS,
    OUTPUT_COLUMNS,
    SOURCETYPES_BY_NAME,
    STALE_BLOCK_HEADER_COLUMNS,
    TRANSACTION_COLUMNS,
)
from btc_parser_app.rpc import stale_blocks
from btc_parser_app.rpc.block_parser import aggregate_block
from btc_parser_app.rpc.catchup import frame_from_rows
from btc_parser_app.rpc.stale_blocks_state import StaleBlockEntry
try:
    from test_catchup import BASE_CONFIG, make_block  # unittest discover -s tests
except ImportError:
    from tests.test_catchup import BASE_CONFIG, make_block  # pytest from the repo root

REPO_ROOT = Path(__file__).resolve().parent.parent
SPLUNK_DIR = REPO_ROOT / "splunk"


def read_conf(path: Path) -> configparser.RawConfigParser:
    parser = configparser.RawConfigParser(strict=False, interpolation=None)
    parser.optionxform = str  # keep key case (TRANSFORMS-..., FORMAT)
    parser.read(path, encoding="utf-8")
    return parser


def to_python_regex(splunk_regex: str) -> re.Pattern[str]:
    return re.compile(splunk_regex.replace("(?<", "(?P<"))


def csv_lines(rows: list[dict]) -> list[str]:
    """The exact lines rpc-ingest writes (header + rows)."""
    return frame_from_rows(rows).write_csv().splitlines()


class ColumnOrderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.blocks = [aggregate_block(make_block(h), BASE_CONFIG) for h in range(1, 40)]

    def test_rpc_rows_match_schema(self) -> None:
        for agg in self.blocks:
            self.assertEqual(tuple(agg.block_event), BLOCK_COLUMNS)
            for row in agg.tx_events:
                self.assertEqual(tuple(row), TRANSACTION_COLUMNS)
            for row in agg.input_events:
                self.assertEqual(tuple(row), INPUT_COLUMNS)
            for row in agg.output_events:
                self.assertEqual(tuple(row), OUTPUT_COLUMNS)

    def test_written_csv_header_matches_schema(self) -> None:
        tx = [row for agg in self.blocks for row in agg.tx_events]
        self.assertEqual(csv_lines(tx)[0], ",".join(TRANSACTION_COLUMNS))

    def test_stale_header_rows_match_schema(self) -> None:
        good_header = "01000000" + "00" * 32 + "3ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a" + "29ab5f49" + "ffff001d" + "1dac2b7c"
        with tempfile.TemporaryDirectory() as tmp:
            for header_hex in (good_header, None):
                stale_blocks._export_header_event(
                    Path(tmp),
                    StaleBlockEntry(
                        height=1,
                        blockhash="00" * 32,
                        status="header_only",
                        header_hex=header_hex,
                        header_valid=None,
                        source="test",
                        chaintip_status=None,
                        branchlen=None,
                        first_seen="2026-01-01T00:00:00Z",
                    ),
                )
            header = (Path(tmp) / "stale_block_headers.csv").read_text().splitlines()[0]
        self.assertEqual(header, ",".join(STALE_BLOCK_HEADER_COLUMNS))


class GeneratedSplunkConfigTest(unittest.TestCase):
    """Replays the generated confs against real rows."""

    @classmethod
    def setUpClass(cls) -> None:
        blocks = [aggregate_block(make_block(h), BASE_CONFIG) for h in range(1, 60)]
        cls.lines = {
            "btc:transactions": csv_lines([r for b in blocks for r in b.tx_events]),
            "btc:inputs": csv_lines([r for b in blocks for r in b.input_events]),
            "btc:outputs": csv_lines([r for b in blocks for r in b.output_events]),
        }
        cls.index_props = read_conf(SPLUNK_DIR / "TA-btc-parser-parsing/default/props.conf")
        cls.index_transforms = read_conf(SPLUNK_DIR / "TA-btc-parser-parsing/default/transforms.conf")
        cls.search_props = read_conf(SPLUNK_DIR / "TA-btc-parser-search/default/props.conf")
        cls.search_transforms = read_conf(SPLUNK_DIR / "TA-btc-parser-search/default/transforms.conf")

    def test_generated_files_are_up_to_date(self) -> None:
        result = subprocess.run(
            [sys.executable, str(SPLUNK_DIR / "build_field_config.py"), "--check"],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_detail_sourcetypes_rebuild_every_column(self) -> None:
        """Indexed fields (index-time WRITE_META) + search-time REPORT fields
        together must give back exactly the CSV row - every non-empty value,
        under its own column name, nothing extra."""
        for name, lines in self.lines.items():
            st = SOURCETYPES_BY_NAME[name]
            props = self.index_props[name]
            header_re = to_python_regex(self.index_transforms[props["TRANSFORMS-btc_00_header"]]["REGEX"])
            index_tf = [
                self.index_transforms[t.strip()]
                for t in props["TRANSFORMS-btc_10_indexed"].split(",")
            ]
            report = to_python_regex(
                self.search_transforms[self.search_props[name]["REPORT-btc_columns"]]["REGEX"]
            )
            time_prefix = to_python_regex(props["TIME_PREFIX"])

            self.assertTrue(header_re.match(lines[0]), name)
            for line in lines[1:]:
                self.assertFalse(header_re.match(line), f"{name}: data row matched the header regex")
                expected = {
                    k: v for k, v in zip(st.columns, next(csv.reader(io.StringIO(line)))) if v != ""
                }
                got: dict[str, str] = {}
                for tf in index_tf:
                    m = to_python_regex(tf["REGEX"]).match(line)
                    if m:
                        field, template = tf["FORMAT"].split("::")
                        self.assertEqual(template, "$1")
                        got[field] = m.group(1)
                m = report.match(line)
                self.assertIsNotNone(m, f"{name}: REPORT regex did not match {line}")
                got.update({k: v for k, v in m.groupdict().items() if v != ""})
                self.assertEqual(got, expected, name)

                t = time_prefix.match(line)
                self.assertEqual(line[t.end():].split(",")[0], expected[st.time_field], name)

    def test_indexed_and_report_fields_do_not_overlap(self) -> None:
        for name in self.lines:
            st = SOURCETYPES_BY_NAME[name]
            report = self.search_transforms[self.search_props[name]["REPORT-btc_columns"]]["REGEX"]
            groups = set(re.findall(r"\(\?<(\w+)>", report))
            self.assertEqual(groups, set(st.search_time_fields), name)
            self.assertTrue(groups.isdisjoint(st.indexed_fields), name)

    def test_empty_values_write_no_indexed_field(self) -> None:
        """Coinbase inputs have no prevout: prevout_type/prevout_generated
        are empty and must not become empty indexed fields."""
        lines = self.lines["btc:inputs"]
        props = self.index_props["btc:inputs"]
        tfs = {
            self.index_transforms[t.strip()]["FORMAT"].split("::")[0]: to_python_regex(
                self.index_transforms[t.strip()]["REGEX"]
            )
            for t in props["TRANSFORMS-btc_10_indexed"].split(",")
        }
        is_coinbase = SOURCETYPES_BY_NAME["btc:inputs"].columns.index("is_coinbase")
        coinbase = [line for line in lines[1:] if line.split(",")[is_coinbase] == "true"]
        self.assertTrue(coinbase)
        for line in coinbase:
            self.assertIsNone(tfs["prevout_type"].match(line))
            self.assertIsNone(tfs["prevout_generated"].match(line))
            self.assertEqual(tfs["is_coinbase"].match(line).group(1), "true")


class CriblPipelineTest(unittest.TestCase):
    def test_keeps_only_indexed_fields_on_detail_sourcetypes(self) -> None:
        pipeline = json.loads((REPO_ROOT / "cribl/btc_parser_pipeline.json").read_text())
        parsers = {
            f["filter"]: f["conf"] for f in pipeline["conf"]["functions"] if f["id"] == "serde"
        }
        for name in ("btc:transactions", "btc:inputs", "btc:outputs"):
            st = SOURCETYPES_BY_NAME[name]
            conf = parsers[f'sourcetype=="{name}"']
            self.assertEqual(conf["fields"], list(st.columns))
            self.assertEqual(set(conf["keep"]), set(st.indexed_fields) | {st.time_field})
        conf = parsers['sourcetype=="btc:blocks"']
        self.assertNotIn("keep", conf)


if __name__ == "__main__":
    unittest.main()
