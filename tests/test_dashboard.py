"""The BTC Mempool Explorer dashboard (splunk/dashboards/build_btc_mempool_explorer.py)
must only use tstats on fields that are actually indexed (export_schema.py,
docs chapter 10.6) - a tstats over a search-time field returns nothing,
silently. Also checks that the generated dashboard/app files are up to date.

    python -m unittest discover -s tests      (or: pytest tests)
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from btc_parser_app.export_schema import SOURCETYPES, SOURCETYPES_BY_NAME

REPO_ROOT = Path(__file__).resolve().parent.parent
BUILDER = REPO_ROOT / "splunk/dashboards/build_btc_mempool_explorer.py"
APP = REPO_ROOT / "splunk/btc-parser-dashboards"
DASHBOARD_JSON = REPO_ROOT / "splunk/dashboards/btc_mempool_explorer.json"
GENERATED = [
    "dashboards/btc_mempool_explorer.json",
    "btc-parser-dashboards/default/data/ui/views/btc_mempool_explorer.xml",
    "btc-parser-dashboards/default/savedsearches.conf",
    "btc-parser-dashboards/default/macros.conf",
    "btc-parser-dashboards/default/collections.conf",
    "btc-parser-dashboards/default/transforms.conf",
    "btc-parser-dashboards/lookups/sanctioned_addresses.csv",
]
ALWAYS_INDEXED = {"_time", "sourcetype", "source", "host", "index"}
# Words in a tstats clause that are not field names.
NOT_FIELDS = {"index", "sourcetype", "span", "fillnull_value", "as", "where", "by", "OR", "AND", "NOT"}


def saved_searches(path: Path) -> dict[str, str]:
    """name -> search of a savedsearches.conf (values continue with a trailing backslash)."""
    out: dict[str, str] = {}
    name, buf = None, None
    for line in path.read_text(encoding="utf-8").splitlines():
        if buf is not None:
            buf.append(line.rstrip("\\").rstrip())
            if not line.endswith("\\"):
                out[name] = "\n".join(buf)
                buf = None
        elif line.startswith("["):
            name = line[1:-1]
        elif line.startswith("search = "):
            first = line[len("search = "):]
            if first.endswith("\\"):
                buf = [first.rstrip("\\").rstrip()]
            else:
                out[name] = first
    return out


def tstats_problems(name: str, query: str) -> list[str]:
    """Every `| tstats ...` clause, top level or inside a subsearch, up to the
    next pipe or the closing bracket. A field passes if it is indexed in at
    least one of the sourcetypes the clause names (`blocks OR prices` mixes
    fields of both)."""
    problems = []
    for match in re.finditer(r"\|\s*tstats\b(.*?)(?=\n\s*\||\]|\Z)", query, re.S):
        segment = match.group(1)
        named = [st for st in SOURCETYPES if st.name in segment]
        if not named:
            problems.append(f"{name}: tstats without an explicit btc sourcetype")
            continue
        allowed = set().union(*(set(st.indexed_fields) for st in named)) | ALWAYS_INDEXED
        used = set(re.findall(r"\(([A-Za-z_]\w*)\)", segment))  # max(x), sum(x), ...
        parts = re.split(r"\bby\b", segment.split(" where ", 1)[-1])
        used |= set(re.findall(r"\b([A-Za-z_]\w*)\s*(?:[<>!]?=|[<>])", parts[0]))
        if len(parts) > 1:
            used |= set(re.findall(r"\b([A-Za-z_]\w*)\b", parts[-1]))
        bad = used - allowed - NOT_FIELDS
        if bad:
            problems.append(f"{name}: tstats on non-indexed {sorted(bad)} ({', '.join(st.name for st in named)})")
    return problems


class DashboardTstatsTest(unittest.TestCase):
    def test_tstats_only_on_indexed_fields(self) -> None:
        dashboard = json.loads(DASHBOARD_JSON.read_text())
        problems = []
        for ds_id, ds in dashboard["dataSources"].items():
            query = ds.get("options", {}).get("query")
            if query:
                problems += tstats_problems(ds_id, query)
        for name, query in saved_searches(APP / "default/savedsearches.conf").items():
            problems += tstats_problems(name, query)
        self.assertEqual(problems, [])

    def test_checker_catches_a_non_indexed_field(self) -> None:
        q = "| tstats max(fee_sats) as m where index=btc_parser sourcetype=btc:transactions by _time span=1d"
        self.assertTrue(tstats_problems("x", q))
        q = "| tstats count where index=btc_parser sourcetype=btc:outputs by _time script_type span=1mon"
        self.assertFalse(tstats_problems("x", q))
        self.assertIn("btc:outputs", SOURCETYPES_BY_NAME)

    def test_saved_search_refs_exist(self) -> None:
        dashboard = json.loads(DASHBOARD_JSON.read_text())
        refs = {ds["options"]["ref"] for ds in dashboard["dataSources"].values() if ds["type"] == "ds.savedSearch"}
        self.assertLessEqual(refs, set(saved_searches(APP / "default/savedsearches.conf")))


class DashboardUpToDateTest(unittest.TestCase):
    def test_generated_files_match_builder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "splunk/dashboards").mkdir(parents=True)
            (root / "config").mkdir()
            shutil.copy(BUILDER, root / "splunk/dashboards" / BUILDER.name)
            shutil.copy(REPO_ROOT / "config/sanctioned_addresses.csv", root / "config")
            subprocess.run([sys.executable, str(root / "splunk/dashboards" / BUILDER.name)],
                           check=True, capture_output=True)
            for rel in GENERATED:
                with self.subTest(rel):
                    self.assertEqual(
                        (root / "splunk" / rel).read_bytes(),
                        (REPO_ROOT / "splunk" / rel).read_bytes(),
                        f"{rel} is out of date - run python3 splunk/dashboards/{BUILDER.name}",
                    )


if __name__ == "__main__":
    unittest.main()
