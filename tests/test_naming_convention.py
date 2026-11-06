"""Every Splunk app and knowledge object under splunk/ must follow the ZRZ
naming convention for use case "btc" (docs/10-splunk-integration-und-feldkonzept.md
10.2):

- app directories   zrz_ds_btc, zrz_in_btc_<log>, zrz_ix_btc, zrz_ko_btc,
                    zrz_rd_btc (or a Splunkbase TA as Splunk_TA_<name>_btc)
- index             zrz_btc[_<name>]
- sourcetype        zrz:btc:<log>
- knowledge objects zrz_btc_<name> (transforms, extractions, lookups,
                    macros, reports/alerts, KV collections, dashboards,
                    server classes)

    python -m unittest discover -s tests      (or: pytest tests)
"""

from __future__ import annotations

import configparser
import re
import unittest
from pathlib import Path

from btc_parser_app.export_schema import INDEX, KO_PREFIX, SOURCETYPE_PREFIX, SOURCETYPES

SPLUNK_DIR = Path(__file__).resolve().parent.parent / "splunk"

APP_DIR = re.compile(r"^(zrz_(ds|ix|ko|rd)_btc|zrz_in_btc_[a-z0-9-]+|Splunk_TA_[a-z]+_btc)$")
KO_NAME = re.compile(rf"^{KO_PREFIX}_[a-z0-9_]+$")
CLASS_KEY = re.compile(r"^(TRANSFORMS|REPORT|EXTRACT|FIELDALIAS|EVAL|LOOKUP|SEDCMD)-(.+)$")


def read_conf(path: Path) -> configparser.RawConfigParser:
    parser = configparser.RawConfigParser(strict=False, interpolation=None, comment_prefixes=("#",))
    parser.optionxform = str
    parser.read(path, encoding="utf-8")
    return parser


def stanzas(path: Path) -> list[str]:
    """Stanza names only (savedsearches.conf values span lines that
    configparser cannot read)."""
    return re.findall(r"^\[([^\]]+)\]\s*$", path.read_text(encoding="utf-8"), re.M)


def conf_files(name: str) -> list[Path]:
    return sorted(SPLUNK_DIR.glob(f"*/*/{name}"))


class NamingConventionTest(unittest.TestCase):
    def test_app_directories(self) -> None:
        apps = [p.name for p in SPLUNK_DIR.iterdir() if p.is_dir()]
        self.assertTrue(apps)
        for app in apps:
            self.assertRegex(app, APP_DIR)

    def test_sourcetypes(self) -> None:
        for st in SOURCETYPES:
            self.assertTrue(st.name.startswith(SOURCETYPE_PREFIX), st.name)
        for path in conf_files("props.conf"):
            for stanza in read_conf(path).sections():
                with self.subTest(f"{path.relative_to(SPLUNK_DIR)} [{stanza}]"):
                    self.assertTrue(stanza.startswith(SOURCETYPE_PREFIX))

    def test_props_class_names(self) -> None:
        for path in conf_files("props.conf"):
            conf = read_conf(path)
            for stanza in conf.sections():
                for key in conf[stanza]:
                    m = CLASS_KEY.match(key)
                    if m:
                        with self.subTest(f"{path.relative_to(SPLUNK_DIR)} [{stanza}] {key}"):
                            self.assertRegex(m.group(2), KO_NAME)

    def test_knowledge_object_stanzas(self) -> None:
        for name in ("transforms.conf", "savedsearches.conf", "macros.conf", "collections.conf"):
            for path in conf_files(name):
                for stanza in stanzas(path):
                    with self.subTest(f"{path.relative_to(SPLUNK_DIR)} [{stanza}]"):
                        self.assertRegex(stanza, KO_NAME)

    def test_views_and_lookups(self) -> None:
        files = list(SPLUNK_DIR.glob("*/default/data/ui/views/*")) + list(SPLUNK_DIR.glob("*/lookups/*"))
        self.assertTrue(files)
        for path in files:
            self.assertRegex(path.name, rf"^{KO_PREFIX}_[a-z0-9_]+\.(xml|csv)$")

    def test_inputs_and_indexes(self) -> None:
        index_re = rf"^{INDEX}(_[a-z0-9_]+)?$"
        for path in conf_files("indexes.conf"):
            for stanza in read_conf(path).sections():
                self.assertRegex(stanza, index_re)
        inputs = conf_files("inputs.conf")
        self.assertTrue(inputs)
        for path in inputs:
            conf = read_conf(path)
            for stanza in conf.sections():
                with self.subTest(f"{path.relative_to(SPLUNK_DIR)} [{stanza}]"):
                    self.assertRegex(conf[stanza]["index"], index_re)
                    self.assertTrue(conf[stanza]["sourcetype"].startswith(SOURCETYPE_PREFIX))

    def test_server_classes(self) -> None:
        for path in conf_files("serverclass.conf"):
            for stanza in read_conf(path).sections():
                name = stanza.split(":")[1]
                self.assertRegex(name, KO_NAME)
                if ":app:" in stanza:
                    self.assertRegex(stanza.rsplit(":", 1)[1], APP_DIR)


if __name__ == "__main__":
    unittest.main()
