# 📦 Splunk-Apps – Use Case `btc`

Splunk-Apps für die Daten des bitcoin-data-exporter, aufgebaut nach dem
ZRZ-Vorlage-Repository (`zrz_*_VORLAGE`, `helper_create_app.sh`) und der
ZRZ-Namenskonvention. Platzhalter `VORLAGE` → Use Case **`btc`**.

Dieses Verzeichnis enthält **nur** die Apps, damit sie 1:1 nach
`splunk-global` übernommen werden können. Generatoren und Betriebsskripte
liegen unter [`../tools/splunk/`](../tools/splunk/).

## 📖 Apps

| App‑Typ | Verzeichnis | Ziel | Inhalt |
|---|---|---|---|
| DS (Deployment-Server) | `zrz_ds_btc` | Deployment-Server | Serverklasse `zrz_btc_exporter`, `clients.txt` |
| IN (Inputs) | `zrz_in_btc_exporter` | UF des Parser-Hosts (via DS) | `inputs.conf`, UF-Durchsatz (`limits.conf`, `server.conf`) |
| IX (Index) | `zrz_ix_btc` | Cluster Manager (Indexer) | `indexes.conf`: Index `zrz_btc` |
| KO (Knowledge Objects) | `zrz_ko_btc` | CM + SHCD | Index-Time-`props`/`transforms` |
| RD (Report & Dashboard) | `zrz_rd_btc` | SHCD | Search-Time-`props`/`fields`, Dashboard, Reports, Makros, Lookups, KV-Store |

Test-Umgebung (Standalone, Parser + Splunk Enterprise auf einem Host): alle
Apps außer `zrz_ds_btc` direkt nach `$SPLUNK_HOME/etc/apps/`. Prod läuft
UF → Cribl → Splunk; dort ersetzt [`../cribl/zrz_btc_pipeline.json`](../cribl/)
die Index-Time-Arbeit von `zrz_ko_btc`.

## 🏷️ Namen

| Objekt | Name |
|---|---|
| Index | `zrz_btc` |
| Sourcetypes | `zrz:btc:blocks`, `zrz:btc:transactions`, `zrz:btc:inputs`, `zrz:btc:outputs`, `zrz:btc:stale_block_headers`, `zrz:btc:sanctioned_tx_events`, `zrz:btc:prices` |
| Dashboard | `zrz_btc_mempool_explorer` |
| Reports | `zrz_btc_rec_*` |
| Makros | `zrz_btc_index`, `zrz_btc_rec_window` |
| Lookups | `zrz_btc_sanctioned_addresses` (`.csv`), KV-Store `zrz_btc_rec_*` |
| Feld-Extraktionen / Transforms | `EXTRACT-zrz_btc_columns`, `TRANSFORMS-zrz_btc_*`, `zrz_btc_idx_*`, `zrz_btc_drop_header_*` |
| Serverklasse | `zrz_btc_exporter` |
| Rollen / Benutzergruppe | `zrz_user_btc`, `zrz_poweruser_btc` / `zrz-splunk-btc` |

`tests/test_naming_convention.py` prüft diese Konvention für alles unter
`splunk/`. Vollständige Tabelle und Begründungen:
[docs/10 – Abschnitt 10.2](../docs/10-splunk-integration-und-feldkonzept.md#102-apps-zielorte-und-namenskonvention).

## ⚙️ Erzeugte Dateien – nicht von Hand bearbeiten

| Datei(en) | Generator |
|---|---|
| `zrz_ko_btc/default/{props,transforms}.conf`, `zrz_rd_btc/default/{props,fields}.conf`, `../cribl/zrz_btc_pipeline.json` | `python3 tools/splunk/build_field_config.py` (aus `btc_parser_app/export_schema.py`) |
| `zrz_rd_btc/default/{savedsearches,macros,collections,transforms}.conf`, `zrz_rd_btc/default/data/ui/views/zrz_btc_mempool_explorer.xml`, `zrz_rd_btc/lookups/zrz_btc_sanctioned_addresses.csv` | `python3 tools/splunk/build_mempool_explorer.py` |

## ✏️ Vor dem Rollout auszufüllen

Siehe [docs/10 – Abschnitt 10.10](../docs/10-splunk-integration-und-feldkonzept.md#1010-vor-dem-rollout-offene-punkte),
v. a.:

* `zrz_ds_btc/clients.txt` – Hostname(n) des Parser-Host-UF
* `zrz_in_btc_exporter/local/inputs.conf` – `$PARSER_DATA_ROOT` durch den echten Pfad ersetzen
* `zrz_ix_btc/default/indexes.conf` – `maxTotalDataSizeMB`, ggf. `volume:`-Pfade
* Rollen `zrz_user_btc` / `zrz_poweruser_btc` und das Mapping der Gruppe `zrz-splunk-btc` (Plattform)
* `author` in den `app.conf`
