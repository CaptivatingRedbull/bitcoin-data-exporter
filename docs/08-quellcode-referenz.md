# 8. Quellcode-Referenz

[← Index](00-index.md)

Datei-für-Datei-Übersicht über das gesamte Repository. Für ausführliche
Verhaltensbeschreibungen siehe die themenspezifischen Kapitel 4–6; dieses
Kapitel dient als schneller Einstieg, um eine Datei im Repository ihrer
Rolle zuzuordnen.

## 8.1 Wurzelverzeichnis (`full_app/`)

| Datei | Rolle |
|---|---|
| `run.py` | Komfort-Einstiegspunkt: hängt `full_app/` an `sys.path` und ruft `btc_parser_app.cli.main()` auf, damit `python run.py <kommando>` von überall funktioniert, ohne `PYTHONPATH` setzen oder in `full_app/` wechseln zu müssen. |
| `start.sh` | Produktionsnaher Start: prüft RPC-Erreichbarkeit, startet alle drei Dauerprozesse losgelöst im Hintergrund, PID-Tracking über `.pids/`. Schreibt rohe `.out`-Logs hart codiert nach `full_app/logs/`. Siehe Kapitel 2. |
| `stop.sh` | Stoppt, was `start.sh` gestartet hat (`SIGTERM`, nach 30 s `SIGKILL`). Siehe Kapitel 2. |
| `lib.sh` | Von `start.sh`/`stop.sh` per `source` eingebundene Hilfsfunktion `pid_matches_component()` – verhindert, dass eine PID-Wiederverwendung nach einem Absturz fälschlich als "läuft noch" erkannt wird. |
| `requirements.txt` | Python-Abhängigkeiten: `polars`, `requests`, `PyYAML`. |
| `README.md` | Kürzere, englischsprachige Projektübersicht. |
| `config/config.yaml` | Standardkonfiguration für lokale/Dev-Nutzung, durchgängig kommentiert. |
| `config/config.production.yaml` | Produktivkonfiguration – gleiches Schema, zeigt auf die Pfade/Netzwerkadressen der Zielumgebung. |
| `config/pools-v2.json` | Mitgeliefertes Mining-Pool-Signaturdatenset (Snapshot von `mempool/mining-pools`, MIT-lizenziert). |
| `config/XBTUSD_1.csv` / `config/XBTEUR_1.csv` | **Nicht mitgeliefert** – hier die Kraken-1-Minuten-OHLC-Exporte ablegen (siehe Kapitel 6). |

Zur Laufzeit zusätzlich erzeugt (nicht im Repository): `logs/`, `.pids/`,
`.venv/`, sowie das in Kapitel 7 beschriebene `parser-data/`-Verzeichnis
mit seinen `state/`- und `export/`-Unterordnern.

## 8.2 `btc_parser_app/` – Python-Hauptpaket

| Datei | Rolle |
|---|---|
| `__init__.py` | Leer – markiert das Verzeichnis als Python-Paket. |
| `cli.py` | `argparse`-Einstiegspunkt (`build_parser()`, `main()`). Definiert alle fünf Subkommandos, lädt die Konfiguration, konfiguriert Logging, dispatcht zum jeweiligen Modul. Enthält das `SIGTERM → KeyboardInterrupt`-Shim für `api-poll`. Siehe Kapitel 2. |
| `config.py` | Lädt und validiert `config.yaml` in typisierte, unveränderliche (`frozen`) Dataclasses (`AppConfig` und Unter-Configs je Sektion). Jedes andere Modul erhält seine Konfiguration als Parameter statt globaler Konstanten. Siehe Kapitel 3. |

### `btc_parser_app/common/` – geteilte Infrastruktur

| Datei | Rolle |
|---|---|
| `csv_writer.py` | Gemeinsamer, größenrotierender Append-only-CSV-Writer (`write_rows_to_csv`, `flush_batch_to_disk`) sowie die Lesefunktionen für rotierte logische CSVs (`read_csv_parts`, `csv_parts_exist`, `all_parts`) und die Part-Adressierungs-Hilfsfunktionen (`part_path`, `existing_part_numbers`, `highest_existing_part`), die `rpc/part_writer.py` und `rpc/reorg_state.py` für die über zwei Verzeichnisse verteilten `blocks/`/`transactions/`-Parts benötigen. Siehe Kapitel 7.3. |
| `atomic_write.py` | `atomic_replace()` – schreibt eine neue Datei komplett an einen Temp-Pfad und ersetzt das Ziel per `os.replace()` (atomar auf POSIX), sodass ein Absturz mitten im Schreiben nie eine abgeschnittene Zustandsdatei hinterlässt. Verwendet von `current.csv`, `latest.csv`, `block_status.csv`, `registry.csv`, `pools-v2.json`, sowie von `part_writer.py`s atomarem Pro-Block-Schreibmodus. |
| `logging_setup.py` | `configure_logging()` – richtet Konsolen- und (falls ein Komponentenname übergeben wird) rotierendes Datei-Logging ein (20 MB × 5 Dateien). Siehe Kapitel 9. |
| `block_header.py` | Rohe 80-Byte-Bitcoin-Blockheader (de)serialisieren und deren Hash validieren (`header_hash()`, `validate_header_hash()`, `parse_header()`). Verwendet ausschließlich von der Stale-Blocks-Pipeline. Siehe Kapitel 5.3. |

### `btc_parser_app/rpc/` – `bitcoin-cli`-Seite

| Datei | Rolle |
|---|---|
| `__init__.py` | Leer. |
| `client.py` | Dünner `bitcoin-cli`-Subprozess-Wrapper mit Retry/Timeout (`run_cli()`), plus die konkreten RPC-Aufrufe (`get_block_count`, `get_block_hash`, `get_block_verbose`, `get_block_header`, `get_block_header_raw`, `get_chain_tips`). Siehe Kapitel 4.7. |
| `block_parser.py` | Wandelt ein `getblock verbosity=3`-JSON in flache Block-/Transaktions-Event-Dicts um (`aggregate_block()`, `aggregate_transaction()`). Enthält die gesamte Feldberechnungslogik für `blocks.csv`/`transactions.csv`; reicht den rohen Coinbase-`scriptSig`/die Auszahlungsadressen nur als internen Rückgabewert an die Mining-Pool-Zuordnung weiter, ohne sie zu exportieren. Siehe Kapitel 4.9–4.10. |
| `mining_pools.py` | Mining-Pool-Extraktor: `PoolMatcher` lädt das Signaturdatenset und matcht per Coinbase-Tag oder Auszahlungsadresse (`PoolMatcher.match()`). Siehe Kapitel 4.8. |
| `reorg_state.py` | Zustandsdateien der Reorg-Logik: `IndexStore` (`index/index.csv`), `BlockStatusStore` (`block_status.csv`), Pointer-Lese-/Schreibfunktionen (`current.csv`/`latest.csv`), `write_reorg_log()` (`reorg/`-Audit-CSVs), `seed_index_from_blocks_csv()` (liest `blocks.csv`-Parts aus sowohl `state_dir` als auch `output_dir`). Siehe Kapitel 4.5. |
| `part_writer.py` | `PartSequencer` – durable Part-Nummerierung sowie Batch-/Atomar-Schreibmodus und die `state_dir → output_dir`-Übergabe für `blocks/`/`transactions/`. Siehe Kapitel 4.6. |
| `ingest.py` | Hauptschleife des RPC-Parsers (`run_rpc_ingest()`): Katch-up, Tip-Following, Reorg-Erkennung (`_recover_from_reorg()`) und -Behebung, Umschalten zwischen Batch- und atomarem Schreibmodus (`_drain()`). Der einzige Einstiegspunkt für das Kommando `rpc-ingest`. Siehe Kapitel 4.2–4.4, 4.6. |
| `stale_blocks.py` | Hauptschleife der Stale-Blocks-Pipeline (`run_stale_blocks_ingest()`): Node-Poll- und GitHub-Pull-Durchlauf, Header-Validierung, Export-Gating. Der einzige Einstiegspunkt für das Kommando `stale-blocks-ingest`. Siehe Kapitel 5.2–5.3. |
| `stale_blocks_github.py` | Zieht `bitcoin-data/stale-blocks`s Header-CSV von GitHub (`fetch_stale_blocks_csv()`). Siehe Kapitel 5.2. |
| `stale_blocks_state.py` | `StaleBlockRegistry` – interne Buchführung (`registry.csv`) darüber, welche nicht-aktiven Blockhashes bereits bekannt sind und welcher Status zuletzt exportiert wurde. Siehe Kapitel 5.7. |

### `btc_parser_app/api/` – `mempool.space`-Seite

| Datei | Rolle |
|---|---|
| `__init__.py` | Leer. |
| `rate_limiter.py` | `TokenBucket` – Thread-sicherer Token-Bucket-Ratenbegrenzer, setzt `mempool_api.rate_limit` durch. Siehe Kapitel 6.2. |
| `client.py` | `ApiClient` – ratenbegrenzter HTTP-GET-Client mit Retry-/Timeout-/429-Handling (`get_json()`), plus die Exceptions `RateLimited`/`FetchError` und die geteilte `handle_rate_limited()`-Reaktion. Siehe Kapitel 6.2, 6.5. |
| `mempool_endpoints.py` | JSON-zu-Zeilen-Parser je mempool.space-Endpunkt (`parse_fees_precise`, `parse_mempool`, `parse_prices`, `parse_difficulty_adjustment`, `parse_mining_pools_24h`) sowie die `PARSER_REGISTRY`, die `config.yaml`s `endpoints[].parser`-Strings auf diese Funktionen abbildet. `parse_prices` schreibt in derselben `date_unix,usd,eur`-Form wie `price_history_import.py`. Siehe Kapitel 6.4. |
| `poller.py` | `run_poller()` – startet einen Thread pro konfiguriertem Endpunkt (`endpoint_loop()`, `fetch_and_write()`); verwaltet Start-Offsets (`compute_start_offsets()`) und das gemeinsame Stop-/429-Signaling. Kein Preis-Lückenfüller-Thread mehr. Der einzige Einstiegspunkt für das Kommando `api-poll`. Siehe Kapitel 6.3. |
| `mining_pools_dataset.py` | Aktualisiert `config/pools-v2.json` von GitHub (`fetch_pools_dataset()`, `refresh()`, `refresh_if_stale()`). Wird sowohl vom Kommando `update-pools-dataset` als auch automatisch von `rpc-ingest` aufgerufen. Siehe Kapitel 4.8. |
| `price_history_import.py` | `import_price_history()` – einmaliger, rein lokaler (kein Netzwerkzugriff) Bulk-Import zweier Kraken-1-Minuten-OHLC-CSVs, gejoint auf Minutenzeitstempel, in `mempool_api.output_dir/prices.csv`. Der einzige Einstiegspunkt für das Kommando `import-price-history`. Siehe Kapitel 6.6. |

## 8.3 Abhängigkeitsrichtung (vereinfacht)

```
run.py
  └─ btc_parser_app.cli
       ├─ btc_parser_app.config
       ├─ btc_parser_app.common.logging_setup
       ├─ btc_parser_app.rpc.ingest
       │    ├─ btc_parser_app.rpc.client
       │    ├─ btc_parser_app.rpc.block_parser
       │    │    └─ btc_parser_app.rpc.mining_pools
       │    ├─ btc_parser_app.rpc.reorg_state
       │    ├─ btc_parser_app.rpc.part_writer
       │    │    └─ btc_parser_app.common.atomic_write
       │    ├─ btc_parser_app.api.mining_pools_dataset
       │    └─ btc_parser_app.common.csv_writer
       ├─ btc_parser_app.rpc.stale_blocks
       │    ├─ btc_parser_app.rpc.client
       │    ├─ btc_parser_app.rpc.stale_blocks_github
       │    ├─ btc_parser_app.rpc.stale_blocks_state
       │    └─ btc_parser_app.common.block_header
       ├─ btc_parser_app.api.poller
       │    ├─ btc_parser_app.api.client
       │    │    └─ btc_parser_app.api.rate_limiter
       │    └─ btc_parser_app.api.mempool_endpoints
       ├─ btc_parser_app.api.mining_pools_dataset
       └─ btc_parser_app.api.price_history_import
            └─ btc_parser_app.common.csv_writer
```

`btc_parser_app.common.*` (nicht vollständig abgebildet, da von
praktisch jedem Modul verwendet) enthält keine Abhängigkeiten auf andere
Pakete innerhalb von `btc_parser_app` außer `config.py`.
