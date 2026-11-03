# 3. Konfigurationsreferenz

[← Index](00-index.md)

Alle Einstellungen der Anwendung liegen in einer einzigen YAML-Datei
(Standard: `config/config.yaml`, alternativ `--config PATH` oder die
Umgebungsvariable `BTC_PARSER_CONFIG` für `start.sh`/`stop.sh`). Geladen
und validiert wird sie von `btc_parser_app/config.py::load_config()` in
typisierte, unveränderliche (`frozen`) Dataclasses – jede fehlende
Pflichtangabe oder jeder ungültige Wert führt zu einer klaren
`ConfigError`-Meldung statt eines rohen Tracebacks tief im Programm.

**Pfadauflösung:** Jeder relative Pfad in `config.yaml` wird gegen
`APP_ROOT` aufgelöst – das ist der Repository-Root, also das Verzeichnis, das
`config/` und das `btc_parser_app`-Paket enthält, unabhängig vom
Arbeitsverzeichnis, aus dem die Anwendung gestartet wird. Absolute Pfade
funktionieren überall gleichermaßen.

## 3.1 `logging`

| Schlüssel | Typ | Default | Beschreibung |
|---|---|---|---|
| `level` | string | `INFO` | Log-Level für Konsole und Datei. |
| `log_dir` | Pfad | `parser-data/logs` | Verzeichnis für rotierende Log-Dateien, ein `<kommando>.log` pro Kommando. |

`log_dir` liegt standardmäßig unter demselben `parser-data/`-Wurzel-
verzeichnis wie `state/` und `export/` (siehe 3.3/3.4) – rein zur
gemeinsamen, leicht auffindbaren Ablage; Logs sind selbst weder
Splunk-Export noch App-Zustand. `start.sh` schreibt das rohe
stdout/stderr jedes Prozesses zusätzlich nach `log_dir/<kommando>.out`
(siehe Kapitel 2.3).

Siehe Kapitel 8 für Details zum Logging-Verhalten.

## 3.2 `mining_pools_dataset`

Quelle der Signaturen für die RPC-seitige Mining-Pool-Zuordnung.

| Schlüssel | Typ | Default | Beschreibung |
|---|---|---|---|
| `source_url` | string | – (Pflicht) | GitHub-Raw-URL des `pools-v2.json`-Datensets. |
| `local_path` | Pfad | – (Pflicht) | Lokale Kopie, standardmäßig `config/pools-v2.json`. |
| `refresh_interval_seconds` | float | – (Pflicht) | Ab welchem Alter der lokalen Datei ein Refresh ausgelöst wird. |

Dies ist die **einzige** Quelle für Mining-Pool-Zuordnung in dieser
Anwendung; der Abgleich läuft vollständig offline gegen RPC-Blockdaten,
ohne Netzwerkaufrufe pro Block. Der mitgelieferte Snapshot
unter `config/pools-v2.json` stammt von
[mempool/mining-pools](https://github.com/mempool/mining-pools)
(MIT-lizenziert), sodass `rpc-ingest` offline funktioniert. Details zur
Verwendung in Kapitel 4.

## 3.3 `rpc`

Konfiguration für `rpc-ingest` (`bitcoin-cli`-Seite).

| Schlüssel | Typ | Default | Beschreibung |
|---|---|---|---|
| `bitcoin_cli_path` | string | – (Pflicht) | Pfad/Name der `bitcoin-cli`-Binary. |
| `extra_args` | Liste[string] | `[]` | Zusätzliche Flags für jeden `bitcoin-cli`-Aufruf, z. B. `-rpcconnect=`, `-rpcport=`, `-datadir=`. |
| `rpcuser_env` | string \| null | `null` | Name der Umgebungsvariable mit dem RPC-Benutzernamen. |
| `rpcpassword_env` | string \| null | `null` | Name der Umgebungsvariable mit dem RPC-Passwort. |
| `batch_size` | int (≥1) | – (Pflicht) | Blöcke pro Zwischen-Checkpoint während einer Aufholjagd, standardmäßig 20. |
| `output_dir` | Pfad | – (Pflicht) | Splunk-seitiger Export, standardmäßig `parser-data/export/rpc`. Enthält ausschließlich vollständige, abgeschlossene `blocks/`/`transactions/`-Parts – siehe Kapitel 4/7. |
| `state_dir` | Pfad | – (Pflicht) | Rein interne Buchführung, standardmäßig `parser-data/state/rpc`. Enthält `current.csv`, `latest.csv`, `index/`, `block_status.csv`, `reorg/`, die `*_part_seq.csv`-Zähler sowie – solange Backlog aufzuholen ist – den aktuell noch wachsenden `blocks/`/`transactions/`-Part. **Nie** als Splunk-Input verwenden. |
| `reorg_confirmations` | int (≥0) | `6` | Anzahl Bestätigungen, die zwischen Node-Tip und Verarbeitungsgrenze liegen müssen. |
| `max_reorg_depth` | int (≥1) | `100` | Sicherheitsgrenze für die Rückwärtssuche nach dem gemeinsamen Vorfahren. |
| `poll_interval_seconds` | float (≥0) | `30` | Wartezeit zwischen Tip-Checks im eingeholten Zustand. |
| `max_cli_retries` | int (≥0) | `3` | Wiederholungen pro `bitcoin-cli`-Aufruf. |
| `cli_retry_backoff_seconds` | float (≥0) | `5` | Wartezeit zwischen `bitcoin-cli`-Wiederholungen. |
| `cli_timeout_seconds` | float (>0) | `30` | Timeout pro `bitcoin-cli`-Aufruf. |
| `transport` | `"cli"` \| `"http"` | `"cli"` | Wie mit bitcoind gesprochen wird: `bitcoin-cli`-Subprozess pro Aufruf oder direktes JSON-RPC über HTTP. Retry/Timeout (die drei Zeilen darüber) gelten für beide. Siehe unten. |
| `http_url` | string \| null | `null` | Nur bei `transport: "http"`: Endpunkt, z. B. `http://127.0.0.1:8332`. Leer = aus `extra_args` (`-rpcconnect`/`-rpcport`) abgeleitet, sonst `127.0.0.1` + Standardport der Chain. |
| `cookie_file` | Pfad \| null | `null` | Nur bei `transport: "http"`: Cookie-Datei. Leer = aus `extra_args` (`-rpccookiefile`/`-datadir`) abgeleitet, sonst `~/.bitcoin/.cookie`. `rpcuser_env`/`rpcpassword_env` haben – wie bei `bitcoin-cli` – Vorrang. |
| `catchup_workers` | int (≥1) | `1` | Worker-Prozesse, die während der Aufholjagd Blöcke vorab holen und parsen. `1` = streng sequenziell. Siehe unten. |
| `blocks_per_day` | int (≥1) | – (Pflicht) | Nominell 144 (10-Minuten-Blöcke); nur zur Näherung des Coin-Alters in Tagen. |
| `max_block_weight` | int (≥1) | – (Pflicht) | Für `weight_utilization_pct`, standardmäßig 4.000.000. |
| `halving_interval_blocks` | int (≥1) | – (Pflicht) | Für die Subsidy-Berechnung, standardmäßig 210.000. |
| `initial_subsidy_sats` | int | – (Pflicht) | Ursprüngliche Blockbelohnung in Satoshi, standardmäßig 5.000.000.000. |

### Zugangsdaten

`-rpcuser`/`-rpcpassword` gehören **niemals** direkt in `extra_args` (oder
sonst irgendwo in diese Datei) – sie ist bewusst so gestaltet, dass sie
gefahrlos eingecheckt werden kann. Stattdessen `rpcuser_env`/
`rpcpassword_env` auf die *Namen* von Umgebungsvariablen setzen; diese
werden nur zur Laufzeit gelesen (`RpcConfig.auth_args()`) und nur dann als
`-rpcuser=`/`-rpcpassword=`-Flags angehängt, wenn beide tatsächlich gesetzt
und nicht leer sind. Ein per CLI-Flag übergebenes Credential ist für jeden
mit `ps`-Zugriff auf demselben Host sichtbar – wo das relevant ist, ist
Cookie-Datei-Auth (die Voreinstellung, wenn nichts gesetzt wird)
vorzuziehen.

### `batch_size`

Steuert, wie viele Blöcke sich zwischen den `current.csv`-Checkpoint-
Logzeilen während einer Backlog-Aufholjagd ansammeln – **jeder** Durchlauf
der Ingest-Schleife (siehe Kapitel 4) schreibt am Ende unbedingt alles
Verarbeitete auf die Platte, unabhängig davon, ob ein voller Batch erreicht
wurde. Beim Tip-Following (wo ein Durchlauf oft nur den einen neuen Block
umfasst) wird also **nicht** auf 20 Blöcke gewartet, bevor Daten
persistiert werden – `batch_size` steuert nur, wie oft die
"Progress: height X/Y"-Logzeile und der Zwischen-Checkpoint während einer
langen Aufholjagd feuern.

### Schnellere Aufholjagd: `transport` und `catchup_workers`

Standard ist `transport: "cli"` und `catchup_workers: 1`. Beide dürfen
auch bei einem bereits laufenden Durchlauf eingeschaltet werden – `rpc-ingest` neu starten genügt.
Bestehende `state_dir`/`output_dir` funktionieren unverändert weiter, und
die Ausgabe (Parts, `*_part_seq.csv`, `current.csv`, `index/`,
`block_status.csv`) ist in beiden Fällen byte-identisch.

- **`transport: "http"`** spricht JSON-RPC direkt mit bitcoind, über eine
  offen gehaltene HTTP-Verbindung pro Prozess, statt pro Block zweimal
  `bitcoin-cli` zu starten (und dessen eigenes Parsen und
  Pretty-Printen jeder mehrere MB großen `getblock`-Antwort abzuwarten).
  Beträge bleiben exakt (`Decimal`), Retry/Timeout wie oben. Endpunkt und
  Cookie-Datei werden standardmäßig genau so aus `extra_args` abgeleitet,
  wie `bitcoin-cli` sie verwenden würde; `http_url`/`cookie_file`
  überschreiben das. Die Cookie-Datei wird bei jedem Neuverbinden und bei
  HTTP 401 neu gelesen – ein bitcoind-Neustart ist damit nur ein
  wiederholter Aufruf. Gilt für alle Kommandos, die den `rpc:`-Abschnitt
  verwenden (`rpc-ingest`, `stale-blocks-ingest`, `mempool-watch`).
- **`catchup_workers: N`** (N > 1) lässt N Worker-Prozesse
  (`rpc/catchup.py`) Blöcke holen und parsen, während der Hauptprozess sie
  streng in Höhen-Reihenfolge übernimmt. Zeilenpuffer, `index/`,
  `block_status.csv`, Parts und `current.csv` schreibt weiterhin nur der
  Hauptprozess – in derselben Reihenfolge und an denselben
  `batch_size`-Grenzen wie bei einem einzelnen Prozess. Nur für Höhen, die mindestens
  `max_reorg_depth` unter dem Tip liegen (so tiefe Blöcke behandelt die
  Reorg-Behebung ohnehin als endgültig); das letzte Stück vor dem Tip und
  das gesamte Tip-Following bleiben sequenziell. Baut ein vorab geholter
  Block wider Erwarten nicht auf dem vorherigen auf, endet der Durchlauf
  dort und der nächste Durchlauf führt die normale Reorg-Prüfung aus.
  bitcoind bearbeitet höchstens `-rpcthreads` Anfragen gleichzeitig (ältere
  Core-Versionen: Standard 4) – dort mindestens N plus einige Threads für
  die übrigen Dienste einstellen.

Empfohlen für die Aufholjagd: `transport: "http"` und `catchup_workers`
knapp unter der Zahl der physischen Kerne (bitcoind braucht für
`getblock … 3` selbst CPU) – auf dem Ryzen 9 5950X 14 als Startwert.
Messwerte: Kapitel 4.14.

## 3.4 `stale_blocks`

Konfiguration für `stale-blocks-ingest`.

| Schlüssel | Typ | Default | Beschreibung |
|---|---|---|---|
| `output_dir` | Pfad | – (Pflicht) | Splunk-seitige Exporte, standardmäßig `parser-data/export/stale`. |
| `state_dir` | Pfad | – (Pflicht) | Nur interne Buchführung, standardmäßig `parser-data/state/stale`. |
| `node_poll_interval_seconds` | float (>0) | `3600` | Intervall des `getchaintips`-Durchlaufs. |
| `request_timeout_seconds` | float (>0) | `30` | Timeout für den GitHub-CSV-Abruf. |
| `github.csv_url` | string | – (Pflicht) | Raw-URL des `bitcoin-data/stale-blocks`-Datensets. |
| `github.poll_interval_seconds` | float (>0) | – (Pflicht) | Intervall des GitHub-Abrufs, standardmäßig 86400 (täglich). |

`output_dir` und `state_dir` sind bewusst getrennt: `output_dir` enthält
die Splunk-seitigen Exporte, `state_dir` nur die interne Buchführung
(`registry.csv`) darüber, was bereits bekannt ist – damit ein
Splunk-Forwarder, der auf `output_dir` zeigt, nicht versehentlich auch
diese interne Datei aufnimmt. Details in Kapitel 5.

## 3.5 `mempool_watch`

Konfiguration für `mempool-watch` (Kapitel 10). Alle `bitcoin-cli`-Aufrufe
laufen über den `rpc`-Abschnitt oben (gleiche Verbindung/Authentifizierung
wie `rpc-ingest`).

| Schlüssel | Typ | Default | Beschreibung |
|---|---|---|---|
| `zmq_endpoint` | string | – (Pflicht, darf leer sein) | ZMQ-`sequence`-Publisher des Nodes, z. B. `tcp://127.0.0.1:28332` – muss zu `zmqpubsequence=` in dessen `bitcoin.conf` passen. Leer (`""`) = reiner Polling-Modus: dann ist der Abgleich (`reconcile_interval_seconds`) der einzige Erkennungsweg und sollte entsprechend kurz sein (z. B. 30). |
| `sanctions_list_paths` | Liste von Pfaden (oder ein einzelner Pfad) | – (Pflicht, nicht leer) | Eine oder mehrere CSV-Dateien mit Kopfzeile `address,name,first_name,sanctions_programs` (nur `address` ist Pflicht). Werden zusammengeführt, bei doppelter Adresse gewinnt die erste Datei. Änderungen werden ohne Neustart übernommen (Kapitel 10.5). |
| `output_dir` | Pfad | – (Pflicht) | Splunk-seitiger Export, standardmäßig `parser-data/export/mempool_watch`. |
| `state_dir` | Pfad | – (Pflicht) | Nur interne Buchführung (`flagged.json`), standardmäßig `parser-data/state/mempool_watch`. |
| `reconcile_interval_seconds` | float (>0) | `300` | Intervall des vollständigen `getrawmempool`-Abgleichs (zusätzlich immer beim Start und sofort nach einer erkannten ZMQ-Lücke). |
| `rpc_workers` | int (≥1) | `4` | Parallele `bitcoin-cli`-Aufrufe beim Prüfen vieler Transaktionen (v. a. beim Startscan des ganzen Mempools). Nicht über `rpcthreads` des Nodes (Default 4) wählen – mehr stauen sich dort nur. |
| `forget_after_confirmations` | int (≥1) | `6` | Ab dieser Tiefe des bestätigenden Blocks wird eine Transaktion nicht mehr auf Reorgs überwacht und aus `flagged.json` entfernt. |
| `max_confirmation_scan_blocks` | int (≥1) | `50` | Verschwindet eine überwachte Transaktion aus dem Mempool, ohne dass ein ZMQ-Ereignis ankam (Downtime, verlorene Nachrichten), wird höchstens so viele Blöcke zurück nach ihr gesucht, bevor sie als `removed` gilt. |

## 3.6 Dateirotation (gilt für die gesamte Anwendung)

Nicht Teil von `config.yaml` (nicht konfigurierbar), aber relevant für
jede Sektion oben: Jede Append-only-CSV dieser Anwendung wächst
unbegrenzt, daher deckelt `btc_parser_app/common/csv_writer.py` jeden
Dateipart auf ~900 MB (`MAX_PART_BYTES = 900_000_000`) und rollt vor
Überschreiten in einen neuen nummerierten Part. Details und
Ingestion-Implikationen in Kapitel 6.

## 3.7 Beispiel: entfernter Node über SSH-Tunnel oder Netzwerk

```yaml
rpc:
  bitcoin_cli_path: "bitcoin-cli"
  extra_args:
    - "-rpcconnect=192.168.x.x"
    - "-rpcport=8332"
  rpcuser_env: "BITCOIN_RPC_USER"
  rpcpassword_env: "BITCOIN_RPC_PASSWORD"
```

Mit gesetzten Umgebungsvariablen `BITCOIN_RPC_USER`/
`BITCOIN_RPC_PASSWORD` werden `-rpcuser=`/`-rpcpassword=` automatisch
angehängt; ansonsten (Standardfall) verwendet `bitcoin-cli` die lokale
Cookie-Datei-Authentifizierung.
