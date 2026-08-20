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
`APP_ROOT` aufgelöst – das ist `full_app/`, also das Verzeichnis, das
`config/` und das `btc_parser_app`-Paket enthält, unabhängig vom
Arbeitsverzeichnis, aus dem die Anwendung gestartet wird. Absolute Pfade
funktionieren überall gleichermaßen.

## 3.1 `logging`

| Schlüssel | Typ | Default | Beschreibung |
|---|---|---|---|
| `level` | string | `INFO` | Log-Level für Konsole und Datei. |
| `log_dir` | Pfad | `parser-data/logs` | Verzeichnis für rotierende Log-Dateien, ein `<kommando>.log` pro Kommando. |

`log_dir` liegt standardmäßig unter demselben `parser-data/`-Wurzel-
verzeichnis wie `state/` und `export/` (siehe 3.5/3.6) – rein zur
gemeinsamen, leicht auffindbaren Ablage; Logs sind selbst weder
Splunk-Export noch App-Zustand. **Wichtig:** `start.sh` schreibt das rohe
stdout/stderr jedes Prozesses zusätzlich fest nach `full_app/logs/
<kommando>.out` – dieser Pfad ist in `start.sh` hart codiert und folgt
**nicht** `log_dir` (siehe Kapitel 2.3).

Siehe Kapitel 9 für Details zum Logging-Verhalten.

## 3.2 `mempool_api`

Der mempool.space-HTTP-Poller (`api-poll`).

| Schlüssel | Typ | Default | Beschreibung |
|---|---|---|---|
| `base_url` | string | `https://mempool.space` | Basis-URL, an die jeder Endpunkt-Pfad angehängt wird. |
| `output_dir` | Pfad | `parser-data/export/api` | Zielverzeichnis für eine `<name>.csv` pro Endpunkt, direkt unter `output_dir` (kein separater State-Ordner für diese Komponente). |
| `request_timeout_seconds` | float | – (Pflicht) | HTTP-Request-Timeout. |
| `max_connection_retries` | int (≥0) | – (Pflicht) | Anzahl Wiederholungen bei transienten Verbindungsfehlern (Reset, SSL-EOF etc.) – **nicht** für HTTP-429, das nie automatisch wiederholt wird. |
| `retry_backoff_seconds` | float | – (Pflicht) | Wartezeit zwischen Wiederholungsversuchen. |
| `rate_limit.requests_per_minute` | float (>0) | – (Pflicht) | Auffüllrate des gemeinsamen Token-Buckets. |
| `rate_limit.bucket_size` | int (≥1) | – (Pflicht) | Burst-Kapazität des Token-Buckets. |
| `endpoints` | Liste | – (Pflicht, nicht leer) | Siehe unten. |

Jeder Eintrag in `endpoints` hat die Form:

```yaml
- name: fees_precise          # bestimmt den Dateinamen <name>.csv
  path: /api/v1/fees/precise  # an base_url angehängt
  parser: fees_precise        # Name der parse_<name>-Funktion
  interval_seconds: 60        # Poll-Intervall dieses Endpunkts
```

`rate_limit.requests_per_minute` / `rate_limit.bucket_size` definieren
**einen einzigen gemeinsamen Token-Bucket**, aus dem sich jede
`endpoints`-Anfrage bedient – eine Erhöhung dieser Werte erhöht also die
effektive Rate gegenüber diesem Host insgesamt, nicht pro Endpunkt. Da
mempool.space seine öffentlichen API-Limits nicht dokumentiert, ist der
Standardwert in `config.yaml` (10 Anfragen/Minute, Burst von 10) bewusst
konservativ gewählt. Der einmalige Preis-Historie-Import
(`import-price-history`, siehe `pricing` unten) macht **keine**
Netzwerkanfragen und berührt dieses Budget nicht.

`parser` muss auf eine `parse_<name>`-Funktion in
`btc_parser_app/api/mempool_endpoints.py` verweisen – ein neuer
mempool.space-Endpunkt wird durch Schreiben dieser Funktion und Ergänzen
eines Eintrags hier hinzugefügt, ohne weitere Code-Änderungen (siehe
Kapitel 6).

Jede Endpunkt-CSV (inklusive `prices.csv`) ist eine einzelne,
kontinuierlich wachsende/rotierende Datei – es gibt keinen Zeitpunkt, an
dem eine dieser Dateien für einen `batch`-Splunk-Input "vollständig" wäre.
`output_dir` sollte deshalb immer mit einem `monitor`-Input (nicht-
destruktives Tailing) angebunden werden, nie mit `batch` (siehe Kapitel 7).
Für `prices.csv` gilt das zusätzlich zwingend: `import-price-history` liest
diese Datei vollständig zurück, um bereits importierte Minuten zu erkennen
(siehe `pricing` unten) – sie darf also nie durch ein destruktives
Splunk-Input-Verhalten unter der Anwendung weggelöscht werden.

Anfragen werden ohne eigenen `User-Agent`-Header gesendet (nur der
Standard der `requests`-Bibliothek) – es gibt kein mempool.space-Abo in
diesem Setup, gegen das man sich identifizieren müsste.

## 3.3 `mining_pools_dataset`

Quelle der Signaturen für die RPC-seitige Mining-Pool-Zuordnung.

| Schlüssel | Typ | Default | Beschreibung |
|---|---|---|---|
| `source_url` | string | – (Pflicht) | GitHub-Raw-URL des `pools-v2.json`-Datensets. |
| `local_path` | Pfad | – (Pflicht) | Lokale Kopie, standardmäßig `config/pools-v2.json`. |
| `refresh_interval_seconds` | float | – (Pflicht) | Ab welchem Alter der lokalen Datei ein Refresh ausgelöst wird. |

Dies ist die **einzige** Quelle für Mining-Pool-Zuordnung in dieser
Anwendung; der Abgleich läuft vollständig offline gegen RPC-Blockdaten,
ohne Aufrufe pro Block gegen mempool.space. Der mitgelieferte Snapshot
unter `config/pools-v2.json` stammt von
[mempool/mining-pools](https://github.com/mempool/mining-pools)
(MIT-lizenziert), sodass `rpc-ingest` offline funktioniert. Details zur
Verwendung in Kapitel 4.

## 3.4 `pricing`

Konfiguriert ausschließlich den einmaligen, rein lokalen Bulk-Import von
historischen Minutenpreisen (Kommando `import-price-history`) in
**dieselbe** `mempool_api.output_dir/prices.csv`, in die auch der live
gepollte `prices`-Endpunkt schreibt – es gibt keine separate Tagestabelle
und kein eigenes `output_dir` für Pricing.

| Schlüssel | Typ | Default | Beschreibung |
|---|---|---|---|
| `xbtusd_csv_path` | Pfad | – (Pflicht) | Pfad zum Kraken-XBTUSD-1-Minuten-OHLC-Export, standardmäßig `config/XBTUSD_1.csv` (Datei selbst wird nicht mitgeliefert). |
| `xbteur_csv_path` | Pfad | – (Pflicht) | Pfad zum Kraken-XBTEUR-1-Minuten-OHLC-Export, standardmäßig `config/XBTEUR_1.csv` (Datei selbst wird nicht mitgeliefert). |

Beide Kraken-Exporte werden auf Minutenzeitstempel gejoint und in
derselben `date_unix,usd,eur`-Zeilenform geschrieben, die auch der Live-
Poller verwendet (siehe Kapitel 6). Es gibt in dieser Version **keine**
automatische Lückenfüllung mehr über die mempool.space-API und keinen
`backfill`-Unterabschnitt – Tiefe (historische Minuten) kommt
ausschließlich aus diesen beiden lokalen CSV-Dateien, die Gegenwart
ausschließlich vom live gepollten `prices`-Endpunkt. Details in Kapitel 6.

## 3.5 `rpc`

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

## 3.6 `stale_blocks`

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

## 3.7 Dateirotation (gilt für die gesamte Anwendung)

Nicht Teil von `config.yaml` (nicht konfigurierbar), aber relevant für
jede Sektion oben: Jede Append-only-CSV dieser Anwendung wächst
unbegrenzt, daher deckelt `btc_parser_app/common/csv_writer.py` jeden
Dateipart auf ~900 MB (`MAX_PART_BYTES = 900_000_000`) und rollt vor
Überschreiten in einen neuen nummerierten Part. Details und
Ingestion-Implikationen in Kapitel 7.

## 3.8 Beispiel: entfernter Node über SSH-Tunnel oder Netzwerk

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
