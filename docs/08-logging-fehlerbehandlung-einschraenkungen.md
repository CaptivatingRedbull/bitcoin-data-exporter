# 8. Logging, Fehlerbehandlung und bekannte Einschränkungen

[← Index](00-index.md)

## 8.1 Logging

Gesteuert über `logging.level` (Standard `INFO`) und `logging.log_dir`
(Standard `parser-data/logs`, relativ zum Repository-Root) in `config.yaml`
(`btc_parser_app/common/logging_setup.py::configure_logging()`).

Jedes Kommando loggt sowohl nach stdout als auch in eine rotierende Datei
unter `log_dir/<kommando>.log` (20 MB pro Datei, 5 aufgehoben). Zusätzlich
schreibt `start.sh` das rohe, unstrukturierte stdout/stderr jedes
losgelösten Prozesses nach `log_dir/<kommando>.out`, direkt neben die
`.log`-Datei (siehe Kapitel 2.3).

Format: `%(asctime)s %(levelname)-7s %(name)s: %(message)s`, Zeitstempel
im Format `%Y-%m-%dT%H:%M:%S%z` (inklusive Zeitzonen-Offset des Hosts).

### Log-Level

Hochfrequente Einzelzeilen-Logs loggen bewusst auf `DEBUG`, nicht `INFO`:

- Die "Parsing block N"-Zeile von `rpc-ingest` pro Block während einer
  Genesis-Aufholjagd (`ingest.py::_process_height()`).
- Der "skipping refresh"-Hinweis bei jedem No-op-Check des
  Mining-Pool-Datenset-Refreshs.

Umgekehrt loggt `mempool-watch` jedes `seen`-Ereignis (eine neue
Transaktion mit Sanktionsadresse im Mempool) bewusst auf `WARNING` mit
Adresse, Name und Sanktionsprogramm – im Journal/Logfile also sofort
sichtbar, auch ohne Splunk. Die Folge-Ereignisse (`confirmed`,
`replaced`, …) loggen auf `INFO`.

`logging.level: DEBUG` in `config.yaml` setzen, falls dieses
Detailniveau gebraucht wird. Das Standardlevel `INFO` liefert weiterhin
Start-Zusammenfassungen, Warnungen, Batch-Fortschrittszeilen und alles,
was tatsächlich Aufmerksamkeit braucht (ein Reorg, ein RPC-Ausfall, ein
gescheiterter GitHub-Abruf).

## 8.2 Fehlerbehandlungs-Philosophie

Die Anwendung folgt durchgängig einem Prinzip: **transiente Fehler
werden mit begrenzten, konfigurierbaren Wiederholungen abgefedert;
Fehler, die auf ein grundsätzliches Problem hindeuten, führen zu einem
bewussten, sofortigen Stopp statt eines möglicherweise falschen
automatischen Entscheids.**

| Fehlerart | Verhalten |
|---|---|
| Transiente `bitcoin-cli`-Fehler (kurzer Node-Restart, Cookie-Rotation, Netzwerk-Blip) | Bis zu `rpc.max_cli_retries`-mal wiederholt, dann `RpcCliError` – `rpc-ingest` fängt dies ab, wartet `rpc.poll_interval_seconds` und versucht den ganzen Durchlauf erneut. Bereits Checkpointetes bleibt erhalten. |
| Transiente HTTP-Verbindungsfehler beim Abruf der GitHub-Datensets (Reset, SSL-EOF, DNS) | Bis zu zweimal wiederholt (`common/http_fetch.py`), dann `FetchError` – `stale-blocks-ingest` versucht den GitHub-Durchlauf beim nächsten Weckzyklus erneut, der Mining-Pool-Refresh behält die lokale Kopie (siehe unten). |
| Reorg-Rückwärtssuche findet keinen gemeinsamen Vorfahren innerhalb `rpc.max_reorg_depth`, oder trifft auf einen nie indizierten Block | `rpc-ingest` wirft eine Exception und stoppt – erfordert manuelles Eingreifen. |
| Fehlendes/kaputtes Mining-Pool-Signaturdatenset | Warnung geloggt, `rpc-ingest` läuft mit `pool_* = None` weiter. |
| Fehlgeschlagener Refresh des Mining-Pool-Datensets | Warnung geloggt, bestehende lokale Kopie bleibt in Verwendung. |
| Fehlerhafte/ungültige Konfiguration (`config.yaml`) | Klare Fehlermeldung auf stderr, Exit-Code 2 – kein roher Traceback. |
| Header, der nicht zum behaupteten Hash passt (Stale-Blocks-Pipeline) | Verworfen (Status `unusable`), Warnung geloggt – kein Abbruch. |
| `mempool-watch`: Node per RPC nicht erreichbar | Fehler geloggt, 30 s Pause, danach ein vollständiger Abgleich – verpasste Ereignisse werden so nachgeholt, kein Abbruch. |
| `mempool-watch`: ZMQ-Nachrichten verloren / Node neu gestartet (Lücke im Nachrichtenzähler) | Warnung geloggt, sofortiger vollständiger Abgleich. |
| `mempool-watch`: seit `reconcile_interval_seconds` keine einzige ZMQ-Nachricht | Warnung geloggt (typisch: `zmqpubsequence` fehlt oder Port nicht erreichbar – ZMQ selbst meldet so etwas nie als Fehler); der periodische Abgleich erkennt Treffer weiterhin, nur mit Verzögerung. |
| `mempool-watch`: Sanktionsliste nach einer Änderung nicht lesbar/ohne `address`-Spalte | Fehler geloggt, die zuletzt gültige Liste bleibt aktiv. Beim Start (keine vorherige Liste) bricht der Prozess dagegen ab. |

## 8.3 Bekannte Einschränkungen / bewusste Design-Grenzen

Diese Punkte sind **keine offenen Bugs**, sondern bewusste
Design-Entscheidungen, die beim Betrieb der Anwendung berücksichtigt
werden sollten:

### Kein automatisches Storage-Cleanup

Siehe Kapitel 6.4 – bewusste Design-Entscheidung, kein offener Punkt.
Welcher rotierte CSV-Part gelöscht werden darf, entscheidet ausschließlich
die Splunk-Input-Konfiguration oder ein eigenes, verifiziertes
Cleanup-Skript, nie die Anwendung selbst.

### `rpc.max_reorg_depth` als Sicherheitsgrenze

Standard 100 – eine Sicherheitsgrenze für die Rückwärtssuche nach dem
gemeinsamen Vorfahren bei einem Reorg. Ein echter Reorg sollte dies nie
annähernd erreichen; wird die Grenze doch erreicht (Bug oder korrupter
`index/`), bricht `rpc-ingest` ab und erfordert manuelles Eingreifen
statt eines möglicherweise falschen automatischen Entscheids.

### Stale-Blocks-Pipeline liefert nur Header, keine vollständigen Blöcke

Bewusste Design-Entscheidung, siehe Kapitel 5.4 – Bitcoin Cores
Checkpoint-Mechanismus würde ohnehin nur einen kleinen, inkonsistenten
Teil des Datensets als vollständigen Block erreichbar machen.

### `mempool-watch` sieht nur, was durch den eigenen Mempool läuft

Eine Transaktion, die nie im Mempool dieses Nodes war (direkt/privat an
einen Miner übermittelt), taucht erst im Block auf – das deckt die
Splunk-Seite über die On-Chain-Daten von `rpc-ingest` ab, nicht
`mempool-watch` (Kapitel 10.9). Ebenso werden nur exakte Adresstreffer
erkannt, keine Wechselgeld-/Cluster-Adressen derselben Akteure.

### `start.sh`/`stop.sh` starten keinen abgestürzten Prozess neu

Für harte Dauerbetriebs-Anforderungen sind systemd-Units mit
`Restart=always`/`Restart=on-failure` vorzuziehen (siehe Kapitel 2.5).

## 8.4 Betriebs-Checkliste für einen neuen Node

1. `bitcoin-cli` einrichten und Erreichbarkeit prüfen (lokal oder über
   `rpc.extra_args` gegen einen entfernten Node).
2. `config/config.yaml` (oder eine Kopie davon) auf die eigene
   Umgebung anpassen (siehe Kapitel 3).
3. Virtuelle Umgebung einrichten (Kapitel 2.2).
4. `./start.sh` ausführen.
5. Logs unter `parser-data/logs/` (strukturiert `.log`, roh `.out`)
   beobachten, insbesondere während der initialen
   Genesis-Aufholjagd von `rpc-ingest` (kann je nach Node-Performance
   mehrere Stunden bis Tage dauern).
6. Für `mempool-watch`: `zmqpubsequence=…` in die `bitcoin.conf` des
   Nodes eintragen, Node neu starten, `getzmqnotifications` prüfen und
   `mempool_watch.zmq_endpoint` setzen (Kapitel 10.2).
7. Splunk-Inputs gemäß Kapitel 6.4 einrichten, sobald die ersten
   abgeschlossenen Parts unter `parser-data/export/` vorliegen.
8. Für einen gehärteten Dauerbetrieb: systemd-Units gemäß Kapitel 2.5
   einrichten statt dauerhaft auf `start.sh`/`stop.sh` zu setzen.
