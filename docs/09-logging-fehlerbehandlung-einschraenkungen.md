# 9. Logging, Fehlerbehandlung und bekannte Einschränkungen

[← Index](00-index.md)

## 9.1 Logging

Gesteuert über `logging.level` (Standard `INFO`) und `logging.log_dir`
(Standard `parser-data/logs`, relativ zu `full_app/`) in `config.yaml`
(`btc_parser_app/common/logging_setup.py::configure_logging()`).

Jedes Kommando loggt sowohl nach stdout als auch in eine rotierende Datei
unter `log_dir/<kommando>.log` (20 MB pro Datei, 5 aufgehoben). Zusätzlich
schreibt `start.sh` das rohe, unstrukturierte stdout/stderr jedes
losgelösten Prozesses fest nach `full_app/logs/<kommando>.out` – dieser
Pfad ist in `start.sh` selbst hart codiert und folgt **nicht**
`log_dir` (siehe Kapitel 2.3), sodass bei der Standard-`config.yaml`
`.log`- und `.out`-Dateien in zwei unterschiedlichen Verzeichnissen
landen.

Format: `%(asctime)s %(levelname)-7s %(name)s: %(message)s`, Zeitstempel
im Format `%Y-%m-%dT%H:%M:%S%z` (inklusive Zeitzonen-Offset des Hosts).

### Log-Level

Hochfrequente Einzelzeilen-Logs loggen bewusst auf `DEBUG`, nicht `INFO`:

- Die "wrote N row(s)"-Zeile des API-Pollers bei jedem einzelnen Abruf
  (`poller.py::fetch_and_write()`).
- Die "Parsing block N"-Zeile von `rpc-ingest` pro Block während einer
  Genesis-Aufholjagd (`ingest.py::_process_height()`).
- Der "skipping refresh"-Hinweis bei jedem No-op-Check des
  Mining-Pool-Datenset-Refreshs.

`logging.level: DEBUG` in `config.yaml` setzen, falls dieses
Detailniveau gebraucht wird. Das Standardlevel `INFO` liefert weiterhin
Start-Zusammenfassungen, Warnungen, Batch-Fortschrittszeilen und alles,
was tatsächlich Aufmerksamkeit braucht (ein 429, ein Reorg, ein
RPC-Ausfall).

## 9.2 Fehlerbehandlungs-Philosophie

Die Anwendung folgt durchgängig einem Prinzip: **transiente Fehler
werden mit begrenzten, konfigurierbaren Wiederholungen abgefedert;
Fehler, die auf ein grundsätzliches Problem hindeuten, führen zu einem
bewussten, sofortigen Stopp statt eines möglicherweise falschen
automatischen Entscheids.**

| Fehlerart | Verhalten |
|---|---|
| Transiente `bitcoin-cli`-Fehler (kurzer Node-Restart, Cookie-Rotation, Netzwerk-Blip) | Bis zu `rpc.max_cli_retries`-mal wiederholt, dann `RpcCliError` – `rpc-ingest` fängt dies ab, wartet `rpc.poll_interval_seconds` und versucht den ganzen Durchlauf erneut. Bereits Checkpointetes bleibt erhalten. |
| Transiente HTTP-Verbindungsfehler (Reset, SSL-EOF, DNS) | Bis zu `mempool_api.max_connection_retries`-mal wiederholt, dann `FetchError` – der betroffene Poll-Zyklus wird übersprungen, der Poller läuft weiter. |
| HTTP 429 (Rate Limit) von mempool.space | **Nie automatisch wiederholt.** Der gesamte `api-poll`-Prozess hält an (siehe 9.3). |
| Reorg-Rückwärtssuche findet keinen gemeinsamen Vorfahren innerhalb `rpc.max_reorg_depth`, oder trifft auf einen nie indizierten Block | `rpc-ingest` wirft eine Exception und stoppt – erfordert manuelles Eingreifen. |
| Fehlendes/kaputtes Mining-Pool-Signaturdatenset | Warnung geloggt, `rpc-ingest` läuft mit `pool_* = None` weiter. |
| Fehlgeschlagener Refresh des Mining-Pool-Datensets | Warnung geloggt, bestehende lokale Kopie bleibt in Verwendung. |
| Fehlerhafte/ungültige Konfiguration (`config.yaml`) | Klare Fehlermeldung auf stderr, Exit-Code 2 – kein roher Traceback. |
| Ungültige `mempool_endpoints`-Antwort (Parser wirft eine Exception) | Warnung geloggt, dieser Poll-Zyklus wird übersprungen; der Poller läuft weiter. |
| Header, der nicht zum behaupteten Hash passt (Stale-Blocks-Pipeline) | Verworfen (Status `unusable`), Warnung geloggt – kein Abbruch. |

## 9.3 Bekannte Einschränkungen / bewusste Design-Grenzen

Diese Punkte sind **keine offenen Bugs**, sondern bewusste
Design-Entscheidungen, die beim Betrieb der Anwendung berücksichtigt
werden sollten:

### Kein Auto-Restart nach einem 429

`api-poll` hält bei einem HTTP-429 von mempool.space bewusst komplett an
(Exit-Code 1) statt automatisch weiterzuversuchen – das erfordert
manuelles Eingreifen (Prozess neu starten, ggf. Poll-Intervalle
anpassen). Für den Dauerbetrieb empfiehlt sich eine systemd-Unit mit
`Restart=on-failure` und einem sinnvollen `RestartSec`, damit ein 429
nicht zu dauerhaftem Stillstand führt, ohne dass ein reines "sofort
wieder anfragen" die eigentliche Ursache (zu aggressive Rate)
verschlimmert (siehe Kapitel 2.5).

### Kein automatisches Storage-Cleanup

Siehe Kapitel 7.4 – bewusste Design-Entscheidung, kein offener Punkt.
Welcher rotierte CSV-Part gelöscht werden darf, entscheidet ausschließlich
die Splunk-Input-Konfiguration oder ein eigenes, verifiziertes
Cleanup-Skript, nie die Anwendung selbst.

### `import-price-history` benötigt zwei manuell besorgte Dateien

Es gibt keinen automatisierten Download der Kraken-Exports – beide CSVs
(XBTUSD, XBTEUR) müssen von Hand von Kraken heruntergeladen und unter
`pricing.xbtusd_csv_path`/`pricing.xbteur_csv_path` abgelegt werden.

### Keine automatische Lückenfüllung in `prices.csv` nach Downtime

Anders als in einer früheren Version dieser Anwendung gibt es aktuell
**keinen** Hintergrund-Mechanismus, der eine durch Absturz/Neustart/
Wartungsfenster entstandene Lücke in `prices.csv` automatisch nachfüllt –
der live `prices`-Endpunkt schreibt einfach ab dem nächsten erfolgreichen
Poll weiter, die dazwischenliegenden Minuten bleiben ohne manuelles
Eingreifen leer. Eine entstandene Lücke lässt sich nachträglich nur über
einen aktualisierten Kraken-Export und einen erneuten
`import-price-history`-Lauf schließen, falls Kraken die betroffenen
Minuten noch führt.

### `rpc.max_reorg_depth` als Sicherheitsgrenze

Standard 100 – eine Sicherheitsgrenze für die Rückwärtssuche nach dem
gemeinsamen Vorfahren bei einem Reorg. Ein echter Reorg sollte dies nie
annähernd erreichen; wird die Grenze doch erreicht (Bug oder korrupter
`index/`), bricht `rpc-ingest` ab und erfordert manuelles Eingreifen
statt eines möglicherweise falschen automatischen Entscheids.

### Kein mempool.space-seitiges Pool-Historie-Backfill

Die Anwendung führt bewusst kein Backfill der eigenen
Mining-Pool-Zuordnung über mempool.spaces `/api/v1/blocks*`-Endpunkte
durch – das würde redundant dieselbe Zuordnung erneut ableiten, die
`rpc/mining_pools.py` bereits lokal und offline produziert, und nur
unnötig Rate-Limit-Budget verbrauchen (siehe Kapitel 4.7).

### Stale-Blocks-Pipeline liefert nur Header, keine vollständigen Blöcke

Bewusste Design-Entscheidung, siehe Kapitel 5.4 – Bitcoin Cores
Checkpoint-Mechanismus würde ohnehin nur einen kleinen, inkonsistenten
Teil des Datensets als vollständigen Block erreichbar machen.

### `start.sh`/`stop.sh` starten keinen abgestürzten Prozess neu

Für harte Dauerbetriebs-Anforderungen sind systemd-Units mit
`Restart=always`/`Restart=on-failure` vorzuziehen (siehe Kapitel 2.5).

## 9.4 Betriebs-Checkliste für einen neuen Node

1. `bitcoin-cli` einrichten und Erreichbarkeit prüfen (lokal oder über
   `rpc.extra_args` gegen einen entfernten Node).
2. `full_app/config/config.yaml` (oder eine Kopie davon) auf die eigene
   Umgebung anpassen (siehe Kapitel 3).
3. Virtuelle Umgebung einrichten (Kapitel 2.2).
4. Optional, aber empfohlen: beide Kraken-1-Minuten-CSVs (XBTUSD, XBTEUR)
   besorgen und `python run.py import-price-history` einmalig ausführen
   (Kapitel 6.6), vor oder nach dem ersten `api-poll`-Start.
5. `./start.sh` ausführen.
6. Logs unter `parser-data/logs/` (strukturiert) und `full_app/logs/`
   (roh, `.out`) beobachten, insbesondere während der initialen
   Genesis-Aufholjagd von `rpc-ingest` (kann je nach Node-Performance
   mehrere Stunden bis Tage dauern).
7. Splunk-Inputs gemäß Kapitel 7.4 einrichten, sobald die ersten
   abgeschlossenen Parts unter `parser-data/export/` vorliegen.
8. Für einen gehärteten Dauerbetrieb: systemd-Units gemäß Kapitel 2.5
   einrichten statt dauerhaft auf `start.sh`/`stop.sh` zu setzen.
