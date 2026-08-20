# 2. Installation und Betrieb

[← Index](00-index.md)

## 2.1 Voraussetzungen

- Python 3.11+ (verwendet `from __future__ import annotations` sowie
  moderne Typannotationen wie `str | None`).
- `bitcoin-cli` muss im `PATH` liegen und den eigenen Node erreichen
  können – entweder lokal konfiguriert (`bitcoin.conf`/Cookie-Datei) oder
  über `rpc.extra_args` gegen einen entfernten Node (siehe Kapitel 3).
- Netzwerkzugriff auf `mempool.space` (HTTPS) für `api-poll`, sowie auf
  `raw.githubusercontent.com` für die Mining-Pool- und
  Stale-Blocks-Datensets. `import-price-history` selbst braucht keinen
  Netzwerkzugriff (siehe Kapitel 6.6).

## 2.2 Einrichtung

```sh
cd full_app
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Abhängigkeiten (`requirements.txt`): `polars` (CSV-/DataFrame-Verarbeitung),
`requests` (HTTP-Client), `PyYAML` (Konfigurationsdatei-Parsing).

> **Hinweis zu `rpc.output_dir`/`rpc.state_dir`:** Die Standardwerte
> (`parser-data/export/rpc` und `parser-data/state/rpc`) werden relativ zu
> `full_app/` aufgelöst. `rpc-ingest` baut seinen eigenen Zustand beim
> ersten Lauf gegen dieses Verzeichnispaar vollständig neu auf – sind
> beide leer, startet der Parser frisch ab Höhe 0, statt anzunehmen, dass
> bereits vorhandene Daten dort zum aktuellen Schema kompatibel sind.
> `output_dir` und `state_dir` sind bewusst getrennt (Splunk-Export vs.
> interne Buchführung) – siehe Kapitel 3 und 4.

## 2.3 Betrieb über start.sh / stop.sh

```sh
./start.sh   # prüft, ob der Node bereits erreichbar ist, startet dann alle drei Dienste im Hintergrund
./stop.sh    # stoppt sie (SIGTERM, nach 30s Gnadenfrist SIGKILL)
```

### start.sh im Detail

1. Prüft, ob der in `config.yaml` konfigurierte Node über RPC erreichbar
   ist (`getblockcount` über die App-eigene Konfiguration, respektiert
   also `rpc.extra_args`, `rpcuser_env`/`rpcpassword_env` und Cookie-Datei-
   Auth). **Startet niemals selbst einen `bitcoind`** – weder lokal noch
   anderswo. Ist RPC nicht erreichbar, bricht das Skript sofort mit einer
   klaren Fehlermeldung ab, statt über den Node-Lebenszyklus zu raten –
   den Node muss man selbst starten/reparieren und `start.sh` danach
   erneut ausführen.
2. Startet `rpc-ingest`, `stale-blocks-ingest` und `api-poll` losgelöst
   (`nohup`, nichts hängt am Terminal). Jeder Prozess wird per PID-Datei
   unter `.pids/<kommando>.pid` nachverfolgt.
3. Erneutes Ausführen von `start.sh` ist gefahrlos: Vor dem Start prüft
   `lib.sh`s `pid_matches_component()`, ob die in einer PID-Datei
   eingetragene PID tatsächlich noch zu genau diesem Kommando gehört
   (Kommandozeile enthält `run.py` **und** den Komponentennamen) – ein
   bereits laufender Prozess wird nicht angefasst, eine verwaiste/durch
   PID-Wiederverwendung ungültig gewordene PID-Datei wird verworfen und
   der Dienst neu gestartet.

Alle drei Kommandos loggen strukturiert nach
`logging.log_dir/<kommando>.log` (rotierend, 20 MB × 5 Dateien – Standard
`parser-data/logs`, siehe Kapitel 3). Zusätzlich schreibt `start.sh`
selbst das rohe stdout/stderr jedes losgelösten Prozesses fest nach
`full_app/logs/<kommando>.out` – dieser Pfad ist in `start.sh` fest
verdrahtet und folgt **nicht** `logging.log_dir`; bei der Standard-
`config.yaml` liegen `.log`- und `.out`-Dateien deshalb in zwei
unterschiedlichen Verzeichnissen (`parser-data/logs/` bzw. `logs/`).
Eine andere Konfigurationsdatei lässt sich mit
`BTC_PARSER_CONFIG=/pfad/andere.yaml ./start.sh` verwenden.

### stop.sh im Detail

Sendet `SIGTERM` an alle drei per PID-Datei bekannten Prozesse, wartet
bis zu 30 Sekunden auf ein sauberes Beenden und sendet danach `SIGKILL`,
falls ein Prozess noch läuft.

- `rpc-ingest` und `stale-blocks-ingest` beenden ihren aktuellen
  Batch/Durchlauf sauber und schreiben ihren Checkpoint, bevor sie sich
  beenden (siehe Kapitel 4 und 5 – ein installierter Signal-Handler setzt
  nur ein `threading.Event`, das die Verarbeitungsschleife zwischen
  Blöcken/Durchläufen prüft).
- `api-poll` stoppt genauso wie bei Strg+C: `cli.py` registriert für
  dieses Kommando einen `SIGTERM → KeyboardInterrupt`-Shim
  (`signal.signal(signal.SIGTERM, signal.default_int_handler)`), da
  `run_poller()` nur auf `KeyboardInterrupt` sauber reagiert.

Keines der Skripte fasst `bitcoind` an, weder beim Start noch beim Stopp –
das liegt bewusst außerhalb des Scopes; `bitcoin-cli stop` bleibt dem
Betreiber selbst überlassen.

### lib.sh

Enthält nur die von `start.sh`/`stop.sh` gemeinsam genutzte Hilfsfunktion
`pid_matches_component()` (siehe oben). Wird per `source` eingebunden,
nicht direkt ausgeführt.

## 2.4 Kommandos manuell ausführen

```sh
# Genesis-zu-Tip-Aufholjagd, danach fortlaufendes Tip-Following - der einzige
# RPC-Parser. Startet bei Höhe 0, falls noch nichts verarbeitet wurde,
# setzt sonst exakt dort fort, wo zuletzt aufgehört wurde. Läuft dauerhaft
# bis SIGTERM/SIGINT.
python run.py rpc-ingest

# Stale/Orphaned-Chain-Tip-Pipeline (getchaintips + das GitHub-Datenset
# bitcoin-data/stale-blocks) - eine eigene Datenquelle, unabhängig von
# rpc-ingests Hauptketten-Ausgabe. Läuft dauerhaft bis SIGTERM/SIGINT.
python run.py stale-blocks-ingest

# Dauerpoller für die mempool.space-Endpunkte (Gebühren, Mempool-Zustand,
# Preise im 60s-Takt, Difficulty-Adjustment, 24h-Pool-Hashrate-Anteil).
# Läuft bis Strg+C/SIGTERM oder einem 429.
python run.py api-poll

# config/pools-v2.json von GitHub aktualisieren (normalerweise automatisch
# durch rpc-ingest erledigt - siehe Kapitel 4)
python run.py update-pools-dataset

# Einmaliger (idempotenter) Bulk-Import zweier Kraken-1-Minuten-OHLC-CSVs
# (XBTUSD, XBTEUR) in dieselbe prices.csv, in die auch api-poll live
# schreibt - siehe Kapitel 6. Rein lokal (kein Netzwerkzugriff), beliebig
# vor oder nach dem ersten api-poll-Start ausführbar.
python run.py import-price-history
```

Jedes Kommando akzeptiert eine `--config path/zu/anderer-config.yaml`-
Option, um gegen eine andere Konfigurationsdatei zu laufen (z. B. für
einen zweiten Node oder eine Testkonfiguration). **`--config` muss vor
dem Subkommando stehen:**

```sh
python run.py --config config/config.production.yaml rpc-ingest   # richtig
python run.py rpc-ingest --config config/config.production.yaml   # Fehler: "unrecognized arguments"
```

Grund: `--config` ist ausschließlich als Top-Level-`argparse`-Option
definiert, nicht zusätzlich auf den Subparsern. `argparse`s
Subparser-Dispatch baut einen eigenen Sub-Namespace mit eigenen Defaults
und kopiert dessen Attribute anschließend bedingungslos in den äußeren
Namespace (`_SubParsersAction.__call__`) – ein gleichnamiges Argument auf
dem Subparser würde `--config` also stillschweigend wieder auf `None`
zurücksetzen, selbst wenn es korrekt vor dem Subkommando übergeben wurde.
Siehe den Kommentar auf `build_parser()` in `btc_parser_app/cli.py` für
die vollständige Begründung.

`run.py` ist ein reiner Komfort-Einstiegspunkt: Er hängt `full_app/` an
`sys.path` und ruft `btc_parser_app.cli.main()` auf – funktional identisch
zu `python -m btc_parser_app.cli`, ausgeführt aus `full_app/`.

## 2.5 Produktivbetrieb

Die Anwendung läuft produktiv per SSH auf einem dedizierten Storage-Host,
der sich über ein gemeinsames Datenvolume denselben Node wie der
Bitcoin-Core-Prozess teilt. Dort kommt
`config/config.production.yaml` statt der Standard-`config.yaml` zum
Einsatz:

```sh
BTC_PARSER_CONFIG=config/config.production.yaml ./start.sh
```

Diese Konfiguration zeigt `rpc.output_dir` / `mempool_api.output_dir` /
`pricing.output_dir` / `logging.log_dir` auf ein großes dediziertes
Datenvolume statt relativer Pfade, da das Home-Verzeichnis des Hosts
(wohin dieses Repository geklont wird) für Chain-Daten viel zu klein ist.
Die RPC-Verbindung geht direkt an den Bitcoin-Core-Prozess und
authentifiziert sich über die Cookie-Datei auf dem mit `bitcoind`
gemeinsam genutzten Volume statt über `rpcuser`/`rpcpassword`.

Die konkreten Verbindungsdetails (Netzwerkadresse, Ports, Mount-Pfade)
stehen ausschließlich im Kopfkommentar von
[`config/config.production.yaml`](../config/config.production.yaml)
selbst, da es sich um umgebungsspezifische Infrastrukturdetails handelt,
die sich ändern können (z. B. bei einer Neuzuweisung der Pod-IP) und
daher an einer einzigen Stelle gepflegt werden sollten statt dupliziert
in dieser Dokumentation.

### Empfehlung für einen gehärteten Dauerbetrieb

Für einen produktiven Linux-Host empfiehlt sich, die drei
`python run.py rpc-ingest` / `python run.py stale-blocks-ingest` /
`python run.py api-poll`-Kommandos statt über `start.sh`/`stop.sh` jeweils
in eine eigene systemd-Unit zu verpacken:

- eigenes Log pro Dienst (`journalctl -u <unit>` zusätzlich zu den
  Dateilogs unter `logs/`)
- eigene Restart-Policy, z. B. `Restart=on-failure` mit einem sinnvollen
  `RestartSec` – insbesondere relevant für `api-poll`, das bei einem
  HTTP-429 bewusst **nicht** automatisch neu startet (siehe Kapitel 9)
- sauberes Stop-Signal: `KillSignal=SIGTERM` (Standard) genügt, da alle
  drei Kommandos `SIGTERM` bereits sauber behandeln

`start.sh`/`stop.sh` decken lokale/Dev-Umgebungen und einfache
Dauerbetriebs-Hosts ab, starten einen abgestürzten Prozess aber nicht
automatisch neu – das ist der Hauptunterschied zu einer
`Restart=always`-systemd-Unit.

## 2.6 CLI-Kommandoreferenz

| Kommando | Läuft | Beendet sich | Exit-Code bei Erfolg | Beschreibung |
|---|---|---|---|---|
| `rpc-ingest` | dauerhaft | `SIGTERM`/`SIGINT` | 0 | Siehe Kapitel 4 |
| `stale-blocks-ingest` | dauerhaft | `SIGTERM`/`SIGINT` | 0 | Siehe Kapitel 5 |
| `api-poll` | dauerhaft | `SIGTERM`/`SIGINT`/HTTP 429 | 0 (sauberer Stop) / 1 (429) | Siehe Kapitel 6 |
| `update-pools-dataset` | einmalig | selbst | 0 / 1 (Fehler) | Siehe Kapitel 4 |
| `import-price-history` | einmalig | selbst | 0 / 1 (Fehler) | Siehe Kapitel 6 |

Ein Konfigurationsfehler (fehlender/ungültiger Wert in `config.yaml`,
fehlende Datei, kaputtes YAML) führt bei jedem Kommando zu Exit-Code 2 und
einer Fehlermeldung auf stderr, statt eines rohen Python-Tracebacks (siehe
`btc_parser_app/cli.py:main()`).
