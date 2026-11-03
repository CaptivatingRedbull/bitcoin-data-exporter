# 2. Installation und Betrieb

[← Index](00-index.md)

## 2.1 Voraussetzungen

- Python 3.11+ (verwendet `from __future__ import annotations` sowie
  moderne Typannotationen wie `str | None`).
- `bitcoin-cli` muss im `PATH` liegen und den eigenen Node erreichen
  können – entweder lokal konfiguriert (`bitcoin.conf`/Cookie-Datei) oder
  über `rpc.extra_args` gegen einen entfernten Node (siehe Kapitel 3).
- Für `mempool-watch`: Bitcoin Core **25+** (liefert in
  `getrawtransaction <txid> 2` die Adresse jedes ausgegebenen Outputs mit;
  ältere Versionen funktionieren über einen langsameren Fallback) und
  `zmqpubsequence=…` in der `bitcoin.conf` des Nodes, erreichbar vom
  Parser-Host aus (siehe Kapitel 10.2). Ohne ZMQ läuft `mempool-watch` im
  reinen Polling-Modus.
- Netzwerkzugriff auf `raw.githubusercontent.com` (HTTPS) für die
  Mining-Pool- und Stale-Blocks-Datensets (siehe Kapitel 4.8 und 5).

## 2.2 Einrichtung

```sh
cd <repository>
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Abhängigkeiten (`requirements.txt`): `polars` (CSV-/DataFrame-Verarbeitung),
`requests` (HTTP-Client für die GitHub-Datensets), `PyYAML` (Konfigurationsdatei-Parsing), `pyzmq`
(ZMQ-Abonnement für `mempool-watch`).

> **Bestehende Installation aktualisieren:** Nach einem `git pull`, der
> `requirements.txt` ändert, die venv mit
> `.venv/bin/pip install -r requirements.txt` nachziehen –
> `systemd/install.sh` bricht sonst mit einem entsprechenden Hinweis ab.

> **Hinweis zu `rpc.output_dir`/`rpc.state_dir`:** Die Standardwerte
> (`parser-data/export/rpc` und `parser-data/state/rpc`) werden relativ zum
> Repository-Root aufgelöst. `rpc-ingest` baut seinen eigenen Zustand beim
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
2. Startet `rpc-ingest`, `stale-blocks-ingest` und `mempool-watch`
   losgelöst (`nohup`, nichts hängt am Terminal). Jeder Prozess wird per PID-Datei
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
selbst das rohe stdout/stderr jedes losgelösten Prozesses nach
`logging.log_dir/<kommando>.out` – also direkt neben die jeweilige
`.log`-Datei (`start.sh` liest `logging.log_dir` dafür aus der aktiven
Konfiguration).
Eine andere Konfigurationsdatei lässt sich mit
`BTC_PARSER_CONFIG=/pfad/andere.yaml ./start.sh` verwenden.

### stop.sh im Detail

Sendet `SIGTERM` an alle drei per PID-Datei bekannten Prozesse, wartet
bis zu 30 Sekunden auf ein sauberes Beenden und sendet danach `SIGKILL`,
falls ein Prozess noch läuft.

`rpc-ingest`, `stale-blocks-ingest` und `mempool-watch` beenden ihren
aktuellen Batch/Durchlauf sauber und schreiben ihren Checkpoint, bevor sie
sich beenden (siehe Kapitel 4 und 5 – ein installierter Signal-Handler,
`common/stop_signal.py`, setzt nur ein `threading.Event`, das die
Verarbeitungsschleife zwischen Blöcken/Durchläufen prüft).

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

# Mempool-Überwachung auf Sanktionsadressen (ZMQ sequence + periodischer
# getrawmempool-Abgleich) - siehe Kapitel 10. Läuft dauerhaft bis
# SIGTERM/SIGINT.
python run.py mempool-watch

# Eine einzelne Transaktion gegen die Sanktionsliste prüfen, ohne etwas zu
# schreiben (bestätigte Tx ohne -txindex: --blockhash angeben).
python run.py mempool-watch-check <txid> [--blockhash <hash>]

# config/pools-v2.json von GitHub aktualisieren (normalerweise automatisch
# durch rpc-ingest erledigt - siehe Kapitel 4)
python run.py update-pools-dataset
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

`run.py` ist ein reiner Komfort-Einstiegspunkt: Er hängt den
Repository-Root an `sys.path` und ruft `btc_parser_app.cli.main()` auf –
funktional identisch zu `python -m btc_parser_app.cli`, ausgeführt aus dem
Repository-Root.

## 2.5 Produktivbetrieb

Die Anwendung läuft produktiv per SSH auf einem dedizierten Storage-Host,
der sich über ein gemeinsames Datenvolume denselben Node wie der
Bitcoin-Core-Prozess teilt. Dort kommt
`config/config.production.yaml` statt der Standard-`config.yaml` zum
Einsatz:

```sh
BTC_PARSER_CONFIG=config/config.production.yaml ./start.sh
```

Diese Konfiguration zeigt alle `output_dir`/`state_dir`-Pfade und
`logging.log_dir` auf ein großes dediziertes
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

### Gehärteter Dauerbetrieb: systemd-Units

Für einen produktiven Linux-Host (getestet auf SUSE Linux Enterprise
Server 15) liegen unter [`systemd/`](../systemd/) fertige Unit-Templates
für die drei Dauerlauf-Kommandos plus ein `btc-parser.target`, das alle
drei gruppiert. Installiert werden sie mit:

```sh
sudo systemd/install.sh                          # config/config.yaml
sudo systemd/install.sh --config config/config.production.yaml
```

Das Skript ermittelt `APP_DIR` selbst (das Verzeichnis, in dem es liegt),
prüft, dass die venv existiert und alle Pakete aus `requirements.txt`
enthält, verweigert die Installation, falls
`start.sh` dieselben Prozesse gerade schon per PID-Datei trackt (sonst
liefen zwei Kopien gegeneinander auf denselben Output-/State-Dateien),
schreibt die drei `.service`-Dateien plus das Target nach
`/etc/systemd/system/`, aktiviert sie (`systemctl enable`) und startet sie.
Deinstallation: `sudo systemd/install.sh --uninstall`.

Auf einem Host, auf dem die Units schon installiert sind, genügt nach
einem `git pull` + `pip install -r requirements.txt` ein erneutes
`sudo systemd/install.sh` (mit derselben `--config`-Angabe bzw. ohne,
dann bleibt `systemd/env` erhalten): Es schreibt die Units neu, aktiviert
sie und startet jede noch nicht installierte Unit einzeln – bereits
laufende Dienste bleiben unberührt.

Jede installierte Unit bringt mit:

- eigenes Log pro Dienst (`journalctl -u btc-parser-<name>.service`
  zusätzlich zu den Dateilogs unter `logging.log_dir`)
- `Restart=on-failure` mit `RestartSec=30` – ein abgestürzter Prozess
  kommt automatisch wieder hoch, ohne bei einem harten Dauerfehler
  endlos im Sekundentakt neu zu starten (`StartLimitBurst=5` je
  `StartLimitIntervalSec=600`)
- sauberes Stop-Signal: `KillSignal=SIGTERM` (Standard) genügt, da alle
  drei Kommandos `SIGTERM` bereits sauber behandeln
- `WantedBy=btc-parser.target` (`btc-parser.target` selbst ist
  `WantedBy=multi-user.target`) – nach einem Reboot (z. B. dem
  wöchentlichen Wartungs-Reboot dieses Hosts) starten alle drei Dienste
  automatisch neu, unabhängig davon, wie/warum sie zuvor gestoppt wurden

`start.sh`/`stop.sh` bleiben für lokale/Dev-Umgebungen und schnelle
manuelle Checks nutzbar, sollten aber nicht **gleichzeitig** mit den
systemd-Units gegen dieselbe Konfiguration laufen (s. o., PID-Check in
`install.sh`).

## 2.6 CLI-Kommandoreferenz

| Kommando | Läuft | Beendet sich | Exit-Code bei Erfolg | Beschreibung |
|---|---|---|---|---|
| `rpc-ingest` | dauerhaft | `SIGTERM`/`SIGINT` | 0 | Siehe Kapitel 4 |
| `stale-blocks-ingest` | dauerhaft | `SIGTERM`/`SIGINT` | 0 | Siehe Kapitel 5 |
| `mempool-watch` | dauerhaft | `SIGTERM`/`SIGINT` | 0 | Siehe Kapitel 10 |
| `mempool-watch-check TXID [--blockhash H]` | einmalig | selbst | 0 / 1 (Tx nicht abrufbar) | Siehe Kapitel 10.8 |
| `update-pools-dataset` | einmalig | selbst | 0 / 1 (Fehler) | Siehe Kapitel 4 |

Ein Konfigurationsfehler (fehlender/ungültiger Wert in `config.yaml`,
fehlende Datei, kaputtes YAML) führt bei jedem der obigen Kommandos zu
Exit-Code 2 und einer Fehlermeldung auf stderr, statt eines rohen
Python-Tracebacks (siehe `btc_parser_app/cli.py:main()`).
