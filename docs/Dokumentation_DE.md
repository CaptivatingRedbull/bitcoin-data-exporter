# btc_parser_app – Technische Dokumentation

> Ausführliche deutschsprachige Dokumentation für Confluence. Beschreibt
> Architektur, Konfiguration, Betrieb und die betrieblichen Entscheidungen
> hinter `full_app/`. Ergänzt die (englische, knappere) `README.md` im
> Repo-Root - bei Widersprüchen ist der tatsächliche Code maßgeblich, nicht
> dieses Dokument.

## Inhaltsverzeichnis

1. [Zweck und Überblick](#zweck-und-überblick)
2. [Architektur](#architektur)
3. [Verzeichnisstruktur](#verzeichnisstruktur)
4. [Installation](#installation)
5. [Betrieb: start.sh / stop.sh](#betrieb-startsh--stopsh)
6. [Produktivbetrieb](#produktivbetrieb)
7. [Konfiguration (config.yaml)](#konfiguration-configyaml)
8. [RPC-Parser: Reorg-Handling](#rpc-parser-reorg-handling)
9. [Mining-Pool-Zuordnung](#mining-pool-zuordnung)
10. [Pricing: minütliche Preishistorie](#pricing-minütliche-preishistorie)
11. [Storage-Strategie und Splunk-Anbindung](#storage-strategie-und-splunk-anbindung)
12. [Logging](#logging)
13. [Bekannte Einschränkungen / offene Punkte](#bekannte-einschränkungen--offene-punkte)

---

## Zweck und Überblick

`btc_parser_app` ist eine Datenpipeline, die Bitcoin-Blockchain- und
Mempool-Daten aus zwei unabhängigen Quellen abgreift, in flache CSV-Zeilen
umwandelt und für den Import in Splunk (oder ein beliebiges anderes
Tool, das CSV-Dateien einlesen kann) aufbereitet:

- **On-Chain-Daten** direkt vom eigenen Bitcoin-Core-Node über
  `bitcoin-cli` (RPC) - jeder Block und jede Transaktion, aggregiert auf
  Zeilenebene, inklusive Mining-Pool-Zuordnung.
- **Netzwerk-/Marktdaten** von der öffentlichen mempool.space-HTTP-API -
  aktuelle Gebühren, Mempool-Zustand, Preise, Difficulty-Adjustment,
  Pool-Hashrate-Anteile.
- **Historische Preisdaten** minütlich, aus zwei Kraken-1-Minuten-CSV-
  Exporten (USD und EUR) importiert in genau dieselbe `prices.csv`, die
  auch der Live-Poller schreibt (siehe Abschnitt 10).

Die Anwendung besteht bewusst aus **drei unabhängigen, dauerhaft
laufenden Prozessen** statt einem einzigen Multithreading-Skript. Sie
teilen sich weder Zustand noch Fehlerdomänen: Ein HTTP-429-Fehler auf der
API-Seite legt niemals die Blockverarbeitung lahm, und ein Ausfall des
Bitcoin-Nodes hat keinen Einfluss auf die Preis-Pipeline. Jeder Tunable-Wert
(Endpunkte, Poll-Intervalle, Rate-Limit-Budget, RPC-Verbindungsdetails,
Ausgabepfade) liegt in `config/config.yaml` - im Code ist nichts fest
verdrahtet.

## Architektur

| Prozess | Kommando | Aufgabe |
|---|---|---|
| RPC-Parser | `rpc-ingest` | Holt Blöcke vom eigenen Node, flacht sie inkl. aller Transaktionen zu CSV-Zeilen ab, ordnet jeden Block anhand blockeigener Daten einem Mining-Pool zu (keine zusätzlichen Netzwerkaufrufe nötig). Reorg-sicher (siehe Abschnitt 8). |
| Stale-Blocks-Pipeline | `stale-blocks-ingest` | Separate Sourcetype für nicht-aktive Chain-Tips (`getchaintips` + das GitHub-Datenset `bitcoin-data/stale-blocks`). Unabhängig von `rpc-ingest`s eigenem Reorg-Handling. |
| API-Poller | `api-poll` | Fragt die mempool.space-Endpunkte in konfigurierbaren Intervallen ab, innerhalb eines gemeinsamen Rate-Limit-Budgets (Token-Bucket) - inklusive eines minütlichen BTC/USD+EUR-Preis-Snapshots (siehe Abschnitt 10). |

Jeder Prozess läuft bis `SIGTERM`/`SIGINT` (bzw. bis `api-poll` einen
HTTP-429 erhält - dann wird bewusst **nicht** automatisch weiterversucht,
sondern der Prozess hält an und muss manuell neu gestartet werden, siehe
Abschnitt "Bekannte Einschränkungen"). Für einen gehärteten Linux-Betrieb
empfiehlt es sich, jeden der drei Befehle in eine eigene systemd-Unit mit
eigenem Log und eigener Restart-Policy (`Restart=always`) zu verpacken -
`start.sh`/`stop.sh` decken lokale/Dev-Umgebungen und einfache
Dauerbetriebs-Hosts ab, starten einen abgestürzten Prozess aber nicht
automatisch neu.

## Verzeichnisstruktur

```
full_app/
  start.sh                      Produktionsnaher Start: prüft bitcoind, startet alle drei Dienste im Hintergrund
  stop.sh                       Stoppt, was start.sh gestartet hat
  run.py                        CLI-Einstiegspunkt
  requirements.txt
  logs/                         Rotierende *.log-Dateien pro Kommando (werden beim ersten Lauf angelegt)
  config/
    config.yaml                 Alle Einstellungen für lokale/Dev-Nutzung
    config.production.yaml      Gleiches Schema, zeigt auf die Pfade des Produktions-Pods
    pools-v2.json                Mitgeliefertes Mining-Pool-Signaturdatenset
    XBTUSD_1.csv / XBTEUR_1.csv  (nicht mitgeliefert) hier die Kraken-1-Minuten-OHLC-Exporte ablegen
  btc_parser_app/
    config.py                    Lädt und validiert config.yaml in typisierte Dataclasses
    common/
      csv_writer.py               Gemeinsamer, größenrotierender Append-only-CSV-Writer
      logging_setup.py             Konsolen- + rotierendes Datei-Logging
    api/                          mempool.space-Seite ("api-poll")
      rate_limiter.py              Token-Bucket-Ratenbegrenzer
      client.py                    Ratenbegrenzter HTTP-GET-Client (Retries, 429-Handling)
      mempool_endpoints.py         JSON-zu-Zeilen-Parser je Endpunkt
      poller.py                    Threaded Interval-Poller
      mining_pools_dataset.py      Aktualisiert config/pools-v2.json von GitHub
      price_history_import.py      Einmaliger Bulk-Import zweier Kraken-Minuten-OHLC-CSVs in prices.csv ("import-price-history")
    rpc/                          bitcoin-cli-Seite ("rpc-ingest")
      client.py                    bitcoin-cli-Subprozess-Wrapper
      block_parser.py              Block-/Transaktions-JSON zu flachen CSV-Zeilen
      mining_pools.py              Mining-Pool-Extraktor (Tag- und Adressabgleich)
      reorg_state.py               index/, current.csv, latest.csv, block_status.csv, reorg/ - Statusdateien
      ingest.py                    Genesis-zu-Tip-Aufholjagd + fortlaufendes reorg-sicheres Tip-Following
      stale_blocks.py               Stale/Orphaned-Chain-Tip-Pipeline ("stale-blocks-ingest")
      stale_blocks_github.py        Zieht das bitcoin-data/stale-blocks-GitHub-Datenset
      stale_blocks_state.py         registry.csv - interne Buchführung für stale_blocks.py
    cli.py                        argparse-Einstiegspunkt, verdrahtet mit run.py
```

## Installation

```sh
cd full_app
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Voraussetzung: `bitcoin-cli` muss im `PATH` liegen und den eigenen Node
erreichen können (lokal konfigurierte `bitcoin.conf`/Cookie-Datei, oder
siehe `rpc.extra_args` für einen entfernten Node).

> **Hinweis:** `rpc.output_dir` (Standard `out_trimmed`) wird relativ zu
> `full_app/` aufgelöst. `rpc-ingest` baut seinen eigenen Zustand beim
> ersten Lauf gegen ein Zielverzeichnis vollständig neu auf - ist
> `out_trimmed` leer, startet der Parser frisch ab Höhe 0, statt
> anzunehmen, dass bereits vorhandene Daten dort zum aktuellen Schema
> kompatibel sind.

## Betrieb: start.sh / stop.sh

```sh
./start.sh   # prüft, ob bitcoind bereits erreichbar ist, startet dann alle drei Dienste im Hintergrund
./stop.sh    # stoppt sie (SIGTERM, nach 30s Gnadenfrist SIGKILL)
```

`start.sh` im Detail:

1. Prüft, ob der in `config.yaml` konfigurierte Node über RPC erreichbar
   ist. **Startet niemals selbst einen bitcoind** - weder lokal noch
   anderswo. Ist RPC nicht erreichbar, bricht das Skript sofort mit einer
   klaren Fehlermeldung ab, statt über den Node-Lebenszyklus zu raten - den
   Node muss man selbst starten/reparieren und das Skript danach erneut
   ausführen.
2. Startet `rpc-ingest`, `stale-blocks-ingest` und `api-poll` losgelöst
   (`nohup`, nichts hängt am Terminal), jeder Prozess wird per PID-Datei
   unter `.pids/` nachverfolgt. Erneutes Ausführen von `start.sh` ist
   gefahrlos - bereits laufende Prozesse werden nicht angefasst.

Alle drei Kommandos loggen sowohl nach `logs/<command>.log` (rotierend,
20 MB × 5 Dateien) als auch roh nach `logs/<command>.out`. Eine andere
Konfigurationsdatei lässt sich mit `BTC_PARSER_CONFIG=/pfad/andere.yaml
./start.sh` verwenden.

`stop.sh` sendet `SIGTERM` an alle drei PIDs - `rpc-ingest` und
`stale-blocks-ingest` beenden ihren aktuellen Batch/Durchlauf sauber und
schreiben ihren Checkpoint, bevor sie sich beenden; `api-poll` (über ein
`SIGTERM → KeyboardInterrupt`-Shim in `cli.py`) stoppt genauso wie bei
Strg+C. Keines der Skripte fasst bitcoind an, weder beim Start noch beim
Stopp - das liegt bewusst außerhalb des Scopes; `bitcoin-cli stop` bleibt
dem Betreiber selbst überlassen.

## Produktivbetrieb

Die Anwendung läuft produktiv per SSH auf einem dedizierten Storage-Pod,
der sich über ein gemeinsames Datenvolume denselben Node wie der
`bitcoin-core`-Pod teilt. Dort kommt
`config/config.production.yaml` statt der Standard-`config.yaml` zum
Einsatz:

```sh
BTC_PARSER_CONFIG=config/config.production.yaml ./start.sh
```

Diese Konfiguration zeigt `rpc.output_dir` / `mempool_api.output_dir` /
`logging.log_dir` auf ein großes dediziertes
Datenvolume statt relativer Pfade, da das Home-Verzeichnis des Pods (wohin
dieses Repository geklont wird) für Chain-Daten viel zu klein ist. Die
RPC-Verbindung geht direkt an den `bitcoin-core`-Pod und authentifiziert
sich über die Cookie-Datei auf dem mit bitcoind gemeinsam genutzten Volume
statt über `rpcuser`/`rpcpassword` - die genauen Verbindungsdetails und wie
die Pod-übergreifende RPC-Erreichbarkeit eingerichtet wurde, stehen im
Kopfkommentar dieser Datei selbst, da das Cluster-Netzwerkkonfiguration ist
und nicht etwas, das `config.yaml` allein steuert.

## Konfiguration (config.yaml)

### `mempool_api`

Der mempool.space-HTTP-Poller. `rate_limit.requests_per_minute` /
`rate_limit.bucket_size` definieren einen einzigen gemeinsamen Token-Bucket,
aus dem sich jede `endpoints`-Anfrage bedient - eine Erhöhung dieser Werte
erhöht also die effektive Rate gegenüber diesem Host insgesamt, nicht pro
Endpunkt. Da
mempool.space seine öffentlichen API-Limits nicht dokumentiert, ist der
Standardwert (10 Anfragen/Minute, Burst von 10) bewusst konservativ
gewählt.

`endpoints` ist eine Liste von `{name, path, parser, interval_seconds}`.
Jeder `parser`-Name muss auf eine `parse_<name>`-Funktion in
`btc_parser_app/api/mempool_endpoints.py` verweisen - ein neuer
mempool.space-Endpunkt wird durch Schreiben dieser Funktion und Ergänzen
eines Eintrags hier hinzugefügt, ohne weitere Code-Änderungen. Jeder
Endpunkt schreibt in seine eigene `<name>.csv` unter `output_dir`.

Anfragen werden ohne eigenen `User-Agent`-Header gesendet - es gibt kein
mempool.space-Abo in diesem Setup, gegen das man sich identifizieren
müsste.

### `mining_pools_dataset`

Quelle der Signaturen für die RPC-seitige Mining-Pool-Zuordnung - die
**einzige** Quelle für Pool-Zuordnung in dieser Anwendung; der Abgleich
läuft vollständig offline gegen RPC-Blockdaten, ohne Aufrufe pro Block
gegen mempool.space. `local_path` (Standard `config/pools-v2.json`)
enthält einen mitgelieferten Snapshot von
[mempool/mining-pools](https://github.com/mempool/mining-pools)
(MIT-lizenziert), sodass `rpc-ingest` offline funktioniert. Mit
`update-pools-dataset` (oder durch Einbindung von `refresh_if_stale` in
einen Scheduler) lassen sich neuere Pool-Signaturen nachziehen -
`refresh_interval_seconds` steht standardmäßig auf einer Woche, passend
zum eigenen Update-Rhythmus des Upstream-Projekts. Ein fehlgeschlagener
Refresh loggt eine Warnung und behält die vorhandene lokale Kopie, statt
den Import abzubrechen.

### `pricing`

Preishistorie durchgehend minütlich, alles in einer Datei:
`mempool_api.output_dir/prices.csv`. Der Live-Endpunkt `prices` (siehe
`mempool_api.endpoints` oben) fragt mempool.space alle 60s ab und hängt
eine Zeile `date_unix,usd,eur` an (`parse_prices` in
`btc_parser_app/api/mempool_endpoints.py`) - `date_unix` ist der eigene
Zeitstempel des Preises, nicht der Abrufzeitpunkt. `python run.py
import-price-history` füllt alles vor diesem live gepollten Zeitfenster aus
zwei Kraken-1-Minuten-OHLC/Candle-CSV-Exporten (Krakens historischer
Datendownload, keine Kopfzeile:
`unix_timestamp,open,high,low,close,volume,trades` - jeweils die
"_1"-Intervall-Datei pro Währungspaar), nach Minuten-Zeitstempel
zusammengeführt und im exakt gleichen `date_unix,usd,eur`-Schema
geschrieben (`btc_parser_app/api/price_history_import.py`):

- **`pricing.xbtusd_csv_path`** - Pfad zum XBTUSD-"_1"-Export; dessen
  Schlusskurs wird `usd` in jeder Zeile.
- **`pricing.xbteur_csv_path`** - Pfad zum XBTEUR-"_1"-Export; dessen
  Schlusskurs wird `eur` in jeder Zeile.

Beide Währungen werden anhand des Minuten-Zeitstempels zusammengeführt -
existiert eine Minute nur in einer der beiden Dateien, entsteht trotzdem
eine Zeile, mit der jeweils anderen Währung als `null` (genau wie beim
Live-Endpunkt, der gelegentlich eine Währung auslässt). Bereits
importierte Minuten werden anhand von `date_unix` übersprungen - daher
ist `import-price-history` beliebig oft wiederholbar (vor oder nach dem
Start von `api-poll`, in beliebiger Reihenfolge) und kollidiert nie mit
dem, was der Live-Poller gerade schreibt.

### `rpc`

`bitcoin_cli_path` + `extra_args` werden an jeden `bitcoin-cli`-Aufruf
angehängt - für einen entfernten/Kubernetes-gehosteten Node zum Beispiel:

```yaml
extra_args: ["-rpcconnect=192.168.x.x", "-rpcport=8332"]
```

`-rpcuser`/`-rpcpassword` gehören **niemals** direkt in `extra_args` (oder
sonst irgendwo in diese Datei) - sie ist bewusst so gestaltet, dass sie
gefahrlos eingecheckt werden kann. Stattdessen `rpcuser_env`/
`rpcpassword_env` auf die *Namen* von Umgebungsvariablen setzen; diese
werden nur zur Laufzeit gelesen und nur dann als
`-rpcuser=`/`-rpcpassword=`-Flags angehängt, wenn beide tatsächlich gesetzt
sind. Ein per CLI-Flag übergebenes Credential ist für jeden mit `ps`-Zugriff
auf demselben Host sichtbar - wo das relevant ist, ist Cookie-Datei-Auth
(die Voreinstellung, wenn nichts gesetzt wird) vorzuziehen.

`batch_size` (Standard 20) steuert, wie viele Blöcke sich zwischen den
`current.csv`-Checkpoint-Logzeilen während einer Backlog-Aufholjagd
ansammeln - **jeder** Durchlauf der Ingest-Schleife (siehe Abschnitt 8)
schreibt am Ende unbedingt alles Verarbeitete auf die Platte, unabhängig
davon, ob ein voller Batch erreicht wurde. Beim Tip-Following (wo ein
Durchlauf oft nur den einen neuen Block umfasst) wird also **nicht** auf
20 Blöcke gewartet, bevor Daten persistiert werden - `batch_size` steuert
nur, wie oft die "Progress: height X/Y"-Logzeile und der Zwischen-Checkpoint
während einer langen Aufholjagd feuern. `output_dir` (Standard
`out_trimmed`) ist der Ort für alle RPC-seitigen Dateien: `blocks.csv`,
`transactions.csv` und sämtliche Reorg-Statusdateien.

### `stale_blocks`

Konfiguration für `stale-blocks-ingest`: `output_dir` (Splunk-seitige
Exporte, Standard `out_stale_blocks`) und `state_dir` (nur interne
Buchführung, Standard `state/stale_blocks`) sind bewusst getrennt.
`node_poll_interval_seconds` (Standard stündlich) steuert den
`getchaintips`-Durchlauf; `github.poll_interval_seconds` (Standard
täglich) steuert den Abruf des `bitcoin-data/stale-blocks`-GitHub-CSV -
siehe Abschnitt 8 dazu, wie diese Pipeline mit `rpc-ingest`s eigenem
Reorg-Handling zusammenhängt (beide sind unabhängig: diese hier verfolgt
nicht-aktive Tips, `rpc-ingest` die aktive Chain).

### Dateirotation

Jede Append-only-CSV dieser Anwendung (`blocks.csv`, `transactions.csv`,
`index/index.csv`, `peer_attempts.csv`, die Stale-Blocks-Exporte,
jede mempool.space-Endpunkt-CSV inklusive `prices.csv`) wächst unbegrenzt,
daher deckelt `common/csv_writer.py` jeden Dateipart auf ~900 MB und rollt
vor Überschreiten in einen neuen nummerierten Part: `blocks.csv` ist der
erste Part, danach `blocks.000002.csv`, `blocks.000003.csv` usw., im
selben Verzeichnis. Am Layout ändert sich sonst nichts - es bleibt
logisch eine CSV, nur aufgeteilt in Dateien, die nie 1 GB überschreiten.
Alles in der Anwendung, das eine logische CSV vollständig zurücklesen
muss (der Index, `peer_attempts.csv`, das Bootstrapping aus `blocks.csv`),
liest automatisch alle Parts in Reihenfolge. Wer ein externes Tool
(Splunk, eine Monitor-Stanza, Ad-hoc-Skripte) direkt auf diese Dateien
ansetzt, sollte auf `<name>*.csv` globben statt auf den exakten ersten
Dateinamen - siehe Abschnitt 11 zum Zusammenspiel mit einem begrenzten
Speicherbudget.

`reorg_confirmations` (Standard 6), `max_reorg_depth` (Standard 100) und
`poll_interval_seconds` (Standard 30 - Wartezeit zwischen Tip-Checks im
eingeholten Zustand; wird ignoriert, solange noch Backlog aufzuholen ist)
justieren alle `rpc-ingest` - siehe Abschnitt 8.

## RPC-Parser: Reorg-Handling

`rpc-ingest` (`btc_parser_app/rpc/ingest.py`) verwendet durchgängig den
Blockhash als eindeutige Identität. Jeder Durchlauf:

1. Liest den Node-Tip und setzt `latest = tip - rpc.reorg_confirmations`
   (Standard 6), geschrieben nach `latest.csv`.
2. Liest `current.csv` (den zuletzt erfolgreich verarbeiteten kanonischen
   Block).
3. Vergleicht den gespeicherten Hash mit dem Hash der aktiven Chain auf
   derselben Höhe.
   - **Übereinstimmung** → normale Schleife: verarbeitet
     `current_height + 1 .. latest`, exportiert jede Höhe, deren exakter
     Blockhash noch nicht in `index/` steht (bereits indizierte Höhen
     werden übersprungen, nicht erneut exportiert).
   - **Abweichung** → ein Reorg hat bereits verarbeitete Daten erreicht.
     Die Anwendung läuft über die in `index/` gespeicherten
     `previousblockhash`-Werte rückwärts, bis eine Höhe gefunden wird, an
     der der gespeicherte Chain-Zustand wieder mit der aktiven Chain
     übereinstimmt (der gemeinsame Vorfahre, "common ancestor"); markiert
     jeden abgehängten Block als `canonical=false` in `block_status.csv`,
     schreibt ein Audit-CSV nach `reorg/` und setzt die Verarbeitung ab
     `ancestor + 1` fort.
4. Schreibt `current.csv` bei jedem Batch-Checkpoint und **unbedingt** am
   Ende jedes Durchlaufs - ein Durchlauf mit nur 1-2 neuen Blöcken
   (stationäres Tip-Following) bekommt trotzdem einen vollständigen,
   persistenten Flush; es wird nicht gewartet, bis sich `batch_size`
   Blöcke angesammelt haben (siehe die Anmerkung zu `rpc.batch_size`
   oben).

Statusdateien (alle unter `rpc.output_dir`, neben `blocks.csv` /
`transactions.csv`):

- `index/index.csv` - unveränderliches, Append-only-Protokoll jedes
  jemals exportierten Blocks (`height,blockhash,previousblockhash`).
  Beantwortet "wurde dieser exakte Blockhash jemals exportiert" - eine
  Höhe kann mehrere Zeilen haben, wenn sie jemals von einem Reorg
  betroffen war.
- `current.csv` / `latest.csv` - einzeilige `height,blockhash`-Zeiger.
- `block_status.csv` - veränderliche `height,blockhash,canonical`-Zeilen.
  Nur Blöcke, die jemals von einem Reorg abgehängt wurden, bekommen hier
  eine Zeile; alles nie Reorgte hat keine Zeile und gilt implizit als
  kanonisch.
- `reorg/reorg_<timestamp>_<lowest>_<highest>.csv` - ein Audit-CSV pro
  Reorg-Ereignis (`action,height,blockhash`, `action` ist `detached` oder
  `attached`). Nur zur Fehlersuche/Auditierung - wird nie zurückgelesen,
  um den aktuellen Zustand zu bestimmen.

Da `index/`/`blocks.csv`/`transactions.csv` für einen Block, der zwischen
`canonical → noncanonical → canonical` wechselt, nie neu geschrieben
werden, sehen nachgelagerte Konsumenten nie einen doppelten Export dafür -
es ändert sich nur das `canonical`-Flag in `block_status.csv`. Findet die
Rückwärtssuche innerhalb von `rpc.max_reorg_depth` Blöcken (Standard 100)
keinen gemeinsamen Vorfahren, oder trifft sie auf einen nie indizierten
Block, bricht `rpc-ingest` mit einer Exception ab, statt zu raten - das
erfordert absichtlich manuelles Eingreifen, nach demselben Prinzip wie das
Anhalten von `api-poll` bei einem 429.

### Startzustand: Genesis oder da weitermachen, wo aufgehört wurde

`rpc-ingest` verhält sich unabhängig davon, wie viele Blöcke (falls
überhaupt) bereits verarbeitet sind - es gibt keinen separaten
"Backfill"-Modus:

- **Noch nichts verarbeitet** (kein `current.csv`, kein `blocks.csv`):
  startet bei Höhe 0 und holt bis zum Tip auf.
- **`current.csv` vorhanden**: macht genau dort weiter - zuvor wie oben
  auf Reorg geprüft.
- **`current.csv` fehlt, `blocks.csv` hat aber Daten** (z. B. gelöscht,
  oder `output_dir` wurde anderweitig befüllt und hat nie ein
  `current.csv` bekommen): liest die eigenen
  `height`/`hash`/`previousblockhash`-Spalten aus `blocks.csv`, um
  `index/` neu aufzubauen, und macht ab dem höchsten gefundenen Block
  weiter, statt diese Arbeit zu verlieren und ab Genesis neu zu
  beginnen.

## Mining-Pool-Zuordnung

`btc_parser_app/rpc/mining_pools.py` ordnet jedem per RPC geholten Block
seinen Mining-Pool zu - ganz ohne zusätzliche RPC-Aufrufe oder
Netzwerkanfragen. Die Anwendung führt bewusst **kein** Backfill des
eigenen Pool-Verlaufs von mempool.space über dessen `/api/v1/blocks*`
-Endpunkte durch, da das redundant dieselbe Zuordnung erneut ableiten
würde, die dieses Modul bereits lokal produziert - das würde nur unnötig
Rate-Limit-Budget verbrauchen. Bitcoin Cores `getblock`-Antwort enthält
kein Pool-Identitätsfeld; Pools identifizieren sich freiwillig in der
Coinbase-Transaktion auf eine von zwei Arten, und `PoolMatcher.match()`
prüft beide, in dieser Priorität:

1. **Coinbase-Tag** - Pools stempeln eine kurze ASCII-Signatur in das
   Coinbase-scriptSig, z. B. `/ViaBTC/`, `/AntPool/`,
   `/Foundry USA Pool #dropgold/`. `getblock verbosity=3` liefert dies als
   `vin[0].coinbase` (Hex); es wird nach ASCII dekodiert und als Teilstring
   gegen die bekannten Tags jedes Pools geprüft. Das ist bewusste
   Selbstidentifikation und nach einem Treffer praktisch eindeutig.
2. **Auszahlungsadresse** - findet sich kein Tag, werden die
   Ausgabeadressen der Coinbase-Transaktion gegen die bekannten
   Auszahlungsadressen jedes Pools geprüft. Schwächeres Signal (eine
   Adresse kann zwischen nicht verwandten Zahlern geteilt werden, z. B.
   über einen Custodian) - daher nur Fallback, kein primäres Signal.

Trifft nichts zu, erhält der Block `pool_id/pool_name/pool_link = None`
und `pool_match_method = "unknown"`, statt einer falschen Vermutung.

Diese Felder landen direkt auf der Block-Zeile von `rpc-ingest`
(`blocks.csv`): `pool_id`, `pool_name`, `pool_link`, `pool_match_method`.
Das Signaturdatenset liegt unter `mining_pools_dataset.local_path` (siehe
oben) und verwendet exakt das Schema von mempool.spaces eigener
`pools-v2.json` - es lässt sich also 1:1 gegen einen anderen Snapshot
austauschen.

Fehlt die Datensetdatei oder lässt sie sich nicht parsen, loggt
`rpc-ingest` eine Warnung und läuft mit jedem Pool-Feld auf `None` weiter,
statt den Import abzubrechen.

## Ausgabeschema-Hinweise

- `blocks.csv` lässt `confirmations` und `nextblockhash` bewusst weg:
  beides sind Momentaufnahmen des aktuellen Chain-Zustands, die in einer
  historischen CSV veraltet/irreführend werden. `previousblockhash` bleibt
  dagegen erhalten - es ist dem Block inhärent, während `nextblockhash`
  von der aktuell gewählten Chain abhängt und nach einem Reorg veraltet.
- Einzelne vin/vout-Zeilen pro Transaktion werden nie exportiert - nur
  aggregierte/skalare Felder (Script-Typ-Zählungen, Gebührenstatistiken,
  Witness-Byte-Zählungen usw.) landen in `transactions.csv`, um die
  Zeilengröße für nachgelagerte Systeme begrenzt zu halten (ursprünglich
  auf Splunks ~10k-Zeichen-Event-Limit zugeschnitten).
- `transactions.csv` lässt jedes Feld weg, das reine Dopplung eines bereits
  exportierten oder anderweitig aufgelösten Felds ist: `has_witness`,
  `has_taproot_input`, `has_taproot_output`, `has_op_return` sowie
  `prevout_values_complete`/`prevout_heights_complete` sind alle
  einzeilig aus bereits vorhandenen Zähl-Feldern ableitbare Booleans (z. B.
  `witness_input_count > 0`); die rohen `coinbase_script_sig_hex` und
  `coinbase_output_addresses_json` entfallen, sobald sie die
  Mining-Pool-Zuordnung gespeist haben - `pool_id`/`pool_name`/`pool_link`
  auf der Block-Zeile tragen dieses Ergebnis bereits weiter.
- `transactions.csv`s `wtxid` bleibt leer, wenn es gleich `txid` ist (jede
  Nicht-Witness-Transaktion), statt den 64-Zeichen-Hash zu wiederholen -
  nachgelagert mit `coalesce(wtxid, txid)` rekonstruieren. Nur
  Witness-Transaktionen, bei denen sich beide tatsächlich unterscheiden,
  kosten diese Spalte.
- `mempool.csv` (Endpunkt `mempool`) enthält bewusst **nicht** das rohe
  `fee_histogram`, das mempool.space zurückgibt - ein JSON-Blob in einer
  einzelnen CSV-Zelle bringt in Splunk keinen Mehrwert. `tx_count`,
  `vsize_total` und `total_fee_sats` decken das relevante skalare Signal
  ab.

## Pricing: minütliche Preishistorie

Siehe Abschnitt "Konfiguration → `pricing`" oben für die Details. Kurz
zusammengefasst der Workflow für einen neuen Node:

1. Zwei Kraken-1-Minuten-OHLC-Exporte (XBTUSD und XBTEUR) besorgen und
   unter `pricing.xbtusd_csv_path`/`pricing.xbteur_csv_path` ablegen.
2. Einmalig `python run.py import-price-history` ausführen - füllt
   `prices.csv` mit der kompletten historischen Zeitreihe (Zeilenschema
   `date_unix,usd,eur`, identisch zum Live-Poller).
3. `api-poll` starten (oder laufen lassen) - der `prices`-Endpunkt hängt ab
   sofort minütlich weitere Zeilen im selben Schema an dieselbe Datei an.

Da beide Schritte dasselbe Zeilenschema in dieselbe Datei schreiben und
`import-price-history` bereits vorhandene Minuten überspringt, spielt die
Reihenfolge keine Rolle - der Import kann jederzeit erneut gegen eine
aktualisierte/erweiterte Exportdatei laufen, ohne Duplikate zu erzeugen
oder mit dem Live-Poller zu kollidieren.

## Storage-Strategie und Splunk-Anbindung

### Ausgangslage

Der Parser-Host hat ein festes, begrenztes Speicherbudget (in der
Zielumgebung z. B. 1 TB), das er sich mit dem eigentlichen
Bitcoin-Core-Datenverzeichnis teilt:

| Posten | Größe (Richtwert) | Bemerkung |
|---|---|---|
| Bitcoin Core Datadir | ~720 GB | wächst kontinuierlich mit der Chain |
| Betriebssystem u. Ä. | ~20 GB | |
| Verbleibend für Exporte/Puffer | Rest | begrenzt und **nicht** für dauerhafte Aufbewahrung der Exportdateien gedacht |

Die exportierten CSV-Dateien (`blocks.csv`, `transactions.csv`,
die mempool.space-Endpunkt-CSVs (inklusive `prices.csv`), die Stale-Blocks-
Exporte) sind also nicht als dauerhafter Datenspeicher auf diesem Host
gedacht - Splunk (oder was auch immer sie konsumiert) muss sie zeitnah
tatsächlich abholen.

### Rolle dieser Anwendung

Die Anwendung bleibt bei diesem Thema bewusst passiv: Sie rotiert jede
logische CSV in ~900-MB-Teile (siehe "Dateirotation" oben) und fasst eine
einmal geschriebene Datei danach nie mehr an, verschiebt oder löscht sie
nie. **Welcher rotierte Teil wann gelöscht werden darf, ist eine
Entscheidung, die außerhalb dieser Anwendung getroffen wird** - über die
Splunk-Input-Konfiguration (`inputs.conf`) oder ein eigenes,
manuell/per Cron ausgeführtes Aufräum-Skript, nachdem verifiziert wurde,
dass Splunk die Datei tatsächlich indiziert hat. Es gibt in der Anwendung
absichtlich **keinen** automatischen Lösch-/Verschiebe-Mechanismus - ein
Konfigurationsfehler bei einer Selbstlöschung wäre eine Fehlkonfiguration
von Splunk entfernt davon, unwiederbringliche Daten zu verlieren (ein
erneuter Parse-Lauf ab Genesis ist teuer; ein erneuter Abruf der
historischen Preis-/API-Daten unter Umständen gar nicht mehr möglich).

### Zwei Splunk-Input-Modi, die die Rotation unterstützt

Die 900-MB-Rotation ist bewusst so gewählt, dass sich zwischen beiden
Splunk-Eingabemodi wechseln lässt, ohne etwas an dieser Anwendung zu
ändern:

- **Bulk-Aufholjagd (Genesis-Backfill oder eine große Lücke):** Ein
  `batch`-Input (Sinkhole) auf `rpc.output_dir` zeigen lassen - dieser
  liest eine abgeschlossene Datei einmal ein und löscht/verschiebt sie
  danach ("destruktiv"). Das ist hier sicher, weil ein rotierter Teil
  (`blocks.000004.csv` usw.) nie wieder angehängt wird, sobald der nächste
  Teil begonnen hat - er ist in dem Moment, in dem er nicht mehr der
  neueste ist, eine abgeschlossene, vollständige Datei. Genau dafür ist
  die 900-MB-Deckelung gewählt (deutlich unter üblichen
  Forwarder-Größenlimits) - jeder Teil ist eine Einheit, die ein
  `batch`-Input als Ganzes konsumieren kann.
- **Stationäres Tip-Following:** Ist einmal aufgeholt, trudeln neue Daten
  deutlich langsamer ein, wodurch dieselbe Rotation deutlich seltenere,
  kleinere Teile erzeugt. Hier lässt sich entweder weiterhin `batch`
  verwenden (weiterhin sicher, aus demselben Grund wie oben - nur eine
  niedrigere Rate kleiner, abgeschlossener Dateien), oder der aktuell noch
  wachsende Teil lässt sich auf einen `monitor`-Input (nicht-destruktives
  Tailing) umstellen, falls Splunk auf diesem Host gar nichts automatisch
  löschen soll; `monitor`-Inputs löschen nie, alte rotierte Teile müssten
  dann manuell (oder per eigenem Cron-Job) aufgeräumt werden, sobald die
  Indizierung durch Splunk bestätigt ist. Die Anwendung selbst trifft hier
  keine Annahme in die eine oder andere Richtung.

### Empfehlung

Für den produktiven 1-TB-Host: `batch`-Input während der initialen
Genesis-Aufholjagd (großes Datenvolumen, viele abgeschlossene Teile, wo
"destruktiv nach Indizierung" den größten Speicher-Druck nimmt), danach
je nach Splunk-Betriebsmodell entweder bei `batch` bleiben oder auf
`monitor` + eigenes, verifiziertes Cleanup umstellen. Diese Entscheidung
sollte zusammen mit dem Splunk-Betrieb getroffen und in der jeweiligen
`inputs.conf` dokumentiert werden - nicht in dieser Anwendung.

## Logging

`logging.level` (Standard `INFO`) und `logging.log_dir` (Standard `logs`,
relativ zu `full_app/`) steuern das Logging. Jedes Kommando loggt sowohl
nach stdout als auch in eine rotierende Datei unter
`log_dir/<kommando>.log` (20 MB pro Datei, 5 aufgehoben) - darauf stützt
sich die Sichtbarkeit der Hintergrunddienste von `start.sh`, da deren
losgelöstes stdout nur ein rohes `*.out`-Redirect ist.

Hochfrequente Einzelzeilen-Logs (die "wrote N row(s)"-Zeile des
API-Pollers bei jedem einzelnen Abruf, die "Parsing block N"-Zeile von
`rpc-ingest` pro Block während einer Genesis-Aufholjagd) loggen auf
`DEBUG`, nicht auf `INFO` - `logging.level: DEBUG` in `config.yaml`
setzen, falls dieses Detailniveau gebraucht wird. Das Standardlevel
`INFO` liefert weiterhin Start-Zusammenfassungen, Warnungen,
Batch-Fortschrittszeilen und alles, was tatsächlich Aufmerksamkeit
braucht (ein 429, ein Reorg, ein RPC-Ausfall).

## Bekannte Einschränkungen / offene Punkte

- **Kein Auto-Restart nach einem 429.** `api-poll` hält bei einem
  HTTP-429 von mempool.space bewusst komplett an (Exit-Code 1) statt
  automatisch weiterzuversuchen - das erfordert manuelles Eingreifen
  (Prozess neu starten, ggf. Poll-Intervalle anpassen). Für den
  Dauerbetrieb empfiehlt sich eine systemd-Unit mit `Restart=on-failure`
  und einem sinnvollen `RestartSec`, damit ein 429 nicht zu dauerhaftem
  Stillstand führt, ohne dass ein reines "sofort wieder anfragen" die
  eigentliche Ursache (zu aggressive Rate) verschlimmert.
- **Kein automatisches Storage-Cleanup**, siehe Abschnitt 11 - bewusste
  Design-Entscheidung, keine offene Aufgabe.
- **`import-price-history` benötigt manuell besorgte Dateien.** Es gibt
  keinen automatisierten Download der Kraken-Exporte - beide CSVs müssen
  von Hand von Kraken heruntergeladen und unter
  `pricing.xbtusd_csv_path`/`pricing.xbteur_csv_path` abgelegt werden.
- **`rpc.max_reorg_depth`** (Standard 100) ist eine Sicherheitsgrenze für
  die Rückwärtssuche nach dem gemeinsamen Vorfahren bei einem Reorg. Ein
  echter Reorg sollte dies nie annähernd erreichen; wird die Grenze doch
  erreicht (Bug oder korrupter `index/`), bricht `rpc-ingest` ab und
  erfordert manuelles Eingreifen statt eines möglicherweise falschen
  automatischen Entscheids.
