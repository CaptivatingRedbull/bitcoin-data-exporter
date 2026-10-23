# 4. RPC-Parser und Mining-Pool-Zuordnung

[← Index](00-index.md)

Kommando: `rpc-ingest` · Einstiegspunkt: `btc_parser_app/rpc/ingest.py::run_rpc_ingest()`

## 4.1 Beteiligte Module

| Modul | Aufgabe |
|---|---|
| `rpc/ingest.py` | Hauptschleife: Katch-up + Tip-Following + Reorg-Erkennung/-Behebung. |
| `rpc/client.py` | RPC-Client mit Retry/Timeout: `bitcoin-cli`-Subprozess oder direktes JSON-RPC über HTTP (`rpc.transport`). |
| `rpc/catchup.py` | `parse_block()` (getblock → Zeilen) und der Prozess-Pool, der Blöcke während der Aufholjagd vorab holt und parst (`rpc.catchup_workers`) – siehe 4.14. |
| `rpc/block_parser.py` | Wandelt ein `getblock verbosity=3`-JSON in flache Block-/Transaktions-/Input-/Output-Zeilen um. |
| `rpc/mining_pools.py` | Mining-Pool-Zuordnung (Tag- und Adressabgleich). |
| `rpc/reorg_state.py` | Zustandsdateien: `index/`, `current.csv`, `latest.csv`, `block_status.csv`, `reorg/`. |
| `rpc/part_writer.py` | Verwaltet die Part-Nummerierung sowie die Übergabe von `state_dir` nach `output_dir` für `blocks/`/`transactions/`/`inputs/`/`outputs/` – siehe 4.5. |
| `api/mining_pools_dataset.py` | Lädt/aktualisiert das Signaturdatenset (`config/pools-v2.json`), das `mining_pools.py` verwendet. |

## 4.2 Es gibt genau eine Implementierung – kein separater Backfill-Modus

`run_rpc_ingest()` verhält sich unabhängig davon, wie viele Blöcke (falls
überhaupt) bereits verarbeitet sind:

- **Noch nichts verarbeitet** (kein `current.csv`, keine `blocks/blocks.csv`
  irgendwo unter `state_dir`/`output_dir`): startet bei Höhe 0 und holt bis
  zum Tip auf.
- **`current.csv` vorhanden**: macht genau dort weiter – zuvor auf Reorg
  geprüft (siehe 4.4).
- **`current.csv` fehlt, `blocks/blocks.csv`-Daten existieren aber** (unter
  `state_dir` und/oder `output_dir` – z. B. gelöscht, oder die Verzeichnisse
  wurden anderweitig befüllt und haben nie ein `current.csv` bekommen):
  `seed_index_from_blocks_csv()` liest die noch vorhandenen
  `height`/`hash`/`previousblockhash`-Spalten aus beiden Verzeichnissen, um
  `index/` neu aufzubauen, und macht ab dem höchsten gefundenen Block
  weiter, statt diese Arbeit zu verlieren und ab Genesis neu zu beginnen.
  Best-effort: `output_dir` ist Splunk-seitig, ältere Parts können also
  bereits gelöscht sein – rekonstruiert wird nur, was tatsächlich noch auf
  der Platte liegt.

Solange Backlog aufzuholen ist, verarbeitet die Schleife Durchläufe
zurück an zurück ohne Wartezeit; erst wenn sie `latest` erreicht hat,
fällt sie auf das Polling-Intervall `rpc.poll_interval_seconds` zurück.
Der Prozess läuft dauerhaft bis `SIGTERM`/`SIGINT` und checkpointet
`current.csv` nach jedem Durchlauf, sodass ein Abbruch/Absturz höchstens
den Fortschritt eines laufenden Batches kostet (der beim nächsten Start
sicher erneut geparst wird).

## 4.3 Ablauf pro Durchlauf (`_run_one_pass`)

1. Node-Tip lesen (`getblockcount`) und
   `latest = tip - rpc.reorg_confirmations` (Standard 6) setzen,
   geschrieben nach `latest.csv`.
2. `current.csv` lesen (den zuletzt erfolgreich verarbeiteten kanonischen
   Block) – oder, falls nicht vorhanden, aus `blocks/blocks.csv`
   rekonstruieren (siehe 4.2), oder als letzten Ausweg bei Genesis starten.
3. Den gespeicherten Hash mit dem Hash der aktiven Chain auf derselben
   Höhe vergleichen:
   - **Übereinstimmung** → normale Schleife: verarbeitet
     `current_height + 1 .. latest`, exportiert jede Höhe, deren exakter
     Blockhash noch nicht in `index/` steht (bereits indizierte Höhen
     werden übersprungen, nicht erneut exportiert).
   - **Abweichung** → ein Reorg hat bereits verarbeitete Daten erreicht,
     siehe 4.4.
   Mit `rpc.catchup_workers > 1` werden dabei alle Höhen, die mindestens
   `rpc.max_reorg_depth` unter dem Tip liegen, von einem Prozess-Pool
   vorab geholt und geparst – die Schleife selbst übernimmt sie trotzdem
   eine Höhe nach der anderen, mit exakt denselben Schritten 4–6
   (siehe 4.14).
4. Für jede verarbeitete Höhe: `block_status.set_canonical_if_present()`
   aufrufen (flippt eine Höhe, die zuvor durch einen Reorg
   non-canonical markiert wurde, zurück auf canonical, sobald sie erneut
   auf der aktiven Chain bestätigt wird).
5. **Schreibmodus je nach Distanz zum Tip** (siehe 4.5 für die Mechanik):
   Ist der verbleibende Backlog dieses Durchlaufs (`latest - current_height`)
   höchstens `rpc.reorg_confirmations` groß, gilt der Durchlauf als
   eingeholt – jeder Block wird sofort einzeln als eigener,
   bereits vollständiger Part direkt nach `output_dir` geschrieben
   (**atomarer Modus**), inklusive Checkpoint nach jedem einzelnen Block.
   Andernfalls (echter Backlog): Zeilen sammeln sich im Speicher und werden
   alle `rpc.batch_size` verarbeiteten Blöcke in den aktuell wachsenden
   Part **unter `state_dir`** geschrieben (**Batch-Modus**) – `index`
   flushen, `block_status` flushen (in genau dieser Reihenfolge – siehe
   4.6), `current.csv` aktualisieren, Fortschritts-Logzeile schreiben.
6. Am Ende des Durchlaufs: **unbedingt** flushen (im jeweils aktiven Modus)
   und `current.csv` schreiben, unabhängig davon, ob ein voller Batch
   erreicht wurde – ein Durchlauf mit nur 1–2 neuen Blöcken (stationäres
   Tip-Following) bekommt trotzdem einen vollständigen, persistenten
   Flush.

## 4.4 Reorg-Erkennung und -Behebung (`_recover_from_reorg`)

`rpc-ingest` verwendet durchgängig den **Blockhash als eindeutige
Identität**. Wird beim Vergleich in Schritt 3 oben eine Abweichung
festgestellt, läuft die Anwendung über die in `index/` gespeicherten
`previousblockhash`-Werte rückwärts, bis eine Höhe gefunden wird, an der
der gespeicherte Chain-Zustand wieder mit der aktiven Chain übereinstimmt
(der gemeinsame Vorfahre, "common ancestor"):

1. Jeder besuchte (Höhe, Hash) auf dem alten Pfad wird als `detached`
   vorgemerkt.
2. Bricht die Suche `rpc.max_reorg_depth` (Standard 100) Blöcke lang ohne
   Fund ab, oder trifft sie auf einen nie indizierten Block, wirft
   `rpc-ingest` eine Exception und stoppt – das erfordert absichtlich
   manuelles Eingreifen, statt zu raten (dieselbe Philosophie wie das
   Anhalten von `api-poll` bei einem 429, siehe Kapitel 9).
3. Ist der gemeinsame Vorfahre gefunden: jeder detached-Block wird in
   `block_status.csv` als `canonical=false` markiert, ein Audit-CSV nach
   `reorg/` geschrieben (Detached- **und** Attached-Bereich, siehe 4.5),
   und die Verarbeitung setzt ab `ancestor_height + 1` fort.

Da `index/` und die `blocks/`/`transactions/`-Parts für einen Block, der
zwischen `canonical → noncanonical → canonical` wechselt, nie neu
geschrieben werden, sehen nachgelagerte Konsumenten nie einen doppelten
Export dafür – es ändert sich nur das `canonical`-Flag in
`block_status.csv`.

## 4.5 Zustandsdateien und die state_dir/output_dir-Aufteilung

`rpc.output_dir` und `rpc.state_dir` sind zwei getrennte Wurzel-
verzeichnisse (siehe Kapitel 3):

- **`output_dir`** (Standard `parser-data/export/rpc`) – Splunk-seitig.
  Enthält ausschließlich die Unterordner `blocks/`, `transactions/`,
  `inputs/` und `outputs/`, und darin **nur vollständige, abgeschlossene**
  Parts. Nichts anderes wird hier jemals geschrieben.
- **`state_dir`** (Standard `parser-data/state/rpc`) – rein intern, nie
  als Splunk-Input verwenden. Enthält:

| Datei/Verzeichnis | Charakter | Spalten | Zweck |
|---|---|---|---|
| `index/index.csv` | unveränderlich, Append-only | `height, blockhash, previousblockhash` | Protokoll jedes jemals exportierten Blocks. Beantwortet "wurde dieser exakte Blockhash jemals exportiert" – eine Höhe kann mehrere Zeilen haben, wenn sie jemals von einem Reorg betroffen war. Rotiert wie jede andere Append-only-CSV (Kapitel 7). |
| `current.csv` | veränderlich, 1 Zeile | `height, blockhash` | Zeiger auf den zuletzt erfolgreich verarbeiteten kanonischen Block. Atomar geschrieben (Temp-Datei + `os.replace`). |
| `latest.csv` | veränderlich, 1 Zeile | `height, blockhash` | Zeiger auf `tip - reorg_confirmations`. Atomar geschrieben. |
| `block_status.csv` | veränderlich | `height, blockhash, canonical` | Nur Blöcke, die jemals von einem Reorg abgehängt wurden, bekommen hier eine Zeile; alles nie Reorgte hat keine Zeile und gilt implizit als kanonisch. Atomar geschrieben. |
| `reorg/reorg_<timestamp>_<lowest>_<highest>.csv` | unveränderlich, ein File pro Ereignis | `action, height, blockhash` | Audit-Trail (`action` ist `detached` oder `attached`). Nur zur Fehlersuche – wird von der Anwendung nie zurückgelesen, um den aktuellen Zustand zu bestimmen. |
| `blocks_part_seq.csv` / `transactions_part_seq.csv` / `inputs_part_seq.csv` / `outputs_part_seq.csv` | veränderlich, 1 Zeile | `current_part, part_is_open` | Durable Part-Nummern-Zähler je logischer CSV (siehe unten). |
| `blocks/`, `transactions/`, `inputs/`, `outputs/` (solange Backlog aufzuholen ist) | wachsender Part | – | Der aktuell noch offene, nicht abgeschlossene Part im Batch-Modus (siehe 4.3/4.6) – erst nach Rotation/Wechsel in den atomaren Modus wandert er vollständig nach `output_dir`. |

`index/index.csv` verwendet dieselbe Rotation wie jede andere
Append-only-CSV (Kapitel 7); `IndexStore` liest beim Start automatisch
alle Parts.

**Zwei-Phasen-Commit-Prinzip:** `IndexStore.add()` staged eine Zeile nur
im Speicher – sie wird erst mit `flush()` persistiert und für
`contains()`/`get()` sichtbar. Der Aufrufer darf `flush()` erst
aufrufen, nachdem die zugehörigen `blocks/`/`transactions/`-Zeilen
bereits durabel sind, damit `index.csv` niemals behauptet, ein Block sei
exportiert worden, bevor er es tatsächlich wurde. Ebenso wird
`block_status` **vor** dem Aktualisieren von `current.csv` geflusht: Da
`set_canonical_if_present()` beim erneuten Ausführen ein sicherer No-op
ist, kann ein Absturz zwischen beiden Schreibvorgängen `current.csv`
niemals auf einen Stand zeigen lassen, dessen zugehöriger
`canonical=true`-Flip nie auf die Platte kam – der Worst Case ist
stattdessen eine harmlose erneute Prüfung beim nächsten Start.

## 4.6 Part-Verwaltung und atomarer Schreibmodus (`rpc/part_writer.py`)

`blocks/blocks*.csv`, `transactions/transactions*.csv`,
`inputs/inputs*.csv` und `outputs/outputs*.csv` sind ein Sonderfall
gegenüber jeder anderen CSV dieser Anwendung, weil sie zusätzlich zur
normalen Größenrotation (Kapitel 7) zwischen zwei Schreibmodi wechseln und
über zwei Verzeichnisse verteilt sind. Das übernimmt `PartSequencer` in
`rpc/part_writer.py` – eine Instanz pro logischer CSV, alle vier
gleichzeitig gedraint (siehe `ingest.py::_drain()`), damit ein Block seine
vier zugehörigen Zeilensätze (Block, Transaktionen, Inputs, Outputs) immer
im selben Schreibmodus und zum selben Zeitpunkt materialisiert:

- **Batch-Modus** (`write_batched()`, solange Backlog über
  `rpc.reorg_confirmations` hinaus besteht): Zeilen sammeln sich im
  Speicher und werden an den aktuellen Part **unter `state_dir`**
  angehängt – rotiert bei ~900 MB genauso wie jede andere CSV. In dem
  Moment, in dem ein Part nicht mehr der aktuell beschriebene ist
  (Rotation durch Größe, oder Wechsel in den atomaren Modus), wird er
  **vollständig** (ein atomarer `os.replace()` auf demselben Dateisystem)
  von `state_dir` nach `output_dir` verschoben. Vor diesem Zeitpunkt
  erscheint er unter `output_dir` überhaupt nicht.
- **Atomarer Modus** (`write_atomic()`, sobald der Backlog auf
  `rpc.reorg_confirmations` oder weniger geschrumpft ist – die Anwendung
  gilt dann als eingeholt): Jeder Block bekommt seinen **eigenen,
  bereits vollständigen Part**, direkt unter `output_dir` geschrieben
  (Temp-Datei + atomares Rename, `common/atomic_write.py`) – `state_dir`
  wird dabei überhaupt nicht berührt.

In beiden Modi gilt: **Ein Part erscheint unter `output_dir` immer erst,
wenn er vollständig fertig ist – nie während er noch beschrieben wird.**
Das ist genau das, was einen Splunk-`batch`-(Sinkhole-)Input, der
`output_dir` konsumiert-und-löscht, dauerhaft sicher macht (siehe
Kapitel 7) – unabhängig davon, ob gerade eine Genesis-Aufholjagd läuft
oder der Prozess längst dem Tip folgt.

Die Part-Nummerierung ist **ein gemeinsamer, dauerhaft persistierter
Zähler** je logischer CSV (`blocks_part_seq.csv`/
`transactions_part_seq.csv`/`inputs_part_seq.csv`/`outputs_part_seq.csv`
unter `state_dir`), nicht durch Scannen des
Verzeichnisses ermittelt. Das ist notwendig, nicht nur bequem: Im
atomaren Modus landet jeder Part sofort bei Splunk, sodass zum Zeitpunkt,
an dem die Anwendung die nächste Nummer vergeben muss, nicht garantiert
ist, dass ältere Parts noch auf der Platte liegen (Splunk könnte sie
bereits gelöscht haben). Der Zähler merkt sich zusätzlich, ob der
aktuelle Part noch offen (Batch-Modus, unter `state_dir`) oder bereits
abgeschlossen/übergeben ist – fällt die Anwendung nach einer Downtime von
atomarem Modus zurück in den Batch-Modus (weil erneut Backlog entstanden
ist), beginnt sie einen frischen Part unter `state_dir`, statt zu
versuchen, an eine bereits nach `output_dir` verschobene (und
möglicherweise dort schon gelöschte) Datei anzuhängen.

## 4.7 RPC-Aufrufe (`rpc/client.py`)

Zwei Transportwege, gewählt über `rpc.transport` (Kapitel 3.5):

- **`cli`** (Standard): ein Subprozess-Wrapper um `bitcoin-cli`, damit
  transparent dieselbe Auth-Methode greift, die der Betreiber auch
  interaktiv nutzt (Cookie-Datei, `bitcoin.conf`, lokal konfigurierter
  `bitcoin-cli`-Alias). `rpc.extra_args` und `RpcConfig.auth_args()`
  erweitern dies für entfernte Nodes.
- **`http`**: direktes JSON-RPC über eine offen gehaltene HTTP-Verbindung
  je Prozess und Thread. Endpunkt/Cookie-Datei werden aus `extra_args`
  abgeleitet (oder `rpc.http_url`/`rpc.cookie_file`). Spart pro Block zwei
  Prozessstarts und den Umweg über `bitcoin-cli`s eigenes JSON-Parsen und
  -Pretty-Printen – bei großen `getblock … 3`-Antworten der größte
  Einzelposten (siehe 4.14).

Beide liefern dieselben Werte: das JSON-Ergebnis von bitcoind, Beträge
auf Wunsch als `Decimal` (beide Wege tragen denselben UniValue-Zahlentext).

Jeder Aufruf läuft durch dieselbe Retry-Logik (`run_cli()` bzw. ihr
HTTP-Gegenstück): Bei einem fehlgeschlagenen Aufruf
(Exit-Code ≠ 0, Timeout, kurzzeitig unerreichbare Binary) wird bis zu
`rpc.max_cli_retries`-mal wiederholt, mit `rpc.cli_retry_backoff_seconds`
Wartezeit dazwischen. Nach Ausschöpfen aller Versuche wird `RpcCliError`
geworfen – `run_rpc_ingest()` fängt diese Exception ab und wartet
`rpc.poll_interval_seconds`, bevor der gesamte Durchlauf erneut versucht
wird, statt den Daemon abstürzen zu lassen. Bereits auf `current.csv`
checkpointeter Fortschritt bleibt davon unberührt.

Verwendete RPC-Aufrufe:

| Funktion | RPC-Kommando | Zweck |
|---|---|---|
| `get_block_count` | `getblockcount` | Aktueller Node-Tip. |
| `get_block_hash` | `getblockhash <height>` | Hash zu einer Höhe. |
| `get_block` | `getblock <hash> 3` | Voller Block inkl. aller Transaktionen (verbosity=3), geparst mit `Decimal` für alle Beträge. |
| `get_block_header` | `getblockheader <hash>` | Nur Header-Felder (ohne die txid-Liste, die `getblock <hash> 1` mitliefern würde) – für `time`/`previousblockhash` in der Reorg-Logik. |
| `get_block_header_raw` | `getblockheader <hash> false` | Rohe 80-Byte-Header-Bytes (hex), für die Stale-Blocks-Pipeline (Kapitel 5). |
| `get_chain_tips` | `getchaintips` | Alle bekannten Chain-Tips, für die Stale-Blocks-Pipeline. |

## 4.8 Mining-Pool-Zuordnung (`rpc/mining_pools.py`)

Ordnet jedem per RPC geholten Block seinen Mining-Pool zu – **ganz ohne
zusätzliche RPC-Aufrufe oder Netzwerkanfragen**. Die Anwendung führt
bewusst **kein** Backfill des eigenen Pool-Verlaufs von mempool.space über
dessen `/api/v1/blocks*`-Endpunkte durch, da das redundant dieselbe
Zuordnung erneut ableiten würde, die dieses Modul bereits lokal
produziert – das würde nur unnötig Rate-Limit-Budget verbrauchen.

Bitcoin Cores `getblock`-Antwort enthält kein Pool-Identitätsfeld; Pools
identifizieren sich freiwillig in der Coinbase-Transaktion auf eine von
zwei Arten, und `PoolMatcher.match()` prüft beide, in dieser Priorität:

1. **Coinbase-Tag** – Pools stempeln eine kurze ASCII-Signatur in das
   Coinbase-`scriptSig`, z. B. `/ViaBTC/`, `/AntPool/`,
   `/Foundry USA Pool #dropgold/`. `getblock verbosity=3` liefert dies
   als `vin[0].coinbase` (Hex); es wird nach ASCII/Latin-1 dekodiert
   (Dekodierfehler werden bewusst ignoriert – die Coinbase ist kein
   Textfeld, sondern beliebige Bytes) und als Teilstring gegen die
   bekannten Tags jedes Pools geprüft. Das ist bewusste
   Selbstidentifikation und nach einem Treffer praktisch eindeutig.
2. **Auszahlungsadresse** – findet sich kein Tag, werden die
   Ausgabeadressen der Coinbase-Transaktion gegen die bekannten
   Auszahlungsadressen jedes Pools geprüft. Schwächeres Signal (eine
   Adresse kann zwischen nicht verwandten Zahlern geteilt werden, z. B.
   über einen Custodian) – daher nur Fallback, kein primäres Signal.

Trifft nichts zu, erhält der Block `pool_id/pool_name/pool_link = None`
und `pool_match_method = "unknown"`, statt einer falschen Vermutung.

Diese vier Felder landen direkt auf der Block-Zeile
(`blocks.csv`): `pool_id`, `pool_name`, `pool_link`, `pool_match_method`
(Werte: `"tag"`, `"address"` oder `"unknown"`).

**Wichtig:** Der rohe Coinbase-`scriptSig`-Hex-String und die
Auszahlungsadressen, die der Matcher liest, werden von
`aggregate_transaction()` nur noch als interner Rückgabewert
(Seitenkanal) an `aggregate_block()` weitergereicht – sie erscheinen
selbst **nicht** in `transactions.csv` (siehe 4.10). Sobald `pool_id`/
`pool_name`/`pool_link` auf der Blockzeile stehen, gibt es für die rohen
Felder keinen weiteren nachgelagerten Nutzen mehr.

Das Signaturdatenset liegt unter `mining_pools_dataset.local_path`
(Standard `config/pools-v2.json`) und verwendet exakt das Schema von
mempool.spaces eigener `pools-v2.json`:
`[{"id", "name", "link", "tags": [...], "addresses": [...]}, ...]` – es
lässt sich also 1:1 gegen einen anderen Snapshot austauschen.

Fehlt die Datensetdatei oder lässt sie sich nicht parsen, loggt
`rpc-ingest` eine Warnung und läuft mit jedem Pool-Feld auf `None` weiter,
statt den Import abzubrechen.

### Aktualisierung des Signaturdatensets

`rpc-ingest` ruft zu Beginn jedes Durchlaufs
`refresh_if_stale(config.mining_pools_dataset)` auf (`api/mining_pools_dataset.py`)
– ein billiger No-op, solange die lokale Datei jünger als
`refresh_interval_seconds` (Standard eine Woche) ist. Löst dies einen
tatsächlichen Refresh aus, wird der `PoolMatcher` neu geladen, damit ein
lang laufender `rpc-ingest`-Prozess nicht dauerhaft mit veralteten
Signaturen weiterläuft. Ein fehlgeschlagener Refresh (Netzwerkfehler,
ungültige Antwort) loggt eine Warnung und behält die vorhandene lokale
Kopie – der Import wird dadurch nie unterbrochen.

Manuell erzwingen: `python run.py update-pools-dataset`.

> **Spaltenreihenfolge ist Vertrag (4.9–4.12):** Die Splunk-/Cribl-
> Feldkonfiguration adressiert die Spalten von `transactions.csv`/
> `inputs.csv`/`outputs.csv` nach Position (Kapitel 10.6). Wer eine Spalte
> in `block_parser.py` hinzufügt, entfernt oder verschiebt, muss
> `btc_parser_app/export_schema.py` mitziehen und
> `python splunk/build_field_config.py` ausführen.
> `tests/test_export_schema.py` schlägt sonst fehl.

## 4.9 Ausgabeschema: `blocks.csv`

Eine Zeile pro Block. Erzeugt von `block_parser.py::aggregate_block()`.
Liegt unter `output_dir/blocks/` (siehe 4.5/4.6).

**Bewusst weggelassen:** `confirmations` und `nextblockhash` – beides
sind Momentaufnahmen des aktuellen Chain-Zustands, die in einer
historischen CSV veraltet/irreführend werden (`confirmations` wächst mit
jedem neuen Block, `nextblockhash` hängt von der aktuell gewählten Chain
ab und wird nach einem Reorg falsch). `previousblockhash` bleibt dagegen
erhalten – es ist dem Block inhärent.

| Feld | Beschreibung |
|---|---|
| `hash`, `height`, `version`, `versionHex`, `merkleroot`, `time`, `mediantime`, `nonce`, `bits`, `target`, `chainwork`, `nTx`, `previousblockhash`, `strippedsize`, `size`, `weight` | Native Blockfelder, unverändert von `getblock` übernommen. |
| `time_since_prev_block_sec` | Zeitdifferenz zum unmittelbaren Vorgängerblock in Sekunden (vom Aufrufer beim Verketten der Durchläufe berechnet). |
| `difficulty` | Als `float` konvertiert. |
| `chainwork_log2` | `log2(int(chainwork, 16))` – log-skaliertes Chainwork für einfachere Visualisierung. |
| `weight_utilization_pct` | `weight / rpc.max_block_weight * 100`. |
| `block_subsidy_sats` | Deterministisch aus der Höhe berechnet (`block_subsidy_sats()`, spiegelt Bitcoin Cores `GetBlockSubsidy`) – hängt **nicht** von RPC-gemeldeten Gebühren ab. |
| `pool_id`, `pool_name`, `pool_link`, `pool_match_method` | Mining-Pool-Zuordnung, siehe 4.8. |
| `regular_tx_count`, `total_vin_count`, `total_vout_count` | Struktur-Aggregate. |
| `coinbase_value_sats`, `coinbase_vout_count` | Aus der Coinbase-Transaktion. |
| `total_fees_sats`, `fee_known_tx_count`, `fee_unknown_tx_count`, `fee_avg_sats`, `fee_median_sats`, `fee_max_sats` | Gebühren-Aggregate über alle Nicht-Coinbase-Transaktionen mit bekannter Gebühr. |
| `fee_rate_avg_sat_vb`, `fee_rate_median_sat_vb`, `fee_rate_max_sat_vb`, `effective_fee_rate_sat_vb` | Gebührenrate in sat/vByte; `effective_fee_rate_sat_vb` nur gesetzt, wenn für **jede** reguläre Transaktion eine Gebühr bekannt ist. |
| `regular_input_value_sats`, `input_value_complete_tx_count`, `regular_output_value_sats` | Werteflüsse – Summen über Transaktions-Ein-/Ausgaben, **nicht** "neu geschaffenes BTC". `input_value_complete_tx_count` wird über den Vergleich `prevout_value_known_count == vin_count` der jeweiligen Transaktion ermittelt. |
| `coin_days_destroyed_btc` | Summe über alle Transaktionen (siehe 4.10). |
| `tx_vsize_avg`, `tx_vsize_median`, `tx_vsize_max` | Transaktionsgrößen-Verteilung. |
| `segwit_tx_count`, `taproot_input_tx_count`, `taproot_output_tx_count`, `op_return_tx_count` | Feature-Adoption pro Block, jeweils aus den entsprechenden Zähl-Feldern der Transaktionen abgeleitet (z. B. `segwit_tx_count` = Anzahl Transaktionen mit `witness_input_count > 0`). |
| `rbf_tx_count`, `op_return_output_count`, `op_return_script_bytes` | Weitere Feature-Adoption. |
| `witness_input_count`, `witness_item_count`, `witness_data_bytes`, `scriptsig_bytes`, `scriptpubkey_bytes` | Block-weite Summen der entsprechenden Transaktionsfelder. |
| `vin_type_<typ>_count`, `vout_type_<typ>_count` (je 12 Spalten) | Block-weite Summen der Skript-Typ-Verteilung, siehe 4.10. |

## 4.10 Ausgabeschema: `transactions.csv`

Eine Zeile pro Transaktion (inkl. Coinbase). Erzeugt von
`block_parser.py::aggregate_transaction()`. Liegt unter
`output_dir/transactions/` (siehe 4.5/4.6). Nur aggregierte/skalare
Felder – die einzelnen vin/vout-Zeilen dieser Transaktion liegen separat
in `inputs.csv`/`outputs.csv` (siehe 4.11/4.12), damit Dashboards, die nur
die Block-/Transaktionsebene brauchen, ihre Kennzahlen (Gebühren-Stats,
Skript-Typ-Verteilung, ...) nie zur Suchzeit aus tausenden Detail-Zeilen
pro Block neu berechnen müssen. Zusätzlich werden reine Duplikate anderer,
bereits exportierter Felder **nicht** geschrieben (siehe die Hinweise
unter der Tabelle).

| Feld | Beschreibung |
|---|---|
| `block_hash`, `block_height`, `block_time`, `tx_index` | Beziehung zum Block; `tx_index` ist die Position innerhalb des Blocks (0 = Coinbase). |
| `txid` | Transaktions-ID. |
| `wtxid` | Witness-Transaktions-ID (`hash`-Feld von `getblock`) – **`null`, wenn identisch mit `txid`** (jede Nicht-Witness-Transaktion), statt den 64-stelligen Hash zu wiederholen. Nachgelagert mit `coalesce(wtxid, txid)` rekonstruieren; nur Witness-Transaktionen (bei denen sich beide Werte tatsächlich unterscheiden) zahlen für diese Spalte. |
| `is_coinbase` | `true` für die erste Transaktion jedes Blocks. |
| `version`, `size`, `vsize`, `weight`, `locktime` | Native Transaktionsfelder. |
| `fee_sats` | Siehe `fee_source` – `0` bei Coinbase, sonst aus RPC, abgeleitet, oder `None`. |
| `fee_source` | `"coinbase"` / `"rpc"` (direkt von `getblock` gemeldet) / `"derived"` (aus Input- minus Output-Wert berechnet, nur wenn alle Prevout-Werte bekannt sind) / `"unavailable"`. |
| `fee_rate_sat_vb` | `fee_sats / vsize`, nur für Nicht-Coinbase mit bekannter Gebühr und `vsize > 0`. |
| `input_value_sats` | Summe der Prevout-Werte (nur Nicht-Coinbase). |
| `output_value_sats`, `output_value_min_sats`, `output_value_max_sats`, `output_value_avg_sats` | Ausgabewerte-Statistik. |
| `vin_count`, `vout_count` | Anzahl Ein-/Ausgänge. |
| `prevout_value_known_count`, `prevout_height_known_count` | Wie viele Prevout-Werte bzw. -Höhen von `getblock verbosity=3` für diese Transaktion bekannt sind – Vollständigkeit lässt sich daraus direkt gegen `vin_count` ableiten (`prevout_value_known_count == vin_count`), ohne ein zusätzliches Bool-Feld zu exportieren. |
| `generated_input_count` | Anzahl Inputs, deren Prevout selbst ein Coinbase-Output war (`prevout.generated`). |
| `input_age_min_blocks`, `input_age_max_blocks`, `input_age_avg_blocks`, `input_age_value_weighted_avg_blocks` | Alter der ausgegebenen Inputs in Blöcken. |
| `coin_days_destroyed_btc` | Summe aus (Input-Alter in Tagen × bewegter Wert in BTC); Alter wird über `age_blocks / rpc.blocks_per_day` genähert statt eines echten Zeitstempel-Lookups pro Input. |
| `witness_input_count`, `witness_item_count`, `witness_data_bytes` | SegWit-Nutzung – ob die Transaktion überhaupt Witness-Daten trägt, lässt sich direkt aus `witness_input_count > 0` ablesen. |
| `scriptsig_bytes`, `scriptpubkey_bytes` | Skript-Byte-Summen. |
| `signals_rbf` | `true`, wenn irgendein Input eine Sequence-Nummer unter `0xFFFFFFFE` trägt (BIP-125). |
| `op_return_count`, `op_return_script_bytes` | OP_RETURN-Ausgaben; ob überhaupt eine vorhanden ist, ergibt sich aus `op_return_count > 0`. |
| `zero_value_output_count` | Anzahl Ausgaben mit Wert 0. |
| `vin_type_<typ>_count`, `vout_type_<typ>_count` (je 12 Spalten) | Verteilung der Skript-Typen über alle Ein-/Ausgänge dieser Transaktion. Typen: `nonstandard, pubkey, pubkeyhash, scripthash, multisig, nulldata, witness_v0_keyhash, witness_v0_scripthash, witness_v1_taproot, witness_unknown, anchor, other` – ein unbekannter/neuer Skript-Typ fällt automatisch unter `other`, sodass ein neu eingeführter Typ das Schema nicht bricht. Taproot-Nutzung lässt sich direkt aus `vin_type_witness_v1_taproot_count > 0` bzw. `vout_type_witness_v1_taproot_count > 0` ablesen. |

**Bewusst nicht (mehr) exportiert:**

- `has_witness`, `has_taproot_input`, `has_taproot_output`, `has_op_return`
  – reine Ein-Zeilen-Ableitungen aus bereits vorhandenen Zählfeldern
  (z. B. `witness_input_count > 0`); nachgelagert genauso einfach
  rekonstruierbar, ohne redundante Spalte.
- `prevout_values_complete`, `prevout_heights_complete` – ebenso ableitbar
  aus `prevout_value_known_count`/`prevout_height_known_count` im
  Vergleich zu `vin_count`.
- `coinbase_script_sig_hex`, `coinbase_output_addresses_json` – die rohen
  Coinbase-Felder, die die Mining-Pool-Zuordnung liest (siehe 4.8), werden
  seit dieser Version **nicht** mehr nach `transactions.csv` geschrieben.
  Sobald `pool_id`/`pool_name`/`pool_link` auf der zugehörigen Blockzeile
  stehen, tragen die rohen Felder keinen zusätzlichen nachgelagerten Wert
  mehr.

## 4.11 Ausgabeschema: `inputs.csv`

Eine Zeile pro vin (inkl. der synthetischen Coinbase-Eingabe). Erzeugt von
`block_parser.py::aggregate_transaction()`. Liegt unter
`output_dir/inputs/` (siehe 4.5/4.6).

| Feld | Beschreibung |
|---|---|
| `block_hash`, `block_height`, `block_time`, `tx_index`, `txid` | Beziehung zu Block/Transaktion – `block_time` ist das für Splunks `_time` gedachte Feld, damit sich auch diese Detail-Zeilen ohne Join gegen `transactions.csv`/`blocks.csv` per Zeitraum eingrenzen lassen. |
| `is_coinbase` | Von der Elterntransaktion übernommen, damit sich Coinbase-Eingaben ohne Join filtern lassen. |
| `input_index` | Position dieses vin innerhalb der Transaktion. |
| `prevout_txid`, `prevout_vout` | Welcher Output ausgegeben wird – `null` bei der Coinbase-Eingabe (die nichts ausgibt). |
| `value_sats` | Wert des ausgegebenen Prevout, sofern von `getblock verbosity=3` bekannt – `null` bei der Coinbase-Eingabe oder falls unbekannt. |
| `prevout_height` | Blockhöhe, in der der ausgegebene Output entstand. |
| `input_age_blocks` | `block_height - prevout_height`, vorab berechnet, damit das nicht bei jeder Suche neu ermittelt werden muss. |
| `prevout_generated` | War der ausgegebene Output selbst eine Coinbase-Ausgabe (`prevout.generated`). |
| `prevout_type` | Skript-Typ des ausgegebenen Outputs, normalisiert (dieselben 12 Werte wie `vin_type_<typ>_count` in `transactions.csv`). |
| `prevout_address` | Adresse des ausgegebenen Outputs, sofern ableitbar (siehe die Hinweise zu `address` unter 4.12). |
| `scriptsig_bytes` | Byte-Länge des `scriptSig` – der rohe Hex-String selbst wird **nicht** exportiert (siehe Hinweis unten). |
| `coinbase_hex` | Rohe Coinbase-Daten (`vin[0].coinbase`) – nur bei der Coinbase-Eingabe gesetzt, sonst `null`. Anders als `scriptsig_bytes`/`scriptSig` wird dieses Feld als Hex-String exportiert: Es ist klein (typischerweise < 100 Byte: BIP-34-Höhe, Extranonce, Pool-Tag) und trägt direkt nutzbare Information (z. B. dasselbe Pool-Tag, das `mining_pools.py` zur Zuordnung liest, siehe 4.8). |
| `witness_item_count`, `witness_data_bytes` | Witness-Nutzung dieser Eingabe. |
| `sequence` | Rohe `nSequence` (uint32). Zusätzlich zu den beiden folgenden dekodierten Feldern exportiert, da unkritisch groß und potenziell für Auswertungen nützlich, die die vorgefertigte Dekodierung nicht abdeckt. |
| `signals_rbf` | Pro-Eingabe-Variante von `transactions.csv`s `signals_rbf` (BIP-125: `sequence < 0xFFFFFFFE`, `false` bei der Coinbase-Eingabe). |
| `relative_locktime_type`, `relative_locktime_value` | BIP-68-relatives Locktime, aus `sequence` dekodiert (`decode_sequence()`) statt als rohes Bitfeld exportiert, da Splunk sonst für jede Auswertung Bitmasken-Arithmetik auf einem gepackten uint32 bräuchte. `relative_locktime_type` ist `"blocks"` oder `"time"` (Einheiten von 512 Sekunden), `relative_locktime_value` die untersten 16 Bit – beide `null`, wenn `tx.version < 2`, das Disable-Flag (Bit 31) gesetzt ist, oder es sich um die Coinbase-Eingabe handelt. |

**Bewusst nicht exportiert: `scriptsig_hex`.** Der volle `scriptSig`-Hex
ist für die überwiegende Mehrheit der Eingaben seit der SegWit-Aktivierung
ohnehin leer (die eigentlichen Entsperrdaten liegen dann in
`txinwitness`) und bietet ohne eigene Dekodierlogik keinen
Splunk-nativen Analysenutzen – SPL kann auf rohem Hex weder sinnvoll
`stats` noch charten. Der einzige echte Grund, ihn dennoch zu behalten,
wäre archivarisch (für vor-SegWit-Blöcke ist `scriptSig` die einzige
je existierende Aufzeichnung der Entsperrdaten). Da der Node hier
archival bleibt und ein erneuter Parse-Lauf ab Genesis damit jederzeit
möglich ist, überwiegt dieser Grund nicht die Kosten, ihn auf jeder
Zeile mitzuführen.

## 4.12 Ausgabeschema: `outputs.csv`

Eine Zeile pro vout. Erzeugt von `block_parser.py::aggregate_transaction()`.
Liegt unter `output_dir/outputs/` (siehe 4.5/4.6).

| Feld | Beschreibung |
|---|---|
| `block_hash`, `block_height`, `block_time`, `tx_index`, `txid` | Beziehung zu Block/Transaktion – siehe 4.11. |
| `is_coinbase` | Von der Elterntransaktion übernommen. |
| `output_index` | Position dieses vout innerhalb der Transaktion (`vout.n`). |
| `value_sats` | Ausgabewert in Satoshi. |
| `script_type` | Normalisierter Skript-Typ (dieselben 12 Werte wie `vout_type_<typ>_count` in `transactions.csv`). |
| `script_bytes` | Byte-Länge des `scriptPubKey` – der rohe Hex-String selbst wird **nicht** exportiert (siehe Hinweis unten). |
| `address` | Von Bitcoin Core direkt geliefert (`scriptPubKey.address`), keine eigene Ableitungslogik nötig. `null` bei: `nulldata` (OP_RETURN, kein Adresskonzept), `multisig` ohne P2SH/P2WSH-Hülle (mehrere Pubkeys, keine einzelne Adresse – seit Bitcoin Core v22 liefert die RPC hierfür kein Feld mehr statt des früheren `addresses[]`-Arrays; historisch nur bis ~2012 verbreitet, seither durch P2SH/P2WSH-Multisig abgelöst), `nonstandard`, `witness_unknown` und `anchor`. Für alle Standard-Typen (`pubkeyhash`, `scripthash`, `pubkey`, `witness_v0_keyhash`, `witness_v0_scripthash`, `witness_v1_taproot`) ist das Feld praktisch immer gesetzt. |
| `is_op_return` | `script_type == "nulldata"`, als eigenes Bool-Feld statt einer String-Vergleichs-Suche bei jeder Auswertung. |

**Bewusst nicht exportiert: `script_hex` (der volle `scriptPubKey`-Hex).**
Für alle Standard-Adresstypen ist er reine Redundanz zu `address` +
`script_type` – ohne eigenen analytischen Mehrwert. Der eine Fall, in dem
der rohe Hex tatsächlich Inhalt trägt, ist `nulldata` (OP_RETURN): Dort
steckt die eigentliche Payload im Skript, und das Taggen bekannter
Protokoll-Präfixe wäre ein echter Splunk-Anwendungsfall. Er wurde trotzdem
komplett weggelassen, weil derselbe Spaltentyp dann pro Zeile beliebig
groß werden kann – die größte bisher beobachtete OP_RETURN-Payload im
Mainnet lag bei 79.870 Byte – und ein einzelnes überlanges Feld ein
komplettes Splunk-Event an dessen Truncate-Grenze (Standard ~10.000
Byte) kappen und damit auch die übrigen Felder derselben Zeile
beschädigen kann. Eine Beschränkung nur auf `nulldata`-Zeilen würde dieses
Risiko lediglich verkleinern, nicht beseitigen.

## 4.13 Beendigung

Ein installierter Signal-Handler (`common/stop_signal.py::install_stop_signal()`) setzt bei
`SIGTERM`/`SIGINT` nur ein `threading.Event`, das zwischen Blöcken und
nach jedem Durchlauf geprüft wird – ein `kill`/Strg+C (oder `stop.sh`)
landet dadurch immer auf einem sauberen `current.csv`-Checkpoint statt
mitten im Schreiben abgebrochen zu werden.

Die Worker-Prozesse der parallelen Aufholjagd (4.14) ignorieren
`SIGTERM`/`SIGINT` – systemd schickt das Signal an die ganze Control-Group,
Strg+C an die ganze Prozessgruppe. Nur der Hauptprozess reagiert: Er
übernimmt den aktuellen Block, schreibt den Checkpoint und beendet den
Pool selbst (noch nicht begonnene Aufträge werden verworfen, laufende
dauern höchstens einen `getblock`-Aufruf).

## 4.14 Parallele Aufholjagd (`rpc/catchup.py`) und Benchmark

Sequenziell kostet jeder Block: `getblockhash` und `getblock <hash> 3`
(je ein `bitcoin-cli`-Prozess), `json.loads(parse_float=Decimal)`,
`aggregate_block()`, dann das Schreiben über polars – alles auf einem
Kern, während Node und Platte größtenteils warten.

Mit `rpc.catchup_workers = N > 1` erledigen N Worker-Prozesse (gestartet
per `spawn`, nicht `fork` – polars' Thread-Pool im Hauptprozess verträgt
kein `fork`) genau diesen Teil – `getblockhash` + `parse_block()` – für bis
zu `4 × N` Höhen im Voraus. `iter_parsed_blocks()` gibt die Ergebnisse
**strikt in Höhen-Reihenfolge** zurück; der Hauptprozess wendet sie mit
denselben Schritten an wie im sequenziellen Fall
(`_process_prefetched()` ↔ `_process_height()`, gemeinsam
`_buffer_export()`): `time_since_prev_block_sec` setzen, Zeilen puffern,
`index.add()`, `block_status`, Drain/Checkpoint alle `batch_size` Blöcke.
Damit entstehen dieselben Batches und damit byte-identische CSVs.

Die Worker bauen außerdem schon die polars-DataFrames für
`transactions`/`inputs`/`outputs` eines Blocks
(`catchup.frame_from_rows()`, exakt wie bisher mit
`infer_schema_length=None`); der Hauptprozess hängt die Frames eines
Batches nur noch per `pl.concat(..., how="vertical_relaxed")` aneinander
und schreibt sie. `vertical_relaxed` hebt jede Spalte auf den Supertyp
über den ganzen Batch (Null + Int64 → Int64, Int64 + Float64 → Float64) –
denselben, den die zeilenweise Inferenz über alle Zeilen des Batches
gewählt hätte –, deshalb sind die geschriebenen Bytes identisch
(`tests/test_catchup.py::FrameEquivalenceTest`, 300 Zufalls-Batches je
Event-Typ, plus der Ende-zu-Ende-Vergleich unten). Das Bauen der
DataFrames aus Tausenden Zeilen-Dicts war der teuerste Schritt, der im
seriellen Hauptprozess übrig war (siehe „Produktion“ unten).

Abgrenzungen:

- **Nur tief unter dem Tip**: Höhen `≤ tip − max_reorg_depth`. Alles
  darüber (das letzte Stück einer Aufholjagd sowie Tip-Following und
  atomarer Modus) läuft unverändert sequenziell. Ein Pool wird nur
  gestartet, wenn der parallele Bereich größer als das Vorab-Fenster
  (`4 × N`) ist.
- **Kettenprüfung**: Passt `previousblockhash` eines vorab geholten Blocks
  nicht zum zuletzt übernommenen Hash, wird nichts davon exportiert; der
  Durchlauf endet mit Checkpoint, und der nächste Durchlauf führt die
  normale Reorg-Erkennung (4.4) aus.
- **Fehler**: Ein `RpcCliError` eines Workers wird erst an seiner Höhe im
  Hauptprozess ausgelöst – nachdem alle niedrigeren Höhen übernommen
  wurden, genau wie sequenziell. `run_rpc_ingest()` behandelt ihn wie
  bisher (`index.discard_pending()`, warten, neuer Durchlauf).
- **Bereits indizierte Blöcke** holt ein Worker trotzdem; der Hauptprozess
  verwirft sie (kommt in der Aufholjagd nur für die wenigen Blöcke
  zwischen letztem Index-Flush und einem Absturz vor).

Bewusst **nicht** umgesetzt: `orjson` statt `json` – es kann Zahlen nicht
als `Decimal` liefern, und das Parsen macht nach dem Wechsel auf HTTP nur
noch ~8 ms von ~60 ms pro Block aus (siehe Profil unten); den
Präzisionsbeweis für einen Float-Umweg wäre das nicht wert. Ebenso kein
gebündeltes `getblockhash`: über HTTP kostet es < 1 ms und wird von den
Workern ohnehin parallel geholt.

### Benchmark

**Aufbau.** Eigener regtest-Node (Bitcoin Core v31.1, `txindex=1`,
`rpcthreads=16`) auf demselben Rechner (Apple M2 Max, 12 Kerne, NVMe). Die
Chain hat 796 Blöcke: Höhe 0–545 nahezu leer, Höhe 546–796 je ~500
Transaktionen / ~1.000 Inputs / ~2.000 Outputs / ~250 kB, gemischte
Skripttypen (P2PKH, P2SH-P2WPKH, P2WPKH, P2TR, OP_RETURN) – vergleichbar
mit Mainnet um Höhe 300k, wo in Produktion sequenziell ~11 Blöcke/s
gemessen wurden. Jeder Lauf startet `run.py rpc-ingest` mit leerem
`state_dir`/`output_dir`, holt bis `tip − reorg_confirmations` auf und
wird dann per SIGTERM beendet; gemessen werden die Höhen 579–699
(große Blöcke, alle im parallelen Bereich `≤ tip − max_reorg_depth`),
über Zeitstempel der `current.csv`-Checkpoints. `batch_size` 20,
`reorg_confirmations` 6, `max_reorg_depth` 100. Der alte Code wurde per
`git archive` des Stands vor dieser Änderung ausgeführt.

| `transport` | `catchup_workers` | Blöcke/s | Faktor |
|---|---|---|---|
| alter Code (`cli`) | – | 8,9 | 1,0× |
| `cli` | 1 | 8,9 | 1,0× |
| `http` | 1 | 17,2 | 1,9× |
| `cli` | 8 | 49,6 | 5,6× |
| `http` | 2 | 32,1 | 3,6× |
| `http` | 4 | 59,2 | 6,7× |
| `http` | 6 | 73,2 | 8,2× |
| `http` | 8 | **79,3** | **8,9×** |
| `http` | 10 | 73,9 | 8,3× |

Bei 8 Workern ist die Maschine ausgelastet (Worker + bitcoind teilen sich
die 12 Kerne); mehr Worker bringen nichts mehr. (Diese Tabelle stammt
noch aus der Version, in der der Hauptprozess die DataFrames selbst
gebaut hat – siehe „Produktion“.)

**Gleichheit der Ausgabe.** `diff -r` über `state_dir` + `output_dir`
(Parts, `*_part_seq.csv`, `current.csv`, `latest.csv`, `index/`) zwischen
dem alten Code und **jedem** Lauf der Tabelle: byte-identisch. Ebenso
byte-identisch: ein mit altem Code bis Höhe 640 gelaufener und dann
gestoppter `state_dir`, einmal mit altem Code und einmal mit `http` + 8
Workern fortgesetzt; und ein mit `http` + 8 Workern per SIGTERM
gestoppter Lauf (Stopp nach 0,5 s, Exit-Code 0, keine verwaisten
Worker – auch bei SIGTERM an die ganze Prozessgruppe wie unter systemd),
fortgesetzt mit altem bzw. neuem Code. Die Unit-Tests
(`tests/test_catchup.py`) prüfen dasselbe gegen eine synthetische Chain,
inklusive atomarem Modus, Stopp mitten im parallelen Bereich, Worker-
Fehler und Ketten-Bruch.

**Profil pro Block** (gleiche Blöcke, Mittel über 40 Blöcke):

| Schritt | ms/Block |
|---|---|
| `bitcoin-cli getblock <hash> 3` (Subprozess inkl. Node-Zeit) | 80 |
| `getblock` über HTTP inkl. `json.loads(parse_float=Decimal)` | 42 |
| `bitcoin-cli getblockhash` / über HTTP | 9,8 / 0,7 |
| `json.loads(parse_float=Decimal)` des `bitcoin-cli`-Texts | 8,1 |
| `orjson.loads` (nur Floats – zum Vergleich) | 3,4 |
| `aggregate_block()` | 10,7 |
| polars `DataFrame` + `write_csv` (alle vier Event-Typen) | 10,2 |
| `pickle` dumps / loads des Ergebnisses (Worker → Hauptprozess) | 3,5 / 3,6 |

### Produktion (Mainnet, Ryzen 9 5950X, Bitcoin Core v31.1, `rpcthreads=16`)

Erste Version (Hauptprozess baut die DataFrames), `http` + 8 Worker,
5-Minuten-Mittel aus den „Progress“-Logzeilen:

| | Höhe | Blöcke/s |
|---|---|---|
| alter Code (sequenziell) | ~320k → 339k | 10 → 6,3 |
| `http`, 8 Worker | ~350k → 389k | 40 → 23 |

Etwa Faktor 6 bei vergleichbarer Höhe, aber mit den 2015 schnell
wachsenden Blöcken stark fallend. `top` zeigte warum: Hauptprozess bei
~105 % CPU (ein Kern voll), bitcoind bei ~190 %, Maschine zu 84 % idle –
der serielle Schreiber war die Grenze, nicht Worker oder Node.

Kosten im Hauptprozess pro Block vorher/nachher (synthetische Blöcke,
Batch von 20, M2 Max; „pro Worker“ = was dafür in jeden Worker wandert):

| Block | Hauptprozess vorher | nachher | zusätzlich pro Worker |
|---|---|---|---|
| 500 Tx / ~2.300 In+Out | 9,9 ms (≤ 101 Blöcke/s) | 0,7 ms | 5,2 ms |
| 2.000 Tx / ~9.000 In+Out | 35,5 ms (≤ 28 Blöcke/s) | 1,8 ms | 20,4 ms |

Die alte Obergrenze von ~28 Blöcken/s für 2.000-Tx-Blöcke passt zu den
~23 Blöcken/s, die in Produktion bei Höhe 389k beobachtet wurden. Danach
skaliert die Aufholjagd mit Workern und bitcoind-CPU: auf dem 5950X
(16 Kerne/32 Threads) `catchup_workers: 14` als Startwert und anhand von
`top` (Worker und bitcoind ausgelastet? Maschine noch idle?) nachjustieren.
