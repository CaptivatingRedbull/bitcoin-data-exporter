# 11. Mempool-Watch: Sanktionsadressen im Mempool

[← Index](00-index.md)

Kommando: `mempool-watch` · Einstiegspunkt:
`btc_parser_app/rpc/mempool_watch.py::run_mempool_watch()`

## 11.1 Zweck

`mempool-watch` prüft jede Transaktion, die in den Mempool des eigenen
Nodes gelangt, gegen eine Liste sanktionierter Adressen und exportiert
pro Treffer ein Ereignis je Zustandswechsel der Transaktion – vom ersten
Auftauchen im Mempool bis zur Bestätigung in einem Block oder zum
Verschwinden aus dem Mempool.

Der Mempool selbst wird **nicht** exportiert: Er umfasst oft
Hunderttausende Transaktionen, die sich laufend ändern, und wäre als
Splunk-Datenbestand weder handhabbar noch sinnvoll auswertbar. Nach
Splunk gehen nur die (seltenen) Treffer, jeweils vollständig mit Adresse,
Betrag, Name und Sanktionsprogramm, sodass ein Alert ohne weiteren
Lookup auskommt.

Ein Treffer liegt vor, wenn eine Sanktionsadresse

- **empfängt** – sie steht in einem Output der Transaktion
  (`direction = output`), oder
- **ausgibt** – ein Input der Transaktion gibt einen Output aus, der an
  diese Adresse ging (`direction = input`).

## 11.2 Node-Konfiguration

In der `bitcoin.conf` des Nodes:

```ini
# Mempool-Hinzufügungen/-Entfernungen und Block-Connect/-Disconnect pushen
zmqpubsequence=tcp://0.0.0.0:28332

# Optional: Sendepuffer vergrößern (Default 1000 Nachrichten), damit
# Lastspitzen (z. B. direkt nach einem Block) keine Nachrichten verwerfen
zmqpubsequencehwm=100000
```

- **Bind-Adresse:** `127.0.0.1`, wenn `mempool-watch` auf demselben Host
  läuft; sonst die Interface-Adresse bzw. `0.0.0.0`. In Kubernetes
  `0.0.0.0` statt der Pod-IP, da sich diese bei einer Neuplanung ändert.
- **Neustart des Nodes erforderlich** – ZMQ-Optionen werden nur beim
  Start gelesen.
- Andere ZMQ-Topics (`zmqpubrawtx`, `zmqpubhashtx`, …) werden nicht
  benötigt; die Transaktionsdetails kommen per RPC.
- `txindex` ist **nicht** nötig: Mempool-Transaktionen sind immer per
  `getrawtransaction` abrufbar, Bestätigungen werden über `getblock`
  erkannt.

Prüfen nach dem Neustart:

```sh
bitcoin-cli getnetworkinfo          # "version" >= 250000 (Bitcoin Core 25) empfohlen
bitcoin-cli getzmqnotifications     # muss "pubsequence" mit der Adresse listen
```

Liefert `getzmqnotifications` eine leere Liste, wurde `bitcoind` ohne
ZMQ-Unterstützung gebaut (offizielle Releases und Homebrew haben sie).

**Sicherheit:** ZMQ hat weder Authentifizierung noch Verschlüsselung.
Jeder, der den Port erreicht, kann den Ereignisstrom mitlesen (nur
öffentliche Mempool-Daten, dennoch) – den Port per Firewall bzw.
NetworkPolicy auf den Parser-Host beschränken.

In `config.yaml` dann `mempool_watch.zmq_endpoint` auf dieselbe Adresse
aus Sicht des Parser-Hosts setzen (z. B. `tcp://10.42.5.56:28332`), siehe
Kapitel 3.7.

## 11.3 Erkennungswege

### ZMQ `sequence` (primär, nahezu in Echtzeit)

`rpc/zmq_sequence.py` abonniert das Topic `sequence`. Jede Nachricht
trägt einen Hash und ein Kennzeichen:

| Kennzeichen | Bedeutung | Reaktion |
|---|---|---|
| `A` | Transaktion in den Mempool aufgenommen | Transaktion abrufen, gegen die Liste prüfen, bei Treffer `seen` |
| `R` | Transaktion aus dem Mempool entfernt – aus jedem Grund **außer** Blockaufnahme (ersetzt, verdrängt, abgelaufen, Konflikt) | bei verfolgter Transaktion `replaced` bzw. `removed` |
| `C` | Block verbunden | `getblock <hash> 1`; verfolgte Transaktionen darin → `confirmed` |
| `D` | Block getrennt (Reorg) | Prüfen, ob der Block einer bestätigten, verfolgten Transaktion die aktive Chain verlassen hat → `unconfirmed` |

Mehrere gleichzeitig eingetroffene Nachrichten werden als Batch
verarbeitet: Alle neuen `A`-Transaktionen des Batches werden parallel
abgerufen (`mempool_watch.rpc_workers`), die Ereignisse danach strikt in
Eingangsreihenfolge angewendet.

ZMQ meldet Probleme nie selbst – ein SUB-Socket auf einen falschen oder
unerreichbaren Endpunkt wartet stillschweigend, und Nachrichten können
bei Überlast verloren gehen. Deshalb:

- Eine **Lücke im Nachrichtenzähler** (verlorene Nachrichten oder
  Node-Neustart, bei dem der Zähler auf 0 zurückspringt) löst sofort einen
  vollständigen Abgleich aus.
- Kam seit `reconcile_interval_seconds` **gar keine** Nachricht an, wird
  eine Warnung geloggt (auf Mainnet praktisch immer ein
  Konfigurationsproblem: `zmqpubsequence` fehlt, Port blockiert, falsche
  Adresse).

### Abgleich (`reconcile`, Sicherheitsnetz)

Beim Start, alle `reconcile_interval_seconds` und nach jeder erkannten
ZMQ-Lücke bzw. RPC-Störung:

1. `getrawmempool` – alle txids, die noch nicht geprüft wurden, werden
   abgerufen und gegen die Liste geprüft (beim Start also einmal der
   gesamte Mempool, in Blöcken zu 500 und parallel; `SIGTERM` wird
   zwischen den Blöcken berücksichtigt).
2. Jede verfolgte Transaktion, die nicht mehr im Mempool ist und für die
   kein Ereignis ankam (Downtime, verlorene Nachrichten), wird in den
   letzten Blöcken gesucht (ab der Höhe ihres ersten Auftauchens, höchstens
   `max_confirmation_scan_blocks` zurück): gefunden → `confirmed`, sonst
   `replaced`/`removed` (siehe 11.6).
3. Bestätigte, verfolgte Transaktionen werden auf Reorgs geprüft und ab
   `forget_after_confirmations` Bestätigungen nicht mehr verfolgt.

Beim Startscan des ganzen Mempools (typisch 50.000–300.000
Transaktionen) fällt pro Transaktion ein `bitcoin-cli`-Aufruf an; mit
`rpc_workers: 4` dauert das je nach Node einige Minuten. Die
ZMQ-Nachrichten, die während dieser Zeit eintreffen, werden gepuffert
(das Abonnement besteht schon vor dem Scan) und danach verarbeitet –
es geht nichts verloren.

### Reiner Polling-Modus

Mit `zmq_endpoint: ""` gibt es kein Abonnement; der Abgleich ist dann der
einzige Erkennungsweg. Funktional identisch, nur mit einer Verzögerung
von bis zu `reconcile_interval_seconds` – dafür in diesem Modus z. B. 30
statt 300 setzen. Nützlich, solange ZMQ am Node noch nicht eingerichtet
ist.

## 11.4 Adressabgleich

Pro neuer Transaktion ein Aufruf `getrawtransaction <txid> 2`:

- **Outputs:** `vout[].scriptPubKey.address` – wer empfängt.
- **Inputs:** `vin[].prevout.scriptPubKey.address` – wer ausgibt. Das
  Feld `prevout` liefert Bitcoin Core erst ab **Version 25**. Auf älteren
  Nodes greift ein Fallback (ein zusätzlicher `gettxout`- bzw.
  `getrawtransaction`-Aufruf pro Input), der funktioniert, den Startscan
  aber deutlich verlangsamt – beim Start wird dafür eine Warnung geloggt.
- Coinbase-Inputs und Outputs ohne Adresse (z. B. `OP_RETURN`) werden
  übersprungen.

Transaktionen, die schon wieder aus dem Mempool verschwunden sind, bevor
sie abgerufen werden konnten (z. B. innerhalb von Millisekunden per RBF
ersetzt), liefern „No such mempool transaction“ und werden ohne
Wiederholung übersprungen (`run_cli(..., retry_rpc_errors=False)`,
Kapitel 8.2).

## 11.5 Sanktionsliste

`mempool_watch.sanctions_list_paths` – eine oder mehrere CSV-Dateien,
standardmäßig [`config/sanctioned_addresses.csv`](../config/sanctioned_addresses.csv):

```csv
address,name,first_name,sanctions_programs
12QtD5BFwRsdNsAZY76UVE1xyCGNTojH9h,YAN,Xiaobing,SDNTK
bc1qv7k70u2zynvem59u88ctdlaw7hc735d8xep9rq,SOUTHFRONT,,NPWMD CYBER2 ELECTION-EO13848
```

- Kopfzeile ist Pflicht, Spaltennamen ohne Beachtung der
  Groß-/Kleinschreibung; nur `address` ist Pflicht, zusätzliche oder leere
  Spalten (z. B. ein abschließendes Komma) werden ignoriert.
- Mehrere Sanktionsprogramme **mit Leerzeichen getrennt** in einem Feld –
  ein Komma würde eine neue Spalte beginnen.
- bech32-Adressen (`bc1…`) werden in Kleinschreibung verglichen (so gibt
  Bitcoin Core sie aus), Base58-Adressen (`1…`, `3…`) exakt, da dort die
  Groß-/Kleinschreibung Teil der Adresse ist.
- Doppelte Adressen: der erste Eintrag gewinnt (Warnung im Log).
- Die Spalte `list_file` im Export nennt die Datei, aus der ein Treffer
  stammt – nützlich bei mehreren Listen.

**Änderungen ohne Neustart:** Ändert sich eine Listendatei auf der
Platte, wird sie beim nächsten Schleifendurchlauf (≤ 1 s) neu geladen und
der **gesamte aktuelle Mempool erneut geprüft** – eine neu
aufgenommene Adresse wird also auch in bereits wartenden Transaktionen
erkannt. Eine kaputte Änderung (Datei unlesbar, keine `address`-Spalte)
wird geloggt, die zuletzt gültige Liste bleibt aktiv.

## 11.6 Ausgabeschema: `sanctioned_tx_events.csv`

`mempool_watch.output_dir/sanctioned_tx_events.csv` (rotiert wie jede
Append-only-CSV, Kapitel 7.3). **Eine Zeile pro Treffer-Input/-Output
pro Ereignis** – zahlt eine Transaktion z. B. an zwei gelistete
Adressen, erzeugt jedes Ereignis zwei Zeilen. Jede Zeile ist damit für
sich allein alarmierbar.

### Ereignisse

| `event` | Wann | Zusätzlich gesetzt |
|---|---|---|
| `seen` | Transaktion erstmals im Mempool beobachtet | – |
| `confirmed` | in einem Block der aktiven Chain enthalten | `block_height`, `block_hash` |
| `unconfirmed` | der bestätigende Block wurde per Reorg entfernt; die Transaktion wird wieder verfolgt | `detail` (alter Block), `block_height`/`block_hash` des alten Blocks |
| `replaced` | Mempool verlassen, weil eine andere Mempool-Transaktion dieselben Inputs ausgibt (RBF/Konflikt) | `replaced_by_txid` |
| `removed` | Mempool ohne Blockaufnahme aus einem anderen Grund verlassen (verdrängt, abgelaufen, durch einen Block in Konflikt geraten) | `detail` |

Wird eine Transaktion ersetzt und berührt die Ersatztransaktion ebenfalls
eine gelistete Adresse (bei einem Input-Treffer praktisch immer der Fall),
bekommt diese ihr eigenes `seen`-Ereignis.

`replaced` vs. `removed` wird über `gettxspendingprevout` (Bitcoin Core
24+) entschieden: Gibt eine andere Mempool-Transaktion einen der Inputs
aus, ist es `replaced`. `detail = left_mempool_unobserved` bedeutet, dass
die Transaktion während einer Downtime/ZMQ-Lücke verschwand und in den
letzten `max_confirmation_scan_blocks` Blöcken nicht gefunden wurde.

### Spalten

| Spalte | Beschreibung |
|---|---|
| `observed_at` | Zeitpunkt des Ereignisses (ISO-8601 UTC) – `_time` in Splunk |
| `event` | siehe oben |
| `txid` | Transaktions-ID |
| `direction` | `input` (Adresse gibt aus) oder `output` (Adresse empfängt) |
| `io_index` | Index des Inputs bzw. `n` des Outputs |
| `address` | die gelistete Adresse |
| `value_sats` / `value_btc` | Betrag dieses Inputs/Outputs (Satoshi bzw. BTC als exakter Dezimalstring) |
| `name`, `first_name`, `sanctions_programs` | aus der Sanktionsliste |
| `list_file` | Listendatei, aus der der Treffer stammt |
| `tx_fee_btc`, `tx_vsize` | Gebühr und virtuelle Größe der gesamten Transaktion |
| `first_seen_at` | erstes Auftauchen der Transaktion im Mempool (ISO-8601 UTC) |
| `block_height`, `block_hash` | bestätigender Block (bei `confirmed`/`unconfirmed`) |
| `replaced_by_txid` | Ersatztransaktion (bei `replaced`) |
| `source` | Erkennungsweg: `zmq`, `startup_scan`, `reconcile` oder `reorg` |
| `detail` | Freitext-Zusatz (siehe oben) |

Die Spaltenreihenfolge ist im Code fest vorgegeben (`EVENT_COLUMNS` in
`mempool_watch.py`), da Zeilen ohne erneute Kopfzeile angehängt werden.

Zusätzlich loggt jedes `seen` eine `WARNING`-Zeile mit allen Treffern in
`mempool-watch.log` bzw. im Journal – ein Treffer ist also auch ohne
Splunk sofort sichtbar.

## 11.7 Interner Zustand: `flagged.json`

`mempool_watch.state_dir/flagged.json` (nie Splunk-seitig) – ein Eintrag
pro aktuell verfolgter Treffer-Transaktion, atomar komplett neu
geschrieben bei jeder Änderung (Kapitel 7, `atomic_write.py`). Ein
Eintrag existiert vom `seen` bis entweder zum Verlassen des Mempools ohne
Blockaufnahme (`replaced`/`removed`) oder bis der bestätigende Block
`forget_after_confirmations` tief ist. Da nur Treffer darin stehen,
bleibt die Datei winzig.

Damit ist ein Neustart unkritisch:

- Eine Transaktion, die beim Neustart noch im Mempool liegt, wird **nicht**
  erneut als `seen` gemeldet.
- Eine, die während der Downtime bestätigt wurde oder verschwand, bekommt
  beim Startscan ihr `confirmed` bzw. `replaced`/`removed` (Quelle
  `startup_scan`).

Nicht persistiert wird die Menge der bereits geprüften, unauffälligen
txids – nach jedem Neustart wird der Mempool daher einmal vollständig
geprüft (11.3).

Absturz genau zwischen dem Schreiben einer Exportzeile und dem Speichern
von `flagged.json`: Das eine Ereignis kann nach dem Neustart doppelt
erscheinen. Das ist derselbe bewusste Kompromiss wie in der
Stale-Blocks-Pipeline (Kapitel 5) – lieber ein seltenes Duplikat als ein
verlorenes Ereignis.

## 11.8 Testen

Die gelisteten Adressen sind überwiegend öffentlich bekannt und inaktiv –
echte Treffer sind selten. Zwei Wege, die Kette trotzdem zu prüfen:

**Einzelne Transaktion prüfen** (schreibt nichts):

```sh
# Transaktion im Mempool
python run.py mempool-watch-check <txid>

# bestätigte Transaktion (ohne -txindex muss der Block angegeben werden)
python run.py mempool-watch-check <txid> --blockhash <blockhash>
```

Gibt die Treffer als JSON aus. Mit einer historischen Transaktion, die
nachweislich an eine gelistete Adresse zahlte, lässt sich so Liste und
Abgleich verifizieren.

**Eigene Testliste:** Eine zweite CSV mit einer aktuell aktiven Adresse
(z. B. einer eigenen Test-Wallet) anlegen und an
`sanctions_list_paths` anhängen:

```yaml
sanctions_list_paths:
  - "config/sanctioned_addresses.csv"
  - "config/test_addresses.csv"
```

Nach einer eigenen Zahlung an diese Adresse erscheint innerhalb von
Sekunden ein `seen` (`list_file = test_addresses.csv`), nach der
Bestätigung ein `confirmed`. Die Testliste danach wieder entfernen – die
Änderung an der Konfiguration braucht einen Neustart, das Leeren der
Testdatei nicht.

## 11.9 Abgrenzung und Splunk-Alert

`mempool-watch` sieht nur, was durch den Mempool **dieses** Nodes läuft.
Eine Transaktion, die direkt/privat an einen Miner übermittelt wurde,
taucht erst im Block auf – diese Fälle deckt ein Splunk-Alert auf den
On-Chain-Daten (`btc:inputs`/`btc:outputs` aus `rpc-ingest`) gegen
dieselbe Liste als Lookup ab, nicht dieser Dienst. Ebenso werden nur
exakte Adresstreffer erkannt, keine weiteren Adressen derselben Akteure
(Wechselgeld, Clustering).

Beispiel-Suche für einen Alert auf neue Mempool-Treffer:

```spl
index=btc_parser sourcetype=btc:sanctioned_tx_events event=seen
| table _time txid direction address value_btc name first_name sanctions_programs source
```

Ein Alert auf `event=confirmed` zeigt entsprechend, dass eine solche
Transaktion tatsächlich gemined wurde. Splunk-Input/Sourcetype:
Kapitel 10.3/10.4.

## 11.10 Beendigung

`SIGTERM`/`SIGINT` setzen wie bei den anderen Diensten nur ein
Stop-Event (`common/stop_signal.py`); die Schleife beendet sich nach dem
aktuellen Batch bzw. dem aktuellen 500er-Block des Startscans. Jede
Zustandsänderung ist zu diesem Zeitpunkt bereits in `flagged.json`
gespeichert.
