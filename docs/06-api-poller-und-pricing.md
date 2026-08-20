# 6. API-Poller und Pricing

[← Index](00-index.md)

Kommando: `api-poll` · Einstiegspunkt: `btc_parser_app/api/poller.py::run_poller()`

## 6.1 Beteiligte Module

| Modul | Aufgabe |
|---|---|
| `api/poller.py` | Ein Thread pro Endpunkt, jeder auf eigenem Intervall. |
| `api/rate_limiter.py` | Thread-sicherer Token-Bucket, der `mempool_api.rate_limit` durchsetzt. |
| `api/client.py` | Ratenbegrenzter HTTP-GET-Client (Retries, 429-Handling), von jedem mempool.space-Aufrufer geteilt. |
| `api/mempool_endpoints.py` | JSON-zu-Zeilen-Parser je Endpunkt + Registry. |
| `api/price_history_import.py` | Einmaliger, rein lokaler Bulk-Import zweier Kraken-1-Minuten-OHLC-CSVs in `prices.csv` (`import-price-history`). |
| `api/mining_pools_dataset.py` | Siehe Kapitel 4 – nutzt denselben `ApiClient`, aber eine eigene, unabhängige Rate-Limit-Instanz (siehe 6.7). |

Es gibt in dieser Version **keinen** eigenen Preis-Lückenfüller-Thread
mehr innerhalb von `api-poll` – `run_poller()` startet ausschließlich die
Endpunkt-Threads. Historische Preisdaten kommen ausschließlich über das
separate, netzwerklose Kommando `import-price-history` (siehe 6.6).

## 6.2 Token-Bucket-Rate-Limiting

`rate_limiter.py::TokenBucket` ist ein einziger, geteilter Zähler:
`requests_per_minute` bestimmt die Auffüllrate (`rate_per_second`),
`bucket_size` die Burst-Kapazität. **Ein** `TokenBucket`-Objekt wird von
jedem Endpunkt-Thread verwendet (`client.py::ApiClient`) – die
konfigurierte Rate ist also ein Budget für die gesamte Anwendung
gegenüber `mempool.space`, nicht pro Aufrufer. `acquire()` blockiert
(bzw. wartet über ein `threading.Event`, falls gegeben), bis ein Token
verfügbar ist, und gibt `False` zurück, falls währenddessen ein Stopp
angefordert wurde – ein Aufrufer kann eine nicht mehr gewünschte Anfrage
so abbrechen, statt sie doch noch abzusetzen.

## 6.3 Endpunkt-Threads (`poller.py`)

`run_poller()` startet für jeden in `mempool_api.endpoints` konfigurierten
Eintrag einen eigenen Thread (`endpoint_loop()`), der:

1. Um einen berechneten Start-Offset verzögert beginnt
   (`compute_start_offsets()`): Endpunkte mit demselben Intervall werden
   gleichmäßig über dieses Intervall gestaffelt (z. B. 4 Endpunkte auf
   60 s starten 15 s versetzt), statt gleichzeitig zu feuern.
2. In einer Schleife `fetch_and_write()` aufruft, dann bis zum nächsten
   fälligen Zeitpunkt wartet. Fällt ein Thread hinter den Zeitplan zurück
   (langsame Antwort), wird der Zeitplan neu synchronisiert statt eine
   Serie von Nachhol-Anfragen abzufeuern.
3. Über `stop_event.wait()` statt `time.sleep()` wartet – ein 429 auf
   **irgendeinem** Endpunkt setzt `stop_event`, wodurch jeder andere
   Thread sofort aufwacht und anhält, statt seine Wartezeit auszuschlafen.

`fetch_and_write()` schlägt den registrierten Parser
(`PARSER_REGISTRY[endpoint.parser]`) nach, ruft `client.get_json(url)` auf
und schreibt das Ergebnis über `write_rows_to_csv()` nach
`output_dir/<name>.csv`. Ein HTTP-429 löst `handle_rate_limited()` aus
(setzt `rate_limited_event` **und** `stop_event`); jeder andere Fehler
wird geloggt, aber toleriert – ein einzelner missglückter Poll-Zyklus
beendet den Poller nicht.

### Konfigurierte Endpunkte (Standard `config.yaml`)

| Name | Pfad | Intervall | Begründung des Intervalls |
|---|---|---|---|
| `fees_precise` | `/api/v1/fees/precise` | 60 s | – |
| `mempool` | `/api/mempool` | 60 s | – |
| `prices` | `/api/v1/prices` | 60 s | Schreibt in dieselbe `prices.csv` wie der historische Import, siehe 6.6. |
| `difficulty_adjustment` | `/api/v1/difficulty-adjustment` | 300 s | Werte ändern sich nur ca. alle 10 Minuten (ein Block) – 60 s-Polling würde nur ~10 identische Zeilen pro Block ohne Mehrwert erzeugen. |
| `mining_pools_24h` | `/api/v1/mining/pools/24h` | 86400 s | Ein rollierendes 24h-Fenster bewegt sich zwischen Polls kaum. |

## 6.4 Ausgabeschema je Endpunkt

Jede Endpunkt-Zeile außer `prices.csv` (siehe unten) trägt ein
`polled_at_unix`-Feld (Unix-Epoch, UTC) – nur der Epoch-Wert wird
exportiert, da Splunk direkt darauf indiziert; ein zusätzliches
ISO-String-Feld wäre redundant.

**`fees_precise.csv`**

| Feld | Quelle |
|---|---|
| `polled_at_unix` | – |
| `fastest_fee_sat_vb` | `fastestFee` |
| `half_hour_fee_sat_vb` | `halfHourFee` |
| `hour_fee_sat_vb` | `hourFee` |
| `economy_fee_sat_vb` | `economyFee` |
| `minimum_fee_sat_vb` | `minimumFee` |

**`mempool.csv`**

| Feld | Quelle |
|---|---|
| `polled_at_unix` | – |
| `tx_count` | `count` |
| `vsize_total` | `vsize` |
| `total_fee_sats` | `total_fee` |

Das von mempool.space zurückgegebene `fee_histogram` (200+
`[feerate, cumulative_vsize]`-Buckets) wird bewusst **nicht** exportiert –
ein JSON-Blob in einer einzelnen CSV-Zelle bringt in Splunk keinen
Mehrwert; `tx_count`/`vsize_total`/`total_fee_sats` decken das relevante
skalare Signal ab.

**`prices.csv`** – Sonderfall gegenüber jeder anderen Endpunkt-CSV: Kein
`polled_at_unix`, dafür `date_unix` als der Preis eigene Zeitstempel
(nicht der Abrufzeitpunkt), und nur zwei Währungen:

| Feld | Quelle |
|---|---|
| `date_unix` | `time` – der Zeitstempel, den der Preis selbst trägt, nicht der Poll-Zeitpunkt. |
| `usd` | `USD` |
| `eur` | `EUR` |

Die übrigen von mempool.space zurückgegebenen Währungen (`GBP`, `CAD`,
`CHF`, `AUD`, `JPY`) werden nicht exportiert, da sie nirgends
nachgelagert verwendet werden. Diese Zeilenform (`date_unix,usd,eur`) ist
bewusst identisch zu der, die `import-price-history` aus den
Kraken-Exporten erzeugt (siehe 6.6) – beide Schreiber befüllen dieselbe
Datei, `mempool_api.output_dir/prices.csv`, ohne separate Tagestabelle.

**`difficulty_adjustment.csv`**

| Feld | Quelle |
|---|---|
| `polled_at_unix` | – |
| `progress_percent` | `progressPercent` |
| `difficulty_change_percent` | `difficultyChange` |
| `estimated_retarget_date_unix_ms` | `estimatedRetargetDate` |
| `remaining_blocks` | `remainingBlocks` |
| `remaining_time_seconds` | `remainingTime` |
| `previous_retarget_percent` | `previousRetarget` |
| `previous_retarget_time_unix` | `previousTime` |
| `next_retarget_height` | `nextRetargetHeight` |
| `block_time_avg_seconds` | `timeAvg` |
| `block_time_adjusted_avg_seconds` | `adjustedTimeAvg` |
| `time_offset_seconds` | `timeOffset` |
| `expected_blocks` | `expectedBlocks` |

**`mining_pools_24h.csv`** – eine Zeile **pro Pool** pro Poll (nicht eine
Zeile pro Poll insgesamt):

| Feld | Quelle |
|---|---|
| `polled_at_unix` | – |
| `network_block_count_24h` | `blockCount` (block-weit, auf jede Pool-Zeile dupliziert) |
| `hashrate_24h`, `hashrate_3d`, `hashrate_1w` | `lastEstimatedHashrate`, `lastEstimatedHashrate3d`, `lastEstimatedHashrate1w` (netzwerkweit, dupliziert) |
| `pool_id`, `pool_name`, `pool_slug`, `pool_rank` | `poolId`, `name`, `slug`, `rank` |
| `pool_block_count_24h`, `pool_empty_blocks_24h` | `blockCount`, `emptyBlocks` (pro Pool) |
| `pool_avg_match_rate`, `pool_avg_fee_delta` | `avgMatchRate`, `avgFeeDelta` |
| `pool_link` | `link` |

## 6.5 429-Verhalten

Ein HTTP-429 von `mempool.space` wird **nie automatisch wiederholt** –
`RateLimited` wird geworfen, `handle_rate_limited()` setzt sowohl
`rate_limited_event` als auch `stop_event`, und **jeder** Endpunkt-Thread
hält an. `run_poller()` gibt Exit-Code 1 zurück, wenn der Stopp durch
einen 429 ausgelöst wurde, sonst 0. Diese bewusste "kompletter Stopp
statt automatischer Wiederholung"-Entscheidung ist in Kapitel 9 näher
begründet.

Transiente Verbindungsfehler (Reset, SSL-EOF, DNS-Hänger) sind davon
getrennt: `client.py` wiederholt diese bis zu
`mempool_api.max_connection_retries`-mal mit
`retry_backoff_seconds`-Pause, bevor `FetchError` geworfen wird (die den
Poller nicht anhält, nur den betroffenen Zyklus überspringt).

## 6.6 Pricing-Pipeline

BTC-Preise, durchgängig minütlich, alles in einer einzigen Datei:
`mempool_api.output_dir/prices.csv`. Zwei unabhängige Schreiber befüllen
dieselbe `date_unix,usd,eur`-Zeilenform:

- **Der live `prices`-Endpunkt** (siehe 6.4) pollt mempool.space alle
  60 s und hängt eine Zeile an – `date_unix` ist der Zeitstempel des
  Preises selbst, nicht der Abrufzeitpunkt.
- **`import-price-history`** (`api/price_history_import.py`) füllt alles
  vor diesem live gepollten Fenster aus zwei Kraken-1-Minuten-OHLC-
  Exporten (`pricing.xbtusd_csv_path`/`pricing.xbteur_csv_path`, keine
  Kopfzeile, Spalten
  `unix_timestamp,open,high,low,close,volume,trades` – jeweils die
  "_1"-Intervall-Datei verwenden), auf Minutenzeitstempel gejoint,
  ausschließlich mit dem Schlusskurs.

Es gibt **keine** separate Tagestabelle und **keinen** automatischen
Lückenfüller-Thread mehr, der gegen mempool.spaces `historical-price`-
Endpunkt läuft – historische Tiefe kommt ausschließlich aus den beiden
lokalen Kraken-Dateien.

### `import-price-history` im Detail

1. Liest beide Kraken-CSVs vollständig ein (`_read_kraken_minute_closes()`)
   und baut je eine `{unix_timestamp: close}`-Abbildung.
2. Liest `prices.csv` vollständig zurück (`csv_parts_exist()`/
   `read_csv_parts()`, über alle rotierten Parts hinweg), um bereits
   vorhandene `date_unix`-Werte zu ermitteln.
3. Vereinigt beide Minutenmengen (`usd_by_minute.keys() | eur_by_minute.keys()`)
   abzüglich der bereits vorhandenen Minuten und schreibt für jede
   verbleibende Minute eine Zeile. Eine Minute, die nur in einer der
   beiden Dateien vorkommt, bekommt trotzdem eine Zeile – die andere
   Währung bleibt `null`, genau wie beim Live-Endpunkt, der gelegentlich
   eine Währung auslässt.
4. Macht **keine** Netzwerkanfrage und berührt das `mempool_api`-
   Rate-Limit-Budget nicht.

Idempotent: Bereits importierte Minuten werden anhand `date_unix`
übersprungen, ein erneuter Lauf gegen eine aktualisierte/erweiterte
Exportdatei fügt also nur das tatsächlich Neue hinzu. Fehlt eine der
beiden Kraken-Dateien, bricht der Import mit einer klaren Fehlermeldung
ab (Exit-Code 1), die auf die betroffene `pricing.*_csv_path`-Einstellung
verweist.

**Beliebige Reihenfolge relativ zu `api-poll`:** `import-price-history`
lässt sich vor oder nach dem ersten `api-poll`-Start ausführen, beliebig
oft wiederholt – da beide Schreiber dieselbe Datei mit identischem
Zeilenschema befüllen und der Import bereits vorhandene Minuten
überspringt, entstehen dabei keine Duplikate.

**Warum `prices.csv` unter `mempool_api.output_dir` liegt, nicht unter
einem eigenen Pricing-Verzeichnis:** `import-price-history` liest die
volle Historie dieser Datei zurück, um Duplikate zu vermeiden (Schritt 2
oben) – genau deshalb muss sie in einem Splunk-`monitor`-Verzeichnis
liegen (siehe Kapitel 7), das nie destruktiv gelöscht wird. Ein
separates Pricing-Verzeichnis mit anderer Aufnahmestrategie hätte dieses
Sicherheitsversprechen gebrochen.

### Workflow für einen neuen Node

1. Zwei Kraken-1-Minuten-OHLC-Exporte (XBTUSD, XBTEUR) besorgen und unter
   `pricing.xbtusd_csv_path`/`pricing.xbteur_csv_path` ablegen (nicht
   automatisiert – siehe Kapitel 9).
2. Einmalig `python run.py import-price-history` ausführen – füllt
   `prices.csv` mit der kompletten historischen Minutenzeitreihe, rein
   lokal.
3. `api-poll` starten (oder laufen lassen) – der live `prices`-Endpunkt
   übernimmt ab dem Moment des ersten erfolgreichen Polls nahtlos weiter,
   Minute für Minute.
4. Nach jeder Downtime bleibt lediglich eine Lücke in `prices.csv` für den
   Ausfallzeitraum – anders als zuvor gibt es dafür in dieser Version
   **keine** automatische Nachfüllung; siehe Kapitel 9 für die
   Einordnung.

## 6.7 Mining-Pool-Signaturdatenset

`api/mining_pools_dataset.py` (siehe auch Kapitel 4) verwendet **nicht**
den geteilten `mempool_api`-Token-Bucket – dieses Modul spricht mit
`raw.githubusercontent.com`, einem anderen Host mit eigenen Limits; ein
eigener, großzügig bemessener Ein-Slot-Bucket
(`_DATASET_FETCH_RATE_LIMIT`) existiert nur, um `ApiClient`s Retry-/
Timeout-Handling wiederzuverwenden, nicht um tatsächlich zu limitieren.

## 6.8 Beendigung

`run_poller()` läuft in der Hauptschleife bis `stop_event` gesetzt wird
(durch einen 429 oder extern). Bei `KeyboardInterrupt` (Strg+C, oder das
`SIGTERM`-Shim aus Kapitel 2) werden alle Endpunkt-Threads mit einem
Timeout von `request_timeout_seconds` gejoint, bevor der Prozess sich
beendet – ohne dieses Join würden bei Interpreter-Ende Daemon-Threads
einfach abgebrochen, eine gerade laufende HTTP-Anfrage oder ein
gepufferter CSV-Schreibvorgang also verloren gehen statt zu Ende geführt
zu werden.
