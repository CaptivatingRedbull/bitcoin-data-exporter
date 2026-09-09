# 6. API-Poller und Pricing

[← Index](00-index.md)

Kommando: `api-poll` · Einstiegspunkt: `btc_parser_app/api/poller.py::run_poller()`

## 6.1 Beteiligte Module

| Modul | Aufgabe |
|---|---|
| `api/poller.py` | Ein Thread pro konfiguriertem Endpunkt, jeder auf eigenem Intervall. |
| `api/rate_limiter.py` | Thread-sicherer Token-Bucket, der `mempool_api.rate_limit` durchsetzt. |
| `api/client.py` | Ratenbegrenzter HTTP-GET-Client (Retries, 429-Handling), von jedem mempool.space-Aufrufer geteilt. |
| `api/mempool_endpoints.py` | JSON-zu-Zeilen-Parser für den `prices`-Endpunkt + Registry. |
| `api/price_gap_backfill.py` | Eigenständiges, manuell gestartetes Skript (`backfill_price_gap.py`), das die Lücke zwischen dem einmaligen Kraken-CSV-Import und dem ersten live gepollten (per Cribl weitergeleiteten) Minutenwert über `historical-price` auffüllt. Kein `run.py`-Subkommando, keine config.yaml-Anbindung. |
| `api/mining_pools_dataset.py` | Siehe Kapitel 4 – nutzt denselben `ApiClient`, aber eine eigene, unabhängige Rate-Limit-Instanz (siehe 6.7). |

`mempool_api.endpoints` enthält derzeit genau einen Eintrag: `prices`
(siehe 6.3). `run_poller()` selbst ist unverändert generisch – ein neuer
Endpunkt lässt sich weiterhin allein über `config.yaml` plus eine neue
`parse_<name>`-Funktion hinzufügen, ohne `poller.py` anzufassen.

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

Das konfigurierte Budget ist 10 Anfragen/Minute (Burst 10), wird aber
derzeit nur vom live `prices`-Poll gezogen (60s-Intervall, ~1 Anfrage/min).
`backfill_price_gap.py` (siehe 6.6) ist ein eigenständiges Skript mit
eigenem `--rate-limit-per-minute` und zieht **nicht** aus diesem Budget.

## 6.3 Endpunkt-Threads (`poller.py`)

`run_poller()` startet für jeden in `mempool_api.endpoints` konfigurierten
Eintrag einen eigenen Thread (`endpoint_loop()`), der:

1. Um einen berechneten Start-Offset verzögert beginnt
   (`compute_start_offsets()`): Endpunkte mit demselben Intervall werden
   gleichmäßig über dieses Intervall gestaffelt, statt gleichzeitig zu
   feuern.
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

| Name | Pfad | Intervall |
|---|---|---|
| `prices` | `/api/v1/prices` | 60 s |

## 6.4 Ausgabeschema

**`prices.csv`**

| Feld | Quelle |
|---|---|
| `date_unix` | `time` – der Zeitstempel, den der Preis selbst trägt, nicht der Poll-Zeitpunkt. |
| `usd` | `USD` |
| `eur` | `EUR` |

Die übrigen von mempool.space zurückgegebenen Währungen (`GBP`, `CAD`,
`CHF`, `AUD`, `JPY`) werden nicht exportiert, da sie nirgends
nachgelagert verwendet werden. Diese Zeilenform (`date_unix,usd,eur`) ist
bewusst identisch zu der, die `backfill_price_gap.py` erzeugt (siehe 6.6)
– beide Schreiber befüllen dieselbe Datei, `mempool_api.output_dir/prices.csv`,
ohne separate Tagestabelle.

## 6.5 429-Verhalten

Ein HTTP-429 von `mempool.space` wird **nie automatisch wiederholt** –
`RateLimited` wird geworfen, `handle_rate_limited()` setzt sowohl
`rate_limited_event` als auch `stop_event`, und **jeder** Endpunkt-Thread
hält an. `run_poller()` gibt `EXIT_RATE_LIMITED` (75) zurück, wenn der
Stopp durch einen 429 ausgelöst wurde, sonst 0 – bewusst nicht der
generische Exit-Code 1, den auch ein unbehandelter Absturz liefern würde,
damit eine systemd-Unit per `RestartPreventExitStatus` zwischen beiden
Fällen unterscheiden kann (siehe Kapitel 2.5 und 9.3). Diese bewusste
"kompletter Stopp statt automatischer Wiederholung"-Entscheidung ist in
Kapitel 9 näher begründet.

Transiente Verbindungsfehler (Reset, SSL-EOF, DNS-Hänger) sind davon
getrennt: `client.py` wiederholt diese bis zu
`mempool_api.max_connection_retries`-mal mit
`retry_backoff_seconds`-Pause, bevor `FetchError` geworfen wird (die den
Poller nicht anhält, nur den betroffenen Zyklus überspringt).

## 6.6 Pricing-Pipeline

BTC-Preise, durchgängig minütlich (soweit verfügbar), alles in einer
einzigen Datei: `mempool_api.output_dir/prices.csv`. Mehrere unabhängige
Schreiber befüllen dieselbe `date_unix,usd,eur`-Zeilenform:

- **Der live `prices`-Endpunkt** (siehe 6.4) pollt mempool.space alle
  60 s und hängt eine Zeile an – `date_unix` ist der Zeitstempel des
  Preises selbst, nicht der Abrufzeitpunkt.
- **Ein einmaliger, extern durchgeführter Kraken-CSV-Import** deckt den
  Großteil der Historie ab (kein eigenes Tool in dieser App mehr).
- **`backfill_price_gap.py`** (`api/price_gap_backfill.py`) füllt die
  verbleibende, meist kurze Lücke zwischen dem Ende dieses Kraken-Imports
  und dem ersten live gepollten (per Cribl weitergeleiteten) Minutenwert.

### `backfill_price_gap.py` im Detail

Eigenständiges, manuell gestartetes Skript – kein `run.py`-Subkommando,
keine config.yaml-Anbindung. Parameter kommen ausschließlich über die
Kommandozeile:

```sh
python backfill_price_gap.py \
  --start-timestamp 1690000000 \
  --end-timestamp 1690003600 \
  --export-dir parser-data/export/api
```

- `--start-timestamp` / `--end-timestamp` – üblicherweise der letzte
  bereits durch den Kraken-Import abgedeckte `date_unix` bzw. der
  `date_unix` der ersten vom live Poller erfassten Minute.
- `--export-dir` – Verzeichnis für `prices.csv` und den eigenen
  Fortschritts-Checkpoint (`price_gap_backfill_state.csv`); auf
  `mempool_api.output_dir` zeigen lassen.
- `--rate-limit-per-minute` (Default 10) – **kein** Token-Bucket, nur ein
  flaches `time.sleep()` zwischen Anfragen: bewusst einfach gehalten, da
  dies ein kurzer, manuell begleiteter Einmallauf ist, kein
  Dauerbetrieb mit geteiltem Budget.

Ablauf pro Schritt:

1. Ruft `GET {base_url}/api/v1/historical-price?currency=<c>&timestamp=<t>`
   für die aktuelle Zeitmarke auf (Startwert: `--start-timestamp` bzw. der
   gespeicherte Checkpoint) und erhöht die Zeitmarke danach um 60s
   (passend zum Minutenschema von `prices.csv`).
2. Der Endpunkt rundet die angefragte Zeitmarke auf den tatsächlich
   nächstgelegenen mempool.space-Preispunkt (Granularität variiert mit
   dem Alter) – geschrieben wird daher der `time`-Wert aus der Antwort,
   nicht die angefragte Zeitmarke. Mehrere aufeinanderfolgende Anfragen
   können so denselben bereits geschriebenen Preispunkt treffen; das wird
   anhand von `date_unix` erkannt und übersprungen statt dupliziert.
3. Ein HTTP-429 wird **nie wiederholt**: geloggt (Konsole +
   `<export-dir>/price_gap_backfill.log`), Prozess beendet sich sofort
   mit `EXIT_RATE_LIMITED` (75, dieselbe Konvention wie `api/poller.py`,
   siehe 6.5) – der Checkpoint bleibt exakt am letzten erfolgreichen
   Schritt stehen.
4. Nach jedem Schritt wird der Fortschritt in
   `<export-dir>/price_gap_backfill_state.csv` persistiert (Zeitmarke,
   nicht das Ergebnis) – für manuelle Neustarts (siehe unten).

**Neustart-sicher (manuell, nicht als Dienst gedacht):** Ein erneuter
Lauf mit demselben `--start-timestamp` setzt am gespeicherten Checkpoint
fort statt von vorne zu beginnen – egal ob der vorige Lauf durch einen
429, einen Fehler oder Strg+C beendet wurde. Ein anderes
`--start-timestamp` wird als neue, unabhängige Lücke behandelt und
verwirft den alten Checkpoint. Da zusätzlich jede Zeile anhand
`date_unix` gegen die bereits in `prices.csv` vorhandenen Werte geprüft
wird, entstehen auch bei überlappenden Läufen keine Duplikate.

**Warum `prices.csv` unter `mempool_api.output_dir` liegt, nicht unter
einem eigenen Pricing-Verzeichnis:** `backfill_price_gap.py` liest die
volle Historie dieser Datei zurück, um Duplikate zu vermeiden (siehe
oben) – genau deshalb muss sie in einem Splunk-`monitor`-Verzeichnis
liegen (siehe Kapitel 7), das nie destruktiv gelöscht wird. Ein
separates Pricing-Verzeichnis mit anderer Aufnahmestrategie hätte dieses
Sicherheitsversprechen gebrochen.

### Workflow für einen neuen Node

1. Historische BTC/USD+EUR-Preise extern besorgen und einmalig in
   `prices.csv` einspielen (Kraken-CSV-Export o.ä. – nicht mehr Teil
   dieser App).
2. `api-poll` starten (oder laufen lassen) – der live `prices`-Endpunkt
   übernimmt ab dem Moment des ersten erfolgreichen Polls Minute für
   Minute.
3. Die verbleibende Lücke zwischen Schritt 1 und dem ersten erfolgreichen
   Poll aus Schritt 2 mit `backfill_price_gap.py` auffüllen (siehe oben).
4. Nach jeder weiteren Downtime bleibt lediglich eine neue Lücke in
   `prices.csv` für den Ausfallzeitraum – siehe Kapitel 9 für die
   Einordnung; ggf. erneut mit `backfill_price_gap.py` schließen.

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
