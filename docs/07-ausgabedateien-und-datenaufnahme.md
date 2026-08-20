# 7. Ausgabedateien und Datenaufnahme

[← Index](00-index.md)

Dieses Kapitel fasst **alle** von der Anwendung erzeugten Dateien an
einer Stelle zusammen und beschreibt, wie sie sicher in Splunk (oder ein
vergleichbares System) eingelesen werden können. Für die genauen
Spaltenschemata siehe die jeweiligen Kapitel (4, 5, 6).

## 7.1 Verzeichnisübersicht (Standard-`config.yaml`, relativ zu `full_app/`)

```
full_app/
  logs/                          hartcodiertes Ziel für start.sh's rohe
                                  *.out-Redirects (NICHT logging.log_dir)
    rpc-ingest.out
    stale-blocks-ingest.out
    api-poll.out
  .pids/                         von start.sh verwaltete PID-Dateien
  parser-data/                   Wurzelverzeichnis für alles Laufzeitdaten-
                                  bezogene (Standard-config.yaml) - erst
                                  beim ersten Lauf angelegt
    logs/                        logging.log_dir - strukturierte, rotierende
                                  <kommando>.log-Dateien, nicht für Splunk
    state/
      rpc/                       rpc.state_dir (NICHT für Splunk)
        current.csv
        latest.csv
        block_status.csv
        blocks_part_seq.csv
        transactions_part_seq.csv
        index/
          index.csv (+ .000002.csv, ...)
        reorg/
          reorg_<timestamp>_<lowest>_<highest>.csv
        blocks/                  nur solange Backlog aufzuholen ist:
        transactions/             der aktuell noch wachsende Part
      stale/                     stale_blocks.state_dir (NICHT für Splunk)
        registry.csv
    export/
      rpc/                       rpc.output_dir - für Splunk `batch`
        blocks/
          blocks.000001.csv, blocks.000002.csv, ...
        transactions/
          transactions.000001.csv, ...
      api/                       mempool_api.output_dir - für Splunk `monitor`
        fees_precise.csv
        mempool.csv
        prices.csv                (Live-Poll UND Kraken-Import, siehe Kap. 6)
        difficulty_adjustment.csv
        mining_pools_24h.csv
      stale/                     stale_blocks.output_dir - für Splunk `monitor`
        stale_block_headers.csv (+ .000002.csv, ...)
  config/
    config.yaml / config.production.yaml
    pools-v2.json                 Mining-Pool-Signaturdatenset
    XBTUSD_1.csv / XBTEUR_1.csv   (nicht mitgeliefert) Kraken-1-Minuten-Exporte
```

Auf dem Produktivhost liegt `parser-data/` komplett auf einem separaten,
großen Datenvolume statt relativ zu `full_app/` (siehe
`config/config.production.yaml` und Kapitel 2.5) – `logging.log_dir`
eingeschlossen. Nur `full_app/logs/*.out` (die rohen stdout/stderr-
Redirects von `start.sh`) bleiben davon unberührt, da dieser Pfad in
`start.sh` selbst hart codiert ist (siehe Kapitel 2.3).

## 7.2 Alle erzeugten Dateien im Überblick

| Datei | Verzeichnis | Charakter | Für Splunk? | Details |
|---|---|---|---|---|
| `blocks/blocks*.csv` | `rpc.output_dir` | Append-only/atomar, rotiert | Ja (`batch`) | Kapitel 4.6, 4.9 |
| `transactions/transactions*.csv` | `rpc.output_dir` | Append-only/atomar, rotiert | Ja (`batch`) | Kapitel 4.6, 4.10 |
| `index/index.csv` | `rpc.state_dir/index` | Append-only, rotiert | Nein (intern) | Kapitel 4.5 |
| `current.csv` | `rpc.state_dir` | 1 Zeile, überschrieben | Nein (intern) | Kapitel 4.5 |
| `latest.csv` | `rpc.state_dir` | 1 Zeile, überschrieben | Nein (intern) | Kapitel 4.5 |
| `block_status.csv` | `rpc.state_dir` | veränderlich, überschrieben | Nein (intern) | Kapitel 4.5 |
| `reorg/reorg_*.csv` | `rpc.state_dir/reorg` | 1 Datei pro Ereignis | Nein (intern, Audit) | Kapitel 4.5 |
| `blocks_part_seq.csv` / `transactions_part_seq.csv` | `rpc.state_dir` | 1 Zeile, überschrieben | Nein (intern) | Kapitel 4.6 |
| `stale_block_headers.csv` | `stale_blocks.output_dir` | Append-only, rotiert | Ja (`monitor`) | Kapitel 5.6 |
| `registry.csv` | `stale_blocks.state_dir` | veränderlich, überschrieben | **Nein** | Kapitel 5.7 |
| `fees_precise.csv` | `mempool_api.output_dir` | Append-only, rotiert | Ja (`monitor`) | Kapitel 6.4 |
| `mempool.csv` | `mempool_api.output_dir` | Append-only, rotiert | Ja (`monitor`) | Kapitel 6.4 |
| `prices.csv` | `mempool_api.output_dir` | Append-only, rotiert | Ja (`monitor`, **nie löschen**) | Kapitel 6.4, 6.6 |
| `difficulty_adjustment.csv` | `mempool_api.output_dir` | Append-only, rotiert | Ja (`monitor`) | Kapitel 6.4 |
| `mining_pools_24h.csv` | `mempool_api.output_dir` | Append-only, rotiert | Ja (`monitor`) | Kapitel 6.4 |
| `pools-v2.json` | `mining_pools_dataset.local_path` | ganze Datei überschrieben | Nein (Konfigurationsdaten) | Kapitel 4.8 |
| `<kommando>.log` | `logging.log_dir` | rotierend (20 MB × 5) | Nein (Betriebslog) | Kapitel 9 |
| `<kommando>.out` | `full_app/logs/` (hart codiert) | rohes stdout/stderr | Nein (Betriebslog) | Kapitel 2 |

"Intern"/"Nein" markierte Dateien sind Zustands- bzw. Buchführungsdateien
der Anwendung selbst – sie in Splunk zu indizieren bringt keinen
analytischen Mehrwert und bläht den Index unnötig auf. Es gibt in dieser
Version keine "optionalen Audit-Dateien" mehr, die absichtlich in
`output_dir` liegen könnten: `state_dir` und `output_dir` sind pro
Komponente strikt getrennt (siehe 7.4), sodass ein Splunk-Input, der auf
`output_dir` zeigt, `block_status.csv`/`reorg/`/`registry.csv` gar nicht
erst zu Gesicht bekommt.

## 7.3 Dateirotation

Jede Append-only-CSV dieser Anwendung wächst unbegrenzt, daher deckelt
`btc_parser_app/common/csv_writer.py` jeden Dateipart auf ~900 MB
(`MAX_PART_BYTES`) und rollt vor Überschreiten in einen neuen
nummerierten Part: `<name>.csv` ist der erste Part, danach
`<name>.000002.csv`, `<name>.000003.csv` usw., im selben Verzeichnis.
Rotation wird nur vor einem Schreibvorgang geprüft, daher kann ein Part
das Limit um höchstens einen Batch Zeilen überschreiten – nie unbegrenzt
wachsen.

Am Layout ändert sich sonst nichts – es bleibt logisch eine CSV, nur
aufgeteilt in Dateien, die nie ~1 GB überschreiten. Alles in der
Anwendung, das eine logische CSV vollständig zurücklesen muss (der Index,
das Bootstrapping aus `blocks/blocks.csv`, `prices.csv`s
Existenz-/Datumsprüfungen), liest automatisch alle Parts in Reihenfolge
(`csv_writer.py::read_csv_parts()`).

**`blocks/blocks*.csv` und `transactions/transactions*.csv` sind ein
Sonderfall** (siehe Kapitel 4.6): Sie rotieren nach demselben ~900-MB-
Prinzip, sind aber zusätzlich über zwei Verzeichnisse verteilt
(`state_dir` während Backlog, `output_dir` sobald ein Part abgeschlossen
ist oder der Prozess eingeholt hat) und wechseln zwischen Batch- und
atomarem Schreibmodus. Unter `output_dir` erscheint dabei nie ein noch
wachsender Part.

**Wichtig für jedes externe Tool** (Splunk, eine Monitor-Stanza,
Ad-hoc-Skripte), das direkt auf diese Dateien zugreift: Immer auf
`<name>*.csv` globben statt auf den exakten ersten Dateinamen, sonst
werden rotierte Parts nach dem ersten nie gesehen.

## 7.4 Storage-Strategie und Splunk-Anbindung

### Ausgangslage

Der Parser-Host hat typischerweise ein festes, begrenztes Speicherbudget,
das er sich mit dem eigentlichen Bitcoin-Core-Datenverzeichnis teilt (in
der Zielumgebung z. B. 1 TB gesamt, wovon Bitcoin Core allein bereits
~720 GB belegt und kontinuierlich weiter wächst). Die exportierten
CSV-Dateien sind also **nicht** als dauerhafter Datenspeicher auf diesem
Host gedacht – Splunk (oder was auch immer sie konsumiert) muss sie
zeitnah tatsächlich abholen.

### Rolle dieser Anwendung

Die Anwendung bleibt bei diesem Thema bewusst passiv: Innerhalb von
`export/` fasst sie eine einmal geschriebene Datei nie mehr an,
verschiebt oder löscht sie nie. **Welcher rotierte Teil wann gelöscht
werden darf, ist eine Entscheidung, die außerhalb dieser Anwendung
getroffen wird** – über die Splunk-Input-Konfiguration (`inputs.conf`)
oder ein eigenes, manuell/per Cron ausgeführtes Aufräum-Skript, nachdem
verifiziert wurde, dass Splunk die Datei tatsächlich indiziert hat.

Es gibt in der Anwendung absichtlich **keinen** automatischen
Lösch-/Verschiebe-Mechanismus – ein Konfigurationsfehler bei einer
Selbstlöschung wäre eine Fehlkonfiguration von Splunk entfernt davon,
unwiederbringliche Daten zu verlieren (ein erneuter Parse-Lauf ab Genesis
ist teuer; ein erneuter Abruf der historischen Preis-/API-Daten unter
Umständen gar nicht mehr möglich).

### Eine feste Zuordnung pro `export/`-Unterverzeichnis

`parser-data/` ist bewusst in zwei Verzeichnisarten aufgeteilt
(`state/` vs. `export/`, siehe 7.1), speziell damit sich jedes
`export/`-Unterverzeichnis dauerhaft mit **einem einzigen**
Splunk-Input-Modus verbinden lässt – kein Umschalten zwischen
Genesis-Backfill und stationärem Betrieb, kein Ausschließen der jeweils
neuesten Datei, keine zusätzliche Betriebsdisziplin nötig:

- **`export/rpc/{blocks,transactions}/` → `batch` (Sinkhole,
  konsumieren-und-löschen).** Ein Part erscheint hier immer erst, wenn er
  vollständig abgeschlossen ist – während einer Backlog-Aufholjagd wird
  er komplett aus `state_dir` herübergeschoben, sobald er fertig ist
  (nie während er noch beschrieben wird); im eingeholten Zustand wird
  jeder Block-Part direkt hier fertig geschrieben (siehe Kapitel 4.6).
  In beiden Fällen ist nie ein noch wachsender Part unter `export/rpc/`
  sichtbar – ein `batch`-Input kann hier jederzeit alles konsumieren und
  löschen, ohne etwas ausschließen zu müssen.
- **`export/api/` und `export/stale/` → `monitor` (nicht-destruktives
  Tailing).** Das sind keine atomaren Pro-Block-Writes, sondern klassisch
  angehängte, größenrotierte Dateien (siehe 7.3) – der jeweils aktuelle
  Part wächst laufend weiter. Ein `batch`-Input würde hier riskieren,
  eine noch wachsende Datei mitten im Schreiben zu greifen. `monitor`
  löscht nie; alte rotierte Parts müssen von Hand oder per eigenem
  Cron-Job aufgeräumt werden, sobald die Indizierung durch Splunk
  bestätigt ist. Für `export/api/prices.csv` gilt das zusätzlich
  zwingend: `import-price-history` liest diese Datei vollständig zurück,
  um Duplikate zu vermeiden (Kapitel 6.6) – sie darf also nie unter der
  Anwendung weggelöscht werden.

### Empfehlung

Da die Zuordnung fest ist (nicht mehr phasenabhängig wie in früheren
Versionen), reicht eine einmalige Einrichtung: `batch`-Input auf
`export/rpc/`, `monitor`-Inputs auf `export/api/` und `export/stale/` –
unabhängig davon, ob gerade eine initiale Genesis-Aufholjagd läuft oder
der Prozess längst dem Tip folgt. **Niemals** einen Splunk-Input auf ein
`state/`-Verzeichnis zeigen lassen.

## 7.5 Zeitfelder und Zeitzonen

Alle Zeitstempel in dieser Anwendung sind, sofern nicht anders vermerkt,
UTC:

- `blocks.csv`/`transactions.csv`: `time`/`block_time` sind Unix-Epoch
  (Sekunden), wie von Bitcoin Core geliefert.
- Jede mempool.space-Endpunkt-CSV außer `prices.csv`: `polled_at_unix`
  ist Unix-Epoch (UTC), vom Poller selbst zum Zeitpunkt der Antwort
  erzeugt.
- `prices.csv`: `date_unix` ist der Zeitstempel, den der Preis selbst
  trägt (bei Live-Polls) bzw. die Minute des Kraken-Kerzen-Exports (beim
  Import) – nicht der Abrufzeitpunkt.
- `stale_block_headers.csv`/`registry.csv`: `observed_at`/`first_seen`
  sind ISO-8601-UTC-Strings (`%Y-%m-%dT%H:%M:%SZ`).
- Log-Dateien: `%Y-%m-%dT%H:%M:%S%z` (inkl. lokaler Zeitzonen-Offset des
  Hosts).

## 7.6 Encoding und CSV-Konventionen

Alle CSV-Dateien werden mit `polars` geschrieben (UTF-8, Standard-
CSV-Quoting). Fehlende/`None`-Werte erscheinen als leere Felder. Jede
Datei hat genau eine Kopfzeile pro Part (nicht pro logischer CSV) – beim
Zurücklesen mehrerer Parts muss also die Kopfzeile jedes einzelnen Parts
korrekt behandelt werden; `read_csv_parts()` übernimmt das für alles
innerhalb der Anwendung automatisch.
