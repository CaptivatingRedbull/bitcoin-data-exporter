# 10. Splunk-Integration und Feldkonzept

[← Index](00-index.md)

Dieses Kapitel ergänzt Kapitel 7 (das beschreibt, **was die Anwendung
selbst erzeugt**) um die konkrete, umsetzbare Splunk-seitige
Konfiguration: welche Dateien liegen den fertigen `.conf`-Dateien unter
[`splunk/`](../splunk/) zugrunde, welches Feld wird in Splunk zu welchem
indizierten Feld, und warum. Bei Widersprüchen zwischen diesem Kapitel und
den tatsächlichen `.conf`-Dateien unter `splunk/` sind Letztere
maßgeblich.

## 10.1 Test- vs. Produktivumgebung

| | Test | Produktiv |
|---|---|---|
| Weg der Daten | Parser, Inputs und Splunk Enterprise auf **einem** Host (Standalone-Instanz, kein UF dazwischen) | Universal Forwarder → **Cribl Stream** → Splunk Enterprise |
| Wer liest die CSVs | Splunk Enterprise selbst (`TA-btc-parser-inputs`) | der UF (`TA-btc-parser-inputs`) |
| Index-Time-Verarbeitung (Header verwerfen, `_time`, welche Spalten indizierte Felder werden) | `TA-btc-parser-parsing` (`props.conf`/`transforms.conf`) auf derselben Instanz | Cribl-Pipeline [`cribl/btc_parser_pipeline.json`](../cribl/btc_parser_pipeline.json) – Splunk behandelt Daten aus Cribl als bereits geparst und wendet **keine** Index-Time-`props`/`transforms` mehr an |
| Search-Time-Felder | `TA-btc-parser-search` | `TA-btc-parser-search` (Search Head) – identisch |
| Index, Durchsatz | `TA-btc-parser-indexes`, `TA-btc-parser-ingest-tuning` | dieselben auf dem Indexer (Tuning zusätzlich auf dem UF wegen `maxKBps`) |

Beide Wege erzeugen dieselben Events: `_raw` ist die CSV-Zeile, dieselben
Spalten sind indizierte Felder, der Rest wird zur Suchzeit von derselben App
extrahiert. Dashboards und Suchen laufen deshalb unverändert in beiden
Umgebungen. Die Cribl-Einrichtung beschreibt
[`cribl/README.md`](../cribl/README.md).

## 10.2 Apps und Zielorte

Alle feldbezogenen Dateien werden von
[`splunk/build_field_config.py`](../splunk/build_field_config.py) aus
[`btc_parser_app/export_schema.py`](../btc_parser_app/export_schema.py)
erzeugt (Spaltenreihenfolge + indizierte Felder je Sourcetype) und nie von
Hand bearbeitet. `tests/test_export_schema.py` prüft, dass das Schema exakt
den Zeilen entspricht, die `block_parser.py` schreibt, spielt die erzeugten
Regexe gegen echte Zeilen ab und schlägt fehl, wenn eine erzeugte Datei
veraltet ist.

| App | Wohin | Inhalt |
|---|---|---|
| [`TA-btc-parser-inputs/`](../splunk/TA-btc-parser-inputs/) | Instanz, die die Dateien liest (Test: Standalone; Prod: UF) | nur `inputs.conf` (plus optional `outputs.conf`) |
| [`TA-btc-parser-parsing/`](../splunk/TA-btc-parser-parsing/) | **nur** Test-Standalone | Index-Time: `props.conf`/`transforms.conf`, siehe 10.6. **Nie auf einen UF**: `INDEXED_EXTRACTIONS` würde den UF die kleinen Sourcetypes selbst parsen und jede Spalte als indiziertes Feld an Cribl schicken lassen. Hinter Cribl wirkungslos. |
| [`TA-btc-parser-search/`](../splunk/TA-btc-parser-search/) | Search Head (Test: Standalone) | Search-Time: `REPORT`-Extraktion der nicht indizierten Spalten, `fields.conf`, `KV_MODE = none`; per `metadata/default.meta` systemweit exportiert |
| `TA-btc-parser-indexes/`, `TA-btc-parser-ingest-tuning/` | Indexer (Tuning auch auf dem UF) | Kapitel 12 |
| [`cribl/btc_parser_pipeline.json`](../cribl/btc_parser_pipeline.json) | Prod: Cribl | Index-Time-Gegenstück zu `TA-btc-parser-parsing` |

## 10.3 Verzeichnis-zu-Input-Zuordnung

Direkte Umsetzung von Kapitel 7.4 in `inputs.conf`-Stanzas
([`TA-btc-parser-inputs/local/inputs.conf`](../splunk/TA-btc-parser-inputs/local/inputs.conf)):

| Verzeichnis | Splunk-Input | Warum |
|---|---|---|
| `export/rpc/{blocks,transactions,inputs,outputs}/` | `batch` (`move_policy = sinkhole`) | Ein Part erscheint hier laut Kapitel 4.6 immer erst vollständig fertig – nie während er noch beschrieben wird. `batch` kann ihn deshalb gefahrlos konsumieren und löschen. |
| `export/api/`, `export/stale/`, `export/mempool_watch/` | `monitor` | Der aktuelle Part wächst hier laufend weiter (Kapitel 3.2/5.5/11.6). Ein `batch`-Input würde riskieren, eine noch offene Datei mitten im Schreiben zu greifen. `monitor` löscht nie – alte rotierte Parts müssen separat (Skript/Cron) aufgeräumt werden, sobald Splunk sie nachweislich indiziert hat. |
| Jedes `state/`-Verzeichnis | **kein Splunk-Input** | Siehe 10.5 – interne Buchführung, nie ansprechen. |

**`crcSalt = <SOURCE>` auf jeder einzelnen Stanza:** Splunks Standard-
Mechanismus zur Duplikaterkennung ("wurde diese Datei schon indiziert?")
hasht nur die ersten/letzten paar hundert Byte des Dateiinhalts. Da jeder
Part derselben logischen CSV mit exakt derselben Kopfzeile beginnt
(Kapitel 7.6), kann Splunk ohne `crcSalt` einen neuen, noch nie gesehenen
Part fälschlich für einen bereits konsumierten/gelöschten halten und ihn
stillschweigend überspringen. `crcSalt = <SOURCE>` bezieht stattdessen den
vollständigen Dateipfad (inkl. Part-Nummer) in die Prüfung ein und macht
sie damit eindeutig – ein Standard-Stolperstein bei vielen kleinen Dateien
mit identischer Kopfzeile, nicht spezifisch für diese Anwendung, aber hier
garantiert relevant.

## 10.4 Sourcetypes und Zeitfelder

Jede Datei bekommt einen eigenen Sourcetype (`btc:<name>`), weil jede ein
eigenes Spaltenschema hat (Kapitel 4.9–4.12, 5.6, 6.4, 11.6) – ein gemeinsamer
Sourcetype für mehrere Schemata würde Feld-Extraktion und
Zeitstempel-Zuordnung gegenseitig stören.

| Datei | Sourcetype | Zeitfeld → `_time` | Format |
|---|---|---|---|
| `blocks*.csv` | `btc:blocks` | `time` (Unix-Epoch) | `%s` |
| `transactions*.csv` | `btc:transactions` | `block_time` (Unix-Epoch, vom Block übernommen) | `%s` |
| `inputs*.csv` | `btc:inputs` | `block_time` | `%s` |
| `outputs*.csv` | `btc:outputs` | `block_time` | `%s` |
| `stale_block_headers*.csv` | `btc:stale_block_headers` | `observed_at` (ISO-8601 UTC) | `%Y-%m-%dT%H:%M:%SZ` |
| `sanctioned_tx_events*.csv` | `btc:sanctioned_tx_events` | `observed_at` (Zeitpunkt des Ereignisses, ISO-8601 UTC) | `%Y-%m-%dT%H:%M:%SZ` |
| `prices*.csv` | `btc:prices` | `date_unix` (Preis-eigener Zeitstempel, **nicht** Poll-Zeitpunkt) | `%s` |

Alle Zeitstempel sind UTC (Kapitel 7.5) – jede Stanza setzt deshalb
zusätzlich explizit `TZ = UTC`, statt sich auf die Host-Zeitzone des
Forwarders/Indexers zu verlassen.

`TIMESTAMP_FIELDS` statt einer positionsbasierten `TIME_PREFIX`/
`MAX_TIMESTAMP_LOOKAHEAD`-Regel: Da ohnehin strukturierte CSV-Extraktion
(`INDEXED_EXTRACTIONS = csv`) läuft, kann Splunk den Zeitstempel direkt
aus dem benannten, bereits geparsten Feld nehmen – robuster als eine
Zeichenposition, die bricht, sobald sich die Spaltenreihenfolge einer
`.csv` in einer zukünftigen Version ändert.

## 10.5 Welche Daten Splunk überhaupt sehen

**Grundsatzentscheidung, auf Verzeichnisebene, nicht auf Feldebene:** Jede
`state/`-Datei (`current.csv`, `latest.csv`, `block_status.csv`,
`index/index.csv`, `reorg/*.csv`, `registry.csv`, `flagged.json`, die
`*_part_seq.csv`-Zähler) bekommt **keinen** Splunk-Input. Gründe (siehe
auch Kapitel 7.2):

- Es sind reine Buchführungs-/Zustandsdateien der Anwendung selbst – kein
  eigenständiger Analysewert, der nicht bereits über die `export/`-Dateien
  verfügbar wäre.
- Mehrere davon (`current.csv`, `latest.csv`, `block_status.csv`,
  `registry.csv`, die `*_part_seq.csv`-Dateien) werden **an Ort und
  Stelle überschrieben**, nicht angehängt. Ein `monitor`-Input auf einer
  laufend überschriebenen Datei erzeugt in Splunk pro Änderung ein neues,
  scheinbar widersprüchliches Event derselben "Zeile" – ohne den
  Two-Phase-Commit-Kontext (Kapitel 4.5), der diese Überschreibungen erst
  konsistent macht.
- `block_status.csv` würde ausgerechnet den Reorg-Mechanismus verwässern:
  ein Block, der `canonical=true → false → true` durchläuft, bekommt in
  `blocks.csv`/`transactions.csv` bewusst **keinen** erneuten Export
  (Kapitel 4.4) – ein Splunk-Input auf `block_status.csv` würde diese
  Flips als eigene Zeitreihe zeigen, obwohl die zugehörigen Detail-Zeilen
  in Splunk unverändert blieben, was zu Verwirrung bei der Auswertung
  führen würde.

## 10.6 Feldebene: welche Spalten indizierte Felder werden

Jede Spalte bleibt in `_raw` und ist zur Suchzeit verfügbar. Entschieden wird
nur, welche Spalten **zusätzlich** als indiziertes Feld in die tsidx-Dateien
kommen. Das kostet pro Event einen Postings-Eintrag je Feld und pro
unterschiedlichem Wert einen Lexikon-Eintrag. Dafür bekommt man `tstats`
(Gruppieren/Filtern über Jahre in Sekunden statt Stunden) und eine
eindeutige Feldsuche bei Werten, die als Rohtext mehrdeutig sind.

**Regeln** (umgesetzt in `export_schema.py`):

1. **Kleine Sourcetypes – alles indiziert.** `btc:blocks` (~1 Zeile pro
   Block), `btc:stale_block_headers`, `btc:sanctioned_tx_events`,
   `btc:prices`: `INDEXED_EXTRACTIONS = csv` (Test) bzw. alle Spalten
   behalten (Cribl). Die Kosten sind bei diesem Volumen vernachlässigbar, und
   `blocks` trägt bereits jede Block-Aggregation (Gebühren, Skripttyp-Zähler,
   SegWit/Taproot/RBF/OP_RETURN, CDD, Pool). Langfristige Trends gehören
   deshalb hierher, per `tstats` über die ganze Chain.
2. **`btc:transactions`/`btc:inputs`/`btc:outputs` (Milliarden Zeilen) – nur
   niedrig-kardinale Dimensionen**, nach denen man über lange Zeiträume
   gruppiert/filtert und deren Rohwerte (`true`, `2`, …) als Suchbegriff
   nichts eingrenzen:

   | Sourcetype | indiziert | Spalten gesamt |
   |---|---|---|
   | `btc:transactions` | `block_height`, `is_coinbase`, `version`, `vin_count`, `vout_count` | 63 |
   | `btc:inputs` | `block_height`, `is_coinbase`, `prevout_type`, `prevout_generated`, `relative_locktime_type` | 23 |
   | `btc:outputs` | `block_height`, `is_coinbase`, `script_type` | 12 |

3. **Nicht indiziert: Hashes, IDs, Adressen** (`txid`, `wtxid`,
   `block_hash`, `prevout_txid`, `address`, `prevout_address`,
   `coinbase_hex`). Jeder Wert ist bereits ein eindeutiges Token im
   Rohtext-Index. `txid=…` oder `address=…` wird über diesen Index und die
   Bloom-Filter je Bucket genauso schnell gefunden wie über ein indiziertes
   Feld. Eine indizierte Kopie würde nur den größten Teil der tsidx
   verdoppeln. Das gilt auch für Subsearches wie den Sanktionslisten-Abgleich
   (`[| inputlookup … | eval search="address=…"]`).
4. **Nicht indiziert: Messwerte** (`value_sats`, `fee_sats`,
   `fee_rate_sat_vb`, Größen, Alter, Zähler) und die 24
   `vin_type_*`/`vout_type_*`-Zähler je Transaktion, die `blocks` bereits
   pro Block summiert.
5. **Ein Feldname, eine Entscheidung.** `fields.conf` (`INDEXED = true`)
   gilt global, nicht je Sourcetype. Ein Name, der in einem Sourcetype
   indiziert ist, muss es überall sein, wo er als Spalte vorkommt (deshalb
   z. B. `is_coinbase` auch auf `btc:inputs`). Der Generator bricht sonst ab.

**Technik (Test-Standalone, `TA-btc-parser-parsing`):** Für die drei großen
Sourcetypes kein `INDEXED_EXTRACTIONS`. Klassischer Zeilenumbruch, `_time`
per `TIME_PREFIX` aus der Spaltenposition von `block_time`, Kopfzeile per
`nullQueue` verworfen, und je indiziertem Feld eine
`WRITE_META`-Transform, deren Regex auf genau diese Spalte festgelegt ist
und einen nicht-leeren Wert verlangt. Ein `null` (z. B. `prevout_type` der
Coinbase-Eingabe) erzeugt so gar kein Feld statt eines leeren.
`TA-btc-parser-search` extrahiert die übrigen Spalten zur Suchzeit mit einer
verankerten Regex über die ganze Zeile. Die indizierten Spalten werden dort
übersprungen, sonst wären sie doppelte Multivalue-Felder. Die Zeilen
enthalten nie Kommas in Werten (nur Hashes, Zahlen, Booleans, Skripttypen,
Adressen), daher ist das Zerlegen an jedem Komma exakt.

**Folge für SPL:** `tstats` funktioniert auf den großen Sourcetypes nur noch
mit den indizierten Feldern oben (plus `_time`, `sourcetype`, …).
`tstats max(fee_sats)`, `perc90(fee_rate_sat_vb)`, `sum(value_sats) by address`
liefern dort nichts mehr. Solche Auswertungen laufen als normale Suche mit
`stats`/`timechart` über ein begrenztes Zeitfenster, oder langfristig über
die Block-Aggregate in `btc:blocks`. Beispiele für beides enthält das
Benchmark-Skript [`splunk/scripts/btc-splunk-bench.sh`](../splunk/scripts/btc-splunk-bench.sh)
(Gruppen T und R). Die Dashboards/Saved Searches in `btc-parser-dashboards`
sind darauf noch nicht umgestellt.

`TRUNCATE` bleibt auf dem Standard: Die einzigen potenziell sehr langen
Rohfelder (`scriptsig_hex`/`script_hex`) werden gar nicht erst exportiert
(Kapitel 4.11/4.12), die längste Zeile (`transactions.csv`) liegt weit unter
10.000 Byte.

## 10.7 Index

Retention und Größe des Index sind für historische Daten ab 2009
kritisch – Splunks Standardwerte würden den Großteil der Chain löschen;
siehe Kapitel 12 und `splunk/TA-btc-parser-indexes/`.

Alle Sourcetypes verwenden in `inputs.conf` denselben Index `btc_parser`
(vor dem ersten Start auf dem Indexer anlegen – `TA-btc-parser-indexes`,
Kapitel 12; Indizes werden nicht automatisch erstellt). Ein gemeinsamer Index für alle Sourcetypes
genügt hier, weil Zugriffskontrolle und Aufbewahrungsfristen für alle
Dateien dieser Anwendung identisch sind; eine Aufteilung nach Index lohnt
sich erst, sobald das für einzelne Sourcetypes nicht mehr zutrifft (z. B.
unterschiedliche Retention für `btc:prices` vs. den Rest).

## 10.8 Testdaten-Staging (kleines Sample statt vollem Export)

Für einen ersten Test mit begrenztem Ingest-Volumen (z. B. ein enges
Lizenz-Tageslimit): **niemals** die `batch`-Stanzas aus 10.3 direkt
gegen den echten, produktiven `export/rpc/`-Ordner laufen lassen, nur um
ein paar Dateien probeweise einzulesen – `batch` konsumiert **und
löscht** jede erfasste Datei unwiderruflich (10.3). Stattdessen die
gewünschten Beispiel-Parts (z. B. den jeweils letzten
`blocks.NNNNNN.csv`/`transactions.NNNNNN.csv`/`inputs.NNNNNN.csv`/
`outputs.NNNNNN.csv`) in ein separates, eigens dafür angelegtes
Staging-Verzeichnis **kopieren** und die Test-`inputs.conf` gegen diese
Kopie zeigen lassen. So bleibt der echte Export unangetastet, falls beim
Ausprobieren etwas falsch konfiguriert ist.

Größenanhaltspunkt: Jeder Part ist auf ~900 MB gedeckelt (Kapitel 7.3);
vier "letzte" Parts (einer je Datei-Typ) können also im ungünstigsten
Fall zusammen nahe an ein enges Ingest-Budget herankommen. Vor dem
Hochladen mit `ls -lh` die tatsächliche Größe prüfen – der jeweils
aktuellste Part ist meist nur teilweise gefüllt – und im Zweifel mit
`head -n <N> <part>.csv > sample.csv` (Kopfzeile bleibt erhalten) weiter
kürzen, um Ingest-Budget für mehrere Testdurchläufe übrig zu behalten.
