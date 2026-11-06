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
| Wer liest die CSVs | Splunk Enterprise selbst (`zrz_in_btc_exporter`) | der UF (`zrz_in_btc_exporter`) |
| Index-Time-Verarbeitung (Header verwerfen, `_time`, welche Spalten indizierte Felder werden) | `zrz_ko_btc` (`props.conf`/`transforms.conf`) auf derselben Instanz | Cribl-Pipeline [`cribl/zrz_btc_pipeline.json`](../cribl/zrz_btc_pipeline.json) – Splunk behandelt Daten aus Cribl als bereits geparst und wendet **keine** Index-Time-`props`/`transforms` mehr an |
| Search-Time-Felder | `zrz_rd_btc` | `zrz_rd_btc` (Search Head) – identisch |
| Index | `zrz_ix_btc` (dieselbe Instanz) | `zrz_ix_btc` über den Cluster Manager |
| Durchsatz-Tuning (`maxKBps`, Pipelines) | `zrz_in_btc_exporter/local/{limits,server}.conf` (wirkt auf derselben Instanz, die auch indexiert) | dieselben Dateien auf dem UF; auf dem Indexer-Cluster Plattform-Entscheidung (Kapitel 12.4) |
| Verteilung | Apps von Hand nach `$SPLUNK_HOME/etc/apps/` | Deployment-Server (`zrz_ds_btc`) → UF; Cluster Manager (`zrz_ix_btc`, `zrz_ko_btc`); SHC-Deployer (`zrz_ko_btc`, `zrz_rd_btc`) |

Beide Wege erzeugen dieselben Events: `_raw` ist die CSV-Zeile, dieselben
Spalten sind indizierte Felder, der Rest wird zur Suchzeit von derselben App
extrahiert. Dashboards und Suchen laufen deshalb unverändert in beiden
Umgebungen. Die Cribl-Einrichtung beschreibt
[`cribl/README.md`](../cribl/README.md).

## 10.2 Apps, Zielorte und Namenskonvention

Die Splunk-Apps unter [`splunk/`](../splunk/) folgen der ZRZ-Vorlage
(`zrz_ds_VORLAGE`, `zrz_in_VORLAGE`, `zrz_ko_VORLAGE`, `zrz_rd_VORLAGE` aus
dem Vorlage-Repository für `helper_create_app.sh`) und der
ZRZ-Namenskonvention, Use Case **`btc`**. `splunk/` enthält nur die Apps
selbst, sodass sie 1:1 nach `splunk-global` übernommen werden können; die
Generatoren und Betriebsskripte liegen unter [`tools/splunk/`](../tools/splunk/).
Kurzüberblick auch in [`splunk/README.md`](../splunk/README.md).

| App (Verzeichnis) | Typ | Wohin | Inhalt |
|---|---|---|---|
| [`zrz_ds_btc/`](../splunk/zrz_ds_btc/) | DS | Deployment-Server | Serverklasse `zrz_btc_exporter` (`local/serverclass.conf`), Client-Liste `clients.txt` – verteilt `zrz_in_btc_exporter` an den UF des Parser-Hosts |
| [`zrz_in_btc_exporter/`](../splunk/zrz_in_btc_exporter/) | IN | Instanz, die die Dateien liest (Test: Standalone; Prod: UF über den DS) | `local/inputs.conf` (10.3), `local/limits.conf` (`maxKBps = 0`), `local/server.conf` (Ingest-Pipelines/Queues, Kapitel 12.4) |
| [`zrz_ix_btc/`](../splunk/zrz_ix_btc/) | IX | Indexer (Prod: Cluster Manager; Test: Standalone) | `indexes.conf`: Index `zrz_btc` mit Retention für Zeitstempel ab 2009, `repFactor = auto` (Kapitel 12) |
| [`zrz_ko_btc/`](../splunk/zrz_ko_btc/) | KO | Prod: CM + SHCD (dort wirkungslos, siehe unten); Test: Standalone | Index-Time: `props.conf`/`transforms.conf`, siehe 10.6. **Nie auf einen UF**: `INDEXED_EXTRACTIONS` würde den UF die kleinen Sourcetypes selbst parsen und jede Spalte als indiziertes Feld an Cribl schicken lassen. Hinter Cribl wirkungslos. |
| [`zrz_rd_btc/`](../splunk/zrz_rd_btc/) | RD | Search Head (Prod: SHCD; Test: Standalone) | Search-Time: `EXTRACT`-Extraktion der nicht indizierten Spalten, `fields.conf`, `KV_MODE = none`; Dashboard, Reports, Makros, KV-Store-Collections und Lookups (10.9); per `metadata/default.meta` systemweit exportiert |
| [`cribl/zrz_btc_pipeline.json`](../cribl/zrz_btc_pipeline.json) | – | Prod: Cribl | Index-Time-Gegenstück zu `zrz_ko_btc` |

Alle feldbezogenen Dateien (`zrz_ko_btc/default/{props,transforms}.conf`,
`zrz_rd_btc/default/{props,fields}.conf`, die Cribl-Pipeline) werden von
[`tools/splunk/build_field_config.py`](../tools/splunk/build_field_config.py) aus
[`btc_parser_app/export_schema.py`](../btc_parser_app/export_schema.py)
erzeugt (Spaltenreihenfolge + indizierte Felder je Sourcetype) und nie von
Hand bearbeitet; Dashboard, Reports, Makros, Collections, Lookup-Definitionen
(`zrz_rd_btc/default/transforms.conf`) und die Lookup-Datei erzeugt
[`tools/splunk/build_mempool_explorer.py`](../tools/splunk/build_mempool_explorer.py)
(10.9). `tests/test_export_schema.py` prüft, dass das Schema exakt
den Zeilen entspricht, die `block_parser.py` schreibt, spielt die erzeugten
Regexe gegen echte Zeilen ab und schlägt fehl, wenn eine erzeugte Datei
veraltet ist. `tests/test_naming_convention.py` prüft die Namenskonvention
unten für jede App und jedes Wissensobjekt unter `splunk/`.

Die Search-Time-Extraktion steht als `EXTRACT-zrz_btc_columns` direkt in
`zrz_rd_btc/default/props.conf` (nicht als `REPORT-` + `transforms.conf`):
`transforms.conf` derselben App enthält die Lookup-Definitionen des
Dashboard-Generators, und zwei Generatoren sollen nie dieselbe Datei
schreiben. Wirkung und Regex sind dieselben.

### Namenskonvention (ZRZ, Use Case `btc`)

| Kategorie | Objekt | Format | hier |
|---|---|---|---|
| Verzeichnis | Index | `zrz_ix_$usecase$` | `zrz_ix_btc` |
| Verzeichnis | Trans/KO | `zrz_ko_$usecase$` | `zrz_ko_btc` |
| Verzeichnis | Report/Dashboard | `zrz_rd_$usecase$` | `zrz_rd_btc` |
| Verzeichnis | Input | `zrz_in_$usecase$_$log$` | `zrz_in_btc_exporter` |
| Verzeichnis | Deployment-Server | `zrz_ds_$usecase$` (Vorlage) | `zrz_ds_btc` |
| Knowledge-Object | Index | `zrz_$usecase$_$beliebig$` | `zrz_btc` |
| Knowledge-Object | Report | `zrz_$usecase$_$beliebig$` | `zrz_btc_rec_*` (13 Reports, 10.9) |
| Knowledge-Object | Dashboard | `zrz_$usecase$_$beliebig$` | `zrz_btc_mempool_explorer` |
| Knowledge-Object | Alarm | `zrz_$usecase$_$beliebig$` | – (keine) |
| Knowledge-Object | App | `zrz_$usecase$` | Label „ZRZ BTC“ (`zrz_rd_btc`) |
| Knowledge-Object | Source Type | `zrz:$usecase$:$log$` | `zrz:btc:blocks`, `zrz:btc:transactions`, `zrz:btc:inputs`, `zrz:btc:outputs`, `zrz:btc:stale_block_headers`, `zrz:btc:sanctioned_tx_events`, `zrz:btc:prices` |
| Knowledge-Object | Felder | `zrz_$usecase$_$feld$` | Extraktions-/Transform-Namen: `EXTRACT-zrz_btc_columns`, `TRANSFORMS-zrz_btc_00_header`, `TRANSFORMS-zrz_btc_10_indexed`, `zrz_btc_drop_header_<st>`, `zrz_btc_idx_<st>_<feld>`. Die **Feldnamen selbst** bleiben die CSV-Spaltennamen (`block_height`, `txid`, …), siehe unten. |
| Knowledge-Object | Lookups | `zrz_$usecase$_$lookup$` | `zrz_btc_sanctioned_addresses` (`.csv`), KV-Store `zrz_btc_rec_*` |
| Knowledge-Object | Tag / Event Type | `zrz_$usecase$_$tag$` / `…_$eventtype$` | – (keine) |
| Knowledge-Object | Macro | `zrz_$usecase$_$macro$` | `zrz_btc_index`, `zrz_btc_rec_window` |
| Knowledge-Object | Benutzergruppe | `zrz-splunk-$usecase$` | `zrz-splunk-btc` (AD-Gruppe → Rollen `zrz_user_btc`/`zrz_poweruser_btc`, siehe 10.10) |
| Knowledge-Object | Serverklasse | `zrz_$usecase$_$beliebig$` | `zrz_btc_exporter` |

**Feldnamen:** Die Konvention `zrz_$usecase$_$feld$` wird hier auf die
*Namen der Feld-Wissensobjekte* (Extraktionen, Transforms) angewendet,
nicht auf jeden einzelnen Feldnamen. Die Feldnamen sind die
Spaltennamen der CSV-Kopfzeile: `INDEXED_EXTRACTIONS` übernimmt sie
direkt, die Cribl-Pipeline und alle Suchen/Dashboards verwenden sie, und
ein Präfix vor jedem der ~180 Felder würde jede SPL-Zeile verlängern,
ohne etwas eindeutiger zu machen (die Felder kommen nur in
`index=zrz_btc` vor). Ist ein Präfix auf jedem Feld gewünscht, wäre das
eine Änderung der CSV-Kopfzeilen oder ein `FIELDALIAS` je Feld – siehe
10.10.

**Alte Namen** (bis zu dieser Umstellung): Index `btc_parser`,
Sourcetypes `btc:<name>`, Apps `TA-btc-parser-inputs` → `zrz_in_btc_exporter`,
`TA-btc-parser-parsing` → `zrz_ko_btc`, `TA-btc-parser-search` und
`btc-parser-dashboards` → `zrz_rd_btc`, `TA-btc-parser-indexes` →
`zrz_ix_btc`, `TA-btc-parser-ingest-tuning` → aufgeteilt (UF-Teil in
`zrz_in_btc_exporter/local/`, Indexer-Teil Plattform). Bereits unter den
alten Namen indizierte Daten werden von den neuen Apps nicht mehr
erfasst – siehe 10.10.

## 10.3 Verzeichnis-zu-Input-Zuordnung

Direkte Umsetzung von Kapitel 7.4 in `inputs.conf`-Stanzas
([`zrz_in_btc_exporter/local/inputs.conf`](../splunk/zrz_in_btc_exporter/local/inputs.conf)):

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

Jede Datei bekommt einen eigenen Sourcetype (`zrz:btc:<name>`), weil jede ein
eigenes Spaltenschema hat (Kapitel 4.9–4.12, 5.6, 6.4, 11.6) – ein gemeinsamer
Sourcetype für mehrere Schemata würde Feld-Extraktion und
Zeitstempel-Zuordnung gegenseitig stören.

| Datei | Sourcetype | Zeitfeld → `_time` | Format |
|---|---|---|---|
| `blocks*.csv` | `zrz:btc:blocks` | `time` (Unix-Epoch) | `%s` |
| `transactions*.csv` | `zrz:btc:transactions` | `block_time` (Unix-Epoch, vom Block übernommen) | `%s` |
| `inputs*.csv` | `zrz:btc:inputs` | `block_time` | `%s` |
| `outputs*.csv` | `zrz:btc:outputs` | `block_time` | `%s` |
| `stale_block_headers*.csv` | `zrz:btc:stale_block_headers` | `observed_at` (ISO-8601 UTC) | `%Y-%m-%dT%H:%M:%SZ` |
| `sanctioned_tx_events*.csv` | `zrz:btc:sanctioned_tx_events` | `observed_at` (Zeitpunkt des Ereignisses, ISO-8601 UTC) | `%Y-%m-%dT%H:%M:%SZ` |
| `prices*.csv` | `zrz:btc:prices` | `date_unix` (Preis-eigener Zeitstempel, **nicht** Poll-Zeitpunkt) | `%s` |

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

1. **Kleine Sourcetypes – alles indiziert.** `zrz:btc:blocks` (~1 Zeile pro
   Block), `zrz:btc:stale_block_headers`, `zrz:btc:sanctioned_tx_events`,
   `zrz:btc:prices`: `INDEXED_EXTRACTIONS = csv` (Test) bzw. alle Spalten
   behalten (Cribl). Die Kosten sind bei diesem Volumen vernachlässigbar, und
   `blocks` trägt bereits jede Block-Aggregation (Gebühren, Skripttyp-Zähler,
   SegWit/Taproot/RBF/OP_RETURN, CDD, Pool). Langfristige Trends gehören
   deshalb hierher, per `tstats` über die ganze Chain.
2. **`zrz:btc:transactions`/`zrz:btc:inputs`/`zrz:btc:outputs` (Milliarden Zeilen) – nur
   niedrig-kardinale Dimensionen**, nach denen man über lange Zeiträume
   gruppiert/filtert und deren Rohwerte (`true`, `2`, …) als Suchbegriff
   nichts eingrenzen:

   | Sourcetype | indiziert | Spalten gesamt |
   |---|---|---|
   | `zrz:btc:transactions` | `block_height`, `is_coinbase`, `version`, `vin_count`, `vout_count` | 63 |
   | `zrz:btc:inputs` | `block_height`, `is_coinbase`, `prevout_type`, `prevout_generated`, `relative_locktime_type` | 23 |
   | `zrz:btc:outputs` | `block_height`, `is_coinbase`, `script_type` | 12 |

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
   z. B. `is_coinbase` auch auf `zrz:btc:inputs`). Der Generator bricht sonst ab.

**Technik (Test-Standalone, `zrz_ko_btc`):** Für die drei großen
Sourcetypes kein `INDEXED_EXTRACTIONS`. Klassischer Zeilenumbruch, `_time`
per `TIME_PREFIX` aus der Spaltenposition von `block_time`, Kopfzeile per
`nullQueue` verworfen, und je indiziertem Feld eine
`WRITE_META`-Transform, deren Regex auf genau diese Spalte festgelegt ist
und einen nicht-leeren Wert verlangt. Ein `null` (z. B. `prevout_type` der
Coinbase-Eingabe) erzeugt so gar kein Feld statt eines leeren.
`zrz_rd_btc` extrahiert die übrigen Spalten zur Suchzeit mit einer
verankerten Regex über die ganze Zeile (`EXTRACT-zrz_btc_columns`). Die indizierten Spalten werden dort
übersprungen, sonst wären sie doppelte Multivalue-Felder. Die Zeilen
enthalten nie Kommas in Werten (nur Hashes, Zahlen, Booleans, Skripttypen,
Adressen), daher ist das Zerlegen an jedem Komma exakt.

**Folge für SPL:** `tstats` funktioniert auf den großen Sourcetypes nur noch
mit den indizierten Feldern oben (plus `_time`, `sourcetype`, …).
`tstats max(fee_sats)`, `perc90(fee_rate_sat_vb)`, `sum(value_sats) by address`
liefern dort nichts mehr. Solche Auswertungen laufen als normale Suche mit
`stats`/`timechart` über ein begrenztes Zeitfenster, oder langfristig über
die Block-Aggregate in `zrz:btc:blocks`. Beispiele für beides enthält das
Benchmark-Skript [`tools/splunk/btc-splunk-bench.sh`](../tools/splunk/btc-splunk-bench.sh)
(Gruppen T und R). Das Dashboard und seine gespeicherten Suchen
(`zrz_rd_btc`, 10.9) sind darauf umgestellt.

`TRUNCATE` bleibt auf dem Standard: Die einzigen potenziell sehr langen
Rohfelder (`scriptsig_hex`/`script_hex`) werden gar nicht erst exportiert
(Kapitel 4.11/4.12), die längste Zeile (`transactions.csv`) liegt weit unter
10.000 Byte.

## 10.7 Index

Retention und Größe des Index sind für historische Daten ab 2009
kritisch – Splunks Standardwerte würden den Großteil der Chain löschen;
siehe Kapitel 12 und `splunk/zrz_ix_btc/`.

Alle Sourcetypes verwenden in `inputs.conf` denselben Index `zrz_btc`
(vor dem ersten Start auf dem Indexer anlegen – `zrz_ix_btc`,
Kapitel 12; Indizes werden nicht automatisch erstellt). Ein gemeinsamer Index für alle Sourcetypes
genügt hier, weil Zugriffskontrolle und Aufbewahrungsfristen für alle
Dateien dieser Anwendung identisch sind; eine Aufteilung nach Index lohnt
sich erst, sobald das für einzelne Sourcetypes nicht mehr zutrifft (z. B.
unterschiedliche Retention für `zrz:btc:prices` vs. den Rest).

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

## 10.9 Beispiel-Dashboard „BTC Mempool Explorer“ (Dashboard Studio)

Ein Test-Dashboard im Dashboard-Studio-Format, das die bekannten
mempool.space-Grafiken auf Basis der eigenen Exporte nachbildet. Es liegt
in zwei inhaltsgleichen Formen vor, beide erzeugt von
[`tools/splunk/build_mempool_explorer.py`](../tools/splunk/build_mempool_explorer.py)
(Änderungen dort vornehmen und das Skript neu ausführen, nicht die
Ausgaben von Hand bearbeiten):

- [`tools/splunk/zrz_btc_mempool_explorer.json`](../tools/splunk/zrz_btc_mempool_explorer.json)
  – für einen schnellen Test: in Splunk Web *Dashboards → Create New
  Dashboard → Dashboard Studio → Grid* anlegen, dann *Source* öffnen und
  den kompletten Inhalt ersetzen. Theme auf *Dark* stellen.
- [`splunk/zrz_rd_btc/`](../splunk/zrz_rd_btc/) –
  dieselbe Definition als View `zrz_btc_mempool_explorer` in der
  RD-App des Use Cases (Search Head; Prod: SHCD; Test: Standalone-Instanz,
  nach `$SPLUNK_HOME/etc/apps/` kopieren und Splunk neu starten). Die App
  bringt auch die Search-Time-Felder mit (10.2); das JSON allein setzt
  eine installierte `zrz_rd_btc` voraus.

Globale Eingaben (über allen Tabs): Zeitraum (Standard: letzte 30 Tage),
Bucket-Größe für alle Zeitreihen (`$span$`, Standard 1 Tag) und Index
(`$idx$`, Standard `zrz_btc`, siehe 10.7).

| Tab | Inhalt | Quelle |
|---|---|---|
| Overview | Preis, Höhe, Median-Fee des Tip-Blocks, mittlere Blockzeit, Hashrate; Tabelle der letzten Blöcke (wie die Block-Leiste auf mempool.space, Fee-Spalten im mempool.space-Farbverlauf) | `zrz:btc:blocks`, `zrz:btc:prices` |
| Mining | Difficulty-Epoche (Fortschritt, geschätzte Anpassung, Termin), Halving-Countdown, Hashrate & Difficulty, Pool-Ranking/-Dominanz, Difficulty-Historie | `zrz:btc:blocks` |
| Fees & Rewards | bestätigte vBytes je Fee-Band (Nachbildung des mempool.space-Mempool-Charts), Fee-Perzentile, Subsidy vs. Fees (BTC/USD), Ø Fee pro Transaktion | `zrz:btc:transactions`, `zrz:btc:blocks`, `zrz:btc:prices` |
| Blocks | Blockgröße/-gewicht, Transaktionen pro Block, Blockintervall und Verteilung, lange Lücken | `zrz:btc:blocks` |
| Transactions | SegWit-/Taproot-/RBF-Anteil, Output-Skripttypen, OP_RETURN, Coin Days Destroyed, ausgegebener Wert nach Coin-Alter | `zrz:btc:blocks`, `zrz:btc:inputs` |
| Price | USD/EUR-Verlauf, Tagesänderung, Tages-OHLC | `zrz:btc:prices` |
| Chain Health | Stale-Tips (Kapitel 5), Höhen mit mehr als einem exportierten Blockhash (Reorgs, Kapitel 4.4) | `zrz:btc:stale_block_headers`, `zrz:btc:blocks` |
| Sanctions Watch | Ereignisse je Typ, noch unbestätigte Transaktionen, Treffer je gelisteter Entität, letzte Ereignisse (Kapitel 11) | `zrz:btc:sanctioned_tx_events` |
| All-time Records | Rekorde über den gesamten Index unabhängig vom Zeitraum-Picker: höchste/niedrigste Fee, größte Transaktion, meiste In-/Outputs, ältester bewegter Coin, Block-Rekorde, größter einzelner Coin, größte unverbrauchte Outputs ≥ 100 BTC, Preis-ATH/-ATL, alle Transaktionen mit gelisteten Adressen in den Chain-Daten; liest nur vorberechnete Ergebnisse (siehe unten); einzig der Saldoverlauf einer Adresse sucht live, sobald die Eingabe *Address* gesetzt ist | alle |

Hinweise:

- Die Exporte enthalten **keinen** Mempool-Snapshot (der API-Poller
  exportiert nur `prices`, Kapitel 6.1). Die „Mempool“-Grafiken zeigen
  deshalb, was im jeweiligen Bucket **bestätigt** wurde – nicht, was
  gerade wartet.
- Alle Block-Panels hängen an einer gemeinsamen Basissuche, die pro Höhe
  nur den zuletzt indizierten Block behält (`dedup height` nach
  `_indextime`), damit von einem Reorg abgehängte Blöcke (die in Splunk
  stehen bleiben, siehe 10.5) nicht doppelt zählen. Als nicht
  transformierende Basissuche behält Splunk davon höchstens 500.000
  Events – für sehr lange Zeiträume (mehrere Jahre Blöcke) die Panels
  einzeln als eigene Suchen ausführen.
- Die Panels auf `zrz:btc:transactions`/`zrz:btc:inputs` (Fee-Bänder,
  Fee-Perzentile, Coin-Alter) brauchen Search-Time-Felder (`fee_rate_sat_vb`,
  `vsize`, `value_sats`, 10.6) und lesen deshalb jede Zeile des Zeitraums –
  den Zeitraum kurz halten (Tage bis wenige Wochen). Die Difficulty-Epoche und der 24h-Preis
  verwenden feste Zeiträume (letzte 20 Tage bzw. 24 Stunden),
  unabhängig vom Zeitraum-Picker.
- `tstats` nur dort, wo alle verwendeten Felder indiziert sind (10.6):
  `zrz:btc:blocks`, `zrz:btc:prices`, `zrz:btc:stale_block_headers`. Auf
  `zrz:btc:transactions`/`zrz:btc:inputs`/`zrz:btc:outputs` gibt es nur `block_height`,
  `is_coinbase`, `version`, `vin_count`, `vout_count`, `prevout_type`,
  `prevout_generated`, `relative_locktime_type` und `script_type` – ein
  `tstats` über ein anderes Feld liefert **stillschweigend nichts**.
  `tests/test_dashboard.py` prüft jedes `tstats` des Dashboards und der
  gespeicherten Suchen dagegen.
- Wo eine Zeile gebraucht wird (z. B. „welche Transaktion hält den
  Rekord“), ermittelt ein erster `stats`-Durchlauf den Wert, und `map`
  holt die passenden Zeilen per exaktem Feldvergleich (nur ganzzahlige
  Felder, da Dezimalwerte anders formatiert sein können als in der CSV).
  Ein Subsearch käme beim Rebuild über die ganze Chain an sein Zeitlimit.
  Panels auf derselben Datenbasis hängen als Chain-Searches an einer
  Basissuche.
- Der Saldoverlauf einer Adresse und der Sanktionslisten-Abgleich suchen
  nach Adressen. Die sind als Rohtext-Token eindeutig, die Bloom-Filter
  überspringen jeden Bucket ohne die Adresse – schnell trotz normaler
  Suche über die ganze Chain.
- **Entfallen:** „Largest address balances now“ (Summe je Adresse über
  jeden je erzeugten Input/Output). Ohne indiziertes `address`/`value_sats`
  ist das ein Durchlauf über Milliarden Zeilen, der nicht fertig wird.
  Einzelne Adressen: Eingabe *Address*.
- Die Spalte `source` von `stale_block_headers.csv`/`sanctioned_tx_events.csv`
  heißt wie Splunks eingebautes Metadatenfeld `source` (Dateipfad). Zeigen
  die zugehörigen Panels Dateipfade statt `own_node`/`zmq` usw., kollidiert
  die Spalte mit dem Metadatenfeld.
- Die Hashrate wird je Tag als Σ(Difficulty × 2³²) / Σ(Blockintervalle)
  geschätzt – robust gegenüber angebrochenen Tagen am Rand des Zeitraums.

### Hintergrund-Suchen für den Tab „All-time Records“

Eine Dashboard-Studio-Definition kann gespeicherte Suchen nur
referenzieren (`ds.savedSearch`), nicht selbst definieren. Die Suchen,
auf denen der Tab aufbaut, liegen deshalb in der App
[`splunk/zrz_rd_btc/`](../splunk/zrz_rd_btc/) und
werden vom selben Skript erzeugt (`savedsearches.conf`, `macros.conf`,
`collections.conf`, `transforms.conf`,
`lookups/zrz_btc_sanctioned_addresses.csv`). Ohne installierte App bleiben die
Panels dieses Tabs leer bzw. melden eine fehlende Suche/Lookup.

| Art | Suchen | Zeitplan | Dashboard liest |
|---|---|---|---|
| Inkrementell (KV Store) | `zrz_btc_rec_tx_values`, `zrz_btc_rec_tx_rows`, `zrz_btc_rec_minfee_rows`, `zrz_btc_rec_io`, `zrz_btc_rec_big_outputs` – je `<name>_update` und `<name>_rebuild` | `_update` stündlich um :15 über die letzte volle Stunde **Indexierungszeit**; `_rebuild` **nicht geplant**, nur von Hand | `\| inputlookup <name>` |
| Geplant, komplett | `zrz_btc_rec_blocks`, `zrz_btc_rec_price`, `zrz_btc_rec_sanc` | stündlich um :05 | `ds.savedSearch` (Ergebnis des letzten Laufs) |

- **Inkrementell:** Rekorde (Max/Min) lassen sich zusammenführen – ein
  neuer Rekord kann nur aus neuen Daten kommen. `_update` aggregiert nur
  Events, die in der letzten vollen Stunde indiziert wurden
  (Makro `zrz_btc_rec_window`: `_index_earliest=-1h@h _index_latest=@h`),
  hängt den bisherigen KV-Store-Inhalt an und schreibt das Ergebnis
  zurück. Die einzige Summe (`zero_fee_txs`) ist nur korrekt, weil sich
  die Stundenfenster nie überlappen und kein Lauf ausfällt
  (`realtime_schedule = 0`). `_update` sucht dabei nur Events der letzten
  7 Tage Ereigniszeit – Daten mit älterem `_time` (Katch-up/Backfill alter
  Blöcke, Preis-Backfill) erfasst erst ein `_rebuild`.
- `zrz_btc_rec_big_outputs` hält Outputs ≥ 100 BTC und ihre Ausgaben. Zeilen,
  deren Erzeugung und Ausgabe beide gesehen wurden, fliegen beim Merge
  raus; übrig bleiben die unverbrauchten (und Ausgaben, deren Erzeugung
  noch nicht indiziert ist – Inputs/Outputs-Parts kommen in beliebiger
  Reihenfolge).
- **Rebuilds:** Ohne indizierte Messwerte ist jeder `_rebuild` ein
  normaler Durchlauf über die ganze Chain (Transaktionen bzw. alle
  Inputs/Outputs) und dauert Stunden. Deshalb nicht mehr wöchentlich
  geplant: **einmal nach dem Backfill** von Hand starten (*Settings →
  Searches, reports, and alerts → Run*), danach halten die stündlichen
  `_update` die KV Stores aktuell. Reihenfolge egal, aber nicht alle fünf
  gleichzeitig.
- **Index:** Gespeicherte Suchen sehen keine Dashboard-Tokens. Sie
  verwenden das Makro `zrz_btc_index` (`index=zrz_btc`) statt `$idx$` – bei
  anderem Indexnamen dort anpassen.
- Alle Objekte der App sind global freigegeben (`metadata/default.meta`,
  `export = system`), damit das JSON auch in einem Dashboard einer anderen
  App (z. B. *Search & Reporting*) funktioniert.

## 10.10 Vor dem Rollout: offene Punkte

Was die Umstellung auf die ZRZ-Vorlage nicht aus dem Repository ableiten
kann – vor dem ersten Deployment ausfüllen bzw. entscheiden:

| # | Was | Wo | Hinweis |
|---|---|---|---|
| 1 | Hostname(n) des UF auf dem Parser-Host | [`splunk/zrz_ds_btc/clients.txt`](../splunk/zrz_ds_btc/clients.txt) | eine Zeile, kommagetrennt (Regex-Hinweis im Kopf von `serverclass.conf`) |
| 2 | Absolute Export-Pfade | `$PARSER_DATA_ROOT` in [`zrz_in_btc_exporter/local/inputs.conf`](../splunk/zrz_in_btc_exporter/local/inputs.conf) (7 Stanzas) | echte `output_dir`-Werte aus `config.production.yaml`; `btc-splunk-enable-backfill.sh --check` meldet übrig gebliebene Platzhalter als FAIL |
| 3 | Index-Größe und Speicherort | [`zrz_ix_btc/default/indexes.conf`](../splunk/zrz_ix_btc/default/indexes.conf) | `maxTotalDataSizeMB` (Platzhalter 6 TB) an das Volume anpassen; nutzt die Plattform `volume:`-Pfade, `homePath`/`coldPath` darauf umstellen |
| 4 | Rollen `zrz_user_btc`, `zrz_poweruser_btc` | Plattform (`authorize.conf` in splunk-global) | von `metadata/default.meta` der KO-/RD-App (Vorlage) referenziert; `srchIndexesAllowed = zrz_btc`; AD-/LDAP-Gruppe `zrz-splunk-btc` auf die Rollen mappen. Ohne die Rollen sehen nur Admins die App. |
| 5 | `author` | `app.conf` aller Apps | in der Vorlage leer |
| 6 | App-Label | [`zrz_rd_btc/default/app.conf`](../splunk/zrz_rd_btc/default/app.conf) | „ZRZ BTC“ (Vorlage: großgeschriebener Use-Case-Name) |
| 7 | Ingest-Tuning auf dem Indexer-Cluster | Plattform | `parallelIngestionPipelines`/Queue-Größe sind global für alle Use Cases und deshalb **nicht** in `zrz_ix_btc`; für den Backfill mit dem Plattform-Team abstimmen (Kapitel 12.4) |
| 8 | Bestandsdaten im alten Index `btc_parser` (`btc:*`) | Test-Standalone | Die neuen Apps greifen nicht für die alten Namen. Entweder Neustart von Null nach `zrz_btc` (Kapitel 12.8, empfohlen), oder die alten Apps bis zum Löschen des alten Index parallel installiert lassen (Muster „UC_Migration“ der KO-Vorlage). Alte App-Verzeichnisse in `$SPLUNK_HOME/etc/apps/` (`TA-btc-parser-*`, `btc-parser-dashboards`) danach entfernen. |
| 9 | Cribl | Cribl Stream | Pipeline [`cribl/zrz_btc_pipeline.json`](../cribl/zrz_btc_pipeline.json) importieren, Route/Event Breaker auf `sourcetype.startsWith('zrz:btc:')` umstellen ([`cribl/README.md`](../cribl/README.md)); eine bestehende Route für den alten Sourcetype `btc:prices` anpassen |
| 10 | Feldnamen mit Präfix? | – | siehe „Feldnamen“ in 10.2; aktuell bewusst ohne Präfix |
| 11 | Use-Case-Kürzel und `$log$` bestätigen | überall | gewählt: Use Case `btc`, Input-`$log$` `exporter` (Quelle aller Dateien ist der bitcoin-data-exporter). Ein anderes Kürzel: `USECASE` in `export_schema.py` und die Konstanten in `tools/splunk/build_mempool_explorer.py` ändern, Generatoren neu laufen lassen, App-Verzeichnisse und statische Confs umbenennen. |
| 12 | Optional: OS-Monitoring des Parser-Hosts | `Splunk_TA_nix_btc` (Vorlage `Splunk_TA_nix_VORLAGE`) | nicht Teil dieses Repos; Index wäre `zrz_btc_linux`, Serverklassen-Eintrag ist in `serverclass.conf` auskommentiert vorbereitet |
