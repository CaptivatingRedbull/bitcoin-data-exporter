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

Die Zielumgebung läuft auf Splunk **Enterprise** (Kapitel 2.5 – SUSE
Linux Enterprise Server 15). Für Vorab-Tests eignet sich stattdessen
Splunk **Cloud** (z. B. ein Trial), weil sich damit keine eigene
Indexer-Instanz lokal betreiben lässt (keine offizielle Splunk-Enterprise-
ARM-Distribution für die Indexer-Seite) – der Universal Forwarder selbst
ist dabei in beiden Fällen exakt derselbe:

| | Test (Splunk Cloud) | Produktiv (Splunk Enterprise) |
|---|---|---|
| Forwarder | Universal Forwarder | Universal Forwarder (identische Binary/Version) |
| `inputs.conf` | unverändert | unverändert |
| `outputs.conf` | wird **nicht** von Hand geschrieben – kommt fertig im "Universal Forwarder credentials"-Paket, das die Cloud-Instanz zum Download anbietet (inkl. TLS-Zertifikaten) | von Hand bzw. per Deployment Server geschrieben, siehe [`outputs.conf.example`](../splunk/TA-btc-parser-inputs/local/outputs.conf.example) |
| Parsing-seitige Konfiguration (`props.conf`) | Self-Service-App-Upload über Splunk Web (Settings → Apps → "Install app from file") | Deployment auf den Indexer(n) bzw. über den Cluster-Manager/Deployer bei einem Indexer-Cluster |

Der einzige Teil, der beim Umstieg von Test auf Produktiv wirklich neu
gemacht werden muss, ist also `outputs.conf` plus die Verteilung der
Parsing-App auf die jeweilige Indexer-Seite – `inputs.conf` bleibt
unverändert wiederverwendbar.

## 10.2 Zwei Apps, zwei Zielorte

Ein Universal Forwarder **parst nichts** – er liest Dateien und leitet sie
roh weiter; Sourcetype-Zuordnung, Zeitstempel-Extraktion und
Feld-Extraktion passieren erst auf der Instanz, die die Daten tatsächlich
parst (bei Splunk Cloud eine von Splunk verwaltete Indexer-Schicht, bei
Enterprise der/die eigene(n) Indexer). Eine `props.conf` mit
`INDEXED_EXTRACTIONS`/`TIMESTAMP_FIELDS`, die nur auf dem Forwarder liegt,
hat deshalb schlicht keine Wirkung. Aus diesem Grund liegen unter
[`splunk/`](../splunk/) zwei getrennte, unabhängig voneinander
installierbare Apps:

- [`TA-btc-parser-inputs/`](../splunk/TA-btc-parser-inputs/) –
  **nur** `inputs.conf` (plus optional `outputs.conf`, siehe 10.1).
  Gehört auf den Universal Forwarder, der auf dem Host mit `parser-data/`
  läuft.
- [`TA-btc-parser-parsing/`](../splunk/TA-btc-parser-parsing/) – **nur**
  `props.conf` (Sourcetypes, Zeitfelder, Feld-Extraktion). Gehört auf die
  jeweilige Parsing-Instanz (Cloud-Self-Service-Upload bzw.
  Enterprise-Indexer/-Cluster).

## 10.3 Verzeichnis-zu-Input-Zuordnung

Direkte Umsetzung von Kapitel 7.4 in `inputs.conf`-Stanzas
([`TA-btc-parser-inputs/local/inputs.conf`](../splunk/TA-btc-parser-inputs/local/inputs.conf)):

| Verzeichnis | Splunk-Input | Warum |
|---|---|---|
| `export/rpc/{blocks,transactions,inputs,outputs}/` | `batch` (`move_policy = sinkhole`) | Ein Part erscheint hier laut Kapitel 4.6 immer erst vollständig fertig – nie während er noch beschrieben wird. `batch` kann ihn deshalb gefahrlos konsumieren und löschen. |
| `export/api/`, `export/stale/` | `monitor` | Der aktuelle Part wächst hier laufend weiter (Kapitel 3.2/5.5). Ein `batch`-Input würde riskieren, eine noch offene Datei mitten im Schreiben zu greifen. `monitor` löscht nie – alte rotierte Parts müssen separat (Skript/Cron) aufgeräumt werden, sobald Splunk sie nachweislich indiziert hat. |
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
eigenes Spaltenschema hat (Kapitel 4.9–4.12, 5.6, 6.4) – ein gemeinsamer
Sourcetype für mehrere Schemata würde Feld-Extraktion und
Zeitstempel-Zuordnung gegenseitig stören.

| Datei | Sourcetype | Zeitfeld → `_time` | Format |
|---|---|---|---|
| `blocks*.csv` | `btc:blocks` | `time` (Unix-Epoch) | `%s` |
| `transactions*.csv` | `btc:transactions` | `block_time` (Unix-Epoch, vom Block übernommen) | `%s` |
| `inputs*.csv` | `btc:inputs` | `block_time` | `%s` |
| `outputs*.csv` | `btc:outputs` | `block_time` | `%s` |
| `stale_block_headers*.csv` | `btc:stale_block_headers` | `observed_at` (ISO-8601 UTC) | `%Y-%m-%dT%H:%M:%SZ` |
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
`index/index.csv`, `reorg/*.csv`, `registry.csv`, die
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

## 10.6 Feldebene: warum (fast) nichts zusätzlich gefiltert wird

Jede Spalte, die tatsächlich in einer `export/`-CSV landet, wird in
Splunk auch zu einem vollwertigen indizierten Feld
(`INDEXED_EXTRACTIONS = csv` extrahiert jede Kopfzeilen-Spalte
automatisch). Das ist eine bewusste Entscheidung, keine Bequemlichkeit:
Die eigentliche Selektion "welches RPC-/API-Rohfeld ist überhaupt
exportwürdig" wurde bereits eine Ebene tiefer, in `block_parser.py`/
`mempool_endpoints.py` selbst, getroffen – siehe die "Bewusst nicht
exportiert"-Abschnitte in Kapitel 4.10–4.12:

- `has_witness`, `has_taproot_input`, `has_taproot_output`,
  `has_op_return`, `prevout_values_complete`, `prevout_heights_complete`
  – als reine Ein-Zeilen-Ableitungen aus bereits vorhandenen Zählfeldern
  gar nicht erst exportiert.
- `coinbase_script_sig_hex`, `coinbase_output_addresses_json` – seit der
  Mining-Pool-Zuordnung als eigene Blockfelder (`pool_id`/`pool_name`/
  `pool_link`) nicht mehr exportiert.
- `scriptsig_hex` (Inputs) und `script_hex` (Outputs) – bewusst
  weggelassen, unter anderem weil eine einzelne überlange
  `nulldata`-Payload (beobachtet bis 79.870 Byte) Splunks
  Event-`TRUNCATE`-Grenze (Standard ~10.000 Byte) reißen und damit auch
  die übrigen Felder derselben Zeile beschädigen könnte (Kapitel 4.12).

Eine zweite Filterrunde in Splunk (z. B. über `TRANSFORMS`, um einzelne
Spalten vor der Indizierung zu verwerfen) würde diese bereits getroffene
Entscheidung nur wiederholen, nicht verbessern – deshalb enthält
`props.conf` bewusst **keine** solche Regel. Aus demselben Grund bleibt
`TRUNCATE` überall auf dem Splunk-Standard: Da die einzigen potenziell
sehr langen Rohfelder (`scriptsig_hex`/`script_hex`) bereits eine Ebene
tiefer entfernt wurden, bestehen die verbleibenden Zeilen ausschließlich
aus kurzen Zahlen-/Kategorie-/Hash-Feldern – selbst `transactions.csv`
mit seinen gut 50 Spalten bleibt damit weit unter der Standardgrenze.

### Hochkardinale Hash-/ID-Felder bleiben trotzdem indiziert

Felder wie `txid`, `wtxid`, `block_hash`/`hash`, `merkleroot`,
`chainwork`, `versionHex`, `prevout_txid`, `header_hex` und
`coinbase_hex` haben praktisch so viele unterschiedliche Werte wie es
Zeilen gibt. Das ist **kein** Grund, sie von der Indizierung
auszuschließen – ohne sie wären die zeilenübergreifenden Bezüge (welcher
Input gehört zu welcher Transaktion, welche Transaktion zu welchem Block)
in Splunk nicht mehr per exaktem Feldwert auffindbar, und genau dafür
tragen `inputs.csv`/`outputs.csv` `block_hash`/`txid` überhaupt auf jeder
Zeile (Kapitel 4.11/4.12, um Joins gegen `transactions.csv`/`blocks.csv`
zu vermeiden). Der einzige praktische Hinweis: Diese Felder eignen sich
für exakte Lookups/`search txid=...`, aber nicht für `stats by`/
Top-Werte-Auswertungen oder Dashboard-Facetten – dafür bleiben die
kategorialen Felder (`script_type`, `pool_id`, `fee_source`,
`vin_type_<typ>_count`/`vout_type_<typ>_count`, `relative_locktime_type`
usw.) die richtige Wahl.

## 10.7 Index

Alle Sourcetypes verwenden in `inputs.conf` denselben Index `btc_parser`
(Platzhaltername – vor dem ersten Forwarder-Start sowohl in Splunk Cloud
als auch in Splunk Enterprise explizit anlegen, Indizes werden nicht
automatisch erstellt). Ein gemeinsamer Index für alle zehn Sourcetypes
genügt hier, weil Zugriffskontrolle und Aufbewahrungsfristen für alle
Dateien dieser Anwendung identisch sind; eine Aufteilung nach Index lohnt
sich erst, sobald das für einzelne Sourcetypes nicht mehr zutrifft (z. B.
unterschiedliche Retention für `btc:prices` vs. den Rest).

## 10.8 Testdaten-Staging (kleines Sample statt vollem Export)

Für einen ersten Test mit begrenztem Ingest-Volumen (z. B. ein
Splunk-Cloud-Trial-Limit): **niemals** die `batch`-Stanzas aus 10.3 direkt
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
