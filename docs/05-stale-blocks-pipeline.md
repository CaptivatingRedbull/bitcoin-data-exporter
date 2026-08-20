# 5. Stale-Blocks-Pipeline

[← Index](00-index.md)

Kommando: `stale-blocks-ingest` · Einstiegspunkt:
`btc_parser_app/rpc/stale_blocks.py::run_stale_blocks_ingest()`

## 5.1 Zweck

Eine von `rpc-ingest` vollständig unabhängige Pipeline, die
**nicht-aktive Chain-Tips** erfasst – also Blöcke, die zwar einmal Teil
eines gültigen (Teil-)Forks waren, aber nicht (mehr) auf der aktiven Chain
liegen ("stale"/"orphaned" Blöcke). `rpc-ingest` verarbeitet ausschließlich
die aktive Chain; diese Pipeline liefert die ergänzende "Reorg-Intel"-
Sicht dazu, mit eigenem Ausgabeverzeichnis und eigener Datenquelle.

**Wichtig:** Diese Pipeline sammelt bewusst **nur Blockheader**, niemals
vollständige Blöcke oder Transaktionsdaten – siehe 5.4 für die
Begründung.

## 5.2 Zwei Durchläufe pro Weckzyklus

Der Prozess wacht alle `stale_blocks.node_poll_interval_seconds`
(Standard stündlich) auf und führt dabei zwei unabhängige Durchläufe aus,
die sich eine gemeinsame `StaleBlockRegistry` teilen:

### Durchlauf 1: Node-Poll (jeder Weckzyklus)

1. `getchaintips` gegen den eigenen Node ausführen.
2. Auf Tips filtern, deren Status **nicht** `active` oder `invalid` ist
   (also `valid-fork`, `valid-headers`, `headers-only`).
3. Für jeden verbleibenden Tip: `getblockheader <hash> false` (rohe
   80-Byte-Header-Bytes) abrufen.
4. Jede Sichtung wird über `_ingest_sighting()` in der Registry erfasst
   (siehe 5.3).

### Durchlauf 2: GitHub-Pull (Kadenz = `stale_blocks.github.poll_interval_seconds`, Standard täglich)

Zieht das crowd-gesourcte Datenset
[`bitcoin-data/stale-blocks`](https://github.com/bitcoin-data/stale-blocks)
– stale/orphaned Tips, die von anderen Node-Betreibern über deren eigene
`getchaintips`-Sicht gemeldet wurden, und die deutlich weiter
zurückreichen als das, was die eigenen (aktuell ~10) Peers dieses Nodes
noch zeigen können.

Bereits mit gültigem Header bekannte Hashes werden übersprungen, sodass
ein täglicher Pull nur für tatsächlich neue Zeilen Arbeit kostet, nicht
für die gesamte mehrere-tausend-Zeilen-Historie jedes Mal erneut.

Der GitHub-Pull hat **keine** RPC-Abhängigkeit und läuft daher außerhalb
des RPC-`try`/`except`-Blocks – ein Node-Ausfall stellt diesen Durchlauf
nicht still. `last_github_pull` wird nur bei einem **erfolgreichen** Pull
aktualisiert, sodass ein transienter GitHub-Fehler beim nächsten
Weckzyklus sofort erneut versucht wird statt einen vollen
`poll_interval_seconds`-Zyklus lang zu warten.

## 5.3 Unabhängige Header-Validierung

Jeder Header – ob vom eigenen Node oder von GitHub – wird eigenständig
verifiziert (`common/block_header.py::validate_header_hash()`), **bevor**
er vertraut wird: doppeltes SHA-256 über die 80 rohen Header-Bytes,
byte-invertiert, muss dem behaupteten Blockhash entsprechen. Ein Header,
der nicht zum behaupteten Hash passt, wird verworfen (Statuswert
`unusable`) – ein Hash ganz ohne Header wäre genauso wertlos, nur
schwerer zu bemerken, daher die explizite Prüfung statt blindem Vertrauen
in die Quelle (das GitHub-Datenset selbst verlangt laut eigenem README
zwar bereits, dass jede Zeile einen Header trägt – diese Anwendung
verifiziert es trotzdem unabhängig).

`StaleBlockRegistry` (`rpc/stale_blocks_state.py`) verwaltet den Zustand:
Ein Status ist monoton (`unusable` → `header_only`, nie rückwärts) – eine
spätere, schlechtere Sichtung (z. B. ein RPC-Fehler beim erneuten Abfragen
eines Tips, für den bereits ein gültiger Header vorliegt) überschreibt nie
bereits vorhandene, bessere Daten.

Jedes tatsächlich neue Faktum (ein Hash wird erstmals gesichtet, ein
Header wird verfügbar) wird über `last_exported_status` genau einmal nach
Splunk exportiert – dasselbe "habe ich das schon exportiert"-Muster wie
`reorg_state.IndexStore` auf der Hauptketten-Seite. Ein späteres
Status-Upgrade ist ein neues, zusätzliches Ereignis; nichts wird
nachträglich mutiert oder erneut ausgegeben.

## 5.4 Warum nur Header, keine vollständigen Blöcke

Diese Pipeline hat früher zusätzlich versucht, vollständige Block-/
Transaktionsdaten zu beschaffen (über `getblockfrompeer` für Tips, die
eigene Peers noch hatten, sowie über `submitheader`/`submitblock`, um die
vollständigen Block-Blobs des GitHub-Datensets in den eigenen Node zu
importieren). In der Praxis erzeugte das jedoch einen lückenhaften,
inkonsistenten Datensatz: Bitcoin Cores Checkpoint-Mechanismus lehnt
grundsätzlich jeden konkurrierenden Header/Block auf oder unterhalb
seines höchsten fest einprogrammierten Checkpoints ab – bestätigt im
produktiven Betrieb deckt das die überwältigende Mehrheit dieses
Datensets ab, bis auf ca. 1000 Blöcke an den aktuellen Tip heran. Nur ein
kleiner, im Wesentlichen zufälliger Teil der Einträge hätte je
"vollständig" werden können, während der Rest permanent hängen bliebe.

Da für den Export ausschließlich Header-Felder benötigt werden, spielt
diese Checkpoint-Grenze keine Rolle: Beide Quellen liefern die rohen
Header-Bytes bereits direkt, und die Auswertung erfolgt vollständig
offline (`common/block_header.py`), ohne den Header jemals über
`submitheader` an den Node zu übergeben. Das macht jeden Eintrag
unabhängig von Quelle oder Alter gleich vollständig.

## 5.5 Zustandsdateien

| Datei | Ort | Charakter | Zweck |
|---|---|---|---|
| `stale_block_headers.csv` | `stale_blocks.output_dir` | Append-only, für Splunk | Ein Ereignis pro tatsächlich neuem Fakt (Sichtung/Header verfügbar), siehe 5.6. |
| `registry.csv` | `stale_blocks.state_dir` | veränderlich, **nicht** für Splunk | Interne Buchführung: was ist bereits bekannt, was wurde zuletzt exportiert. |

`output_dir` und `state_dir` sind bewusst getrennt (siehe Kapitel 3), damit
ein Splunk-Forwarder, der auf `output_dir` zeigt, `registry.csv` nicht
versehentlich mit aufnimmt.

## 5.6 Ausgabeschema: `stale_block_headers.csv`

Eine Zeile pro tatsächlich neuem Ereignis (nicht pro Poll-Durchlauf).

| Feld | Beschreibung |
|---|---|
| `observed_at` | ISO-8601-UTC-Zeitstempel des Exports. |
| `height`, `hash` | Höhe und Blockhash des Tips. |
| `status` | `header_only` oder `unusable`. |
| `header_hex` | Rohe 80-Byte-Header-Bytes (hex), falls gültig. |
| `header_valid` | Ergebnis der Hash-Validierung. |
| `source` | `own_node` oder `github`. |
| `chaintip_status` | Nur bei `source=own_node`: der `getchaintips`-Statuswert (`valid-fork`, `valid-headers`, `headers-only`). |
| `branchlen` | Nur bei `source=own_node`: Länge des Forks laut `getchaintips`. |
| `version`, `previousblockhash`, `merkleroot`, `time`, `bits`, `nonce` | Aus dem Header dekodierte Felder (identisch zum Schema von `getblockheader`); alle `None`, falls der Header fehlt oder ungültig ist. |

## 5.7 Interner Zustand: `registry.csv`

Ein Eintrag pro bekanntem nicht-aktiven Blockhash, vollständig
überschrieben bei jedem Flush (wie `block_status.csv` auf der
Hauptketten-Seite – kein Append-Log).

| Feld | Beschreibung |
|---|---|
| `height`, `blockhash` | Identität. |
| `status` | `unusable` oder `header_only` (monoton). |
| `header_hex`, `header_valid` | Wie oben. |
| `source` | Quelle der zuletzt eingegangenen brauchbaren Information. |
| `chaintip_status`, `branchlen` | Nur relevant für `source=own_node`. |
| `first_seen` | ISO-8601-UTC-Zeitstempel der ersten Sichtung. |
| `last_exported_status` | Welcher Status zuletzt nach `stale_block_headers.csv` exportiert wurde – verhindert doppelte Exporte. |

## 5.8 Beendigung

Wie `rpc-ingest`: ein Signal-Handler setzt bei `SIGTERM`/`SIGINT` nur ein
`threading.Event`, das zwischen den beiden Durchläufen und vor dem
nächsten Schlafzyklus geprüft wird – ein sauberer Stopp checkpointet die
Registry vor dem Beenden.
