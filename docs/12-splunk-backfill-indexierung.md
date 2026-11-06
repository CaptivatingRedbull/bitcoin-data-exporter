# 12. Splunk: Backfill der vollständigen Chain indexieren

[← Index](00-index.md)

Runbook für den Moment, in dem `rpc-ingest` die komplette Chain nach
`export/rpc/` geschrieben hat und Splunk sie einlesen soll – so schnell wie
möglich, und ohne dass Daten verloren gehen.

## 12.1 Warum hier Vorsicht nötig ist

Die `batch`-Inputs für `export/rpc/` (Kapitel 10.3) verwenden
`move_policy = sinkhole`: Splunk **löscht** jeden Part, sobald er
indexiert ist. `rpc-ingest` exportiert einen Block nie ein zweites Mal
(`index/index.csv`, Kapitel 4.5). Was Splunk beim Einlesen falsch
macht oder hinterher wegwirft, lässt sich nur durch einen kompletten
Neu-Parse (neue `state_dir`/`output_dir`) wiederherstellen.

Mit Splunks Standardwerten passiert bei diesen Daten genau das – sie sind
für frische Logdaten gedacht, nicht für Ereignisse ab 2009:

| Standardwert | Folge für die Chain-Daten |
|---|---|
| `props.conf` `MAX_DAYS_AGO = 2000` (~5,5 Jahre) | Jeder Zeitstempel vor ~2021 gilt als ungültig; das Event bekommt eine Ersatzzeit → falsches `_time` für den Großteil der Chain. |
| `indexes.conf` `frozenTimePeriodInSecs = 188697600` (6 Jahre) | Buckets, deren neuestes Event älter als 6 Jahre ist, werden beim Rollen eingefroren – ohne `coldToFrozenDir` heißt das **gelöscht**. |
| `indexes.conf` `quarantinePastSecs = 77760000` (900 Tage) | Fast alles landet im Quarantäne-Bucket statt in normalen Buckets. |
| `indexes.conf` `maxTotalDataSizeMB = 500000` (~488 GB) | Wird die Grenze überschritten, löscht Splunk die ältesten Buckets. |
| Universal Forwarder `limits.conf` `maxKBps = 256` | Der UF drosselt sich auf 256 KB/s – ein Multi-TB-Backfill würde Monate dauern. |
| `server.conf` `parallelIngestionPipelines = 1` | Parsing/Indexierung nutzt effektiv einen Kern, egal wie groß der Host ist. |

Außerdem: Bei `INDEXED_EXTRACTIONS` parst die Instanz, die die Datei
**liest**, nicht der Indexer. Auf der Test-Standalone-Instanz ist das
dieselbe; dort liegen `zrz_in_btc_exporter`, `zrz_ko_btc` und
`zrz_rd_btc` zusammen. In Prod (UF → Cribl → Splunk) übernimmt die
Cribl-Pipeline die Index-Time-Arbeit, und `zrz_ko_btc` gehört
**nicht** auf den UF (Kapitel 10.1/10.2).

## 12.2 Die Bausteine im Repository

| Pfad | Wohin | Inhalt |
|---|---|---|
| `splunk/zrz_ko_btc/` | lesende Instanz (Test-Standalone; Prod: CM + SHCD, dort hinter Cribl wirkungslos) | Index-Time-`props.conf`/`transforms.conf` (Kapitel 10.6, erzeugt von `tools/splunk/build_field_config.py`), mit `MAX_DAYS_AGO = 10951` (Maximum, ~30 Jahre) und `ANNOTATE_PUNCT = false` für alle Sourcetypes. |
| `splunk/zrz_rd_btc/` | Search Head (Test-Standalone; Prod: SHCD) | Search-Time-Extraktion und `fields.conf` (Kapitel 10.6), Dashboard und Reports (Kapitel 10.9). |
| `splunk/zrz_ix_btc/` | Indexer (Prod: CM) | `[zrz_btc]`: Retention ~50 Jahre (`frozenTimePeriodInSecs`, `quarantinePastSecs`), `maxTotalDataSizeMB` 6 TB (an die Platte anpassen!), `maxDataSize = auto_high_volume`, `maxHotBuckets = 10`, `tsidxWritingLevel = 4`. Existiert der Index schon (z. B. über Splunk Web angelegt), gewinnen dessen Pfade; Retention-Werte, die dort explizit gesetzt sind, müssen dort korrigiert werden – `btool --debug` zeigt, woher jeder Wert kommt. |
| `splunk/zrz_in_btc_exporter/local/limits.conf`, `local/server.conf` | lesende Instanz (UF; Test-Standalone = zugleich Indexer) | `maxKBps = 0`, `parallelIngestionPipelines = 4`, Queue-Größe 32 MB. Früher eine eigene Tuning-App; nach der ZRZ-Vorlage gibt es dafür keinen App-Typ. Der UF-Teil gehört in die Input-App (wird nur an den Parser-Host verteilt), der Indexer-Teil ist global für alle Use Cases und damit Sache der Plattform (10.10). |
| `tools/splunk/btc-splunk-enable-backfill.sh` | Host (als root) | `--check`: Bestandsaufnahme und Prüfung aller Punkte aus 12.1, ändert nichts. `--enable`: dieselbe Prüfung; nur wenn nichts FAIL ist, werden die `disabled`-Zeilen der `export/rpc/`-Batch-Stanzas entfernt und Splunk neu gestartet (Indexer zuerst, dann die lesende Instanz), danach 5 Minuten Beobachtung, ob Parts verschwinden. Optional `--snapshot-dir` (Hardlink-Snapshot, siehe 12.3 Schritt 5), `--allow-empty` (Neustart von Null, noch keine Parts). Log: `/var/log/btc-splunk-enable-backfill.log`. |
| `tools/splunk/btc-fresh-start.sh` | Host (als root) | Neustart von Null mit zeitgesteuertem Start, siehe 12.8. |
| `tools/splunk/btc-splunk-bench.sh` | Host | Suchperformance-Benchmark, siehe 12.9. |

Die Prüfungen des Skripts:

| Prüfung | Schwelle | Stufe |
|---|---|---|
| je Sourcetype auf der lesenden Instanz `INDEXED_EXTRACTIONS = csv` **oder** `TIME_PREFIX` + `TRANSFORMS-zrz_btc_10_indexed` (Kapitel 10.6) | – | FAIL |
| `MAX_DAYS_AGO` je Sourcetype auf der lesenden Instanz | ≥ Tage seit 2009 + 1 Jahr | FAIL |
| Index existiert auf dem Indexer | – | FAIL |
| `frozenTimePeriodInSecs`, `quarantinePastSecs` | ≥ Sekunden seit 2009 + 5 Jahre | FAIL |
| `maxTotalDataSizeMB` | ≥ 1,2 × aktuelle Exportgröße | FAIL |
| freier Platz unter `SPLUNK_DB` | gleiches Dateisystem wie der Export: ≥ 0,5 × Export (WARN), sonst ≥ 1,2 × Export (FAIL) | WARN/FAIL |
| `maxKBps`, `parallelIngestionPipelines`, `maxDataSize` | 0 / > 1 / `auto_high_volume` | WARN |
| Transparent Huge Pages, `nofile`-Limit von splunkd | `never` / ≥ 64000 | WARN |

Die Lizenz lässt sich offline nicht prüfen – siehe 12.6.

## 12.3 Ablauf

Alles bis Schritt 6 vor dem geplanten Start; die Batch-Inputs bleiben bis
dahin deaktiviert.

1. **Code holen**:
   ```bash
   cd /opt/btc_parser/app && git pull
   ```
2. **Apps installieren** (Pfade anpassen: `/opt/splunk` = Enterprise,
   `/opt/splunkforwarder` = UF; bei Enterprise-only beides nach
   `/opt/splunk`):
   ```bash
   READ=/opt/splunkforwarder   # Instanz mit zrz_in_btc_exporter
   IDX=/opt/splunk             # Indexer
   APPS=/opt/btc_parser/app/splunk
   # Input-App: beim ersten Mal komplett kopieren und $PARSER_DATA_ROOT in
   # local/inputs.conf ersetzen (Kapitel 10.10 #2). Danach nur noch die
   # Tuning-Dateien nachziehen, damit die angepasste inputs.conf bleibt:
   cp $APPS/zrz_in_btc_exporter/local/{limits,server}.conf $READ/etc/apps/zrz_in_btc_exporter/local/
   cp -r $APPS/zrz_ko_btc $READ/etc/apps/
   cp -r $APPS/zrz_ix_btc $IDX/etc/apps/
   cp -r $APPS/zrz_rd_btc $IDX/etc/apps/
   chown -R "$(stat -c %U $READ)": $READ/etc/apps/zrz_*_btc*
   chown -R "$(stat -c %U $IDX)":  $IDX/etc/apps/zrz_*_btc*
   ```
   Sind UF und Indexer getrennte Instanzen, braucht der Indexer die
   `server.conf`-Werte zusätzlich (dort nicht über eine Use-Case-App,
   siehe 12.2). Im ZRZ-Betrieb verteilen DS (`zrz_ds_btc` →
   `zrz_in_btc_exporter`), Cluster Manager (`zrz_ix_btc`, `zrz_ko_btc`) und
   SHC-Deployer (`zrz_ko_btc`, `zrz_rd_btc`) die Apps statt `cp`.
   Vorher `maxTotalDataSizeMB` in
   `zrz_ix_btc/default/indexes.conf` an die tatsächliche Größe
   des `SPLUNK_DB`-Volumes anpassen (lieber dort per `local/indexes.conf`
   überschreiben als die Datei aus dem Repo ändern).
3. **Prüfen**:
   ```bash
   /opt/btc_parser/app/tools/splunk/btc-splunk-enable-backfill.sh --check --count-rows
   ```
   Muss mit `Preflight: 0 FAIL` und `Would enable: yes` enden. Die
   ausgegebenen Zeilenzahlen notieren (Abgleich in 12.5). WARNs ansehen –
   die zu Durchsatz (12.4) sollten weg sein.
4. **Optional, aber empfohlen – Canary**: Neustart jetzt (Batch-Inputs
   sind noch aus, es wird nichts konsumiert), dann ein paar alte Zeilen in
   einen Testindex:
   ```bash
   systemctl restart Splunkd            # bzw. SplunkForwarder + Splunkd
   mkdir -p /tmp/zrz_btc_canary
   head -n 101 /parser-data/export/rpc/blocks/blocks.csv > /tmp/zrz_btc_canary/blocks_2009.csv
   # Index zrz_btc_canary vorher in Splunk Web anlegen; dann (auf der lesenden Instanz):
   $READ/bin/splunk add oneshot /tmp/zrz_btc_canary/blocks_2009.csv -index zrz_btc_canary -sourcetype zrz:btc:blocks
   ```
   Suche `index=zrz_btc_canary | stats count min(_time) as min max(_time) as max | convert ctime(min) ctime(max)`
   → 100 Events, Minimum 03.01.2009. Danach Index `zrz_btc_canary` löschen.
5. **Snapshot entscheiden**: `--snapshot-dir` legt direkt vor dem
   Aktivieren einen Hardlink-Snapshot (`cp -al`) der Export-Ordner an.
   Das kostet zunächst keinen Platz; aber solange er existiert, gibt das
   Löschen durch Splunk keinen Platz frei – es braucht also Platz für
   Export **und** Index gleichzeitig. Dafür lässt sich bei einem Fehler
   alles neu einlesen, ohne die Chain neu zu parsen. Muss auf demselben
   Dateisystem liegen und außerhalb der Input-Pfade, z. B.
   `/parser-data/export-snapshot`.
6. **Start einplanen** (einmaliger systemd-Timer, verschwindet nach dem
   Lauf):
   ```bash
   systemd-run --unit=btc-splunk-enable --on-calendar='2026-10-18 01:00:00 Europe/Berlin' \
     /opt/btc_parser/app/tools/splunk/btc-splunk-enable-backfill.sh --enable \
       --snapshot-dir /parser-data/export-snapshot
   systemctl list-timers 'btc-splunk-enable*'     # prüfen
   systemctl stop btc-splunk-enable.timer         # abbrechen
   journalctl -u btc-splunk-enable                # Ausgabe danach (auch im Log-File)
   ```
   Schlägt um 01:00 eine Prüfung fehl, ändert das Skript nichts und
   endet mit Exit-Code 1.

## 12.4 Durchsatz-Hebel

In der Reihenfolge ihrer Wirkung:

1. **`maxKBps = 0` auf einem UF** (`zrz_in_btc_exporter/local/limits.conf`). Ohne das ist alles andere
   egal.
2. **`parallelIngestionPipelines`** (`zrz_in_btc_exporter/local/server.conf`: 4;
   auf einem separaten Indexer Plattform-Einstellung). Jede Pipeline-Gruppe
   liest, parst und indexiert unabhängig, mit eigenen Hot-Buckets. Auf
   dem 16-Kern-Host mit bitcoind daneben ist 4 der Startwert, 6 das
   sinnvolle Maximum. Im Monitoring Console (*Indexing → Performance:
   Instance*) ablesen: Sind die Queues aller Pipelines dauerhaft voll, ist
   die Indexierung/Platte die Grenze und mehr Pipelines helfen nicht; sind
   sie leer und CPU frei, erhöhen (Neustart nötig).
3. **UF-Hop sparen**, falls UF und Enterprise auf demselben Host laufen:
   `zrz_in_btc_exporter` und `zrz_ko_btc` direkt in
   `/opt/splunk/etc/apps` installieren und den UF für diese Inputs
   deaktivieren. Spart das Weiterleiten über TCP und eine zweite
   Pipeline-Kette. Für `batch`-Inputs unkritisch, weil die Dateien nach dem
   Einlesen ohnehin weg sind (kein Fishbucket-Zustand, der umziehen
   müsste). Vor dem Aktivieren erledigen, nicht mittendrin.
4. **Große Buckets** (`maxDataSize = auto_high_volume`, `maxHotBuckets = 10`,
   `zrz_ix_btc`): weniger Bucket-Rolls und Optimize-Läufe.
5. **Konkurrenz abschalten** während des Backfills: Data-Model-
   Beschleunigungen und geplante Suchen auf `zrz_btc` pausieren (sie
   würden jeden neuen Bucket sofort wieder durchsuchen), danach wieder
   einschalten.
6. **Betriebssystem**: Transparent Huge Pages auf `never` (Splunk-
   Empfehlung; `echo never > /sys/kernel/mm/transparent_hugepage/enabled`,
   dauerhaft per Kernel-Parameter `transparent_hugepage=never`), `nofile`
   ≥ 64000 für splunkd (die von `splunk enable boot-start -systemd-managed 1`
   erzeugte Unit setzt das bereits).

Der größte Hebel war eine **Designentscheidung**: Ursprünglich legte
`INDEXED_EXTRACTIONS = csv` für jede Spalte jedes Events ein indiziertes
Feld an. Seit Kapitel 10.6 werden auf `zrz:btc:transactions`/`zrz:btc:inputs`/
`zrz:btc:outputs` nur noch 5/5/3 von 63/23/12 Spalten indiziert, der Rest wird
zur Suchzeit extrahiert. Das heißt weniger Index-Time-CPU und deutlich
kleinere tsidx-Dateien, dafür kein `tstats` mehr auf Messwerten wie
Gebühren oder Beträgen.

## 12.5 Beobachten und verifizieren

Durchsatz je Sourcetype (KB pro 5 Minuten):

```
index=_internal source=*metrics.log group=per_sourcetype_thruput series=zrz:btc:*
| timechart span=5m sum(kb) by series
```

Queue-Füllstand (welche Stufe ist der Engpass):

```
index=_internal source=*metrics.log group=queue
| eval fill_pct = round(current_size_kb / max_size_kb * 100)
| timechart span=5m perc90(fill_pct) by name
```

Zeitstempel-Probleme:

```
index=_internal sourcetype=splunkd (component=DateParserVerbose OR component=AggregatorMiningProcessor) btc
```

Fortschritt und Zeitbereich:

```
| tstats count min(_time) as min max(_time) as max where index=zrz_btc by sourcetype
| convert ctime(min) ctime(max)
```

Am Ende müssen die Zählungen den Zeilenzahlen aus `--check --count-rows`
entsprechen (plus die seitdem am Tip hinzugekommenen Blöcke), `min` bei
`zrz:btc:blocks`/`zrz:btc:transactions`/`zrz:btc:outputs`/`zrz:btc:inputs` am
03.01.2009 liegen und die Export-Ordner bis auf die laufend
hinzukommenden Tip-Parts leer sein. Erst dann den Snapshot löschen:

```bash
rm -rf /parser-data/export-snapshot
```

## 12.6 Lizenz

Splunk Enterprise lizenziert das **täglich indexierte Volumen**. Ein
Backfill von mehreren TB an einem Tag überschreitet jede kleine Lizenz um
Größenordnungen; was dann passiert (Warnung, Lizenzverstoß, ggf.
gesperrte Suche), hängt vom Lizenztyp ab. Vor dem Start unter
*Settings → Licensing* prüfen, ggf. einen Lizenzschlüssel für den
Backfill-Zeitraum besorgen oder den Backfill über mehrere Tage verteilen
(Inputs zwischendurch wieder deaktivieren).

## 12.7 Danach

- `parallelIngestionPipelines` kann für den Dauerbetrieb (ein Block alle
  ~10 Minuten) auf 1–2 zurück; es schadet aber auch nicht.
- Data-Model-Beschleunigungen und geplante Suchen wieder einschalten.
- `rpc.catchup_workers`/`rpc.transport` (Kapitel 3.5) beeinflussen nur die
  Aufholjagd; im Tip-Following spielen sie keine Rolle.

## 12.8 Neustart von Null (Index leeren, Parser ab Genesis)

`tools/splunk/btc-fresh-start.sh` (als root, Test-Standalone):

1. **Vorher (optional): Benchmark auf dem alten Stand** – 12.9,
   `btc-splunk-bench.sh before`. Die Suchen laufen auch auf einem Index, in
   dem jede Spalte indiziert ist, und ergeben so die Vergleichsbasis.
2. **Code holen** (`git pull` im App-Verzeichnis, Branch mit Kapitel 10.6).
3. **`btc-fresh-start.sh prepare`** – zeigt, was passiert, und verlangt die
   Eingabe `FRESH`:
   - stoppt `btc-parser-rpc-ingest` (die anderen drei Dienste laufen weiter)
   - stellt sicher, dass die `export/rpc`-Batch-Stanzas `disabled = 1`
     tragen
   - installiert `zrz_ko_btc` und `zrz_rd_btc` aus dem
     Checkout (nur `default/`, `metadata/`, `lookups/`; `local/` bleibt)
   - stoppt Splunk, führt `splunk clean eventdata -index zrz_btc` aus und
     setzt den Fishbucket-Eintrag jeder Datei der `monitor`-Stanzas zurück
     (Preise, Stale-Header, Sanktionsereignisse werden ab Byte 0 neu
     gelesen)
   - löscht `state/rpc` und `export/rpc` und legt die vier leeren
     Export-Ordner wieder an
   - lässt Splunk bis zum Start **gestoppt**, damit vor dem Startzeitpunkt
     nichts indiziert wird (`--keep-splunk-running`: sofort wieder starten)
   - schreibt `/etc/cron.d/btc-fresh-start` für `--at`
     (Standard: nächstes 01:00 Ortszeit)
4. **Um `--at`** führt der Cronjob `btc-fresh-start.sh start` aus. Er löscht
   zuerst seine eigene Cron-Datei (einmalig) und ruft dann
   `btc-splunk-enable-backfill.sh --enable --allow-empty` auf (Prüfung,
   `disabled`-Zeilen entfernen, Splunk starten). **Nur wenn das klappt**,
   folgt `systemctl start btc-parser.target`. Bei einer fehlgeschlagenen
   Prüfung bleibt `rpc-ingest` absichtlich aus, weil ohne Konsumenten
   `export/rpc` die Platte füllen würde.
5. **Danach**: `btc-fresh-start.sh status`, Log
   `/var/log/btc-fresh-start.log` und
   `/var/log/btc-splunk-enable-backfill.log`, Fortschritt per 12.5.

**Dashboard nach dem Backfill:** Die stündlichen `zrz_btc_rec_*_update`-Suchen
von `zrz_rd_btc` sehen nur Events der letzten 7 Tage
Ereigniszeit, also keine historischen Blöcke. Ist der Backfill am Tip
angekommen, die fünf `zrz_btc_rec_*_rebuild`-Suchen einmal von Hand starten,
nacheinander, jede dauert Stunden (Kapitel 10.9). Vorher bleiben die
Rekord-Panels leer bzw. zeigen nur neue Blöcke.

## 12.9 Benchmark

```bash
SPLUNK_AUTH='admin:…' tools/splunk/btc-splunk-bench.sh <label> [GRUPPE…]
tools/splunk/btc-splunk-bench.sh --compare before after
```

Die Suchen spiegeln das Dashboard wider (Gruppen im Skriptkopf: L
Lookups, B Block-Panels, P Preise, S Sanktionen/Stale, T `tstats` auf den
indizierten Detailfeldern, R normale Suchen über feste Zeitfenster für die
früheren `tstats`-Panels, C Parallelität, H schwere Volllauf-Suchen, nur
explizit). Ergebnis: `/root/bench_<label>.txt` und `.csv`. Pro Suche gibt es
ein Timeout (`BENCH_TIMEOUT`, Standard 1800 s, Status `TIMEOUT`). Die
festen Fenster liegen in 2024; ein Vergleich ist erst sinnvoll, wenn der
Backfill über 2025-01-01 hinaus ist.
