# 1. Überblick und Architektur

[← Index](00-index.md)

## 1.1 Zweck

`btc_parser_app` ist eine Datenpipeline, die Bitcoin-Blockchain- und
Mempool-Daten vom eigenen Bitcoin-Core-Node abgreift, in flache
CSV-Zeilen umwandelt und für den Import in Splunk (oder ein beliebiges
anderes Tool, das CSV-Dateien einlesen kann) aufbereitet:

- **On-Chain-Daten** direkt vom eigenen Bitcoin-Core-Node über
  `bitcoin-cli` bzw. JSON-RPC – jeder Block und jede Transaktion,
  aggregiert auf Zeilenebene, inklusive Mining-Pool-Zuordnung.
- **Nicht-aktive Chain-Tips** (verwaiste/stale Blöcke) als eigene
  Datenquelle, aus `getchaintips` des eigenen Nodes und dem
  GitHub-Datenset `bitcoin-data/stale-blocks`.
- **Sanktionierte Adressen im Mempool**: Transaktionen, die an eine
  sanktionierte Adresse zahlen oder von einer solchen ausgeben – ohne den
  Mempool selbst zu exportieren (Kapitel 10).

BTC-Preise gehören nicht zu dieser Anwendung: Sie gelangen über einen
eigenen Forwarder-Input in denselben Splunk-Index (`btc:prices`, siehe
Kapitel 9.4) und werden vom Dashboard mit ausgewertet.

## 1.2 Drei unabhängige Prozesse

Die Anwendung besteht bewusst aus **drei unabhängigen, dauerhaft
laufenden Prozessen** statt einem einzigen Multithreading-Skript. Sie
teilen sich weder Zustand noch Fehlerdomänen: Ein Fehler beim Abruf des
GitHub-Datensets legt niemals die Blockverarbeitung lahm, und ein
Absturz von `mempool-watch` hat keinen Einfluss auf `rpc-ingest`.

| Prozess | CLI-Kommando | Quellcode | Aufgabe |
|---|---|---|---|
| RPC-Parser | `rpc-ingest` | `btc_parser_app/rpc/ingest.py` | Holt Blöcke vom eigenen Node, flacht sie inkl. aller Transaktionen zu CSV-Zeilen ab, ordnet jeden Block anhand blockeigener Daten einem Mining-Pool zu (keine zusätzlichen Netzwerkaufrufe nötig). Reorg-sicher. |
| Stale-Blocks-Pipeline | `stale-blocks-ingest` | `btc_parser_app/rpc/stale_blocks.py` | Separate Datenquelle für nicht-aktive Chain-Tips (`getchaintips` + das GitHub-Datenset `bitcoin-data/stale-blocks`). Unabhängig von `rpc-ingest`s eigenem Reorg-Handling. |
| Mempool-Watch | `mempool-watch` | `btc_parser_app/rpc/mempool_watch.py` | Prüft jede Transaktion, die in den Mempool des eigenen Nodes gelangt (ZMQ `sequence` + periodischer `getrawmempool`-Abgleich), gegen eine Sanktionsliste und exportiert pro Treffer ein Ereignis je Zustandswechsel (`seen`/`confirmed`/`replaced`/`removed`/`unconfirmed`, siehe Kapitel 10). |

Jeder Prozess läuft bis `SIGTERM`/`SIGINT`. Für einen gehärteten
Linux-Betrieb gibt es für jeden der drei Befehle eine eigene systemd-Unit
mit eigener Restart-Policy (`systemd/install.sh`, siehe Kapitel 2.5) –
`start.sh`/`stop.sh` decken lokale/Dev-Umgebungen und einfache
Dauerbetriebs-Hosts ab, starten einen abgestürzten Prozess aber nicht
automatisch neu.

## 1.3 Punktuell auszuführende Kommandos

Neben den drei Dauerprozessen gibt es:

- `mempool-watch-check TXID [--blockhash HASH]` – prüft eine einzelne
  Transaktion mit derselben Logik wie `mempool-watch` gegen die
  Sanktionsliste und gibt die Treffer aus, ohne etwas zu schreiben (zum
  Testen, siehe Kapitel 10.8).
- `update-pools-dataset` – aktualisiert das Mining-Pool-Signaturdatenset
  von GitHub (normalerweise automatisch durch `rpc-ingest` erledigt, siehe
  Kapitel 4).

## 1.4 Konfigurationsprinzip

Jeder Tunable-Wert (RPC-Verbindungsdetails, Poll-Intervalle,
Ausgabepfade, Sanktionsliste) liegt in `config/config.yaml` – im Code ist
nichts fest verdrahtet. Jedes Modul erhält seine Konfiguration als
typisiertes Dataclass-Objekt (`btc_parser_app/config.py`) statt aus
globalen Konstanten zu lesen; dadurch lässt sich das Verhalten (eine
andere Konfigurationsdatei, eine in einem Test erzeugte Konfiguration)
austauschen, ohne Code zu verändern (siehe Kapitel 3).

## 1.5 Datenflussdiagramm (konzeptionell)

```
                    ┌─────────────────────┐
                    │  eigener Bitcoin-    │
                    │  Core-Node           │
                    └──────────┬───────────┘
                               │ bitcoin-cli / JSON-RPC  +  ZMQ sequence
              ┌────────────────┼──────────────────────┬──────────────────┐
              │                                        │                  │
     ┌────────▼─────────┐              ┌───────────────▼────┐   ┌─────────▼─────────┐
     │   rpc-ingest      │              │ stale-blocks-ingest │   │  mempool-watch     │
     │  (aktive Chain)   │              │ (nicht-aktive Tips)  │   │ (Mempool vs.       │
     └────────┬──────────┘              └─────────┬───────────┘   │  Sanktionsliste)   │
              │                                    │               └─────────┬─────────┘
     export/rpc/{blocks,transactions,     stale_block_headers.csv            │
       inputs,outputs}/                           │              sanctioned_tx_events.csv
     + state/rpc (Status-/Index-Dateien,  bitcoin-data/stale-blocks   + state/mempool_watch
       intern)                             (GitHub-Datenset, HTTP)     (flagged.json, intern)
```

Alle erzeugten CSV-Dateien sind für den Import in Splunk (oder ein
vergleichbares Log-/Event-Analysesystem) gedacht – siehe Kapitel 6 für
Details zum Schema und zur empfohlenen Aufnahmestrategie.
