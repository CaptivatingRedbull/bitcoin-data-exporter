# 1. Überblick und Architektur

[← Index](00-index.md)

## 1.1 Zweck

`btc_parser_app` ist eine Datenpipeline, die Bitcoin-Blockchain- und
Mempool-Daten aus zwei unabhängigen Quellen abgreift, in flache CSV-Zeilen
umwandelt und für den Import in Splunk (oder ein beliebiges anderes Tool,
das CSV-Dateien einlesen kann) aufbereitet:

- **On-Chain-Daten** direkt vom eigenen Bitcoin-Core-Node über
  `bitcoin-cli` (RPC) – jeder Block und jede Transaktion, aggregiert auf
  Zeilenebene, inklusive Mining-Pool-Zuordnung.
- **Netzwerk-/Marktdaten** von der öffentlichen mempool.space-HTTP-API –
  aktuelle Gebühren, Mempool-Zustand, Preise, Difficulty-Adjustment,
  Pool-Hashrate-Anteile.
- **Historische Preisdaten** aus zwei Kraken-Minutenkerzen-Exporten
  (XBTUSD/XBTEUR), die in dieselbe `prices.csv` eingespielt werden, in die
  auch der live gepollte Minutenpreis schreibt – eine einzige,
  durchgängig minütliche Preiszeitreihe ohne separate Tagestabelle.

Zusätzlich verfolgt eine dritte, unabhängige Pipeline nicht-aktive
Chain-Tips (verwaiste/stale Blöcke) als eigene Datenquelle.

## 1.2 Drei unabhängige Prozesse

Die Anwendung besteht bewusst aus **drei unabhängigen, dauerhaft
laufenden Prozessen** statt einem einzigen Multithreading-Skript. Sie
teilen sich weder Zustand noch Fehlerdomänen: Ein HTTP-429-Fehler auf der
API-Seite legt niemals die Blockverarbeitung lahm, und ein Ausfall des
Bitcoin-Nodes hat keinen Einfluss auf die Preis-Pipeline.

| Prozess | CLI-Kommando | Quellcode | Aufgabe |
|---|---|---|---|
| RPC-Parser | `rpc-ingest` | `btc_parser_app/rpc/ingest.py` | Holt Blöcke vom eigenen Node, flacht sie inkl. aller Transaktionen zu CSV-Zeilen ab, ordnet jeden Block anhand blockeigener Daten einem Mining-Pool zu (keine zusätzlichen Netzwerkaufrufe nötig). Reorg-sicher. |
| Stale-Blocks-Pipeline | `stale-blocks-ingest` | `btc_parser_app/rpc/stale_blocks.py` | Separate Datenquelle für nicht-aktive Chain-Tips (`getchaintips` + das GitHub-Datenset `bitcoin-data/stale-blocks`). Unabhängig von `rpc-ingest`s eigenem Reorg-Handling. |
| API-Poller | `api-poll` | `btc_parser_app/api/poller.py` | Fragt die mempool.space-Endpunkte in konfigurierbaren Intervallen ab, innerhalb eines gemeinsamen Rate-Limit-Budgets (Token-Bucket) – darunter der minütliche `prices`-Endpunkt, der in dieselbe `prices.csv` schreibt, die auch der einmalige Preis-Historie-Import befüllt (siehe Kapitel 6). |

Jeder Prozess läuft bis `SIGTERM`/`SIGINT` (bzw. bis `api-poll` einen
HTTP-429 erhält – dann hält der Prozess bewusst an und muss manuell neu
gestartet werden, siehe Kapitel 9). Für einen gehärteten Linux-Betrieb
empfiehlt es sich, jeden der drei Befehle in eine eigene systemd-Unit mit
eigenem Log und eigener Restart-Policy (`Restart=always`) zu verpacken –
`start.sh`/`stop.sh` decken lokale/Dev-Umgebungen und einfache
Dauerbetriebs-Hosts ab, starten einen abgestürzten Prozess aber nicht
automatisch neu.

## 1.3 Zwei zusätzliche, einmalig auszuführende Kommandos

Neben den drei Dauerprozessen gibt es zwei Kommandos, die punktuell
ausgeführt werden:

- `update-pools-dataset` – aktualisiert das Mining-Pool-Signaturdatenset
  von GitHub (normalerweise automatisch durch `rpc-ingest` erledigt, siehe
  Kapitel 4).
- `import-price-history` – einmaliger (aber beliebig wiederholbarer)
  Bulk-Import zweier Kraken-1-Minuten-OHLC-CSV-Exporte (XBTUSD, XBTEUR) in
  dieselbe `prices.csv`, in die auch `api-poll`s `prices`-Endpunkt live
  schreibt (siehe Kapitel 6). Rein lokal, ohne Netzwerkzugriff.

## 1.4 Konfigurationsprinzip

Jeder Tunable-Wert (Endpunkte, Poll-Intervalle, Rate-Limit-Budget,
RPC-Verbindungsdetails, Ausgabepfade) liegt in `config/config.yaml` – im
Code ist nichts fest verdrahtet. Jedes Modul erhält seine Konfiguration
als typisiertes Dataclass-Objekt (`btc_parser_app/config.py`) statt aus
globalen Konstanten zu lesen; dadurch lässt sich das Verhalten (eine
andere Konfigurationsdatei, eine in einem Test erzeugte Konfiguration)
austauschen, ohne Code zu verändern (siehe Kapitel 3).

## 1.5 Datenflussdiagramm (konzeptionell)

```
                    ┌─────────────────────┐
                    │  eigener Bitcoin-    │
                    │  Core-Node           │
                    └──────────┬───────────┘
                               │ bitcoin-cli (RPC)
              ┌────────────────┼────────────────┐
              │                                  │
     ┌────────▼─────────┐              ┌─────────▼──────────┐
     │   rpc-ingest      │              │ stale-blocks-ingest │
     │  (aktive Chain)   │              │ (nicht-aktive Tips)  │
     └────────┬──────────┘              └─────────┬───────────┘
              │                                    │
     export/rpc/{blocks,transactions}/    stale_block_headers.csv
     + state/rpc (Status-/Index-Dateien,          │
       intern)                            bitcoin-data/stale-blocks
                                           (GitHub-Datenset, HTTP)

                    ┌──────────────────────┐
                    │  mempool.space API    │
                    └──────────┬─────────────┘
                               │ HTTP GET (Token-Bucket-limitiert)
                    ┌──────────▼─────────────┐
                    │       api-poll          │
                    │   (5 Endpunkt-Threads,  │
                    │  einer davon "prices")  │
                    └──────────┬─────────────┘
                               │
        fees_precise.csv, mempool.csv, prices.csv (date_unix,usd,eur;
        60s-Takt), difficulty_adjustment.csv, mining_pools_24h.csv

     ┌────────────────────────────┐
     │ 2× Kraken-1-Minuten-Export  │
     │ (XBTUSD_1.csv, XBTEUR_1.csv,│
     │  manuell heruntergeladen)   │
     └────────────┬────────────────┘
                  │ import-price-history (einmalig, rein lokal)
                  ▼
     dieselbe prices.csv (auf Minute gejoint, keine Duplikate)
```

Alle erzeugten CSV-Dateien sind für den Import in Splunk (oder ein
vergleichbares Log-/Event-Analysesystem) gedacht – siehe Kapitel 7 für
Details zum Schema und zur empfohlenen Aufnahmestrategie.

## 1.6 Woher stammt diese Anwendung

Die Anwendung ist ein umstrukturierter, konfigurationsgetriebener Nachfolger
früherer eigenständiger Prototyp-Skripte für dieselben zwei Datenquellen
(ein RPC-Block-/Transaktions-Flattener und ein mempool.space-Endpunkt-
Poller), erweitert um Mining-Pool-Zuordnung, Reorg-Handling,
durchgängige Konfigurierbarkeit und die in Kapitel 6 beschriebene
Preis-Pipeline.
