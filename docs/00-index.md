# btc_parser_app – Dokumentation

Technische Referenzdokumentation für `full_app/` (Python-Paket
`btc_parser_app`). Zielgruppe: jede Person, die diese Anwendung betreiben,
warten, erweitern oder von Grund auf verstehen muss, ohne vorheriges Wissen
über das Projekt vorauszusetzen.

Bei Widersprüchen zwischen dieser Dokumentation und dem tatsächlichen
Quellcode ist immer der Code maßgeblich – die Dokumentation beschreibt den
Stand zum Zeitpunkt ihrer Erstellung.

## Kapitelübersicht

1. [Überblick und Architektur](01-ueberblick-und-architektur.md)
   Was die Anwendung tut, welche drei Prozesse sie umfasst, warum sie so
   aufgeteilt ist, und welche Datenquellen sie anzapft.
2. [Installation und Betrieb](02-installation-und-betrieb.md)
   Voraussetzungen, Einrichtung, `start.sh`/`stop.sh`, alle CLI-Kommandos,
   Produktivbetrieb.
3. [Konfigurationsreferenz](03-konfiguration-referenz.md)
   Jede Einstellung in `config.yaml`, ihre Bedeutung, ihr Default-Wert und
   ihre Validierungsregeln.
4. [RPC-Parser und Mining-Pool-Zuordnung](04-rpc-parser-und-mining-pools.md)
   Wie `rpc-ingest` Blöcke von `bitcoin-cli` holt, in CSV-Zeilen umwandelt,
   Reorgs erkennt und behandelt, und Blöcke Mining-Pools zuordnet.
5. [Stale-Blocks-Pipeline](05-stale-blocks-pipeline.md)
   Wie `stale-blocks-ingest` nicht-aktive Chain-Tips verfolgt und warum sie
   bewusst nur Blockheader statt vollständiger Blöcke sammelt.
6. [API-Poller und Pricing](06-api-poller-und-pricing.md)
   Wie `api-poll` die mempool.space-Endpunkte abfragt, das
   Rate-Limit-Budget verwaltet, und wie die tägliche Preis-Pipeline
   (Kraken-Import + Lückenfüllung) funktioniert.
7. [Ausgabedateien und Datenaufnahme](07-ausgabedateien-und-datenaufnahme.md)
   Vollständige Übersicht aller von der Anwendung erzeugten Dateien, deren
   Spaltenschema, die Dateirotation, und wie diese Dateien in Splunk (oder
   ein vergleichbares System) eingelesen werden sollten.
8. [Quellcode-Referenz](08-quellcode-referenz.md)
   Datei-für-Datei-Übersicht über das gesamte Repository: was jede Datei
   enthält und wofür sie zuständig ist.
9. [Logging, Fehlerbehandlung und bekannte Einschränkungen](09-logging-fehlerbehandlung-einschraenkungen.md)
   Wie Logging konfiguriert ist, die grundsätzliche Fehlerbehandlungs-
   Philosophie der Anwendung, und eine Liste bewusster Design-Grenzen.

## Kurzeinstieg

```sh
cd full_app
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
./start.sh
```

Voraussetzung: ein erreichbarer Bitcoin-Core-Node, ansprechbar über
`bitcoin-cli` (siehe Kapitel 2). Details zu jedem Schritt in den folgenden
Kapiteln.

## Ergänzende Dokumente im Repository

- [`README.md`](../README.md) – kürzere, englischsprachige Fassung im
  Repo-Root, primär für schnelles Nachschlagen von Kommandos.
- [`config/config.yaml`](../config/config.yaml) – die Konfigurationsdatei
  selbst, durchgängig kommentiert.
- [`config/config.production.yaml`](../config/config.production.yaml) –
  Produktivkonfiguration; der Dateikopf enthält die konkreten
  Infrastrukturdetails (Pfade, Netzwerkadressen) der Zielumgebung.
