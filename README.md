# btc_parser_app

> For a full German-language reference (architecture, every config
> option, every output file/schema, ingestion guidance, source-file-by-
> source-file walkthrough), see [`docs/`](docs/00-index.md).

A Bitcoin block/mempool/price data pipeline with three independent
long-running processes/services instead of one multithreaded main script -
they don't share state or failure domains, so a 429 on the API side never
touches block parsing, and a node hiccup never touches pricing:

- **RPC parser** (`rpc-ingest`) - pulls blocks from your own `bitcoin-cli`,
  flattens each block + its transactions into CSV rows, and attributes each
  block to a mining pool purely from data already in the block (no network
  calls needed - see **Mining pool extractor** below). Reorg-aware
  throughout (see **RPC Parser Reorg Handling** below).
- **Stale/orphaned chain-tip pipeline** (`stale-blocks-ingest`) - a separate
  sourcetype from `rpc-ingest`'s main-chain output, tracking non-active
  chain tips (see its own section below).
- **API fetcher** (`api-poll`) - polls the mempool.space HTTP API on a
  budget so it never trips a 429, including a minutely BTC/USD+EUR price
  snapshot (see **Pricing** below).

Everything that's a tunable - endpoints, polling cadence, the rate-limit
budget, RPC connection details, output paths - lives in
[`config/config.yaml`](config/config.yaml). Nothing is hardcoded in code.

## Layout

```
full_app/
  start.sh                      production-style startup: checks bitcoind, launches all three services in the background
  stop.sh                       stops what start.sh started
  run.py                       convenience CLI entrypoint
  requirements.txt
  parser-data/                  all runtime data (created on first run) - logs/, state/{rpc,stale}
                                 (internal bookkeeping, never Splunk-facing), export/{api,rpc,stale}
                                 (Splunk-facing - see config.yaml reference below)
  config/
    config.yaml                 all settings (see below)
    config.production.yaml      same schema, pointed at the production pod's paths (see "Production deployment" below)
    pools-v2.json                bundled mining-pool signature dataset
    XBTUSD_1.csv / XBTEUR_1.csv  (not bundled) drop your Kraken 1-minute OHLC exports here - see Pricing below
  btc_parser_app/
    config.py                    loads+validates config.yaml
    common/
      csv_writer.py               shared append-only, size-rotated CSV writer
      logging_setup.py             console + rotating-file logging
    api/                          mempool.space side ("api-poll")
      rate_limiter.py              token-bucket rate limiter
      client.py                    rate-limited HTTP GET client (retries, 429 handling)
      mempool_endpoints.py         per-endpoint JSON -> row parsers
      poller.py                    threaded interval poller
      mining_pools_dataset.py      refreshes config/pools-v2.json from GitHub
      price_history_import.py      one-time bulk import of two Kraken minute OHLC CSVs into prices.csv ("import-price-history")
    rpc/                          bitcoin-cli side ("rpc-ingest") - one implementation, no separate backfill script
      client.py                    bitcoin-cli subprocess wrapper
      block_parser.py              block/tx JSON -> flat CSV rows
      mining_pools.py              mining pool extractor (tag + address matching)
      reorg_state.py               index/current.csv/latest.csv/block_status.csv/reorg/ state files
      part_writer.py                state_dir->output_dir handoff + atomic per-block writes for blocks/transactions
      ingest.py                     genesis-to-tip catch-up + continuous reorg-aware tip-following
      stale_blocks.py               stale/orphaned chain-tip pipeline ("stale-blocks-ingest")
      stale_blocks_github.py        pulls the bitcoin-data/stale-blocks GitHub dataset
      stale_blocks_state.py         registry.csv - internal bookkeeping for stale_blocks.py
    cli.py                        argparse entrypoint, wired to run.py
```

## Setup

```sh
cd full_app
python3 -m venv .venv        # or reuse the repo's existing .venv
.venv/bin/pip install -r requirements.txt
```

`bitcoin-cli` must be on `PATH` and able to reach your node (locally
configured `bitcoin.conf`/cookie file, or see `rpc.extra_args` below for a
remote node).

> **Note:** `rpc.output_dir`/`rpc.state_dir` (defaults `parser-data/export/rpc`
> and `parser-data/state/rpc`) resolve relative to `full_app/`. `rpc-ingest`
> builds its own state from scratch the first time it's pointed at a pair of
> directories - if they're empty, it starts fresh from genesis (height 0)
> rather than assuming any pre-existing data in there is compatible with its
> schema.

## Running it in production: `start.sh` / `stop.sh`

```sh
./start.sh   # checks bitcoind is already reachable, then launches all three
             # services in the background
./stop.sh    # stops them (SIGTERM, then SIGKILL after a 30s grace period)
```

`start.sh`:

1. Checks whether the node configured in `config.yaml` is reachable over
   RPC. **It never starts a bitcoind itself - local or otherwise.** If RPC
   isn't reachable, it fails immediately with a clear message instead of
   guessing at node lifecycle; go start/fix the node yourself, then re-run
   this script.
2. Launches `rpc-ingest`, `stale-blocks-ingest`, and `api-poll` detached
   (`nohup`, nothing tied to your terminal), tracking each as a PID file
   under `.pids/`. Re-running `start.sh` is safe - anything already running
   is left alone.

All three commands log to `logs/<command>.log` (rotating, 20MB x5) in
addition to their raw stdout/stderr at `logs/<command>.out`. Point at a
different config file with `BTC_PARSER_CONFIG=/path/other.yaml ./start.sh`.

`stop.sh` sends SIGTERM to all three PIDs - `rpc-ingest`/`stale-blocks-ingest`
finish their current batch/pass and checkpoint cleanly before exiting;
`api-poll` (via a `SIGTERM` -> `KeyboardInterrupt` shim in `cli.py`) stops
the same way it would on Ctrl-C. Neither script ever touches bitcoind, on
startup or shutdown - it's entirely out of scope for both; stop it yourself
with `bitcoin-cli stop` if you want it down too.

For a hardened Linux deployment, wrap the three `python run.py rpc-ingest` /
`python run.py stale-blocks-ingest` / `python run.py api-poll` commands in
their own systemd units instead (each gets its own unit, logs, and restart
policy) - `start.sh`/`stop.sh` cover local/dev use and simple always-on
hosts, but don't restart a crashed process the way `Restart=always` would.

### Production deployment

This app runs over SSH on a dedicated storage pod, co-located on the same
node as the `bitcoin-core` pod via a shared data volume. Use
[`config/config.production.yaml`](config/config.production.yaml) there
instead of the default `config.yaml`:

```sh
BTC_PARSER_CONFIG=config/config.production.yaml ./start.sh
```

That config points `rpc.output_dir`/`mempool_api.output_dir`/
`logging.log_dir` at a large dedicated data volume instead of relative
paths, since the pod's own home directory (where this repo is cloned) is
far too small for chain data. It also connects straight to the
`bitcoin-core` pod's RPC port and authenticates via the RPC cookie file on
the volume shared with bitcoind, instead of `rpcuser`/`rpcpassword` - see
that file's own header comment for the exact connection details and how
that cross-pod RPC reachability was set up, since that's cluster-networking
configuration, not something `config.yaml` alone controls.

## Running it manually

```sh
# Genesis-to-tip catch-up, then continuous tip-following - the one and only
# RPC parser. Starts from height 0 if nothing's been parsed yet, resumes
# exactly where it left off otherwise. Runs forever until SIGTERM/SIGINT.
python run.py rpc-ingest

# Stale/orphaned chain-tip pipeline (getchaintips + the bitcoin-data/
# stale-blocks GitHub dataset) - a separate sourcetype from rpc-ingest's
# main-chain output. Runs forever until SIGTERM/SIGINT.
python run.py stale-blocks-ingest

# Long-running poller for the mempool.space endpoints (fees, mempool
# state, prices, difficulty adjustment, 24h pool hashrate share). Runs
# until Ctrl-C/SIGTERM or a 429.
python run.py api-poll

# Refresh config/pools-v2.json from GitHub (normally automatic - see below)
python run.py update-pools-dataset

# One-time (idempotent) bulk import of two Kraken 1-minute OHLC CSVs into
# prices.csv - see Pricing below. Safe to run before or after api-poll has
# started; already-imported minutes are skipped.
python run.py import-price-history
```

Every command accepts a `--config path/to/other-config.yaml` option to run
against a different config file (e.g. for a second node, or a test config).
**`--config` must come before the subcommand** - it's a top-level argparse
option, e.g. `python run.py --config config/config.production.yaml
rpc-ingest`, not `rpc-ingest --config ...` (that fails with "unrecognized
arguments" - see the comment on `build_parser()` in `cli.py` for why it's
deliberately not fixed to accept both orders).

## config.yaml reference

### `mempool_api`

The mempool.space HTTP poller. `rate_limit.requests_per_minute` /
`rate_limit.bucket_size` define a single shared token bucket that every
`endpoints` request draws from, so raising either value raises the
effective rate for the whole poller against that host, not per-endpoint.
mempool.space's public API limits are intentionally undisclosed, so the
default (10 req/min, burst of 10) is deliberately conservative.

`endpoints` is a list of `{name, path, parser, interval_seconds}`. Each
`parser` name must match a `parse_<name>` function registered in
`btc_parser_app/api/mempool_endpoints.py` - add a new mempool.space endpoint
by writing that function and adding an entry here; no other code changes
needed. Each endpoint writes to its own `<name>.csv` directly under
`output_dir` (default `parser-data/export/api`) - there's no separate state
dir for this component, since nothing in this app ever reads these files
back except `prices.csv` (see **Pricing** below). Point a Splunk `monitor`
(tailing, non-destructive) input here, not `batch`: unlike `rpc.output_dir`,
these are single ever-growing/rotating files, not one-complete-file-per-write,
so there's no point where a `batch` input could safely consume-and-delete
one without risking a partial read of the still-growing part.

Requests are sent without a custom `User-Agent` header (just the
`requests` library default) - there's no mempool.space subscription tier in
play here, so there's nothing to identify against.

### `mining_pools_dataset`

Where the RPC-side mining pool extractor gets its signatures from - the
*only* source of mining-pool attribution in this app; matching happens
entirely offline against RPC block data, with no per-block API calls to
mempool.space. `local_path` (default `config/pools-v2.json`) ships a
bundled snapshot from
[mempool/mining-pools](https://github.com/mempool/mining-pools) (MIT
licensed), so `rpc-ingest` works offline out of the box. Run
`update-pools-dataset` (or wire `refresh_if_stale` into a scheduler) to pull
newer pool signatures - `refresh_interval_seconds` defaults to one week,
matching the upstream project's own update cadence. A failed refresh logs a
warning and keeps using the existing local copy rather than breaking
ingestion.

### `pricing`

BTC price history, minutely throughout, all in one file:
`mempool_api.output_dir/prices.csv`. The live `prices` endpoint (see
`mempool_api.endpoints` above) polls mempool.space every 60s and appends a
`date_unix,usd,eur` row (`btc_parser_app/api/mempool_endpoints.py`'s
`parse_prices`) - `date_unix` is the price's own timestamp, not when it was
fetched. `python run.py import-price-history` fills in everything before
that live-polled window from two Kraken 1-minute OHLC/candle CSV exports
(their historical-data download, no header row:
`unix_timestamp,open,high,low,close,volume,trades` - use the "_1"-interval
file for each pair), joined on minute timestamp and written in the exact
same `date_unix,usd,eur` shape
(`btc_parser_app/api/price_history_import.py`):

- **`pricing.xbtusd_csv_path`** - path to the XBTUSD "_1" export; its
  close price becomes each row's `usd`.
- **`pricing.xbteur_csv_path`** - path to the XBTEUR "_1" export; its
  close price becomes each row's `eur`.

Both currencies get merged by minute timestamp (a minute present in only
one file still gets a row, with the other currency left null - same as
mempool.space's live endpoint occasionally omitting a currency).
Already-imported minutes are skipped by `date_unix`, so it's safe to run
`import-price-history` repeatedly (before or after `api-poll` has started,
in any order) - re-running against a refreshed/extended export only adds
what's new, and it never collides with what the live poller is writing.

This dedupe reads `prices.csv`'s full history back, which is exactly why it
lives under `mempool_api.output_dir` (Splunk-facing, `export/api/`) with a
`monitor` input rather than a `batch` one, same as every other endpoint
there: `monitor` never deletes, so the file this app depends on for
correctness and the file Splunk indexes from stay the same file - no
separate copy to keep in sync, no risk of `import-price-history` silently
missing already-indexed-and-deleted minutes and re-importing them as
duplicates.

### `rpc`

`bitcoin_cli_path` + `extra_args` are appended to every `bitcoin-cli`
invocation - for a remote/Kubernetes-hosted node set something like:

```yaml
extra_args: ["-rpcconnect=192.168.x.x", "-rpcport=8332"]
```

Never put `-rpcuser`/`-rpcpassword` directly in `extra_args` (or anywhere in
this file) - it's meant to be safe to commit. Set `rpcuser_env`/
`rpcpassword_env` to the *names* of environment variables instead; they're
only read at runtime, and are only appended as `-rpcuser=`/`-rpcpassword=`
flags if both are actually set. Note that any credential passed as a CLI
flag is visible to `ps` on a shared host - prefer cookie-file auth
(the default, if you set nothing) when that matters.

`batch_size` (default 20) controls how many blocks accumulate between
`current.csv` checkpoint log lines during backlog catch-up - every pass of
the ingest loop (see **RPC Parser Reorg Handling** below) flushes whatever
it processed to disk unconditionally when the pass ends, regardless of
whether a full batch was reached, so tip-following (where a pass is often
just the one new block) is never left waiting on 20 blocks to accumulate
before it's durable - `batch_size` only paces how often the "Progress:
height X/Y" log line and mid-pass checkpoint fire during a long backlog.
`output_dir`/`state_dir` (defaults `parser-data/export/rpc` and
`parser-data/state/rpc`) split the same way `stale_blocks` below does:
`output_dir` holds only `blocks/` and `transactions/`, and only complete,
Splunk-safe parts - nothing is ever written there directly while it's still
being appended to (see **File rotation** below). `state_dir` holds
everything internal - `current.csv`, `latest.csv`, `index/`,
`block_status.csv`, `reorg/`, the `*_part_seq.csv` part counters, and, while
there's backlog, the currently-still-growing `blocks/`/`transactions/` part
itself. Point Splunk at `output_dir` only, with a `batch`
(consume-and-delete) input - never at `state_dir`.

### `stale_blocks`

Config for `stale-blocks-ingest`: `output_dir` (Splunk-facing exports,
default `parser-data/export/stale`) and `state_dir` (internal bookkeeping
only, default `parser-data/state/stale`) are kept separate on purpose.
`node_poll_interval_seconds`
(default hourly) controls the `getchaintips` pass; `github.poll_interval_seconds`
(default daily) controls the `bitcoin-data/stale-blocks` GitHub CSV pull -
see **RPC Parser Reorg Handling** below for how this pipeline relates to
`rpc-ingest`'s own reorg handling (they're independent: this one tracks
non-active tips, `rpc-ingest` tracks the active chain).

### File rotation

Every append-only CSV this app writes (`index/index.csv`,
`peer_attempts.csv`, the stale-blocks exports, each mempool.space endpoint's
`<name>.csv` including `prices.csv`, and `blocks.csv`/`transactions.csv`
while there's backlog - see below) grows forever, so `common/csv_writer.py`
caps each on-disk part at ~900MB and rolls over into a new numbered part
before exceeding it: `<name>.csv` is the first part, then
`<name>.000002.csv`, `<name>.000003.csv`, and so on, alongside it in the
same directory. Nothing else about the layout changes - it's still one
logical CSV, just split into files that never cross ~1GB. Anything in this
app that needs to read a logical CSV back in full (the index,
`peer_attempts.csv`, bootstrapping from `blocks/blocks.csv`) reads every
part in order automatically. If you point an external tool (Splunk, a
monitoring stanza, ad-hoc scripts) at these files directly, make sure it
globs `<name>*.csv` rather than the exact first filename - see **Storage &
Splunk ingestion** below for how this interacts with a constrained disk
budget.

`blocks/blocks*.csv` and `transactions/transactions*.csv` are a special
case, via `rpc/part_writer.py`, on top of also being split across two
directories (`rpc.state_dir`/`rpc.output_dir` - see above): while
`rpc-ingest` has backlog beyond `rpc.reorg_confirmations`, rows accumulate
into the current part *under `state_dir`*, rotating by size exactly like
every other CSV above - and the instant a part is rotated away from (or
superseded by the atomic mode below), it's moved whole into `output_dir`
(a same-filesystem rename, atomic) and never written to again. So a part
only ever appears under `output_dir` once it's completely done - nothing
growing is ever visible there, in either mode. Once a pass's backlog is
within `reorg_confirmations` (i.e. it's caught up to the tip, not just
catching up towards it), every block instead gets written as its own
already-complete part straight into `output_dir` - a new
`blocks.NNNNNN.csv`/`transactions.NNNNNN.csv` per block, written via a temp
file + atomic rename, never touching `state_dir` at all. Either way, a
Splunk `batch` (sinkhole, consume-and-delete) input pointed at `output_dir`
never needs switching to `monitor`, during backfill or steady-state
tip-following alike (see **Storage & Splunk ingestion**) - there's nothing
to exclude and nothing to switch, ever. The part numbering is shared and
durable across both modes (persisted in
`state_dir/blocks_part_seq.csv`/`state_dir/transactions_part_seq.csv`, not
re-derived by scanning either directory), specifically so it survives
Splunk deleting older parts from `output_dir` and never reuses or collides
with a number, in either mode, across restarts.

`reorg_confirmations` (default 6), `max_reorg_depth` (default 100), and
`poll_interval_seconds` (default 30 - how long to sleep between tip checks
once caught up; ignored while there's backlog to catch up on) all tune
`rpc-ingest` - see **RPC Parser Reorg Handling** below.

### `logging`

`level` (default `INFO`) and `log_dir` (default `logs`, relative to
`full_app/`). Every command logs to both stdout and a rotating file at
`log_dir/<command-name>.log` (20MB per file, 5 kept). This is what
`start.sh`'s background services rely on for visibility, since their
detached stdout is just a raw `*.out` redirect.

High-volume per-item lines (the API poller's "wrote N row(s)" line on every
single fetch, `rpc-ingest`'s per-block "Parsing block N" line during a
genesis catch-up) log at `DEBUG`, not `INFO` - set `logging.level: DEBUG`
in config.yaml if you need that level of detail; the default `INFO` still
gets startup summaries, warnings, batch-level progress lines, and anything
that actually needs attention (a 429, a reorg, an RPC outage).

## Mining pool extractor (RPC side)

`btc_parser_app/rpc/mining_pools.py` is what "parses out" the mining pool
for every block fetched via RPC, with zero extra RPC calls or network
requests - this app deliberately does not backfill mempool.space's own
per-block pool history via its `/api/v1/blocks*` endpoints, since redundantly
re-deriving attribution this module already produces locally would burn a
lot of the rate-limit budget for no benefit. Bitcoin Core's `getblock`
output has no pool-identity field; pools identify themselves voluntarily in
the coinbase transaction in one of two ways, and `PoolMatcher.match()`
checks both, in this priority order:

1. **Coinbase tag** - pools stamp a short ASCII signature into the coinbase
   scriptSig, e.g. `/ViaBTC/`, `/AntPool/`, `/Foundry USA Pool #dropgold/`.
   `getblock verbosity=3` returns this as `vin[0].coinbase` (hex); it's
   decoded to ASCII and checked as a substring against every pool's known
   tags. This is deliberate self-identification and effectively
   unambiguous once matched.
2. **Payout address** - if no tag matches, the coinbase transaction's
   output addresses are checked against each pool's known payout
   addresses. This is a weaker signal (an address can end up shared
   between unrelated payers, e.g. via a custodian), so it's only a
   fallback, not the primary signal.

If neither matches, the block gets `pool_id/pool_name/pool_link = None` and
`pool_match_method = "unknown"` rather than a wrong guess.

These fields land directly on the block-level CSV row from `rpc-ingest`
(`blocks.csv`): `pool_id`, `pool_name`, `pool_link`, `pool_match_method`.
The signature dataset lives at `mining_pools_dataset.local_path` (see
above) and uses the exact schema mempool.space's own `pools-v2.json` uses,
so it's a drop-in replacement if you ever want to point it at a different
snapshot.

If the dataset file is missing or fails to parse, `rpc-ingest` logs a
warning and continues with every pool field set to `None` rather than
failing ingestion.

## Output schema notes

- `blocks.csv` intentionally omits `confirmations` and `nextblockhash`:
  both are current-chain-state snapshots that go stale/misleading once
  written to a historical CSV. `previousblockhash` is kept instead - it's
  intrinsic to the block, whereas `nextblockhash` depends on the currently
  selected chain and goes stale across a reorg.
- Per-transaction vin/vout rows are never exported individually - only
  aggregate/scalar fields (script-type counts, fee stats, witness byte
  counts, etc.) make it into `transactions.csv`, to keep row size bounded
  for downstream ingestion (originally sized for Splunk's ~10k char/event
  limit).
- `transactions.csv` drops any field that's a pure duplicate of one already
  exported, or of one already resolved elsewhere: `has_witness`,
  `has_taproot_input`, `has_taproot_output`, `has_op_return`, and
  `prevout_values_complete`/`prevout_heights_complete` are one-line derived
  booleans from count fields you already have (e.g.
  `witness_input_count > 0`); the raw `coinbase_script_sig_hex` and
  `coinbase_output_addresses_json` are dropped once they've fed the mining
  pool matcher, since `pool_id`/`pool_name`/`pool_link` on the block row
  already carry that result forward.
- `transactions.csv`'s `wtxid` is left null whenever it's equal to `txid`
  (every non-witness transaction) instead of repeating the 64-char hash -
  reconstruct the real value downstream with `coalesce(wtxid, txid)`. Only
  witness transactions, where the two genuinely differ, pay for the column.
- `mempool.csv` (the `mempool` API endpoint) intentionally does not include
  the raw `fee_histogram` mempool.space returns - a JSON blob stuffed into
  a single CSV cell isn't useful once it lands in Splunk. `tx_count`,
  `vsize_total`, and `total_fee_sats` cover the scalar signal worth
  indexing.

## RPC Parser Reorg Handling

`rpc-ingest` (`btc_parser_app/rpc/ingest.py`) uses blockhash as the unique
identity throughout. Every pass:

1. Reads the node tip and sets `latest = tip - rpc.reorg_confirmations`
   (default 6), written to `latest.csv`.
2. Reads `current.csv` (the last successfully processed canonical block).
3. Compares its stored hash against the live chain's hash at that height.
   - **Match** -> normal loop: parse `current_height + 1 .. latest`,
     exporting any height whose exact blockhash isn't already in `index/`
     yet (already-indexed heights are skipped, not re-exported).
   - **Mismatch** -> a reorg reached already-processed data. Walk backwards
     through `index/`'s recorded `previousblockhash` values until finding a
     height where the stored chain matches the live chain again (the
     common ancestor); mark every detached block `canonical=false` in
     `block_status.csv`, write an audit CSV to `reorg/`, then resume
     parsing from `ancestor + 1`.
4. Writes `current.csv` at each batch checkpoint and unconditionally at the
   end of the pass - a pass that only had 1-2 new blocks (steady-state
   tip-following) still gets a full, durable flush, it doesn't wait for
   `batch_size` blocks to accumulate (see the `rpc.batch_size` note above).

State files (all under `rpc.state_dir` - never `output_dir`, which holds
only the completed `blocks/`/`transactions/` exports):

- `index/index.csv` - immutable, append-only record of every block ever
  exported (`height,blockhash,previousblockhash`). Answers "has this exact
  blockhash ever been exported" - a height can have more than one row if
  it was ever reorged.
- `current.csv` / `latest.csv` - single-row `height,blockhash` pointers.
- `block_status.csv` - mutable `height,blockhash,canonical` rows. Only
  blocks a reorg has ever touched get a row here; anything never reorged
  has no row and is implicitly canonical.
- `reorg/reorg_<timestamp>_<lowest>_<highest>.csv` - one audit CSV per
  reorg event (`action,height,blockhash`, `action` is `detached` or
  `attached`). Debugging/audit only - never read back to determine current
  state.

Because `index/`/`blocks.csv`/`transactions.csv` are never rewritten for a
block that flips `canonical -> noncanonical -> canonical`, downstream
consumers never see a duplicate export for it - only `block_status.csv`'s
`canonical` flag changes. If the walk-back can't find a common ancestor
within `rpc.max_reorg_depth` blocks (default 100), or hits a block that was
never indexed, `rpc-ingest` raises and stops rather than guessing - this is
meant to require manual intervention, the same philosophy as `api-poll`
halting on a 429.

### Starting state: genesis, or wherever you left off

`rpc-ingest` works the same way no matter how many blocks (if any) are
already parsed - there's no separate "backfill" mode to run first:

- **Nothing parsed yet** (no `current.csv`, no `blocks.csv`): starts at
  height 0 and catches up to the tip.
- **`current.csv` present**: resumes from exactly there - reorg-checked
  first, as above.
- **`current.csv` missing but `blocks.csv` data exists** (under `state_dir`
  and/or `output_dir` - e.g. `current.csv` was deleted, or these
  directories were populated some other way and never got one): reads
  whatever `blocks.csv` parts are still on disk across both directories for
  their `height`/`hash`/`previousblockhash` columns to rebuild `index/` and
  resumes from the highest block found, instead of losing that work and
  re-parsing from genesis. Best-effort - `output_dir` is Splunk-facing, so
  older parts may already be gone by the time this runs.

Either way, once it has a starting point it just runs the normal loop
above. While there's a backlog (catching up from genesis, or from any gap),
it processes batches back-to-back with no delay; once it reaches `latest`
it falls back to polling every `rpc.poll_interval_seconds`. It runs forever
until SIGTERM/SIGINT, checkpointing `current.csv` after every pass so a
kill/crash loses at most one in-flight batch's worth of (safely re-parsed)
progress.

If a reorg happened at some point in the past but never touched anything
this run's starting point cares about, it makes no difference - the
normal loop and the reorg-recovery path are the same code either way, so
there's nothing to reconcile specially at startup.

This app intentionally does not implement a mempool.space-side per-block
pool-history backfill (paging through `/api/v1/blocks*` to redundantly
re-derive attribution the RPC-side extractor already produces locally) -
see **Mining pool extractor** above for why.

## Storage & Splunk ingestion

The parser host has a fixed, limited disk budget shared with the Bitcoin
Core datadir itself (which only grows), so exported CSVs are not meant to
live on this host forever - Splunk (or whatever's consuming them) needs to
actually pull them off in a timely way. This app deliberately stays
hands-off about that: it never touches, moves, or deletes a file once it's
handed it to a Splunk-facing `export/` directory - which rotated part gets
deleted when is entirely a decision for however you've configured Splunk's
inputs, since only Splunk (or you) actually knows what's been indexed.

`parser-data/` (see **Layout** above) is split into two kinds of directory
specifically so each `export/` subdirectory can be pointed at with a single,
permanent Splunk input mode - no switching between backfill and
steady-state, no excluding the newest file, no config discipline required:

- **`export/rpc/{blocks,transactions}/` - `batch` (sinkhole,
  consume-and-delete).** A part only ever appears here once it's completely
  done - during backlog catch-up it's moved in whole from `state/rpc/` the
  instant it's rotated away from (never while still being appended to); once
  caught up to the tip, each block's part is written directly here, already
  complete (see **File rotation** above). Either way, nothing growing is
  ever visible under `export/rpc/` - a `batch` input can consume and delete
  anything it finds there, at any time, with nothing excluded.
- **`export/api/` and `export/stale/` - `monitor` (tailing,
  non-destructive).** These aren't per-block atomic writes - each endpoint's
  `<name>.csv` (including `prices.csv`) and the stale-tip exports are a
  single file that's appended to and rotated by size like any other CSV in
  this app (see **File rotation** above), so the *current* part is always
  still growing. A `batch` input here would risk grabbing that file
  mid-write. `monitor` never deletes, so clean up old rotated parts
  yourself (by hand, or a cron job) once you've confirmed Splunk has indexed
  them - and for `export/api/prices.csv` specifically, this isn't just the
  safer default: `import-price-history` reads that file's full history back
  to dedupe (see **Pricing** above), so it must never be deleted out from
  under this app in the first place.

None of this is wired up automatically on purpose: an app-side "delete
this file, I'm sure Splunk has it" is one config mistake away from losing
data with no way to re-derive it (re-parsing from genesis is expensive;
re-fetching the API/pricing history may not even be possible). Configure
retention explicitly in Splunk (or in your own cleanup cron, once you've
verified indexing) rather than relying on this app to guess - and never
point any Splunk input at a `state/` directory.

## Reused from earlier scripts

This app is a restructured, config-driven port of earlier standalone
prototype scripts for the same two data sources (an RPC block/transaction
flattener and a mempool.space endpoint poller), extended with mining-pool
attribution, reorg handling, config-driven everything, and now the pricing
pipeline described above.
