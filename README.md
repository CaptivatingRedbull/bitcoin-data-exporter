# btc_parser_app

A Bitcoin block/mempool data pipeline with two independent data sources,
matching the two-service design in [`../Script_plan.md`](../Script_plan.md):

- **RPC parser** (`rpc_ingest`) - pulls blocks from your own `bitcoin-cli`,
  flattens each block + its transactions into CSV rows, and attributes each
  block to a mining pool purely from data already in the block (no network
  calls needed - see **Mining pool extractor** below).
- **API fetcher** (`api_ingest`) - polls the mempool.space HTTP API on a
  budget so it never trips a 429.

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
  logs/                         rotating *.log per command (created on first run)
  config/
    config.yaml                 all settings (see below)
    pools-v2.json                bundled mining-pool signature dataset
  btc_parser_app/
    config.py                    loads+validates config.yaml
    common/
      csv_writer.py               shared append-only, size-rotated CSV writer
      json_utils.py                compact JSON helper (fee histograms, address lists)
      logging_setup.py             console + rotating-file logging
    api/                          mempool.space side ("api_ingest")
      rate_limiter.py              token-bucket rate limiter
      client.py                    rate-limited HTTP GET client (retries, 429 handling)
      mempool_endpoints.py         per-endpoint JSON -> row parsers
      poller.py                    threaded interval poller
      mining_pools_dataset.py      refreshes config/pools-v2.json from GitHub
    rpc/                          bitcoin-cli side ("rpc_ingest") - one implementation, no separate backfill script
      client.py                    bitcoin-cli subprocess wrapper
      block_parser.py              block/tx JSON -> flat CSV rows
      mining_pools.py              mining pool extractor (tag + address matching)
      reorg_state.py                index/current.csv/latest.csv/block_status.csv/reorg/ state files
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

> **Note:** `rpc.output_dir` (default `out_trimmed`) resolves relative to
> `full_app/`, i.e. `full_app/out_trimmed` - a different location from any
> `out_trimmed/` you may have at the repo root from the older standalone
> `../rpc_parser_modified.py` script. That older output also uses a
> different schema (it still has `confirmations`, which this app
> deliberately omits - see **Output schema notes**). `rpc-ingest` does not
> read it automatically; if `full_app/out_trimmed` has nothing in it yet,
> `rpc-ingest` starts fresh from genesis (height 0) rather than silently
> mixing in incompatible historical data.

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
their own systemd units instead (see Script_plan.md's "own systemd unit,
logs, and restart policy" per service) - `start.sh`/`stop.sh` cover
local/dev use and simple always-on hosts, but don't restart a crashed
process the way `Restart=always` would.

### Actual production target: the `bitcoin-storage-ssh` pod

This app is meant to run over SSH on `developer@192.168.15.21`
(`bitcoin-storage-ssh` in `../bitcoin-storage-ssh.yaml`, co-located on the
same node as the `bitcoin-core` pod from `../bitcoin-core.yaml` via a
shared `bitcoin-core-data` PVC). Use
[`config/config.production.yaml`](config/config.production.yaml) there
instead of the default `config.yaml`:

```sh
BTC_PARSER_CONFIG=config/config.production.yaml ./start.sh
```

That config points `rpc.output_dir`/`mempool_api.output_dir`/`logging.log_dir`
at `/parser-data` (the 4TiB `bitcoin-parser-data` PVC) instead of relative
paths, since the pod's home directory (where this repo is presumably
cloned) is only a 5Gi PVC. It also authenticates via the RPC cookie file at
`/bitcoin/.cookie` (the `bitcoin-core-data` PVC, shared read/write with
bitcoind's own `/var/lib/bitcoin`) instead of `rpcuser`/`rpcpassword`.

**Not yet resolved:** `bitcoin-core.yaml` currently sets
`rpcbind=127.0.0.1` / `rpcallowip=127.0.0.1` on bitcoind and its
NetworkPolicy has `ingress: []` - RPC is only reachable over that pod's own
loopback. Since `bitcoin-storage-ssh` is a *different* pod (same node,
but its own network namespace), it can see the shared data on disk but
currently cannot reach bitcoind's RPC port at all. `config.production.yaml`
has a `# TODO` where `-rpcconnect`/`-rpcport` need to go once that
reachability is sorted out on the cluster side (a scoped NetworkPolicy
exception, a sidecar, or similar) - that's infrastructure, not something
this app's config alone can fix.

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
# state, prices, difficulty adjustment, 24h pool hashrate share). Runs until
# Ctrl-C/SIGTERM or a 429.
python run.py api-poll

# Refresh config/pools-v2.json from GitHub (normally automatic - see below)
python run.py update-pools-dataset
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
needed. Each endpoint writes to its own `<name>.csv` under `output_dir`.

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

### `rpc`

`bitcoin_cli_path` + `extra_args` are appended to every `bitcoin-cli`
invocation - for a remote/Kubernetes-hosted node (see
`../bitcoin-core-rpc-external.yaml`) set something like:

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

`batch_size` (default 20) controls how many blocks accumulate between disk
flushes / `current.csv` checkpoints - a crash mid-run loses at most one
batch's worth of progress (which just gets re-parsed, safely - see
**RPC Parser Reorg Handling** below). `output_dir` (default `out_trimmed`)
is where every RPC-side file lives: `blocks.csv`, `transactions.csv`, and
all the reorg-tracking state files.

### `stale_blocks`

Config for `stale-blocks-ingest`: `output_dir` (Splunk-facing exports,
default `out_stale_blocks`) and `state_dir` (internal bookkeeping only,
default `state/stale_blocks`) are kept separate on purpose. `node_poll_interval_seconds`
(default hourly) controls the `getchaintips` pass; `github.poll_interval_seconds`
(default daily) controls the `bitcoin-data/stale-blocks` GitHub CSV pull -
see **RPC Parser Reorg Handling** below for how this pipeline relates to
`rpc-ingest`'s own reorg handling (they're independent: this one tracks
non-active tips, `rpc-ingest` tracks the active chain).

### File rotation

Every append-only CSV this app writes (`blocks.csv`, `transactions.csv`,
`index/index.csv`, `peer_attempts.csv`, the stale-blocks exports, each
mempool.space endpoint's `<name>.csv`) grows forever, so `common/csv_writer.py`
caps each on-disk part at ~900MB and rolls over into a new numbered part
before exceeding it: `blocks.csv` is the first part, then
`blocks.000002.csv`, `blocks.000003.csv`, and so on, alongside it in the same
directory. Nothing else about the layout changes - it's still one logical
CSV, just split into files that never cross ~1GB. Anything in this app that
needs to read a logical CSV back in full (the index, `peer_attempts.csv`,
bootstrapping from `blocks.csv`) reads every part in order automatically. If
you point an external tool (Splunk, a monitoring stanza, ad-hoc scripts) at
these files directly, make sure it globs `<name>*.csv` rather than the exact
first filename.

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

## Mining pool extractor (RPC side)

`btc_parser_app/rpc/mining_pools.py` is what "parses out" the mining pool
for every block fetched via RPC, with zero extra RPC calls or network
requests - this app deliberately does not backfill mempool.space's own
per-block pool history, since paging through `/api/v1/blocks*` to redundantly
re-derive attribution this module already produces locally would burn a lot
of the rate-limit budget for no benefit. Bitcoin Core's `getblock` output
has no pool-identity field; pools identify themselves voluntarily in the
coinbase transaction in one of two ways, and `PoolMatcher.match()` checks
both, in this priority order:

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
  written to a historical CSV (see `../Script_plan.md`).
- Per-transaction vin/vout rows are never exported individually - only
  aggregate/scalar fields (script-type counts, fee stats, witness byte
  counts, etc.) make it into `transactions.csv`, to keep row size bounded
  for downstream ingestion (originally sized for Splunk's ~10k char/event
  limit).
- `coinbase_script_sig_hex` and `coinbase_output_addresses_json` are the
  two exceptions kept as raw-ish fields on the coinbase transaction row -
  they're small, bounded by consensus rules, and are exactly what the
  mining pool extractor reads.

## RPC Parser Reorg Handling

`rpc-ingest` (`btc_parser_app/rpc/ingest.py`) implements the reorg-aware
design from the "RPC Parser Reorg Handling" section of `../Script_plan.md`,
using blockhash as the unique identity throughout. Every run:

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
4. Writes `current.csv` at each batch flush and at the end of the run.

State files (all under `rpc.output_dir`, alongside `blocks.csv` /
`transactions.csv`):

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
meant to require manual intervention, the same philosophy as `api_ingest`
halting on a 429.

### Starting state: genesis, or wherever you left off

`rpc-ingest` works the same way no matter how many blocks (if any) are
already parsed - there's no separate "backfill" mode to run first:

- **Nothing parsed yet** (no `current.csv`, no `blocks.csv`): starts at
  height 0 and catches up to the tip.
- **`current.csv` present**: resumes from exactly there - reorg-checked
  first, as above.
- **`current.csv` missing but `blocks.csv` has data** (e.g. it was deleted,
  or this `output_dir` was populated some other way and never got a
  `current.csv`): reads `blocks.csv`'s own `height`/`hash`/`previousblockhash`
  columns to rebuild `index/` and resumes from its highest block, instead
  of losing that work and re-parsing from genesis.

Either way, once it has a starting point it just runs the normal loop
above. While there's a backlog (catching up from genesis, or from any gap),
it processes batches back-to-back with no delay; once it reaches `latest`
it falls back to polling every `rpc.poll_interval_seconds`. It runs forever
until SIGTERM/SIGINT, checkpointing `current.csv` after every batch so a
kill/crash loses at most one `batch_size` worth of (safely re-parsed)
progress.

If a reorg happened at some point in the past but never touched anything
this run's starting point cares about, it makes no difference - the
normal loop and the reorg-recovery path are the same code either way, so
there's nothing to reconcile specially at startup.

`Script_plan.md` also describes a mempool.space-side per-block pool-history
backfill; this app intentionally does not implement it (see **Mining pool
extractor** above for why) - `../mempool_block_pool_history.py` still has
the original standalone version if that cross-check is ever wanted later.

## Reused from the original scripts

This app is a restructured, config-driven port - not a rewrite - of:

- [`../mempool_api_parser.py`](../mempool_api_parser.py) - endpoint
  registry, per-endpoint parsers, and the threaded interval-poller design
  (`btc_parser_app/api/*`).
- [`../rpc_parser_modified.py`](../rpc_parser_modified.py) - the
  block/transaction aggregation logic (`btc_parser_app/rpc/block_parser.py`),
  extended with mining-pool attribution.
