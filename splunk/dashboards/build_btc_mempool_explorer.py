"""Generates the "BTC Mempool Explorer" Dashboard Studio dashboard.

Writes the dashboard definition twice:
  - splunk/dashboards/btc_mempool_explorer.json - paste into a new Dashboard
    Studio dashboard (Edit > Source) for a quick test.
  - splunk/btc-parser-dashboards/default/data/ui/views/btc_mempool_explorer.xml
    - the same JSON wrapped as a view inside an installable Splunk app.

plus the app's background searches the "All-time Records" tab reads from
(savedsearches.conf, macros.conf, collections.conf, transforms.conf and
lookups/sanctioned_addresses.csv in btc-parser-dashboards) - a Dashboard
Studio definition can only reference saved searches, not define them.

Edit this script rather than the outputs, then rerun it:
    python3 splunk/dashboards/build_btc_mempool_explorer.py

See docs/10-splunk-integration-und-feldkonzept.md, section 10.9.
"""

import json
import re
from pathlib import Path

SPLUNK_DIR = Path(__file__).resolve().parent.parent

ORANGE = "#f7931a"
BLUE = "#4a68b9"
PURPLE = "#9339f4"
GREEN = "#3bcc49"
RED = "#dc3545"
YELLOW = "#ffc107"
TEAL = "#1bd8f4"
GREY = "#8a8fa8"

# mempool.space-style fee-rate gradient (olive green -> magenta red)
FEE_BANDS = [
    ("<1", 0, 1, "#557d00"),
    ("1-2", 1, 2, "#5d7d01"),
    ("2-3", 2, 3, "#637d02"),
    ("3-4", 3, 4, "#6d7d04"),
    ("4-6", 4, 6, "#757d05"),
    ("6-8", 6, 8, "#867d08"),
    ("8-10", 8, 10, "#8c7d09"),
    ("10-15", 10, 15, "#957d0b"),
    ("15-20", 15, 20, "#a67d0e"),
    ("20-30", 20, 30, "#b27d10"),
    ("30-50", 30, 50, "#bf7815"),
    ("50-100", 50, 100, "#bd5c28"),
    ("100-200", 100, 200, "#bb3d3c"),
    ("200+", 200, None, "#b30e55"),
]

AGE_BANDS = [
    ("< 1 day", 144, "#1bd8f4"),
    ("1 day - 1 week", 1008, "#2fa6e0"),
    ("1 week - 1 month", 4320, "#4a68b9"),
    ("1 - 6 months", 26280, "#653b9c"),
    ("6 - 12 months", 52560, "#9339f4"),
    ("1 - 2 years", 105120, "#c03fb0"),
    ("2 - 5 years", 262800, "#e0607e"),
    ("5+ years", None, "#f7931a"),
]

fee_band_case = "case(" + ", ".join(
    f'fr<{hi}, "{label}"' for label, _, hi, _ in FEE_BANDS if hi is not None
) + ', true(), "200+")'
fee_band_fields = " ".join(f'"{label}"' for label, *_ in FEE_BANDS)
fee_band_colors = {label: color for label, _, _, color in FEE_BANDS}
fee_range_context = []
for label, lo, hi, color in FEE_BANDS:
    entry = {"value": color}
    if lo:
        entry["from"] = lo
    if hi is not None:
        entry["to"] = hi
    fee_range_context.append(entry)

age_case = "case(" + ", ".join(
    f'age<{hi}, "{label}"' for label, hi, _ in AGE_BANDS if hi is not None
) + f', true(), "{AGE_BANDS[-1][0]}")'
age_fields = " ".join(f'"{label}"' for label, *_ in AGE_BANDS)
age_colors = {label: color for label, _, color in AGE_BANDS}

ds = {}
viz = {}


def search(ds_id, name, query, **options):
    ds[ds_id] = {"type": "ds.search", "name": name, "options": {"query": query.strip(), **options}}


def chain(ds_id, name, extend, query):
    ds[ds_id] = {"type": "ds.chain", "name": name, "options": {"extend": extend, "query": query.strip()}}


def add_viz(viz_id, vtype, title, ds_id, description=None, **options):
    v = {"type": vtype, "options": options}
    if title:
        v["title"] = title
    if ds_id:
        v["dataSources"] = {"primary": ds_id}
    if description:
        v["description"] = description
    viz[viz_id] = v
    return v


def single(viz_id, title, ds_id, field, unit=None, unit_before=False, precision=None, color=None, description=None):
    opts = {
        "majorValue": f"> primary | seriesByName('{field}') | lastPoint()",
        "sparklineDisplay": "off",
        "trendDisplay": "off",
    }
    if unit:
        opts["unit"] = unit
        opts["unitPosition"] = "before" if unit_before else "after"
    if precision is not None:
        opts["numberPrecision"] = precision
    if color:
        opts["majorColor"] = color
    add_viz(viz_id, "splunk.singlevalue", title, ds_id, description, **opts)


CHART_BASE = {"legendDisplay": "bottom", "xAxisTitleVisibility": "hide"}
LINE_BASE = {**CHART_BASE, "nullValueDisplay": "connect"}

# ---------------------------------------------------------------- base searches

BLOCK_FIELDS = (
    "_time time height hash pool_name nTx regular_tx_count size strippedsize weight weight_utilization_pct "
    "difficulty time_since_prev_block_sec total_fees_sats block_subsidy_sats coinbase_value_sats "
    "fee_avg_sats fee_rate_median_sat_vb fee_rate_avg_sat_vb fee_rate_max_sat_vb tx_vsize_avg tx_vsize_median "
    "segwit_tx_count taproot_input_tx_count taproot_output_tx_count rbf_tx_count "
    "op_return_output_count op_return_script_bytes coin_days_destroyed_btc total_vin_count total_vout_count "
    "vout_type_*_count"
)

# One row per height: a block replaced by a reorg stays in the index, so keep
# the most recently indexed hash for every height.
search(
    "ds_blocks",
    "Blocks (base, one row per height)",
    f"""
index=$idx$ sourcetype=btc:blocks
| eval _it=_indextime
| sort 0 - _it
| dedup height
| fields {BLOCK_FIELDS}
""",
)

search(
    "ds_epoch",
    "Difficulty epoch + halving (fixed: last 20 days)",
    """
| tstats count where index=$idx$ sourcetype=btc:blocks by height time
| eventstats max(height) as tip
| where floor(height/2016)==floor(tip/2016)
| stats max(height) as tip min(height) as first_h min(time) as t0 max(time) as t1
| eval epoch_start=floor(tip/2016)*2016, done=tip-epoch_start+1, remaining=2016-done
| eval progress_pct=round(done/2016*100,1)
| eval avg_interval=if(tip>first_h, (t1-t0)/(tip-first_h), 600)
| eval est_change_pct=round((600/avg_interval-1)*100,2)
| eval avg_block_min=round(avg_interval/60,2)
| eval retarget=strftime(t1+remaining*avg_interval, "%Y-%m-%d %H:%M")
| eval next_halving=(floor(tip/210000)+1)*210000, halving_blocks=next_halving-tip
| eval halving_date=strftime(t1+halving_blocks*600, "%Y-%m-%d")
""",
    queryParameters={"earliest": "-20d", "latest": "now"},
)

# ---------------------------------------------------------------- overview

search(
    "ds_price_24h",
    "Price - last 24h",
    """
| tstats avg(usd) as USD avg(eur) as EUR where index=$idx$ sourcetype=btc:prices by _time span=1h
""",
    queryParameters={"earliest": "-24h", "latest": "now"},
    refresh="5m",
    refreshType="delay",
)
chain(
    "ds_price_24h_change",
    "Price - 24h change",
    "ds_price_24h",
    """
| stats earliest(USD) as first latest(USD) as last
| eval change_pct=round((last-first)/first*100,2), sats_per_usd=round(100000000/last,0)
""",
)
chain("ds_tip", "Tip block", "ds_blocks", """
| sort 0 - height | head 1
| eval median_fee=round(fee_rate_median_sat_vb,1)
| table height median_fee pool_name
""")
chain("ds_range_stats", "Range stats", "ds_blocks", """
| stats count as blocks avg(time_since_prev_block_sec) as avg_sec sum(difficulty) as sum_diff sum(time_since_prev_block_sec) as sum_sec
| eval avg_block_min=round(avg_sec/60,2), hashrate_ehs=round(sum_diff*pow(2,32)/sum_sec/1e18,1)
""")

single("viz_kpi_price", "BTC / USD (now)", "ds_price_24h", "USD", unit="$", unit_before=True, precision=0, color=ORANGE)
viz["viz_kpi_price"]["options"].update({
    "sparklineDisplay": "below",
    "sparklineValues": "> primary | seriesByName('USD')",
    "sparklineStrokeColor": ORANGE,
})
single("viz_kpi_change", "24h change", "ds_price_24h_change", "change_pct", unit="%", precision=2)
viz["viz_kpi_change"]["options"]["majorColor"] = (
    "> majorValue | rangeValue(changeColors)"
)
viz["viz_kpi_change"]["context"] = {
    "changeColors": [{"to": 0, "value": RED}, {"from": 0, "value": GREEN}]
}
single("viz_kpi_height", "Block height", "ds_tip", "height", precision=0, color=TEAL)
viz["viz_kpi_height"]["options"]["shouldUseThousandSeparators"] = False
single("viz_kpi_median_fee", "Tip block median fee", "ds_tip", "median_fee", unit="sat/vB", precision=1, color=YELLOW)
single("viz_kpi_blocktime", "Avg block time (range)", "ds_range_stats", "avg_block_min", unit="min", precision=2, color=BLUE)
single("viz_kpi_hashrate", "Avg hashrate (range)", "ds_range_stats", "hashrate_ehs", unit="EH/s", precision=1, color=PURPLE)

search("ds_price_range", "Price over range", """
| tstats avg(usd) as USD avg(eur) as EUR where index=$idx$ sourcetype=btc:prices by _time span=$span$
""")
add_viz("viz_price_overview", "splunk.area", "BTC price", "ds_price_range",
        **LINE_BASE, seriesColors=[ORANGE, BLUE], areaOpacity=0.25, yAxisTitleText="Fiat")

chain("ds_fee_rates_blocks", "Block fee rates (block aggregates)", "ds_blocks", """
| timechart span=$span$ median(fee_rate_median_sat_vb) as Median avg(fee_rate_avg_sat_vb) as Average
""")
add_viz("viz_fee_rates_overview", "splunk.line", "Block fee rates", "ds_fee_rates_blocks",
        "Median of per-block median fee rate vs. average fee rate (sat/vB)",
        **LINE_BASE, seriesColors=[YELLOW, ORANGE], yAxisTitleText="sat/vB")

chain("ds_latest_blocks", "Latest blocks", "ds_blocks", """
| sort 0 - height | head 25
| eval mined=strftime(_time, "%Y-%m-%d %H:%M"), ago=tostring(round(now()-_time), "duration")
| eval median=round(fee_rate_median_sat_vb,1), avg=round(fee_rate_avg_sat_vb,1), max=round(fee_rate_max_sat_vb,0)
| eval fees=round(total_fees_sats/1e8,4), size_mb=round(size/1e6,2), full=round(weight_utilization_pct,1)
| eval interval=round(time_since_prev_block_sec/60,1)
| table height mined ago pool_name nTx median avg max fees size_mb full interval
| rename height as Height, mined as "Mined (UTC)", ago as Ago, pool_name as Pool, nTx as TXs, median as "Median sat/vB", avg as "Avg sat/vB", max as "Max sat/vB", fees as "Fees BTC", size_mb as "Size MB", full as "Full %", interval as "Interval min"
""")
latest_blocks = add_viz(
    "viz_latest_blocks", "splunk.table", "Latest blocks", "ds_latest_blocks",
    "Like mempool.space's block strip - fee colors use the mempool.space gradient",
    count=10,
    columnFormat={
        "Median sat/vB": {"rowBackgroundColors": '> table | seriesByName("Median sat/vB") | rangeValue(feeColors)'},
        "Avg sat/vB": {"rowBackgroundColors": '> table | seriesByName("Avg sat/vB") | rangeValue(feeColors)'},
        "Full %": {"rowBackgroundColors": '> table | seriesByName("Full %") | rangeValue(fullColors)'},
    },
)
latest_blocks["context"] = {
    "feeColors": fee_range_context,
    "fullColors": [
        {"to": 50, "value": "#2b2d42"},
        {"from": 50, "to": 90, "value": "#3a4a8a"},
        {"from": 90, "value": BLUE},
    ],
}

chain("ds_pool_top", "Pool share", "ds_blocks", """
| top limit=10 useother=t pool_name
| fields pool_name count
""")
add_viz("viz_pool_pie_overview", "splunk.pie", "Mining pools (blocks in range)", "ds_pool_top")

chain("ds_throughput", "Throughput", "ds_blocks", """
| timechart span=$span$ per_second(weight) as wps per_second(nTx) as tps
| eval vbps=round(wps/4,0), tps=round(tps,2)
| fields _time vbps tps
| rename vbps as "vB/s", tps as "tx/s"
""")
add_viz("viz_throughput", "splunk.line", "Confirmed throughput", "ds_throughput",
        "Mempool.space shows incoming vB/s; here: vB/s confirmed into blocks",
        **LINE_BASE, seriesColors=[TEAL, ORANGE], overlayFields=["tx/s"], showOverlayY2Axis=True,
        yAxisTitleText="vB/s", y2AxisTitleText="tx/s")

add_viz("viz_about", "splunk.markdown", "", None,
        markdown=(
            "#### About\n"
            "Mempool.space-style views built on the **btc_parser** exports "
            "(`btc:blocks`, `btc:transactions`, `btc:inputs`, `btc:prices`, "
            "`btc:stale_block_headers`, `btc:sanctioned_tx_events`). "
            "There is no live mempool snapshot in the exports, so the "
            "\"mempool\" charts show what was **confirmed** in each time bucket. "
            "Charts over `btc:transactions`/`btc:inputs` scan every row - keep the "
            "time range short on a full export."
        ))

# ---------------------------------------------------------------- mining

single("viz_epoch_progress", "Epoch progress", "ds_epoch", "progress_pct", unit="%", precision=1, color=BLUE)
single("viz_epoch_remaining", "Blocks to retarget", "ds_epoch", "remaining", precision=0, color=TEAL)
single("viz_epoch_change", "Est. difficulty change", "ds_epoch", "est_change_pct", unit="%", precision=2)
viz["viz_epoch_change"]["options"]["majorColor"] = "> majorValue | rangeValue(changeColors)"
viz["viz_epoch_change"]["context"] = {
    "changeColors": [{"to": 0, "value": RED}, {"from": 0, "value": GREEN}]
}
single("viz_epoch_date", "Est. retarget (UTC)", "ds_epoch", "retarget", color=GREY)
single("viz_halving_blocks", "Blocks to halving", "ds_epoch", "halving_blocks", precision=0, color=ORANGE)
single("viz_halving_date", "Est. halving date", "ds_epoch", "halving_date", color=ORANGE)

chain("ds_hashrate", "Hashrate & difficulty", "ds_blocks", """
| timechart span=1d sum(difficulty) as sd sum(time_since_prev_block_sec) as ss avg(difficulty) as d
| eval hr=round(sd*pow(2,32)/ss/1e18,1), diff=round(d/1e12,2)
| trendline sma7(hr) as hr7
| eval hr7=round(hr7,1)
| fields _time hr hr7 diff
| rename hr as "Hashrate (EH/s)", hr7 as "Hashrate 7d MA (EH/s)", diff as "Difficulty (T)"
""")
add_viz("viz_hashrate", "splunk.line", "Hashrate & difficulty", "ds_hashrate",
        "Hashrate estimated per day as sum(difficulty x 2^32) / sum(block intervals)",
        **LINE_BASE, seriesColors=["#5b6aa8", YELLOW, TEAL],
        overlayFields=["Difficulty (T)"], showOverlayY2Axis=True,
        yAxisTitleText="EH/s", y2AxisTitleText="Difficulty (T)")

chain("ds_pool_ranking", "Pool ranking", "ds_blocks", """
| stats count as blocks sum(eval(if(regular_tx_count==0,1,0))) as empty avg(total_fees_sats) as af avg(fee_rate_median_sat_vb) as amf avg(weight_utilization_pct) as full by pool_name
| eventstats sum(blocks) as total
| eval share=round(blocks/total*100,2), af=round(af/1e8,4), amf=round(amf,1), full=round(full,1)
| sort - blocks
| streamstats count as rank
| table rank pool_name blocks share empty af amf full
| rename rank as "#", pool_name as Pool, blocks as Blocks, share as "Share %", empty as "Empty blocks", af as "Avg fees BTC", amf as "Avg median sat/vB", full as "Avg full %"
""")
pool_ranking = add_viz("viz_pool_ranking", "splunk.table", "Pool ranking", "ds_pool_ranking", count=15,
                       columnFormat={"Share %": {"rowBackgroundColors": '> table | seriesByName("Share %") | rangeValue(shareColors)'}})
pool_ranking["context"] = {"shareColors": [
    {"to": 5, "value": "#2b2d42"}, {"from": 5, "to": 15, "value": "#3a4a8a"},
    {"from": 15, "to": 30, "value": "#653b9c"}, {"from": 30, "value": "#9339f4"},
]}
add_viz("viz_pool_pie", "splunk.pie", "Pool dominance", "ds_pool_top")

chain("ds_pool_time", "Pool dominance over time", "ds_blocks", """
| timechart span=$span$ count by pool_name limit=10 useother=t
""")
add_viz("viz_pool_time", "splunk.area", "Pool dominance over time", "ds_pool_time",
        **LINE_BASE, stackMode="stacked100", areaOpacity=0.85, yAxisTitleText="% of blocks")

chain("ds_difficulty_epochs", "Difficulty adjustments", "ds_blocks", """
| eval epoch=floor(height/2016)
| stats min(height) as start_h max(height) as end_h count as blocks avg(difficulty) as d min(_time) as t0 max(_time) as t1 by epoch
| sort 0 epoch
| streamstats current=f last(d) as prev
| eval change=round((d-prev)/prev*100,2), diff_t=round(d/1e12,2), start=strftime(t0,"%Y-%m-%d"), avg_min=round((t1-t0)/60/(end_h-start_h),2)
| sort 0 - epoch
| table epoch start start_h blocks diff_t change avg_min
| rename epoch as Epoch, start as "Start (UTC)", start_h as "Start height", blocks as "Blocks seen", diff_t as "Difficulty (T)", change as "Change %", avg_min as "Avg block min"
""")
diff_table = add_viz("viz_difficulty_epochs", "splunk.table", "Difficulty adjustments", "ds_difficulty_epochs",
                     "First epoch in range has no previous value; partial epochs are averaged over the blocks seen",
                     count=10,
                     columnFormat={"Change %": {"rowColors": '> table | seriesByName("Change %") | rangeValue(changeColors)'}})
diff_table["context"] = {"changeColors": [{"to": 0, "value": RED}, {"from": 0, "value": GREEN}]}

# ---------------------------------------------------------------- fees & rewards

chain("ds_fees_subsidy", "Fees vs subsidy", "ds_blocks", """
| timechart span=$span$ sum(total_fees_sats) as f sum(block_subsidy_sats) as s
| eval subsidy=round(s/1e8,4), fees=round(f/1e8,4), share=round(f/(f+s)*100,2)
| fields _time subsidy fees share
| rename subsidy as "Subsidy BTC", fees as "Fees BTC", share as "Fee share %"
""")
add_viz("viz_fees_subsidy", "splunk.column", "Block rewards: subsidy vs fees", "ds_fees_subsidy",
        **CHART_BASE, stackMode="stacked", seriesColors=[BLUE, ORANGE, YELLOW],
        overlayFields=["Fee share %"], showOverlayY2Axis=True,
        yAxisTitleText="BTC", y2AxisTitleText="Fee share %")

search("ds_fees_usd", "Fees & rewards in USD", """
| tstats sum(total_fees_sats) as f sum(block_subsidy_sats) as s avg(usd) as usd
    where index=$idx$ (sourcetype=btc:blocks OR sourcetype=btc:prices) by _time span=$span$
| eval fees=round(f/1e8*usd,0), reward=round((f+s)/1e8*usd,0)
| fields _time reward fees
| rename reward as "Total reward USD", fees as "Fees USD"
""")
add_viz("viz_fees_usd", "splunk.area", "Block rewards in USD", "ds_fees_usd",
        "BTC amounts per bucket x average btc:prices USD in the same bucket",
        **LINE_BASE, seriesColors=[BLUE, ORANGE], areaOpacity=0.3, yAxisTitleText="USD")

chain("ds_avg_fee", "Average fee per tx", "ds_blocks", """
| timechart span=$span$ sum(total_fees_sats) as f sum(regular_tx_count) as n
| eval avg=round(f/n,0)
| fields _time avg
| rename avg as "Avg fee per tx (sats)"
""")
add_viz("viz_avg_fee", "splunk.column", "Average fee per transaction", "ds_avg_fee",
        **{**CHART_BASE, "legendDisplay": "off"}, seriesColors=[ORANGE], yAxisTitleText="sats")

# fee_rate_sat_vb is a search-time field (docs chapter 10.6), so this scans
# the transactions of the selected range - like the fee-band chart below.
search("ds_fee_percentiles", "Fee-rate percentiles (transactions)", """
index=$idx$ sourcetype=btc:transactions is_coinbase=false fee_sats>0
| timechart span=$span$ min(fee_rate_sat_vb) as Min perc10(fee_rate_sat_vb) as p10 perc25(fee_rate_sat_vb) as p25 median(fee_rate_sat_vb) as Median perc75(fee_rate_sat_vb) as p75 perc90(fee_rate_sat_vb) as p90
| rename p10 as "10th", p25 as "25th", p75 as "75th", p90 as "90th"
""")
add_viz("viz_fee_percentiles", "splunk.line", "Block fee rates (percentiles)", "ds_fee_percentiles",
        "Per-transaction fee rates of confirmed transactions (zero-fee package parents excluded), log scale",
        **LINE_BASE, yAxisScale="log", yAxisTitleText="sat/vB",
        seriesColors=["#557d00", "#757d05", "#957d0b", YELLOW, "#bd5c28", "#b30e55"])

search("ds_fee_bands_time", "Confirmed vBytes by fee band", f"""
index=$idx$ sourcetype=btc:transactions is_coinbase=false fee_rate_sat_vb=*
| eval fr=tonumber(fee_rate_sat_vb), vmb=vsize/1000000
| eval band={fee_band_case}
| timechart span=$span$ sum(vmb) by band limit=0
| fillnull value=0
| table _time {fee_band_fields}
""")
add_viz("viz_fee_bands_time", "splunk.area", "Confirmed vMB by fee rate", "ds_fee_bands_time",
        "The mempool.space 'Memory usage by fee rate' chart, but for vbytes confirmed per bucket",
        **{**LINE_BASE, "legendDisplay": "right"}, stackMode="stacked", areaOpacity=1, 
        seriesColorsByField=fee_band_colors, yAxisTitleText="vMB")

chain("ds_fee_bands_total", "Block space share by fee band", "ds_fee_bands_time", f"""
| fields - _time
| stats sum(*) as *
| eval x="Block space"
| table x {fee_band_fields}
""")
add_viz("viz_fee_bands_total", "splunk.bar", "Block space by fee rate (range total)", "ds_fee_bands_total",
        stackMode="stacked100", seriesColorsByField=fee_band_colors, legendDisplay="bottom",
        xAxisTitleVisibility="hide", yAxisTitleVisibility="hide")

# ---------------------------------------------------------------- blocks

chain("ds_size_weight", "Block size & weight", "ds_blocks", """
| timechart span=$span$ avg(size) as s avg(weight) as w avg(weight_utilization_pct) as u
| eval size=round(s/1e6,3), weight=round(w/1e6,3), full=round(u,1)
| fields _time size weight full
| rename size as "Size (MB)", weight as "Weight (MWU)", full as "Full %"
""")
add_viz("viz_size_weight", "splunk.line", "Block sizes & weights", "ds_size_weight",
        **LINE_BASE, seriesColors=[ORANGE, BLUE, GREY],
        overlayFields=["Full %"], showOverlayY2Axis=True, yAxisTitleText="MB / MWU", y2AxisTitleText="Full %")

chain("ds_tx_count", "Transactions per block", "ds_blocks", """
| timechart span=$span$ avg(nTx) as avg sum(nTx) as total
| eval avg=round(avg,0)
| rename avg as "Avg TXs per block", total as "TXs confirmed"
""")
add_viz("viz_tx_count", "splunk.column", "Transactions", "ds_tx_count",
        **CHART_BASE, seriesColors=[TEAL, BLUE], overlayFields=["Avg TXs per block"], showOverlayY2Axis=True,
        yAxisTitleText="TXs confirmed", y2AxisTitleText="Avg per block")

chain("ds_block_time", "Block interval over time", "ds_blocks", """
| timechart span=$span$ avg(time_since_prev_block_sec) as s count as blocks
| eval avg=round(s/60,2), target=10
| fields _time avg target blocks
| rename avg as "Avg interval (min)", target as "Target (10 min)", blocks as Blocks
""")
add_viz("viz_block_time", "splunk.line", "Block interval", "ds_block_time",
        **LINE_BASE, seriesColors=[BLUE, GREY, ORANGE], overlayFields=["Blocks"], showOverlayY2Axis=True,
        yAxisTitleText="minutes", y2AxisTitleText="blocks")

chain("ds_interval_hist", "Block interval histogram", "ds_blocks", """
| eval m=floor(time_since_prev_block_sec/60)
| where m>=0
| eval m=if(m>=60, 60, m)
| stats count by m
| sort 0 m
| eval m=if(m==60, "60+", tostring(m))
| rename m as "Minutes since previous block", count as Blocks
""")
add_viz("viz_interval_hist", "splunk.column", "Block interval distribution", "ds_interval_hist",
        "Should look exponential with a mean of ~10 min",
        seriesColors=[PURPLE], legendDisplay="off")

chain("ds_vsize", "Tx vsize per block", "ds_blocks", """
| timechart span=$span$ avg(tx_vsize_avg) as a median(tx_vsize_median) as m
| eval a=round(a,0), m=round(m,0)
| rename a as "Avg tx vsize", m as "Median tx vsize"
""")
add_viz("viz_vsize", "splunk.line", "Transaction vsize", "ds_vsize",
        **LINE_BASE, seriesColors=[ORANGE, TEAL], yAxisTitleText="vB")

chain("ds_long_gaps", "Long block gaps", "ds_blocks", """
| where time_since_prev_block_sec>=3600
| eval gap=round(time_since_prev_block_sec/60,1), mined=strftime(_time,"%Y-%m-%d %H:%M")
| sort 0 - gap
| table height mined gap pool_name nTx
| rename height as Height, mined as "Mined (UTC)", gap as "Gap (min)", pool_name as Pool, nTx as TXs
""")
add_viz("viz_long_gaps", "splunk.table", "Block gaps >= 60 min", "ds_long_gaps", count=10)

# ---------------------------------------------------------------- transactions

chain("ds_adoption", "Feature adoption", "ds_blocks", """
| timechart span=$span$ sum(segwit_tx_count) as sw sum(taproot_input_tx_count) as tpi sum(taproot_output_tx_count) as tpo sum(rbf_tx_count) as rbf sum(regular_tx_count) as n
| eval segwit=round(sw/n*100,2), tp_spend=round(tpi/n*100,2), tp_out=round(tpo/n*100,2), rbf=round(rbf/n*100,2)
| fields _time segwit tp_spend tp_out rbf
| rename segwit as "SegWit spending %", tp_spend as "Taproot spending %", tp_out as "Paying to Taproot %", rbf as "RBF signalling %"
""")
add_viz("viz_adoption", "splunk.line", "SegWit / Taproot / RBF adoption", "ds_adoption",
        "Share of non-coinbase transactions per bucket",
        **LINE_BASE, seriesColors=[GREEN, PURPLE, TEAL, ORANGE], yAxisTitleText="% of transactions")

OUT_TYPES = [
    ("P2WPKH", "witness_v0_keyhash", GREEN),
    ("P2TR", "witness_v1_taproot", PURPLE),
    ("P2PKH", "pubkeyhash", ORANGE),
    ("P2SH", "scripthash", YELLOW),
    ("P2WSH", "witness_v0_scripthash", TEAL),
    ("OP_RETURN", "nulldata", RED),
    ("P2A (anchor)", "anchor", BLUE),
]
OTHER_TYPES = ["pubkey", "multisig", "nonstandard", "witness_unknown", "other"]
out_sums = " ".join(f"sum(vout_type_{t}_count) as {t}" for _, t, _ in OUT_TYPES + [(None, o, None) for o in OTHER_TYPES])
out_other = "+".join(f"coalesce({o},0)" for o in OTHER_TYPES)
out_renames = ", ".join(f'{t} as "{label}"' for label, t, _ in OUT_TYPES)
out_fields = " ".join(t for _, t, _ in OUT_TYPES)
chain("ds_out_types", "Output script types", "ds_blocks", f"""
| timechart span=$span$ {out_sums}
| eval Other={out_other}
| fields _time {out_fields} Other
| rename {out_renames}
""")
out_colors = {label: color for label, _, color in OUT_TYPES}
out_colors["Other"] = GREY
add_viz("viz_out_types", "splunk.area", "New outputs by script type", "ds_out_types",
        **LINE_BASE, stackMode="stacked100", areaOpacity=0.9, seriesColorsByField=out_colors,
        yAxisTitleText="% of outputs")

out_case = "case(" + ", ".join(f'type=="{t}", "{label}"' for label, t, _ in OUT_TYPES) + ', true(), "Other")'
chain("ds_out_types_total", "Output script types (range total)", "ds_blocks", f"""
| stats sum(vout_type_*_count) as *
| transpose 0 column_name=type
| rename "row 1" as outputs
| eval type={out_case}
| stats sum(outputs) as outputs by type
| sort 0 - outputs
""")
add_viz("viz_out_types_total", "splunk.pie", "Output types (range total)", "ds_out_types_total")

chain("ds_op_return", "OP_RETURN", "ds_blocks", """
| timechart span=$span$ sum(op_return_output_count) as c sum(op_return_script_bytes) as b
| eval kb=round(b/1000,1)
| fields _time c kb
| rename c as "OP_RETURN outputs", kb as "OP_RETURN kB"
""")
add_viz("viz_op_return", "splunk.column", "OP_RETURN usage", "ds_op_return",
        **CHART_BASE, seriesColors=[RED, YELLOW], overlayFields=["OP_RETURN kB"], showOverlayY2Axis=True,
        yAxisTitleText="outputs", y2AxisTitleText="kB")

chain("ds_cdd", "Coin days destroyed", "ds_blocks", """
| timechart span=$span$ sum(coin_days_destroyed_btc) as cdd
| eval cdd=round(cdd,0)
| rename cdd as "Coin days destroyed"
""")
add_viz("viz_cdd", "splunk.column", "Coin days destroyed", "ds_cdd",
        "Old coins moving show up as spikes",
        **{**CHART_BASE, "legendDisplay": "off"}, seriesColors=[ORANGE], yAxisTitleText="BTC-days")

search("ds_coin_age", "Spent value by coin age (inputs)", f"""
index=$idx$ sourcetype=btc:inputs is_coinbase=false input_age_blocks=*
| eval age=tonumber(input_age_blocks), btc=value_sats/1e8
| eval band={age_case}
| timechart span=$span$ sum(btc) by band limit=0
| fillnull value=0
| table _time {age_fields}
""")
add_viz("viz_coin_age", "splunk.area", "Spent BTC by coin age", "ds_coin_age",
        "Value of spent inputs, grouped by how long the coins were unspent (from btc:inputs)",
        **{**LINE_BASE, "legendDisplay": "right"}, stackMode="stacked100", areaOpacity=0.9, 
        seriesColorsByField=age_colors, yAxisTitleText="% of spent BTC")

# ---------------------------------------------------------------- price

chain("ds_price_range_stats", "Price range stats", "ds_price_range", """
| stats earliest(USD) as first latest(USD) as last max(USD) as hi min(USD) as lo latest(EUR) as eur
| eval change=round((last-first)/first*100,2), sats=round(100000000/last,0)
""")
single("viz_price_last", "Last USD", "ds_price_range_stats", "last", unit="$", unit_before=True, precision=0, color=ORANGE)
single("viz_price_eur", "Last EUR", "ds_price_range_stats", "eur", unit="€", unit_before=True, precision=0, color=BLUE)
single("viz_price_change", "Change over range", "ds_price_range_stats", "change", unit="%", precision=2)
viz["viz_price_change"]["options"]["majorColor"] = "> majorValue | rangeValue(changeColors)"
viz["viz_price_change"]["context"] = {"changeColors": [{"to": 0, "value": RED}, {"from": 0, "value": GREEN}]}
single("viz_price_hi", "Range high (bucket avg)", "ds_price_range_stats", "hi", unit="$", unit_before=True, precision=0, color=GREEN)
single("viz_price_lo", "Range low (bucket avg)", "ds_price_range_stats", "lo", unit="$", unit_before=True, precision=0, color=RED)
single("viz_price_sats", "Sats per dollar", "ds_price_range_stats", "sats", precision=0, color=YELLOW)

add_viz("viz_price_main", "splunk.line", "BTC price", "ds_price_range",
        **LINE_BASE, seriesColors=[ORANGE, BLUE], yAxisTitleText="Fiat")

search("ds_price_daily", "Daily OHLC", """
| tstats earliest(usd) as open max(usd) as high min(usd) as low latest(usd) as close
    where index=$idx$ sourcetype=btc:prices by _time span=1d
| eval change=round((close-open)/open*100,2), range=round((high-low)/low*100,2)
""")
chain("ds_price_daily_change", "Daily change", "ds_price_daily", """
| eval Up=if(change>=0, change, null()), Down=if(change<0, change, null())
| fields _time Up Down
""")
add_viz("viz_price_daily_change", "splunk.column", "Daily change % (open to close)", "ds_price_daily_change",
        **{**CHART_BASE, "legendDisplay": "off"}, stackMode="stacked", seriesColorsByField={"Up": GREEN, "Down": RED}, 
        yAxisTitleText="%")
chain("ds_price_ohlc_table", "Daily OHLC table", "ds_price_daily", """
| sort 0 - _time
| eval day=strftime(_time, "%Y-%m-%d"), open=round(open,0), high=round(high,0), low=round(low,0), close=round(close,0)
| table day open high low close change range
| rename day as "Day (UTC)", open as Open, high as High, low as Low, close as Close, change as "Change %", range as "Range %"
""")
ohlc = add_viz("viz_price_ohlc", "splunk.table", "Daily OHLC (USD)", "ds_price_ohlc_table", count=12,
               columnFormat={"Change %": {"rowColors": '> table | seriesByName("Change %") | rangeValue(changeColors)'}})
ohlc["context"] = {"changeColors": [{"to": 0, "value": RED}, {"from": 0, "value": GREEN}]}

# ---------------------------------------------------------------- chain health

search("ds_stale", "Stale block headers", """
| tstats latest(status) as status latest(source) as source latest(chaintip_status) as tip_status max(branchlen) as branchlen latest(header_valid) as valid min(_time) as first
    where index=$idx$ sourcetype=btc:stale_block_headers by height hash
| sort 0 - height
""")
chain("ds_stale_kpi", "Stale KPIs", "ds_stale", """
| stats dc(hash) as tips dc(height) as heights max(branchlen) as max_branch
""")
single("viz_stale_tips", "Stale tips seen", "ds_stale_kpi", "tips", precision=0, color=YELLOW)
single("viz_stale_heights", "Heights affected", "ds_stale_kpi", "heights", precision=0, color=ORANGE)
single("viz_stale_branch", "Longest fork (blocks)", "ds_stale_kpi", "max_branch", precision=0, color=RED)

search("ds_export_reorgs", "Reorgs in export", """
| tstats dc(hash) as n values(hash) as hashes values(pool_name) as pools where index=$idx$ sourcetype=btc:blocks by height
| where n>1
| sort 0 - height
| rename height as Height, n as "Hashes exported", hashes as Hashes, pools as Pools
""")
chain("ds_export_reorgs_kpi", "Reorg KPI", "ds_export_reorgs", "| stats count as reorg_heights")
single("viz_reorg_heights", "Reorged heights in export", "ds_export_reorgs_kpi", "reorg_heights", precision=0, color=PURPLE,
       description="Heights with more than one exported block hash")

chain("ds_stale_table", "Stale table", "ds_stale", """
| eval first=strftime(first, "%Y-%m-%d %H:%M")
| table height hash status source tip_status branchlen valid first
| rename height as Height, hash as Hash, status as Status, source as Source, tip_status as "Chaintip status", branchlen as "Branch len", valid as "Header valid", first as "First observed (UTC)"
""")
add_viz("viz_stale_table", "splunk.table", "Stale / orphaned tips", "ds_stale_table", count=10)

search("ds_stale_time", "Stale headers over time", """
| tstats dc(hash) as tips where index=$idx$ sourcetype=btc:stale_block_headers by _time span=$span$ source
| xyseries _time source tips
""")
add_viz("viz_stale_time", "splunk.column", "Stale headers observed", "ds_stale_time",
        **CHART_BASE, stackMode="stacked", seriesColorsByField={"own_node": TEAL, "github": PURPLE})
add_viz("viz_export_reorgs", "splunk.table", "Heights with more than one exported block", "ds_export_reorgs", count=10)

# ---------------------------------------------------------------- sanctions

search("ds_sanctions", "Sanctioned tx events (base)", """
index=$idx$ sourcetype=btc:sanctioned_tx_events
| fields _time event txid direction io_index address value_sats value_btc name first_name sanctions_programs tx_fee_btc tx_vsize first_seen_at block_height replaced_by_txid source detail
""")
chain("ds_sanctions_kpi", "Sanctions KPIs", "ds_sanctions", """
| dedup txid direction io_index
| stats dc(txid) as txs dc(address) as addrs sum(value_sats) as v
| eval btc=round(v/1e8,4)
""")
chain("ds_sanctions_pending", "Sanctioned txs still in mempool", "ds_sanctions", """
| stats latest(event) as state latest(_time) as last first(first_seen_at) as first_seen values(address) as addresses values(name) as names sum(eval(if(event=="seen", value_sats, 0))) as v by txid
| where state=="seen" OR state=="unconfirmed"
| eval btc=round(v/1e8,8), last=strftime(last, "%Y-%m-%d %H:%M")
| table txid state first_seen last btc addresses names
| rename txid as TXID, state as State, first_seen as "First seen", last as "Last event (UTC)", btc as BTC, addresses as Addresses, names as Names
""")
chain("ds_sanctions_pending_kpi", "Pending count", "ds_sanctions_pending", "| stats count as pending")
single("viz_sanc_txs", "Sanctioned txs", "ds_sanctions_kpi", "txs", precision=0, color=RED)
single("viz_sanc_addrs", "Listed addresses hit", "ds_sanctions_kpi", "addrs", precision=0, color=ORANGE)
single("viz_sanc_btc", "BTC moved (matched I/O)", "ds_sanctions_kpi", "btc", unit="BTC", precision=4, color=YELLOW)
single("viz_sanc_pending", "Unconfirmed now", "ds_sanctions_pending_kpi", "pending", precision=0, color=PURPLE)

chain("ds_sanctions_time", "Events over time", "ds_sanctions", """
| timechart span=$span$ count by event
""")
add_viz("viz_sanc_time", "splunk.column", "Events by type", "ds_sanctions_time",
        **CHART_BASE, stackMode="stacked",
        seriesColorsByField={"seen": YELLOW, "confirmed": GREEN, "replaced": BLUE, "removed": GREY, "unconfirmed": RED})

chain("ds_sanctions_entities", "By sanctioned entity", "ds_sanctions", """
| dedup txid direction io_index
| eval who=coalesce(name, address)
| stats dc(txid) as txs sum(eval(if(direction=="input", value_sats, 0))) as out_v sum(eval(if(direction=="output", value_sats, 0))) as in_v values(sanctions_programs) as programs by who
| eval out_v=round(out_v/1e8,8), in_v=round(in_v/1e8,8)
| sort 0 - txs
| rename who as Entity, txs as TXs, in_v as "Received BTC", out_v as "Sent BTC", programs as Programs
""")
add_viz("viz_sanc_entities", "splunk.table", "By listed entity", "ds_sanctions_entities", count=10)
add_viz("viz_sanc_pending_table", "splunk.table", "Still unconfirmed", "ds_sanctions_pending", count=10)

chain("ds_sanctions_latest", "Latest events", "ds_sanctions", """
| sort 0 - _time | head 200
| eval t=strftime(_time, "%Y-%m-%d %H:%M:%S")
| table t event txid direction address name value_btc block_height replaced_by_txid source detail
| rename t as "Time (UTC)", event as Event, txid as TXID, direction as Dir, address as Address, name as Name, value_btc as BTC, block_height as Block, replaced_by_txid as "Replaced by", source as Source, detail as Detail
""")
latest_sanc = add_viz("viz_sanc_latest", "splunk.table", "Latest events", "ds_sanctions_latest", count=15,
                      columnFormat={"Event": {"rowBackgroundColors": '> table | seriesByName("Event") | matchValue(eventColors)'}})
latest_sanc["context"] = {"eventColors": [
    {"match": "seen", "value": "#7a5c00"}, {"match": "confirmed", "value": "#1f6b2a"},
    {"match": "replaced", "value": "#2c3e7a"}, {"match": "removed", "value": "#44475a"},
    {"match": "unconfirmed", "value": "#7a1f2a"},
]}

# ---------------------------------------------------------------- all-time records
#
# These panels ignore the time picker, and none of them searches the raw
# data when the dashboard opens. They read results Splunk keeps up to date in
# the background, defined in btc-parser-dashboards (savedsearches.conf etc.,
# also written by this script):
#   - KV store lookups for everything that needs transactions/inputs/outputs
#     columns that are only extracted at search time (fees, values,
#     addresses - docs chapter 10.6), where an all-time scan takes hours:
#     <name>_update merges the last full hour of newly indexed data every
#     hour; <name>_rebuild recomputes everything from scratch. The rebuilds
#     are NOT scheduled - run them by hand once after installing or after a
#     backfill (the hourly windows only cover events whose _time is in the
#     last 7 days, so backfilled history only arrives through a rebuild).
#     Max/min merge safely; the one sum (zero_fee_txs) relies on the hourly
#     windows never overlapping, hence realtime_schedule = 0 (no skipped runs).
#     Each search finds the record values in a first pass and fetches the
#     matching rows with `map` in a second one - a plain subsearch would hit
#     the subsearch time limit on a rebuild.
#   - scheduled saved searches read via ds.savedSearch, which shows the result
#     of the last scheduled run: tstats over the fully indexed btc:blocks /
#     btc:prices, and the sanctions match (raw-term lookups of ~50 addresses,
#     answered by the bloom filters).
# Saved searches can't see dashboard tokens, so they use the `btc_index`
# macro (macros.conf) instead of $idx$.

SAVED = []  # (name, description, query, settings)
KV = {}  # collection -> {field: kvstore type}

HOURLY = {"enableSched": "1", "cron_schedule": "5 * * * *",
          "dispatch.earliest_time": "0", "dispatch.latest_time": "now"}
DAILY = {"enableSched": "1", "cron_schedule": "40 2 * * *",
         "dispatch.earliest_time": "0", "dispatch.latest_time": "now"}
UPDATE = {"enableSched": "1", "cron_schedule": "15 * * * *", "realtime_schedule": "0",
          "dispatch.earliest_time": "-7d@d", "dispatch.latest_time": "now"}
REBUILD = {"enableSched": "0", "dispatch.earliest_time": "0", "dispatch.latest_time": "now"}

# Dashboard tokens can't appear in a saved search (it doesn't see them);
# `map`'s own $field$ placeholders can.
DASHBOARD_TOKEN = re.compile(r"\$(idx|span|addr|global_time[.\w]*)\$")


def saved_search(name, description, query, settings):
    assert not DASHBOARD_TOKEN.search(query), name
    SAVED.append((name, description, query.strip(), settings))


def saved(ds_id, name, ref):
    ds[ds_id] = {"type": "ds.savedSearch", "name": name, "options": {"ref": ref}}


def incremental(name, description, build, fields):
    """build(window, merge) -> SPL. window limits to newly indexed data;
    merge appends the current KV store contents before re-aggregating."""
    KV[name] = fields
    saved_search(f"{name}_update", f"{description} - hourly merge of the last hour of indexed data",
                 build("`btc_rec_window`", True), UPDATE)
    saved_search(f"{name}_rebuild", f"{description} - full rebuild (run by hand after install or a backfill; hours)",
                 build("", False), REBUILD)


def merge_line(name, merge):
    return f"| append [| inputlookup {name}]\n| fields - _key\n" if merge else ""


TXW = "`btc_index` sourcetype=btc:transactions is_coinbase=false"
NUM = "number"


def build_tx_values(w, merge):
    return f"""
{TXW} {w}
| stats max(fee_sats) as max_fee_sats max(fee_rate_sat_vb) as max_fee_rate max(vsize) as max_vsize max(vin_count) as max_vin max(vout_count) as max_vout max(input_age_max_blocks) as max_age max(coin_days_destroyed_btc) as max_cdd max(output_value_sats) as max_out
    min(eval(if(fee_sats>0, fee_sats, null()))) as min_fee_sats min(eval(if(fee_sats>0, fee_rate_sat_vb, null()))) as min_fee_rate
    count(eval(fee_sats==0)) as zero_fee_txs
{merge_line("btc_rec_tx_values", merge)}| stats max(max_fee_sats) as max_fee_sats max(max_fee_rate) as max_fee_rate max(max_vsize) as max_vsize max(max_vin) as max_vin max(max_vout) as max_vout max(max_age) as max_age max(max_cdd) as max_cdd max(max_out) as max_out min(min_fee_sats) as min_fee_sats min(min_fee_rate) as min_fee_rate sum(zero_fee_txs) as zero_fee_txs
| outputlookup override_if_empty=false btc_rec_tx_values
"""


incremental("btc_rec_tx_values", "All-time transaction record values", build_tx_values,
            {f: NUM for f in ["max_fee_sats", "max_fee_rate", "max_vsize", "max_vin", "max_vout", "max_age",
                              "max_cdd", "max_out", "min_fee_sats", "min_fee_rate", "zero_fee_txs"]})


# Only whole-number fields go through the exact-match subsearch: a decimal
# field can render differently from its CSV string and silently match nothing.
def build_tx_rows(w, merge):
    return f"""
{TXW} {w}
| stats count max(fee_sats) as a max(vsize) as b max(vin_count) as c max(vout_count) as d max(input_age_max_blocks) as e max(output_value_sats) as f
| where count>0
| fillnull value=-1 a b c d e f
| map maxsearches=1 search="search {TXW} {w} (fee_sats=$a$ OR vsize=$b$ OR vin_count=$c$ OR vout_count=$d$ OR input_age_max_blocks=$e$ OR output_value_sats=$f$)"
| eval t=_time
| table t block_height txid fee_sats fee_rate_sat_vb vsize vin_count vout_count input_age_max_blocks output_value_sats
{merge_line("btc_rec_tx_rows", merge)}| dedup txid
| eventstats max(fee_sats) as m1 max(vsize) as m2 max(vin_count) as m3 max(vout_count) as m4 max(input_age_max_blocks) as m5 max(output_value_sats) as m6
| where fee_sats==m1 OR vsize==m2 OR vin_count==m3 OR vout_count==m4 OR input_age_max_blocks==m5 OR output_value_sats==m6
| fields - m1 m2 m3 m4 m5 m6
| outputlookup override_if_empty=false btc_rec_tx_rows
"""


incremental("btc_rec_tx_rows", "Transactions holding an all-time record", build_tx_rows,
            {"t": NUM, "block_height": NUM, "txid": "string", "fee_sats": NUM, "fee_rate_sat_vb": NUM, "vsize": NUM,
             "vin_count": NUM, "vout_count": NUM, "input_age_max_blocks": NUM, "output_value_sats": NUM})


def build_minfee(w, merge):
    return f"""
{TXW} fee_sats>0 {w}
| stats count min(fee_sats) as m
| where count>0
| map maxsearches=1 search="search {TXW} {w} fee_sats=$m$"
| eval t=_time
| table t block_height txid fee_sats fee_rate_sat_vb vsize
{merge_line("btc_rec_minfee_rows", merge)}| dedup txid
| eventstats min(fee_sats) as m
| where fee_sats==m
| sort 0 fee_rate_sat_vb
| head 25
| fields - m
| outputlookup override_if_empty=false btc_rec_minfee_rows
"""


incremental("btc_rec_minfee_rows", "Transactions with the lowest non-zero fee", build_minfee,
            {"t": NUM, "block_height": NUM, "txid": "string", "fee_sats": NUM, "fee_rate_sat_vb": NUM, "vsize": NUM})


def build_io(w, merge):
    return f"""
`btc_index` (sourcetype=btc:outputs OR (sourcetype=btc:inputs is_coinbase=false)) {w}
| stats max(value_sats) as v by sourcetype
| where isnotnull(v)
| map maxsearches=2 search="search `btc_index` sourcetype=$sourcetype$ {w} value_sats=$v$"
| eval kind=if(sourcetype=="btc:outputs", "created", "spent"), addr=coalesce(address, prevout_address), io=coalesce(output_index, input_index), t=_time
| table kind t block_height txid io addr value_sats input_age_blocks
{merge_line("btc_rec_io", merge)}| where isnotnull(value_sats)
| eventstats max(value_sats) as m by kind
| where value_sats==m
| dedup kind txid io
| fields - m
| outputlookup override_if_empty=false btc_rec_io
"""


incremental("btc_rec_io", "Largest single coin created / spent", build_io,
            {"kind": "string", "t": NUM, "block_height": NUM, "txid": "string", "io": NUM, "addr": "string",
             "value_sats": NUM, "input_age_blocks": NUM})

saved_search("btc_rec_blocks", "All-time block records (tstats, hourly)", """
| tstats fillnull_value="-" max(_time) as t
    where `btc_index` sourcetype=btc:blocks
    by height hash pool_name nTx total_fees_sats size time_since_prev_block_sec fee_rate_median_sat_vb
| foreach * [eval <<FIELD>>=if('<<FIELD>>'=="-", null(), '<<FIELD>>')]
""", HOURLY)

saved_search("btc_rec_price", "Daily BTC price, all time (tstats, hourly)", """
| tstats max(usd) as high min(usd) as low latest(usd) as close where `btc_index` sourcetype=btc:prices by _time span=1d
""", HOURLY)

saved_search("btc_rec_sanc", "Transactions touching a sanctioned address, all time (raw-term lookups, hourly)", """
`btc_index` (sourcetype=btc:outputs OR sourcetype=btc:inputs)
    [| inputlookup sanctioned_addresses.csv
     | eval search="address=\\"".address."\\" OR prevout_address=\\"".address."\\""
     | fields search]
| eval addr=if(sourcetype=="btc:outputs", address, prevout_address)
| stats sum(value_sats) as v min(_time) as t max(block_height) as height by txid sourcetype addr
| lookup sanctioned_addresses.csv address AS addr OUTPUT name sanctions_programs
| where isnotnull(name)
| eval received=if(sourcetype=="btc:outputs", v/1e8, 0), sent=if(sourcetype=="btc:inputs", v/1e8, 0)
| stats min(t) as t max(height) as height sum(received) as received_btc sum(sent) as sent_btc values(addr) as addresses values(name) as names values(sanctions_programs) as programs by txid
""", HOURLY)

# "Largest balances" (sum per address over every input/output ever) is gone:
# without indexed address/value_sats it is a scan of billions of rows that
# never finishes. Single addresses: the Address input below.


# Outputs >= 100 BTC and their spends, merged hourly. Rows with both ends seen
# are dropped, so the store only holds unspent big outputs (plus spends whose
# creation isn't indexed yet - inputs/outputs parts arrive in any order).
# override_if_empty=true: the merge always re-reads the whole store, so an
# empty result really means "nothing left" (e.g. the last one got spent).
def build_big_outputs(w, merge):
    return f"""
`btc_index` value_sats>=10000000000 (sourcetype=btc:outputs OR (sourcetype=btc:inputs is_coinbase=false)) {w}
| eval key=if(sourcetype=="btc:outputs", txid.":".output_index, prevout_txid.":".prevout_vout)
| eval created=if(sourcetype=="btc:outputs", block_height, null()), spent=if(sourcetype=="btc:inputs", block_height, null()), address=if(sourcetype=="btc:outputs", address, null())
| table key address value_sats created spent
{merge_line("btc_rec_big_outputs", merge)}| stats values(address) as address max(value_sats) as value_sats min(created) as created max(spent) as spent by key
| where isnull(created) OR isnull(spent)
| outputlookup override_if_empty=true btc_rec_big_outputs
"""


incremental("btc_rec_big_outputs", "Unspent outputs >= 100 BTC", build_big_outputs,
            {"key": "string", "address": "string", "value_sats": NUM, "created": NUM, "spent": NUM})

# --- dashboard side: read the KV stores / scheduled results

search("ds_rec_tx_values", "Record values (KV store)", """
| inputlookup btc_rec_tx_values
| eval max_fee_btc=round(max_fee_sats/1e8,8), age_years=round(max_age/52560,1), max_out_btc=round(max_out/1e8,2), max_cdd=round(max_cdd,0)
""")

single("viz_rec_max_fee", "Highest fee ever", "ds_rec_tx_values", "max_fee_btc", unit="BTC", precision=4, color=RED)
single("viz_rec_max_rate", "Highest fee rate ever", "ds_rec_tx_values", "max_fee_rate", unit="sat/vB", precision=0, color=RED)
single("viz_rec_min_fee", "Lowest non-zero fee", "ds_rec_tx_values", "min_fee_sats", unit="sats", precision=0, color=GREEN)
single("viz_rec_zero_fee", "Zero-fee transactions", "ds_rec_tx_values", "zero_fee_txs", precision=0, color=GREY)
single("viz_rec_vsize", "Largest transaction", "ds_rec_tx_values", "max_vsize", unit="vB", precision=0, color=TEAL)
single("viz_rec_vin", "Most inputs", "ds_rec_tx_values", "max_vin", precision=0, color=BLUE)
single("viz_rec_vout", "Most outputs", "ds_rec_tx_values", "max_vout", precision=0, color=BLUE)
single("viz_rec_age", "Oldest coin moved", "ds_rec_tx_values", "age_years", unit="years", precision=1, color=PURPLE)
single("viz_rec_cdd", "Most coin days destroyed", "ds_rec_tx_values", "max_cdd", unit="BTC-days", precision=0, color=ORANGE)
single("viz_rec_out", "Largest tx output total", "ds_rec_tx_values", "max_out_btc", unit="BTC", precision=2, color=YELLOW)

search("ds_rec_tx_rows", "Record-holding transactions (KV store)", """
| inputlookup btc_rec_tx_rows
| eval _time=t
""")
chain("ds_rec_tx_table", "Record transactions table", "ds_rec_tx_rows", """
| eventstats max(fee_sats) as m1 max(vsize) as m2 max(vin_count) as m3 max(vout_count) as m4 max(input_age_max_blocks) as m5 max(output_value_sats) as m6
| eval record=mvappend(if(fee_sats==m1,"Highest fee",null()), if(vsize==m2,"Largest transaction",null()), if(vin_count==m3,"Most inputs",null()), if(vout_count==m4,"Most outputs",null()), if(input_age_max_blocks==m5,"Oldest coin moved",null()), if(output_value_sats==m6,"Largest output total",null()))
| where isnotnull(record)
| mvexpand record
| eval date=strftime(_time,"%Y-%m-%d"), fee_btc=round(fee_sats/1e8,8), out_btc=round(output_value_sats/1e8,8), age_years=round(input_age_max_blocks/52560,1), rate=round(fee_rate_sat_vb,1)
| sort 0 record
| table record date block_height txid fee_btc rate vsize vin_count vout_count age_years out_btc
| rename record as Record, date as "Date (UTC)", block_height as Block, txid as TXID, fee_btc as "Fee BTC", rate as "sat/vB", vsize as vB, vin_count as Inputs, vout_count as Outputs, age_years as "Oldest input (y)", out_btc as "Output total BTC"
""")
add_viz("viz_rec_tx_table", "splunk.table", "Record transactions", "ds_rec_tx_table", count=10)

search("ds_rec_minfee_rows", "Lowest non-zero fee transactions (KV store)", """
| inputlookup btc_rec_minfee_rows
| sort 0 fee_rate_sat_vb
| eval date=strftime(t,"%Y-%m-%d"), rate=round(fee_rate_sat_vb,3)
| table date block_height txid fee_sats rate vsize
| rename date as "Date (UTC)", block_height as Block, txid as TXID, fee_sats as "Fee sats", rate as "sat/vB", vsize as vB
""")
add_viz("viz_rec_minfee", "splunk.table", "Lowest non-zero fee (up to 25 ties)", "ds_rec_minfee_rows", count=10)

saved("ds_rec_blocks", "Block records (scheduled hourly)", "btc_rec_blocks")
chain("ds_rec_blocks_table", "Block records table", "ds_rec_blocks", """
| eventstats max(nTx) as a max(total_fees_sats) as b max(size) as c max(time_since_prev_block_sec) as d min(time_since_prev_block_sec) as e max(fee_rate_median_sat_vb) as f
| eval record=mvappend(if(nTx==a,"Most transactions",null()), if(total_fees_sats==b,"Highest total fees",null()), if(size==c,"Largest block",null()), if(time_since_prev_block_sec==d,"Longest gap",null()), if(time_since_prev_block_sec==e,"Timestamp most before parent",null()), if(fee_rate_median_sat_vb==f,"Highest median fee rate",null()))
| where isnotnull(record)
| mvexpand record
| eval date=strftime(t,"%Y-%m-%d"), fees_btc=round(total_fees_sats/1e8,4), size_mb=round(size/1e6,2), gap_min=round(time_since_prev_block_sec/60,1), med=round(fee_rate_median_sat_vb,1)
| sort 0 record
| table record height date pool_name nTx fees_btc med size_mb gap_min
| rename record as Record, height as Height, date as "Date (UTC)", pool_name as Pool, nTx as TXs, fees_btc as "Fees BTC", med as "Median sat/vB", size_mb as "Size MB", gap_min as "Gap to prev (min)"
""")
add_viz("viz_rec_blocks_table", "splunk.table", "Block records", "ds_rec_blocks_table",
        "A block replaced by a reorg is still a row of its own here", count=10)

search("ds_rec_io", "Largest coin created / spent (KV store)", """
| inputlookup btc_rec_io
| eval kind=if(kind=="created", "Largest output ever created", "Largest coin ever spent"), btc=value_sats/1e8, date=strftime(t,"%Y-%m-%d"), held=round(input_age_blocks/52560,1)
| sort 0 kind
| table kind date block_height txid io addr btc held
| rename kind as Record, date as "Date (UTC)", block_height as Block, txid as TXID, io as Index, addr as Address, btc as BTC, held as "Held (years)"
""")
add_viz("viz_rec_io", "splunk.table", "Largest single coin", "ds_rec_io")

saved("ds_rec_price", "Daily price, all time (scheduled hourly)", "btc_rec_price")
chain("ds_rec_price_line", "All-time price line", "ds_rec_price", """
| fields _time close
| rename close as "USD (daily close)"
""")
add_viz("viz_rec_price_line", "splunk.line", "BTC price, all time (log)", "ds_rec_price_line",
        **{**LINE_BASE, "legendDisplay": "off"}, seriesColors=[ORANGE], yAxisScale="log", yAxisTitleText="USD")
chain("ds_rec_price_extremes", "ATH / ATL", "ds_rec_price", """
| eventstats max(high) as ath min(low) as atl
| eval which=case(high==ath, "All-time high", low==atl, "All-time low")
| where isnotnull(which)
| eval usd=round(if(which=="All-time high", high, low),2), date=strftime(_time,"%Y-%m-%d")
| table which date usd
| rename which as Record, date as "Day (UTC)", usd as USD
""")
add_viz("viz_rec_price_extremes", "splunk.table", "Price extremes", "ds_rec_price_extremes")
chain("ds_rec_price_kpi", "ATH + drawdown", "ds_rec_price", """
| stats max(high) as ath latest(close) as now
| eval drawdown=round((now-ath)/ath*100,1)
""")
single("viz_rec_ath", "Price all-time high", "ds_rec_price_kpi", "ath", unit="$", unit_before=True, precision=0, color=ORANGE)
single("viz_rec_drawdown", "Below all-time high", "ds_rec_price_kpi", "drawdown", unit="%", precision=1, color=RED)

saved("ds_rec_sanc", "Sanctioned addresses in chain data (scheduled hourly)", "btc_rec_sanc")
chain("ds_rec_sanc_table", "Sanctioned txs table", "ds_rec_sanc", """
| sort 0 - t
| eval date=strftime(t,"%Y-%m-%d"), received_btc=round(received_btc,8), sent_btc=round(sent_btc,8)
| table date height txid names addresses received_btc sent_btc programs
| rename date as "Date (UTC)", height as Block, txid as TXID, names as Names, addresses as Addresses, received_btc as "Received BTC", sent_btc as "Sent BTC", programs as Programs
""")
chain("ds_rec_sanc_kpi", "Sanctioned KPIs", "ds_rec_sanc", """
| stats count as txs sum(received_btc) as r sum(sent_btc) as s dc(addresses) as addrs
| eval r=round(r,4), s=round(s,4)
""")
chain("ds_rec_sanc_time", "Sanctioned txs per month", "ds_rec_sanc", """
| eval _time=t
| timechart span=1mon count as Transactions
""")
single("viz_rec_sanc_txs", "Txs touching listed addresses", "ds_rec_sanc_kpi", "txs", precision=0, color=RED)
single("viz_rec_sanc_recv", "BTC received by listed addresses", "ds_rec_sanc_kpi", "r", unit="BTC", precision=4, color=YELLOW)
single("viz_rec_sanc_sent", "BTC sent by listed addresses", "ds_rec_sanc_kpi", "s", unit="BTC", precision=4, color=ORANGE)
single("viz_rec_sanc_addrs", "Listed addresses with activity", "ds_rec_sanc_kpi", "addrs", precision=0, color=PURPLE)
add_viz("viz_rec_sanc_time", "splunk.column", "Per month", "ds_rec_sanc_time",
        **{**CHART_BASE, "legendDisplay": "off"}, seriesColors=[RED])
add_viz("viz_rec_sanc_table", "splunk.table", "All transactions with a listed address (chain data)", "ds_rec_sanc_table", count=10)

search("ds_rec_unspent", "Largest unspent outputs (KV store)", """
| inputlookup btc_rec_big_outputs
| where isnotnull(created) AND isnull(spent)
| eval btc=value_sats/1e8
| sort 25 - btc
| table key address created btc
| rename key as "Outpoint", address as Address, created as "Created at block", btc as BTC
""")
add_viz("viz_rec_unspent", "splunk.table", "Largest unspent outputs >= 100 BTC (hourly)", "ds_rec_unspent",
        "An output whose spend is not in the index shows up as unspent", count=10)

# Per-address and token-driven, so it can't be precomputed: an ad-hoc search
# that waits until the Address input is filled in. The address is a unique
# raw token, so the bloom filters skip every bucket that doesn't contain it.
search("ds_rec_addr", "Address balance history", """
index=$idx$ ((sourcetype=btc:outputs address="$addr$") OR (sourcetype=btc:inputs prevout_address="$addr$"))
| eval d=if(sourcetype=="btc:outputs", value_sats, -value_sats)
| bin _time span=1d
| stats sum(d) as d by _time
| sort 0 _time
| streamstats sum(d) as bal
| makecontinuous _time span=1d
| filldown bal
| eval balance_btc=round(bal/1e8,8)
| fields _time balance_btc
""", queryParameters={"earliest": "0", "latest": "now"})
chain("ds_rec_addr_kpi", "Address peak / now", "ds_rec_addr", """
| stats max(balance_btc) as peak latest(balance_btc) as now
""")
add_viz("viz_rec_addr_line", "splunk.area", "Balance history of the address in the Address input", "ds_rec_addr",
        **{**LINE_BASE, "legendDisplay": "off"}, seriesColors=[ORANGE], areaOpacity=0.3, yAxisTitleText="BTC")
single("viz_rec_addr_peak", "Peak balance ever", "ds_rec_addr_kpi", "peak", unit="BTC", precision=4, color=ORANGE)
single("viz_rec_addr_now", "Balance now", "ds_rec_addr_kpi", "now", unit="BTC", precision=4, color=TEAL)

# ---------------------------------------------------------------- inputs

inputs = {
    "input_time": {
        "type": "input.timerange",
        "title": "Time range",
        "options": {"token": "global_time", "defaultValue": "-30d@d,now"},
    },
    "input_span": {
        "type": "input.dropdown",
        "title": "Bucket",
        "options": {
            "token": "span",
            "defaultValue": "1d",
            "items": [
                {"label": "10 minutes", "value": "10m"},
                {"label": "1 hour", "value": "1h"},
                {"label": "4 hours", "value": "4h"},
                {"label": "1 day", "value": "1d"},
                {"label": "1 week", "value": "1w"},
                {"label": "1 month", "value": "1mon"},
            ],
        },
    },
    "input_idx": {
        "type": "input.text",
        "title": "Index",
        "options": {"token": "idx", "defaultValue": "btc_parser"},
    },
    # No default on purpose: the address-history panels wait until it is set.
    "input_addr": {
        "type": "input.text",
        "title": "Address (Records)",
        "options": {"token": "addr"},
    },
}

# ---------------------------------------------------------------- layout

W = 1440


def grid(rows):
    """rows: list of (height, [viz_id, ...] or [(viz_id, width), ...]); equal widths if unspecified."""
    structure = []
    y = 0
    for height, items in rows:
        if items and isinstance(items[0], str):
            n = len(items)
            widths = [W // n] * n
            widths[-1] += W - sum(widths)
            items = list(zip(items, widths))
        assert sum(w for _, w in items) == W, (items, sum(w for _, w in items))
        x = 0
        for item, w in items:
            structure.append({"item": item, "type": "block", "position": {"x": x, "y": y, "w": w, "h": height}})
            x += w
        y += height
    return {"type": "grid", "options": {"width": W, "height": y}, "structure": structure}


KPI_H = 120
tabs = [
    ("Overview", grid([
        (KPI_H, ["viz_kpi_price", "viz_kpi_change", "viz_kpi_height", "viz_kpi_median_fee", "viz_kpi_blocktime", "viz_kpi_hashrate"]),
        (320, ["viz_price_overview", "viz_fee_rates_overview"]),
        (420, ["viz_latest_blocks"]),
        (320, [("viz_pool_pie_overview", 480), ("viz_throughput", 960)]),
        (110, ["viz_about"]),
    ])),
    ("Mining", grid([
        (KPI_H, ["viz_epoch_progress", "viz_epoch_remaining", "viz_epoch_change", "viz_epoch_date", "viz_halving_blocks", "viz_halving_date"]),
        (360, ["viz_hashrate"]),
        (420, [("viz_pool_pie", 520), ("viz_pool_ranking", 920)]),
        (340, ["viz_pool_time"]),
        (360, ["viz_difficulty_epochs"]),
    ])),
    ("Fees & Rewards", grid([
        (360, ["viz_fee_bands_time"]),
        (160, ["viz_fee_bands_total"]),
        (340, ["viz_fee_percentiles", "viz_avg_fee"]),
        (340, ["viz_fees_subsidy", "viz_fees_usd"]),
    ])),
    ("Blocks", grid([
        (340, ["viz_size_weight", "viz_tx_count"]),
        (340, ["viz_block_time", "viz_interval_hist"]),
        (340, ["viz_vsize", "viz_long_gaps"]),
    ])),
    ("Transactions", grid([
        (340, ["viz_adoption"]),
        (360, [("viz_out_types", 960), ("viz_out_types_total", 480)]),
        (320, ["viz_op_return", "viz_cdd"]),
        (360, ["viz_coin_age"]),
    ])),
    ("Price", grid([
        (KPI_H, ["viz_price_last", "viz_price_eur", "viz_price_change", "viz_price_hi", "viz_price_lo", "viz_price_sats"]),
        (380, ["viz_price_main"]),
        (380, ["viz_price_daily_change", "viz_price_ohlc"]),
    ])),
    ("Chain Health", grid([
        (KPI_H, ["viz_stale_tips", "viz_stale_heights", "viz_stale_branch", "viz_reorg_heights"]),
        (300, ["viz_stale_time"]),
        (380, ["viz_stale_table"]),
        (340, ["viz_export_reorgs", "viz_long_gaps"]),
    ])),
    ("Sanctions Watch", grid([
        (KPI_H, ["viz_sanc_txs", "viz_sanc_addrs", "viz_sanc_btc", "viz_sanc_pending"]),
        (320, ["viz_sanc_time"]),
        (360, ["viz_sanc_entities", "viz_sanc_pending_table"]),
        (440, ["viz_sanc_latest"]),
    ])),
    ("All-time Records", grid([
        (KPI_H, ["viz_rec_max_fee", "viz_rec_max_rate", "viz_rec_min_fee", "viz_rec_zero_fee", "viz_rec_ath", "viz_rec_drawdown"]),
        (KPI_H, ["viz_rec_vsize", "viz_rec_vin", "viz_rec_vout", "viz_rec_age", "viz_rec_cdd", "viz_rec_out"]),
        (320, ["viz_rec_tx_table"]),
        (320, ["viz_rec_blocks_table"]),
        (200, [("viz_rec_io", 960), ("viz_rec_price_extremes", 480)]),
        (320, ["viz_rec_price_line"]),
        (320, ["viz_rec_minfee"]),
        (KPI_H, ["viz_rec_sanc_txs", "viz_rec_sanc_recv", "viz_rec_sanc_sent", "viz_rec_sanc_addrs"]),
        (380, [("viz_rec_sanc_time", 480), ("viz_rec_sanc_table", 960)]),
        (380, ["viz_rec_unspent"]),
        (300, [("viz_rec_addr_line", 960), ("viz_rec_addr_peak", 240), ("viz_rec_addr_now", 240)]),
    ])),
]

# A viz may appear on several tabs only if it is a distinct item; Dashboard Studio
# rejects one viz id in two layouts, so clone shared ones.
seen = set()
for _, layout in tabs:
    for block in layout["structure"]:
        item = block["item"]
        if item in seen:
            clone = f"{item}_2"
            assert clone not in viz
            viz[clone] = json.loads(json.dumps(viz[item]))
            block["item"] = clone
        seen.add(block["item"])

layout_defs = {}
tab_items = []
for i, (label, layout) in enumerate(tabs, 1):
    layout_id = f"layout_{i}"
    layout_defs[layout_id] = layout
    tab_items.append({"label": label, "layoutId": layout_id})

definition = {
    "title": "BTC Mempool Explorer",
    "description": "mempool.space-style views over the btc_parser exports (index=btc_parser)",
    "inputs": inputs,
    "defaults": {
        "dataSources": {
            "ds.search": {
                "options": {
                    "queryParameters": {"earliest": "$global_time.earliest$", "latest": "$global_time.latest$"}
                }
            }
        }
    },
    "visualizations": viz,
    "dataSources": ds,
    "layout": {
        "globalInputs": ["input_time", "input_span", "input_idx", "input_addr"],
        "tabs": {"items": tab_items},
        "layoutDefinitions": layout_defs,
    },
}

# ---------------------------------------------------------------- self-checks

used_items = {b["item"] for l in layout_defs.values() for b in l["structure"]}
assert used_items == set(viz), (set(viz) - used_items, used_items - set(viz))
for vid, v in viz.items():
    for ref in v.get("dataSources", {}).values():
        assert ref in ds, (vid, ref)
for did, d in ds.items():
    if d["type"] == "ds.chain":
        assert d["options"]["extend"] in ds, did
used_ds = {r for v in viz.values() for r in v.get("dataSources", {}).values()}
used_ds |= {d["options"]["extend"] for d in ds.values() if d["type"] == "ds.chain"}
assert used_ds == set(ds), set(ds) - used_ds

out_json = SPLUNK_DIR / "dashboards" / "btc_mempool_explorer.json"
out_xml = SPLUNK_DIR / "btc-parser-dashboards" / "default" / "data" / "ui" / "views" / "btc_mempool_explorer.xml"
text = json.dumps(definition, indent=4, ensure_ascii=False) + "\n"
out_json.write_text(text)
assert "]]>" not in text
out_xml.parent.mkdir(parents=True, exist_ok=True)
out_xml.write_text(
    '<dashboard version="2" theme="dark">\n'
    "    <label>BTC Mempool Explorer</label>\n"
    "    <description>mempool.space-style views over the btc_parser exports</description>\n"
    f"    <definition><![CDATA[\n{text}]]></definition>\n"
    '    <meta type="hiddenElements"><![CDATA[\n{\n    "hideEdit": false,\n    "hideOpenInSearch": false,\n    "hideExport": false\n}\n]]></meta>\n'
    "</dashboard>\n"
)

# ---------------------------------------------------------------- app conf files

APP = SPLUNK_DIR / "btc-parser-dashboards"
HEADER = "# Generated by splunk/dashboards/build_btc_mempool_explorer.py - edit that script, not this file.\n"


def write_conf(path, stanzas):
    out = [HEADER]
    for stanza, settings in stanzas:
        out.append(f"\n[{stanza}]\n")
        for key, value in settings.items():
            # multi-line values continue with a trailing backslash
            out.append(f"{key} = " + " \\\n".join(str(value).splitlines()) + "\n")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(out))


refs = {d["options"]["ref"] for d in ds.values() if d["type"] == "ds.savedSearch"}
assert refs <= {name for name, *_ in SAVED}, refs
write_conf(APP / "default" / "savedsearches.conf",
           [(name, {"description": desc, "search": q, **settings}) for name, desc, q, settings in SAVED])
write_conf(APP / "default" / "macros.conf", [
    ("btc_index", {"definition": "index=btc_parser"}),
    ("btc_rec_window", {"definition": "_index_earliest=-1h@h _index_latest=@h"}),
])
write_conf(APP / "default" / "collections.conf",
           [(name, {f"field.{f}": t for f, t in fields.items()}) for name, fields in KV.items()])
write_conf(APP / "default" / "transforms.conf",
           [(name, {"external_type": "kvstore", "collection": name, "fields_list": ", ".join(["_key", *fields])})
            for name, fields in KV.items()]
           + [("sanctioned_addresses", {"filename": "sanctioned_addresses.csv"})])

# The sanctions list as a lookup file. config/sanctioned_addresses.csv ends
# every line with a comma (an unnamed fifth column) - drop it for Splunk.
src = SPLUNK_DIR.parent / "config" / "sanctioned_addresses.csv"
lines = [line[:-1] if line.endswith(",") else line for line in src.read_text().splitlines()]
(APP / "lookups").mkdir(exist_ok=True)
(APP / "lookups" / "sanctioned_addresses.csv").write_text("\n".join(lines) + "\n")

print(f"{len(viz)} visualizations, {len(ds)} data sources, {len(tabs)} tabs, {len(SAVED)} saved searches, {len(KV)} KV collections")
