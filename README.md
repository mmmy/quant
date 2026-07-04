# Quant Binance Sync

HTTP-based synchronizer for Binance USD-M USDT perpetual futures klines.

For a beginner-friendly end-to-end workflow, see
[`docs/beginner-workflow.md`](docs/beginner-workflow.md).

## Install

```powershell
uv sync
```

## Full sync

```powershell
uv run quant-binance-sync sync-all --interval 1m --bootstrap-days 30
```

`sync-all` does the full MVP data loop:

```text
1. Fetch Binance USD-M exchangeInfo.
2. Append a symbol snapshot JSONL record.
3. Write the current active USDT perpetual universe.
4. Mark removed symbols inactive in checkpoints.
5. Bootstrap symbols with no checkpoint from the last N days.
6. Resume existing symbols from the next missing candle.
7. Use Binance public data ZIP files for older complete UTC days.
8. Use REST for recent or archive-missing candles.
9. Upsert closed klines to partitioned Parquet.
```

## Separate steps

Refresh symbols only:

```powershell
uv run quant-binance-sync refresh-symbols
```

Sync klines using the latest saved symbol universe:

```powershell
uv run quant-binance-sync sync-klines --interval 1m --bootstrap-days 30
```

Sync selected symbols only:

```powershell
uv run quant-binance-sync sync-klines --symbol BTCUSDT --symbol ETHUSDT --interval 1m
```

## Realtime stream

Run the websocket kline streamer after `refresh-symbols` or `sync-all` has created the current
symbol universe:

```powershell
uv run quant-binance-sync stream-klines --interval 1m
```

`stream-klines` subscribes to Binance USD-M combined kline streams for the active USDT perpetual
symbols. Unclosed websocket candles are kept in process only and are not written to disk by default.
Closed websocket candles are buffered briefly in memory, batch-upserted to a small SQLite hot store,
then flushed in larger batches to the raw and silver Parquet layouts. This avoids a SQLite commit
and Parquet partition rewrite for every single closed candle while keeping the latest streamed data
available on disk. By default the hot store is:

```text
data/state/binance/stream_closed_klines_1m.sqlite
data/state/binance/stream_closed_klines_15m.sqlite
```

Tune the SQLite and Parquet flush batch sizes:

```powershell
uv run quant-binance-sync stream-klines --interval 1m --hot-flush-size 100 --hot-flush-interval-seconds 0.5 --stream-flush-size 1000
```

By default it opens websocket streams first, then runs startup REST gap-fill in the background with
the same request weight limiter used by `sync-klines`. This avoids missing candles that close while
a large startup gap-fill is still running. After each websocket disconnect, it also uses REST gap-fill
before reconnecting.

For a smoke test that exits after the first disconnect:

```powershell
uv run quant-binance-sync stream-klines --interval 1m --symbol BTCUSDT --once
```

Skip the startup REST gap-fill when you only want websocket data:

```powershell
uv run quant-binance-sync stream-klines --interval 1m --no-startup-gap-fill
```

## Files

Symbol metadata:

```text
data/meta/binance/usdm_symbols_snapshots.jsonl
data/meta/binance/usdm_symbols_current.json
```

Kline checkpoints:

```text
data/state/binance/usdm_kline_checkpoints_1m.json
data/state/binance/usdm_kline_checkpoints_15m.json
```

Realtime closed-kline hot stores:

```text
data/state/binance/stream_closed_klines_1m.sqlite
data/state/binance/stream_closed_klines_15m.sqlite
```

Kline Parquet data:

```text
data/raw/binance/usdm_futures/klines/interval=1m/symbol=BTCUSDT/date=YYYY-MM-DD/klines.parquet
data/silver/binance/usdm_futures/klines/interval=1m/symbol=BTCUSDT/date=YYYY-MM-DD/klines.parquet
```

## First sync size

The default first sync is `--bootstrap-days 30`. For `1m` candles, that is about 43,200 candles
per symbol.

By default, the synchronizer uses Binance public data daily ZIP files for complete UTC days at
least two days before today, then uses REST for the recent window. For a 30 day `1m` bootstrap,
that means roughly 28 days come from `data.binance.vision` and the newest two days come from
REST.

Disable public data archives and use REST only:

```powershell
uv run quant-binance-sync sync-klines --interval 1m --bootstrap-days 30 --no-use-archives
```

Tune the archive cutoff and cache directory:

```powershell
uv run quant-binance-sync sync-all --archive-threshold-days 2 --archive-cache-dir data/cache/binance-vision
```

Removed or non-trading symbols are removed from the active universe and marked `inactive` in
checkpoints. Historical Parquet data is kept to avoid survivorship bias in backtests.

## Rate limiting

The CLI defaults to `--limit 499` and `--max-weight-per-minute 900`. This is intentionally
conservative for large first-time backfills across all active contracts.

```powershell
uv run quant-binance-sync sync-klines --interval 1m --bootstrap-days 30 --limit 499 --max-weight-per-minute 900
```

If Binance returns `429`, the client honors `Retry-After` when present and retries with backoff.
For slower but gentler syncs, also reduce concurrency:

```powershell
uv run quant-binance-sync sync-klines --interval 1m --bootstrap-days 30 --concurrency 2 --max-weight-per-minute 600
```

## Progress

Kline sync commands show a progress bar by default:

```text
sync klines ... 42% requests=123 klines=61377 current=BTCUSDT
```

Disable it for logs or schedulers:

```powershell
uv run quant-binance-sync sync-klines --interval 1m --no-progress
```

`stream-klines` also shows compact live stats by default:

```text
stream klines symbols=520 conns=3 ws=12480 rest=320 requests=42 sqlbuf=30 sqlflush=124 pending=2480 pqflush=2/5000 current=BTCUSDT
```

`ws` is the number of closed klines received from websocket streams. `sqlbuf` is the in-memory
buffer waiting for the next SQLite batch write. `sqlflush` counts SQLite batch writes. `pending`
is the number of SQLite hot-store rows not yet flushed to Parquet. `pqflush` is the Parquet flush
count and threshold. `rest` and `requests` track startup and reconnect gap-fill work. Disable the
live stats for logs or schedulers:

```powershell
uv run quant-binance-sync stream-klines --interval 1m --no-progress
```

## Feature engine

Build hourly cross-sectional features from normalized silver `1m` klines:

```powershell
uv run quant-binance-sync build-features --base-interval 1m --feature-interval 1h
```

Include streamed closed candles that are still in the SQLite hot store and have not yet been flushed
to Parquet:

```powershell
uv run quant-binance-sync build-features --base-interval 1m --feature-interval 1h --realtime-closed-db-path data/state/binance/stream_closed_klines_1m.sqlite
```

Write a filtered feature set for selected symbols:

```powershell
uv run quant-binance-sync build-features --symbol BTCUSDT --symbol ETHUSDT
```

The feature engine reads:

```text
data/silver/binance/usdm_futures/klines/interval=1m/symbol=BTCUSDT/date=YYYY-MM-DD/klines.parquet
```

and writes:

```text
data/gold/binance/usdm_futures/features/interval=1h/date=YYYY-MM-DD/features.parquet
```

Each feature row uses `ts_ms` as the feature bar open time and
`feature_available_time_ms = ts_ms + feature_interval_ms`, so downstream backtests can avoid
lookahead. Incomplete feature bars are kept with `is_tradable=false` and no score.

Build Top N equal-weight selection signals from those features:

```powershell
uv run quant-binance-sync build-signals --feature-interval 1h --top-n 10
```

Inspect the latest signal basket:

```powershell
uv run quant-binance-sync show-signals
```

This writes:

```text
data/gold/binance/usdm_futures/signals/interval=1h/signals.parquet
```

Run a simple next-bar equal-weight backtest:

```powershell
uv run quant-binance-sync backtest-signals --feature-interval 1h
```

Include one-way fee and slippage assumptions:

```powershell
uv run quant-binance-sync backtest-signals --feature-interval 1h --fee-rate 0.0004 --slippage-rate 0.0002
```

This writes an equity curve:

```text
data/gold/binance/usdm_futures/backtests/interval=1h/equity.parquet
```

Print the backtest summary:

```powershell
uv run quant-binance-sync backtest-report
```

## BTC relative strength

Build a single-timeframe BTC-relative strength board:

```powershell
uv run quant-binance-sync build-relative-strength --tf 15
```

Include streamed closed candles that are still in the SQLite hot store:

```powershell
uv run quant-binance-sync build-relative-strength --tf 15 --realtime-closed-db-path data/state/binance/stream_closed_klines_15m.sqlite
```

Build multiple practical boards with tuned defaults:

```powershell
uv run quant-binance-sync build-relative-strength-presets --preset scalp
uv run quant-binance-sync build-relative-strength-presets --preset intraday
uv run quant-binance-sync build-relative-strength-presets --preset swing
uv run quant-binance-sync build-relative-strength-presets --preset all
```

Preset builds automatically choose the hot store by source interval when `--state-dir` is set or left
at its default:

```text
1, 2, 3, 4, 5, 8, 10, 20 -> stream_closed_klines_1m.sqlite
15, 30, 45, 60, 90, 120, 180, 240, 360, 480, 720, D -> stream_closed_klines_15m.sqlite
```

Preview the resolved parameters without writing files:

```powershell
uv run quant-binance-sync build-relative-strength-presets --preset intraday --dry-run
```

Override the built-in defaults with a TOML config:

```powershell
uv run quant-binance-sync build-relative-strength-presets --preset intraday --config configs/relative_strength_presets.toml
```

Build one configured timeframe:

```powershell
uv run quant-binance-sync build-relative-strength-presets --tf 60 --config configs/relative_strength_presets.toml
```

Preset groups:

```text
scalp:    1, 3, 5, 15
intraday: 15, 30, 60, 120, 240, 360
swing:    240, 360, 720, D
all:      1, 2, 3, 4, 5, 8, 10, 15, 20, 30, 45, 60, 90, 120, 180, 240, 360, 480, 720, D
```

For faster watchlist-style use, build only the latest 100 target bars:

```powershell
uv run quant-binance-sync build-relative-strength --tf 15 --tail-bars 100 --liquidity-top-n 100
```

`--warmup-bars` controls how many extra target bars are read for EMA/ATR warmup:

```powershell
uv run quant-binance-sync build-relative-strength --tf 60 --tail-bars 100 --warmup-bars 50
```

`--liquidity-top-n` keeps only the highest quote-volume symbols before factor calculation. The
liquidity universe is measured over the latest `--liquidity-lookback-bars` target candles:

```powershell
uv run quant-binance-sync build-relative-strength --tf 360 --tail-bars 100 --liquidity-top-n 100 --liquidity-lookback-bars 20
```

Supported target timeframes:

```text
1, 2, 3, 4, 5, 8, 10, 15, 20, 30, 45, 60, 90, 120, 180, 240, 360, 480, 720, D
```

Source kline selection:

```text
1, 2, 3, 4, 5, 8, 10, 20 -> silver 1m
15, 30, 45, 60, 90, 120, 180, 240, 360, 480, 720, D -> silver 15m
```

The output is written to:

```text
data/gold/binance/usdm_futures/relative_strength/tf=15/relative_strength.parquet
```

Show the latest stronger-than-BTC names:

```powershell
uv run quant-binance-sync show-relative-strength --tf 15 --side strong --rank-pct 0.2 --top-n 10
```

`--rank-pct 0.2` means "only consider the top 20% of the non-overheated universe",
then `--top-n 10` prints at most 10 rows from that slice.

Show the latest weaker-than-BTC names:

```powershell
uv run quant-binance-sync show-relative-strength --tf 15 --side weak --rank-pct 0.2 --top-n 10
```

`show-relative-strength` filters `is_overheated=true` rows by default. Use
`--include-overheated` when you want to inspect them too.

Use `--max-abs-gap-atr` and `--max-ret` to exclude names that are already too stretched:

```powershell
uv run quant-binance-sync build-relative-strength --tf 15 --max-abs-gap-atr 2.5 --max-ret 0.25
```
