# Quant Binance Sync

HTTP-based synchronizer for Binance USD-M USDT perpetual futures klines.

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
Closed candles are written to raw Parquet and upserted to the silver Parquet layout, then checkpoints
are updated. By default it opens websocket streams first, then runs startup REST gap-fill in the
background with the same request weight limiter used by `sync-klines`. This avoids missing candles
that close while a large startup gap-fill is still running. After each websocket disconnect, it also
uses REST gap-fill before reconnecting.

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
stream klines symbols=520 conns=3 ws=12480 rest=320 requests=42 current=BTCUSDT
```

`ws` is the number of closed klines saved from websocket streams. `rest` and `requests` track
startup and reconnect gap-fill work. Disable the live stats for logs or schedulers:

```powershell
uv run quant-binance-sync stream-klines --interval 1m --no-progress
```

## Feature engine

Build hourly cross-sectional features from normalized silver `1m` klines:

```powershell
uv run quant-binance-sync build-features --base-interval 1m --feature-interval 1h
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
