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

## Files

Symbol metadata:

```text
data/meta/binance/usdm_symbols_snapshots.jsonl
data/meta/binance/usdm_symbols_current.json
```

Kline checkpoints:

```text
data/state/binance/usdm_kline_checkpoints.json
```

Kline Parquet data:

```text
data/raw/binance/usdm_futures/klines/interval=1m/symbol=BTCUSDT/date=YYYY-MM-DD/klines.parquet
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
