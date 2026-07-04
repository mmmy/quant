# Beginner Workflow

This guide walks through the simplest end-to-end workflow for syncing Binance USD-M futures
klines, building BTC-relative strength boards, and reading the output.

## 1. Install

From the project root:

```powershell
uv sync
```

All commands below should also be run from the project root.

## 2. Sync The Required Klines

The BTC-relative strength board needs silver kline data. For the current design, sync both `1m`
and `15m`.

```powershell
uv run quant-binance-sync sync-all --interval 1m --bootstrap-days 30
uv run quant-binance-sync sync-all --interval 15m --bootstrap-days 30
```

Why both intervals:

```text
1, 2, 3, 4, 5, 8, 10, 20 -> built from silver 1m
15, 30, 45, 60, 90, 120, 180, 240, 360, 480, 720, D -> built from silver 15m
```

The synced data is written under:

```text
data/silver/binance/usdm_futures/klines/interval=1m/...
data/silver/binance/usdm_futures/klines/interval=15m/...
```

## 3. Build A Relative Strength Board

Recommended: build a preset group first. Each preset uses practical `tail-bars`, warmup,
liquidity, and overheat limits for that trading horizon.

```powershell
uv run quant-binance-sync build-relative-strength-presets --preset scalp
uv run quant-binance-sync build-relative-strength-presets --preset intraday
uv run quant-binance-sync build-relative-strength-presets --preset swing
uv run quant-binance-sync build-relative-strength-presets --preset all
```

Preview the parameters first:

```powershell
uv run quant-binance-sync build-relative-strength-presets --preset intraday --dry-run
```

To tune the preset parameters, edit `configs/relative_strength_presets.toml` and pass it in:

```powershell
uv run quant-binance-sync build-relative-strength-presets --preset intraday --config configs/relative_strength_presets.toml
```

Build only one configured timeframe:

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

You can still build one timeframe manually.

Build a 15-minute BTC-relative strength board:

```powershell
uv run quant-binance-sync build-relative-strength --tf 15 --tail-bars 100 --liquidity-top-n 100
```

Build a 1-hour board:

```powershell
uv run quant-binance-sync build-relative-strength --tf 60 --tail-bars 100 --liquidity-top-n 100
```

Build a daily board:

```powershell
uv run quant-binance-sync build-relative-strength --tf D --tail-bars 100 --liquidity-top-n 100
```

`--tail-bars 100` writes only the latest 100 target candles. The command still reads extra history
for EMA/ATR warmup. By default this warmup is 50 target candles; change it with `--warmup-bars`.

`--liquidity-top-n 100` first keeps the top 100 quote-volume symbols, plus BTC. This removes most
low-liquidity symbols before EMA/ATR and relative-strength calculations. The liquidity ranking uses
the latest 20 target candles by default; change it with `--liquidity-lookback-bars`.

Output path example:

```text
data/gold/binance/usdm_futures/relative_strength/tf=15/relative_strength.parquet
```

## 4. Read Strong And Weak Coins

Show the latest coins stronger than BTC:

```powershell
uv run quant-binance-sync show-relative-strength --tf 15 --side strong --rank-pct 0.2 --top-n 10
```

Show the latest coins weaker than BTC:

```powershell
uv run quant-binance-sync show-relative-strength --tf 15 --side weak --rank-pct 0.2 --top-n 10
```

For a different timeframe, pass the output path:

```powershell
uv run quant-binance-sync show-relative-strength --path data/gold/binance/usdm_futures/relative_strength/tf=60/relative_strength.parquet --side strong
```

Or use `--tf` directly:

```powershell
uv run quant-binance-sync show-relative-strength --tf 60 --side strong --rank-pct 0.2 --top-n 10
```

Display filters:

```text
--rank-pct 0.2          Keep only ranks inside the top 20% of the non-overheated universe.
--top-n 10              Print at most 10 rows.
--include-overheated    Include rows where is_overheated=true.
```

## 5. Avoid Coins That Are Too Stretched

Use `--max-abs-gap-atr` to exclude coins too far from EMA20 by ATR distance.

Use `--max-ret` to exclude coins whose single-bar return is already too large.

Example:

```powershell
uv run quant-binance-sync build-relative-strength --tf 15 --max-abs-gap-atr 2.5 --max-ret 0.25
```

This means:

```text
Exclude if abs(gap_atr) > 2.5
Exclude if abs(ret) > 25%
```

## 6. How To Interpret The Columns

Important columns in `relative_strength.parquet`:

```text
tf              Target timeframe, such as 15, 60, D
source_interval Source kline interval actually used, 1m or 15m
symbol          Contract symbol
ret             Symbol return on this timeframe
btc_ret         BTC return on the same timeframe
rs_ret          ret - btc_ret
gap_atr         Symbol EMA20 distance normalized by ATR14
btc_gap_atr     BTC EMA20 distance normalized by ATR14
rs_gap          gap_atr - btc_gap_atr
slope_atr       Symbol EMA20 slope normalized by ATR14
btc_slope_atr   BTC EMA20 slope normalized by ATR14
rs_slope        slope_atr - btc_slope_atr
is_overheated   True if excluded by stretch filters
strong_rank     Lower is stronger than BTC
weak_rank       Lower is weaker than BTC
```

Simple interpretation:

```text
strong_rank = 1 -> currently the strongest valid coin versus BTC on this timeframe
weak_rank = 1   -> currently the weakest valid coin versus BTC on this timeframe
is_overheated   -> skip it even if it moved strongly
```

## 7. Practical Trading Use

If you trade BTC pullbacks and only want to choose a better coin:

```text
1. Use BTC chart for direction and timing.
2. Choose the timeframe you care about, for example 15 or 60.
3. Build the relative strength board for that timeframe.
4. Use --side strong to find coins stronger than BTC.
5. Avoid overheated names.
6. Keep your original entry logic unchanged.
```

Example:

```powershell
uv run quant-binance-sync build-relative-strength --tf 15 --tail-bars 100 --liquidity-top-n 100 --max-abs-gap-atr 2.5 --max-ret 0.25
uv run quant-binance-sync show-relative-strength --tf 15 --side strong --rank-pct 0.2 --top-n 10
```

## 8. Optional Cross-Section Feature Workflow

The project also has a broader feature/signal/backtest workflow. This is separate from the simple
BTC-relative strength board.

```powershell
uv run quant-binance-sync build-features --base-interval 1m --feature-interval 1h
uv run quant-binance-sync build-signals --feature-interval 1h --top-n 10
uv run quant-binance-sync show-signals
uv run quant-binance-sync backtest-signals --feature-interval 1h --fee-rate 0.0004 --slippage-rate 0.0002
uv run quant-binance-sync backtest-report
```

Use the BTC-relative strength board first if your main goal is discretionary BTC-timed entries.

## 9. Common Problems

No output rows:

```text
Check that BTCUSDT exists in the synced data.
Check that the required source interval was synced.
For tf=60, you need interval=15m silver data.
For tf=20, you need interval=1m silver data.
```

A coin looks strong but does not appear:

```text
It may be filtered by --max-abs-gap-atr or --max-ret.
Try a looser filter and compare the output.
```

The command is slow:

```text
Use --tail-bars 100 for relative strength boards.
Use --liquidity-top-n 100 to avoid calculating and displaying very low-liquidity symbols.
Large first-time data syncs can still be slow.
Use fewer bootstrap days while testing, such as --bootstrap-days 7.
```
