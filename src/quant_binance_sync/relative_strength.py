from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl

from quant_binance_sync.sync import interval_to_milliseconds


SUPPORTED_RELATIVE_STRENGTH_TFS = [
    "1",
    "2",
    "3",
    "4",
    "5",
    "8",
    "10",
    "15",
    "20",
    "30",
    "45",
    "60",
    "90",
    "120",
    "180",
    "240",
    "360",
    "480",
    "720",
    "D",
]


@dataclass(frozen=True)
class RelativeStrengthBuildResult:
    rows_written: int
    files_written: int


def relative_strength_source_interval(tf: str) -> str:
    normalized = tf.upper()
    if normalized == "D":
        return "15m"
    minutes = int(normalized)
    if minutes >= 15 and minutes % 15 == 0:
        return "15m"
    return "1m"


def build_relative_strength(
    *,
    silver_dir: Path | str,
    output_dir: Path | str,
    tf: str,
    btc_symbol: str = "BTCUSDT",
    max_abs_gap_atr: float = 2.5,
    max_ret: float | None = None,
    tail_bars: int | None = None,
    warmup_bars: int = 50,
    liquidity_top_n: int | None = None,
    liquidity_lookback_bars: int = 20,
) -> RelativeStrengthBuildResult:
    source_interval = relative_strength_source_interval(tf)
    files = sorted(
        Path(silver_dir).glob(f"interval={source_interval}/symbol=*/date=*/klines.parquet")
    )
    if not files:
        return RelativeStrengthBuildResult(rows_written=0, files_written=0)

    frame = pl.concat((pl.read_parquet(path) for path in files), how="vertical_relaxed")
    if tail_bars is not None:
        frame = filter_source_tail(
            frame,
            tf=tf,
            source_interval=source_interval,
            tail_bars=tail_bars,
            warmup_bars=warmup_bars,
        )
    result = build_relative_strength_frame(
        frame,
        tf=tf,
        btc_symbol=btc_symbol,
        max_abs_gap_atr=max_abs_gap_atr,
        max_ret=max_ret,
        source_interval=source_interval,
        tail_bars=tail_bars,
        liquidity_top_n=liquidity_top_n,
        liquidity_lookback_bars=liquidity_lookback_bars,
    )
    if result.is_empty():
        return RelativeStrengthBuildResult(rows_written=0, files_written=0)

    output_path = Path(output_dir) / f"tf={tf}" / "relative_strength.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.write_parquet(output_path)
    return RelativeStrengthBuildResult(rows_written=result.height, files_written=1)


def build_relative_strength_frame(
    frame: pl.DataFrame,
    *,
    tf: str,
    btc_symbol: str = "BTCUSDT",
    max_abs_gap_atr: float = 2.5,
    max_ret: float | None = None,
    source_interval: str | None = None,
    tail_bars: int | None = None,
    liquidity_top_n: int | None = None,
    liquidity_lookback_bars: int = 20,
) -> pl.DataFrame:
    source = source_interval or relative_strength_source_interval(tf)
    target_interval_ms = tf_to_milliseconds(tf)
    bars = aggregate_to_tf(frame, target_interval_ms=target_interval_ms)
    if liquidity_top_n is not None:
        bars = filter_liquidity_universe(
            bars,
            btc_symbol=btc_symbol,
            liquidity_top_n=liquidity_top_n,
            liquidity_lookback_bars=liquidity_lookback_bars,
        )
    factors = add_symbol_factors(bars)
    btc = factors.filter(pl.col("symbol") == btc_symbol).select(
        [
            "ts_ms",
            pl.col("ret").alias("btc_ret"),
            pl.col("gap_atr").alias("btc_gap_atr"),
            pl.col("slope_atr").alias("btc_slope_atr"),
        ]
    )
    joined = (
        factors.filter(pl.col("symbol") != btc_symbol)
        .join(btc, on="ts_ms", how="inner")
        .with_columns(
            [
                pl.lit(tf).alias("tf"),
                pl.lit(source).alias("source_interval"),
                (pl.col("ret") - pl.col("btc_ret")).round(12).alias("rs_ret"),
                (pl.col("gap_atr") - pl.col("btc_gap_atr")).round(12).alias("rs_gap"),
                (pl.col("slope_atr") - pl.col("btc_slope_atr")).round(12).alias("rs_slope"),
                pl.col("gap_atr").abs().alias("overheat"),
            ]
        )
        .with_columns(
            (
                (pl.col("overheat") > max_abs_gap_atr)
                | (
                    pl.lit(max_ret is not None)
                    & (pl.col("ret").abs() > (max_ret if max_ret is not None else 0.0))
                )
            ).alias("is_overheated")
        )
    )
    result = add_relative_strength_scores(joined)
    if tail_bars is None:
        return result
    return filter_target_tail(result, tail_bars=tail_bars)


def filter_source_tail(
    frame: pl.DataFrame,
    *,
    tf: str,
    source_interval: str,
    tail_bars: int,
    warmup_bars: int,
) -> pl.DataFrame:
    if tail_bars < 1:
        raise ValueError("tail_bars must be positive")
    if warmup_bars < 0:
        raise ValueError("warmup_bars must be non-negative")

    source_ms = interval_to_milliseconds(source_interval)
    target_ms = tf_to_milliseconds(tf)
    source_per_target = max(1, target_ms // source_ms)
    keep_source_bars = (tail_bars + warmup_bars) * source_per_target
    latest_by_symbol = frame.group_by("symbol").agg(pl.col("open_time_ms").max().alias("latest_ms"))
    return (
        frame.join(latest_by_symbol, on="symbol", how="left")
        .filter(pl.col("open_time_ms") >= pl.col("latest_ms") - ((keep_source_bars - 1) * source_ms))
        .drop("latest_ms")
    )


def filter_target_tail(frame: pl.DataFrame, *, tail_bars: int) -> pl.DataFrame:
    recent_times = (
        frame.select("ts_ms")
        .unique()
        .sort("ts_ms", descending=True)
        .head(tail_bars)
    )
    return frame.join(recent_times, on="ts_ms", how="inner").sort(
        ["ts_ms", "strong_rank", "weak_rank"],
        nulls_last=True,
    )


def filter_liquidity_universe(
    frame: pl.DataFrame,
    *,
    btc_symbol: str,
    liquidity_top_n: int,
    liquidity_lookback_bars: int,
) -> pl.DataFrame:
    if liquidity_top_n < 1:
        raise ValueError("liquidity_top_n must be positive")
    if liquidity_lookback_bars < 1:
        raise ValueError("liquidity_lookback_bars must be positive")

    recent_times = (
        frame.select("ts_ms")
        .unique()
        .sort("ts_ms", descending=True)
        .head(liquidity_lookback_bars)
    )
    liquidity = (
        frame.join(recent_times, on="ts_ms", how="inner")
        .filter(pl.col("symbol") != btc_symbol)
        .group_by("symbol")
        .agg(pl.col("quote_volume").sum().alias("liquidity_quote_volume"))
        .sort("liquidity_quote_volume", descending=True)
        .head(liquidity_top_n)
        .select("symbol")
    )
    keep_symbols = pl.concat(
        [liquidity, pl.DataFrame({"symbol": [btc_symbol]})],
        how="vertical",
    ).unique()
    return frame.join(keep_symbols, on="symbol", how="inner")


def tf_to_milliseconds(tf: str) -> int:
    normalized = tf.upper()
    if normalized == "D":
        return interval_to_milliseconds("1d")
    return int(normalized) * 60_000


def aggregate_to_tf(frame: pl.DataFrame, *, target_interval_ms: int) -> pl.DataFrame:
    return (
        frame.lazy()
        .with_columns(
            ((pl.col("open_time_ms") // target_interval_ms) * target_interval_ms).alias("ts_ms")
        )
        .sort(["symbol", "open_time_ms"])
        .group_by(["symbol", "ts_ms"], maintain_order=True)
        .agg(
            [
                pl.col("open").first().alias("open"),
                pl.col("high").max().alias("high"),
                pl.col("low").min().alias("low"),
                pl.col("close").last().alias("close"),
                pl.col("volume").sum().alias("volume"),
                pl.col("quote_volume").sum().alias("quote_volume"),
                pl.len().alias("bar_count"),
            ]
        )
        .with_columns(pl.from_epoch("ts_ms", time_unit="ms").alias("ts"))
        .sort(["symbol", "ts_ms"])
        .collect()
    )


def add_symbol_factors(frame: pl.DataFrame) -> pl.DataFrame:
    return (
        frame.lazy()
        .sort(["symbol", "ts_ms"])
        .with_columns(
            [
                pl.col("close").shift(1).over("symbol").alias("prev_close"),
                pl.col("close")
                .ewm_mean(span=20, adjust=False, min_samples=1)
                .over("symbol")
                .alias("ema20"),
            ]
        )
        .with_columns(
            pl.max_horizontal(
                [
                    pl.col("high") - pl.col("low"),
                    (pl.col("high") - pl.col("prev_close")).abs(),
                    (pl.col("low") - pl.col("prev_close")).abs(),
                ]
            ).alias("true_range")
        )
        .with_columns(
            [
                pl.col("true_range")
                .rolling_mean(window_size=14, min_samples=1)
                .over("symbol")
                .shift(1)
                .over("symbol")
                .alias("atr14_lag1"),
                (pl.col("close") / pl.col("close").shift(1).over("symbol") - 1)
                .round(12)
                .alias("ret"),
            ]
        )
        .with_columns(
            [
                ((pl.col("close") - pl.col("ema20")) / pl.col("atr14_lag1"))
                .round(12)
                .alias("gap_atr"),
                ((pl.col("ema20") - pl.col("ema20").shift(3).over("symbol")) / pl.col("atr14_lag1"))
                .round(12)
                .alias("slope_atr"),
            ]
        )
        .collect()
    )


def add_relative_strength_scores(frame: pl.DataFrame) -> pl.DataFrame:
    rankable = ~pl.col("is_overheated")
    rs_slope = pl.coalesce([pl.col("rs_slope"), pl.lit(0.0)])
    strong_expr = (
        (0.45 * pl.col("rs_ret").rank(descending=True).over("ts_ms"))
        + (0.30 * pl.col("rs_gap").rank(descending=True).over("ts_ms"))
        + (0.15 * rs_slope.rank(descending=True).over("ts_ms"))
        - (0.10 * pl.col("overheat").rank(descending=True).over("ts_ms"))
    )
    weak_expr = (
        (0.45 * (-pl.col("rs_ret")).rank(descending=True).over("ts_ms"))
        + (0.30 * (-pl.col("rs_gap")).rank(descending=True).over("ts_ms"))
        + (0.15 * (-rs_slope).rank(descending=True).over("ts_ms"))
        - (0.10 * pl.col("overheat").rank(descending=True).over("ts_ms"))
    )
    return (
        frame.lazy()
        .with_columns(
            [
                pl.when(rankable & (pl.col("rs_ret") > 0) & (pl.col("rs_gap") > 0))
                .then(strong_expr)
                .otherwise(None)
                .alias("strong_score"),
                pl.when(rankable & (pl.col("rs_ret") < 0) & (pl.col("rs_gap") < 0))
                .then(weak_expr)
                .otherwise(None)
                .alias("weak_score"),
            ]
        )
        .with_columns(
            [
                pl.col("strong_score")
                .rank(descending=False)
                .over("ts_ms")
                .cast(pl.Int64)
                .alias("strong_rank"),
                pl.col("weak_score")
                .rank(descending=False)
                .over("ts_ms")
                .cast(pl.Int64)
                .alias("weak_rank"),
            ]
        )
        .sort(["ts_ms", "strong_rank", "weak_rank"], nulls_last=True)
        .collect()
    )
