from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl

from quant_binance_sync.sync import interval_to_milliseconds


@dataclass(frozen=True)
class FeatureBuildResult:
    rows_written: int
    files_written: int


def build_features(
    *,
    silver_dir: Path | str,
    output_dir: Path | str,
    base_interval: str = "1m",
    feature_interval: str = "1h",
    symbol: list[str] | None = None,
) -> FeatureBuildResult:
    silver_path = Path(silver_dir)
    output_path = Path(output_dir)
    files = sorted(silver_path.glob(f"interval={base_interval}/symbol=*/date=*/klines.parquet"))
    if not files:
        return FeatureBuildResult(rows_written=0, files_written=0)

    frame = pl.concat((pl.read_parquet(path) for path in files), how="vertical_relaxed")
    if symbol is not None:
        frame = frame.filter(pl.col("symbol").is_in(symbol))
    if frame.is_empty():
        return FeatureBuildResult(rows_written=0, files_written=0)

    features = build_feature_frame(
        frame,
        base_interval=base_interval,
        feature_interval=feature_interval,
    )
    return write_feature_partitions(
        features,
        output_dir=output_path,
        feature_interval=feature_interval,
    )


def build_feature_frame(
    frame: pl.DataFrame,
    *,
    base_interval: str = "1m",
    feature_interval: str = "1h",
) -> pl.DataFrame:
    base_interval_ms = interval_to_milliseconds(base_interval)
    feature_interval_ms = interval_to_milliseconds(feature_interval)
    expected_rows = feature_interval_ms // base_interval_ms
    if expected_rows < 1 or feature_interval_ms % base_interval_ms != 0:
        raise ValueError("feature_interval must be a positive multiple of base_interval")

    hourly = aggregate_klines(
        frame,
        feature_interval_ms=feature_interval_ms,
    )
    with_time_series = add_time_series_features(
        hourly,
        feature_interval_ms=feature_interval_ms,
        expected_rows=expected_rows,
    )
    return add_cross_sectional_features(with_time_series)


def aggregate_klines(frame: pl.DataFrame, *, feature_interval_ms: int) -> pl.DataFrame:
    return (
        frame.lazy()
        .with_columns(
            ((pl.col("open_time_ms") // feature_interval_ms) * feature_interval_ms).alias("ts_ms")
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
                pl.col("trade_count").sum().alias("trade_count"),
                pl.col("taker_buy_base_volume").sum().alias("taker_buy_base_volume"),
                pl.col("taker_buy_quote_volume").sum().alias("taker_buy_quote_volume"),
                pl.len().alias("minute_count"),
            ]
        )
        .with_columns(pl.from_epoch("ts_ms", time_unit="ms").alias("ts"))
        .sort(["symbol", "ts_ms"])
        .collect()
    )


def add_time_series_features(
    frame: pl.DataFrame,
    *,
    feature_interval_ms: int,
    expected_rows: int,
) -> pl.DataFrame:
    return (
        frame.lazy()
        .sort(["symbol", "ts_ms"])
        .with_columns(
            [
                (pl.col("ts_ms") + feature_interval_ms).alias("feature_available_time_ms"),
                (pl.col("close") / pl.col("close").shift(1).over("symbol") - 1)
                .round(12)
                .alias("ret_1h"),
                (pl.col("close") / pl.col("close").shift(4).over("symbol") - 1)
                .round(12)
                .alias("ret_4h"),
                (pl.col("close") / pl.col("close").shift(24).over("symbol") - 1)
                .round(12)
                .alias("ret_24h"),
                (pl.col("close") / pl.col("close").shift(168).over("symbol") - 1)
                .round(12)
                .alias("ret_7d"),
                (pl.col("close") / pl.col("close").shift(1).over("symbol") - 1).alias("bar_return"),
                (pl.col("high") / pl.col("low") - 1).alias("high_low_range"),
            ]
        )
        .with_columns(
            [
                pl.col("quote_volume")
                .rolling_sum(window_size=24, min_samples=1)
                .over("symbol")
                .alias("quote_volume_24h"),
                pl.col("bar_return")
                .rolling_std(window_size=24, min_samples=2)
                .over("symbol")
                .alias("volatility_24h"),
                pl.col("high_low_range")
                .rolling_sum(window_size=24, min_samples=1)
                .over("symbol")
                .alias("high_low_range_24h"),
                (pl.col("high") - pl.col("low")).rolling_sum(window_size=24, min_samples=1)
                .over("symbol")
                .alias("atr_24h"),
            ]
        )
        .with_columns(
            (
                (pl.col("minute_count") == expected_rows)
                & pl.col("quote_volume_24h").is_not_null()
            ).alias("is_tradable")
        )
        .drop("bar_return")
        .collect()
    )


def add_cross_sectional_features(frame: pl.DataFrame) -> pl.DataFrame:
    tradable = pl.col("is_tradable")
    ranked = (
        frame.lazy()
        .with_columns(
            [
                pl.when(tradable & pl.col("ret_24h").is_not_null())
                .then(pl.col("ret_24h").rank(descending=True).over("ts_ms"))
                .otherwise(None)
                .alias("momentum_rank"),
                pl.when(tradable & pl.col("quote_volume_24h").is_not_null())
                .then(pl.col("quote_volume_24h").rank(descending=True).over("ts_ms"))
                .otherwise(None)
                .alias("liquidity_rank"),
                pl.when(tradable & pl.col("volatility_24h").is_not_null())
                .then(pl.col("volatility_24h").rank(descending=False).over("ts_ms"))
                .otherwise(None)
                .alias("low_vol_rank"),
            ]
        )
        .with_columns(
            pl.when(tradable & pl.col("liquidity_rank").is_not_null())
            .then(
                (1.0 / pl.col("liquidity_rank"))
                + pl.when(pl.col("momentum_rank").is_not_null())
                .then(0.5 / pl.col("momentum_rank"))
                .otherwise(0.0)
                + pl.when(pl.col("low_vol_rank").is_not_null())
                .then(0.2 / pl.col("low_vol_rank"))
                .otherwise(0.0)
            )
            .otherwise(None)
            .alias("score")
        )
        .with_columns(
            pl.from_epoch("feature_available_time_ms", time_unit="ms").alias(
                "feature_available_time"
            )
        )
        .sort(["ts_ms", "score"], descending=[False, True], nulls_last=True)
        .collect()
    )
    return ranked


def write_feature_partitions(
    frame: pl.DataFrame,
    *,
    output_dir: Path,
    feature_interval: str,
) -> FeatureBuildResult:
    rows_written = 0
    files_written = 0
    with_dates = frame.with_columns(pl.col("ts").dt.strftime("%Y-%m-%d").alias("date"))
    for date_value in with_dates.select("date").unique().to_series().to_list():
        partition = with_dates.filter(pl.col("date") == date_value)
        path = output_dir / f"interval={feature_interval}" / f"date={date_value}" / "features.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        partition.write_parquet(path)
        rows_written += partition.height
        files_written += 1
    return FeatureBuildResult(rows_written=rows_written, files_written=files_written)
