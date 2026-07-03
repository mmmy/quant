from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl


@dataclass(frozen=True)
class SignalBuildResult:
    rows_written: int
    files_written: int


def build_top_n_signals(features: pl.DataFrame, *, top_n: int) -> pl.DataFrame:
    if top_n < 1:
        raise ValueError("top_n must be positive")

    return (
        features.lazy()
        .filter(pl.col("is_tradable") & pl.col("score").is_not_null())
        .with_columns(
            pl.col("score")
            .rank(descending=True)
            .over("feature_available_time_ms")
            .cast(pl.Int64)
            .alias("rank")
        )
        .filter(pl.col("rank") <= top_n)
        .sort(["feature_available_time_ms", "rank", "symbol"])
        .with_columns(pl.lit(1.0 / top_n).alias("target_weight"))
        .select(
            [
                "feature_available_time_ms",
                "symbol",
                "score",
                "rank",
                "target_weight",
            ]
        )
        .collect()
    )


def build_signals(
    *,
    features_dir: Path | str,
    output_dir: Path | str,
    feature_interval: str = "1h",
    top_n: int = 10,
) -> SignalBuildResult:
    features_path = Path(features_dir)
    output_path = Path(output_dir)
    files = sorted(features_path.glob(f"interval={feature_interval}/date=*/features.parquet"))
    if not files:
        return SignalBuildResult(rows_written=0, files_written=0)

    features = pl.concat((pl.read_parquet(path) for path in files), how="vertical_relaxed")
    signals = build_top_n_signals(features, top_n=top_n)
    if signals.is_empty():
        return SignalBuildResult(rows_written=0, files_written=0)

    path = output_path / f"interval={feature_interval}" / "signals.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    signals.write_parquet(path)
    return SignalBuildResult(rows_written=signals.height, files_written=1)
