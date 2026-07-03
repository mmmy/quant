from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl


@dataclass(frozen=True)
class BacktestResult:
    rows_written: int
    final_equity: float | None


def backtest_equal_weight_signals(
    *,
    signals: pl.DataFrame,
    features: pl.DataFrame,
    fee_rate: float = 0.0,
    slippage_rate: float = 0.0,
) -> pl.DataFrame:
    returns = (
        features
        .select(["ts_ms", "symbol", "close"])
        .sort(["symbol", "ts_ms"])
        .with_columns(
            [
                (pl.col("close").shift(-1).over("symbol") / pl.col("close") - 1)
                .round(12)
                .alias("next_return"),
                pl.col("ts_ms").shift(-1).over("symbol").alias("return_time_ms"),
            ]
        )
        .select(
            [
                pl.col("ts_ms").alias("feature_available_time_ms"),
                "symbol",
                "return_time_ms",
                "next_return",
            ]
        )
    )
    signal_weights = signals.select(["feature_available_time_ms", "symbol", "target_weight"])
    rebalance_times = signal_weights.select("feature_available_time_ms").unique()
    symbols = signal_weights.select("symbol").unique()
    dense_weights = (
        rebalance_times.join(symbols, how="cross")
        .join(signal_weights, on=["feature_available_time_ms", "symbol"], how="left")
        .with_columns(pl.coalesce([pl.col("target_weight"), pl.lit(0.0)]).alias("target_weight"))
        .sort(["symbol", "feature_available_time_ms"])
        .with_columns(
            pl.coalesce(
                [pl.col("target_weight").shift(1).over("symbol"), pl.lit(0.0)]
            ).alias("previous_weight")
        )
    )
    turnover_by_signal_time = (
        dense_weights
        .group_by("feature_available_time_ms")
        .agg((pl.col("target_weight") - pl.col("previous_weight")).abs().sum().alias("turnover"))
    )
    return_time_by_signal_time = (
        returns.filter(pl.col("return_time_ms").is_not_null())
        .group_by("feature_available_time_ms")
        .agg(pl.col("return_time_ms").min())
    )
    turnover = (
        turnover_by_signal_time.join(
            return_time_by_signal_time,
            on="feature_available_time_ms",
            how="inner",
        )
        .group_by("return_time_ms")
        .agg(pl.col("turnover").sum())
        .rename({"return_time_ms": "time_ms"})
    )
    portfolio_returns = (
        signals.join(returns, on=["feature_available_time_ms", "symbol"], how="inner")
        .filter(pl.col("return_time_ms").is_not_null() & pl.col("next_return").is_not_null())
        .with_columns((pl.col("target_weight") * pl.col("next_return")).alias("weighted_return"))
        .group_by("return_time_ms")
        .agg(pl.col("weighted_return").sum().round(12).alias("gross_return"))
        .rename({"return_time_ms": "time_ms"})
    )
    timeline = signals.select(pl.col("feature_available_time_ms").alias("time_ms")).unique()
    return (
        timeline.join(portfolio_returns, on="time_ms", how="full")
        .with_columns(
            pl.coalesce([pl.col("time_ms"), pl.col("time_ms_right")]).alias("time_ms")
        )
        .drop("time_ms_right")
        .sort("time_ms")
        .join(turnover, on="time_ms", how="left")
        .with_columns(
            [
                pl.coalesce([pl.col("gross_return"), pl.lit(0.0)]).alias("gross_return"),
                pl.coalesce([pl.col("turnover"), pl.lit(0.0)]).alias("turnover"),
            ]
        )
        .with_columns((pl.col("turnover") * (fee_rate + slippage_rate)).round(12).alias("cost"))
        .with_columns((pl.col("gross_return") - pl.col("cost")).round(12).alias("period_return"))
        .with_columns((1.0 + pl.col("period_return")).cum_prod().round(12).alias("equity"))
    )


def run_backtest(
    *,
    signals_path: Path | str,
    features_dir: Path | str,
    output_path: Path | str,
    feature_interval: str = "1h",
    fee_rate: float = 0.0,
    slippage_rate: float = 0.0,
) -> BacktestResult:
    signals_file = Path(signals_path)
    feature_root = Path(features_dir)
    output_file = Path(output_path)
    if not signals_file.exists():
        return BacktestResult(rows_written=0, final_equity=None)

    feature_files = sorted(feature_root.glob(f"interval={feature_interval}/date=*/features.parquet"))
    if not feature_files:
        return BacktestResult(rows_written=0, final_equity=None)

    signals = pl.read_parquet(signals_file)
    features = pl.concat((pl.read_parquet(path) for path in feature_files), how="vertical_relaxed")
    equity = backtest_equal_weight_signals(
        signals=signals,
        features=features,
        fee_rate=fee_rate,
        slippage_rate=slippage_rate,
    )
    output_file.parent.mkdir(parents=True, exist_ok=True)
    equity.write_parquet(output_file)
    final_equity = None if equity.is_empty() else float(equity.item(equity.height - 1, "equity"))
    return BacktestResult(rows_written=equity.height, final_equity=final_equity)


def summarize_equity(equity: pl.DataFrame, *, periods_per_year: int = 365 * 24) -> dict[str, Any]:
    if equity.is_empty():
        return {
            "periods": 0,
            "final_equity": None,
            "total_return": None,
            "max_drawdown": None,
            "sharpe": None,
            "win_rate": None,
            "avg_period_return": None,
            "avg_turnover": None,
        }

    frame = equity.with_columns(
        [
            pl.col("equity").cum_max().alias("equity_peak"),
            (pl.col("equity") / pl.col("equity").cum_max() - 1).alias("drawdown"),
        ]
    )
    final_equity = float(frame.item(frame.height - 1, "equity"))
    returns = frame.filter(pl.col("period_return") != 0).select("period_return").to_series()
    avg_return = float(frame.select(pl.col("period_return").mean()).item())
    return_std = float(returns.std()) if len(returns) > 1 else 0.0
    sharpe = (
        (float(returns.mean()) / return_std) * (periods_per_year ** 0.5)
        if return_std > 0
        else None
    )
    wins = returns.filter(returns > 0).len()
    win_rate = wins / len(returns) if len(returns) else None
    return {
        "periods": frame.height,
        "final_equity": final_equity,
        "total_return": final_equity - 1.0,
        "max_drawdown": float(frame.select(pl.col("drawdown").min()).item()),
        "sharpe": sharpe,
        "win_rate": win_rate,
        "avg_period_return": avg_return,
        "avg_turnover": float(frame.select(pl.col("turnover").mean()).item()),
    }
