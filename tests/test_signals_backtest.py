import pytest
import polars as pl

from quant_binance_sync.backtest import backtest_equal_weight_signals, summarize_equity
from quant_binance_sync.signals import build_top_n_signals


def test_build_top_n_signals_selects_equal_weight_ranked_symbols() -> None:
    features = pl.DataFrame(
        {
            "ts_ms": [1000, 1000, 1000, 2000, 2000],
            "feature_available_time_ms": [2000, 2000, 2000, 3000, 3000],
            "symbol": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BTCUSDT", "ETHUSDT"],
            "score": [0.9, 0.7, 0.2, 0.3, 0.8],
            "is_tradable": [True, True, True, True, True],
        }
    )

    signals = build_top_n_signals(features, top_n=2)

    assert signals.select(["feature_available_time_ms", "symbol", "rank", "target_weight"]).to_dicts() == [
        {
            "feature_available_time_ms": 2000,
            "symbol": "BTCUSDT",
            "rank": 1,
            "target_weight": 0.5,
        },
        {
            "feature_available_time_ms": 2000,
            "symbol": "ETHUSDT",
            "rank": 2,
            "target_weight": 0.5,
        },
        {
            "feature_available_time_ms": 3000,
            "symbol": "ETHUSDT",
            "rank": 1,
            "target_weight": 0.5,
        },
        {
            "feature_available_time_ms": 3000,
            "symbol": "BTCUSDT",
            "rank": 2,
            "target_weight": 0.5,
        },
    ]


def test_build_top_n_signals_ignores_untradable_and_null_scores() -> None:
    features = pl.DataFrame(
        {
            "ts_ms": [1000, 1000, 1000],
            "feature_available_time_ms": [2000, 2000, 2000],
            "symbol": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
            "score": [0.9, None, 0.8],
            "is_tradable": [False, True, True],
        }
    )

    signals = build_top_n_signals(features, top_n=2)

    assert signals.select(["symbol", "rank", "target_weight"]).to_dicts() == [
        {"symbol": "SOLUSDT", "rank": 1, "target_weight": 0.5}
    ]


def test_backtest_equal_weight_signals_uses_next_bar_returns_after_feature_available_time() -> None:
    signals = pl.DataFrame(
        {
            "feature_available_time_ms": [2000, 2000, 3000],
            "symbol": ["BTCUSDT", "ETHUSDT", "BTCUSDT"],
            "target_weight": [0.5, 0.5, 1.0],
        }
    )
    features = pl.DataFrame(
        {
            "ts_ms": [2000, 2000, 3000, 3000, 4000],
            "symbol": ["BTCUSDT", "ETHUSDT", "BTCUSDT", "ETHUSDT", "BTCUSDT"],
            "close": [100.0, 200.0, 110.0, 180.0, 121.0],
        }
    )

    equity = backtest_equal_weight_signals(signals=signals, features=features)

    assert equity.select(["time_ms", "period_return", "equity"]).to_dicts() == [
        {"time_ms": 2000, "period_return": 0.0, "equity": 1.0},
        {"time_ms": 3000, "period_return": 0.0, "equity": 1.0},
        {"time_ms": 4000, "period_return": 0.1, "equity": 1.1},
    ]


def test_backtest_subtracts_fee_and_slippage_from_turnover() -> None:
    signals = pl.DataFrame(
        {
            "feature_available_time_ms": [2000, 3000],
            "symbol": ["BTCUSDT", "ETHUSDT"],
            "target_weight": [1.0, 1.0],
        }
    )
    features = pl.DataFrame(
        {
            "ts_ms": [2000, 3000, 3000, 4000],
            "symbol": ["BTCUSDT", "BTCUSDT", "ETHUSDT", "ETHUSDT"],
            "close": [100.0, 110.0, 200.0, 220.0],
        }
    )

    equity = backtest_equal_weight_signals(
        signals=signals,
        features=features,
        fee_rate=0.001,
        slippage_rate=0.001,
    )

    assert equity.select(["time_ms", "gross_return", "turnover", "cost", "period_return"]).to_dicts() == [
        {"time_ms": 2000, "gross_return": 0.0, "turnover": 0.0, "cost": 0.0, "period_return": 0.0},
        {
            "time_ms": 3000,
            "gross_return": 0.1,
            "turnover": 1.0,
            "cost": 0.002,
            "period_return": 0.098,
        },
        {
            "time_ms": 4000,
            "gross_return": 0.1,
            "turnover": 2.0,
            "cost": 0.004,
            "period_return": 0.096,
        },
    ]


def test_backtest_preserves_negative_returns_when_signal_time_matches_return_time() -> None:
    signals = pl.DataFrame(
        {
            "feature_available_time_ms": [2000, 3000],
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "target_weight": [1.0, 1.0],
        }
    )
    features = pl.DataFrame(
        {
            "ts_ms": [2000, 3000, 4000],
            "symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT"],
            "close": [100.0, 90.0, 81.0],
        }
    )

    equity = backtest_equal_weight_signals(signals=signals, features=features)

    assert equity.select(["time_ms", "gross_return", "period_return", "equity"]).to_dicts() == [
        {"time_ms": 2000, "gross_return": 0.0, "period_return": 0.0, "equity": 1.0},
        {"time_ms": 3000, "gross_return": -0.1, "period_return": -0.1, "equity": 0.9},
        {"time_ms": 4000, "gross_return": -0.1, "period_return": -0.1, "equity": 0.81},
    ]


def test_summarize_equity_reports_core_metrics() -> None:
    equity = pl.DataFrame(
        {
            "time_ms": [1000, 2000, 3000, 4000],
            "period_return": [0.0, 0.1, -0.2, 0.25],
            "turnover": [0.0, 1.0, 2.0, 0.5],
            "equity": [1.0, 1.1, 0.88, 1.1],
        }
    )

    report = summarize_equity(equity, periods_per_year=4)

    assert report["periods"] == 4
    assert report["final_equity"] == 1.1
    assert report["total_return"] == pytest.approx(0.1)
    assert report["max_drawdown"] == pytest.approx(-0.2)
    assert report["win_rate"] == pytest.approx(2 / 3)
    assert report["avg_turnover"] == pytest.approx(0.875)
    assert "sharpe" in report
