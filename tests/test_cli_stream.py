import json

import pytest
from typer.testing import CliRunner

from quant_binance_sync import cli
from quant_binance_sync.backtest import BacktestResult
from quant_binance_sync.checkpoints import Checkpoint
from quant_binance_sync.features import FeatureBuildResult
from quant_binance_sync.normalize_existing import NormalizeExistingProgress, NormalizeExistingResult
from quant_binance_sync.relative_strength import RelativeStrengthBuildResult
from quant_binance_sync.signals import SignalBuildResult
from quant_binance_sync.storage import NormalizedKlineStore
from quant_binance_sync.stream import StreamResult, StreamUpdate
from quant_binance_sync.sync import SyncResult


runner = CliRunner()


def test_stream_klines_cli_does_not_enable_realtime_disk_cache_by_default(tmp_path, monkeypatch) -> None:
    calls = []

    async def fake_stream_klines(**kwargs):
        calls.append(kwargs)
        return StreamResult(connections_seen=1, klines_saved=0)

    monkeypatch.setattr(cli, "_stream_klines", fake_stream_klines)

    result = runner.invoke(
        cli.app,
        [
            "stream-klines",
            "--interval",
            "15m",
            "--data-dir",
            str(tmp_path / "raw"),
            "--silver-dir",
            str(tmp_path / "silver"),
            "--quarantine-dir",
            str(tmp_path / "quarantine"),
            "--gap-report-path",
            str(tmp_path / "gaps.parquet"),
            "--meta-dir",
            str(tmp_path / "meta"),
            "--state-dir",
            str(tmp_path / "state"),
            "--no-progress",
            "--once",
        ],
    )

    assert result.exit_code == 0
    assert calls[0]["realtime_dir"] is None


@pytest.mark.asyncio
async def test_stream_klines_loads_symbols_and_wires_gap_sync(tmp_path, monkeypatch) -> None:
    meta_dir = tmp_path / "meta"
    state_dir = tmp_path / "state"
    data_dir = tmp_path / "raw"
    silver_dir = tmp_path / "silver"
    realtime_dir = tmp_path / "realtime"
    meta_dir.mkdir()
    (meta_dir / "usdm_symbols_current.json").write_text(
        json.dumps(
            {
                "snapshot_time": "2024-07-01T00:00:00+00:00",
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "base_asset": "BTC",
                        "quote_asset": "USDT",
                        "contract_type": "PERPETUAL",
                        "status": "TRADING",
                        "onboard_date": 1,
                        "delivery_date": 2,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    stream_calls = []
    gap_sync = None

    async def fake_stream_closed_klines(**kwargs):
        nonlocal gap_sync
        stream_calls.append(kwargs)
        gap_sync = kwargs["gap_sync_callback"]
        return StreamResult(connections_seen=1, klines_saved=2)

    async def fake_sync_missing_klines(**kwargs):
        stream_calls.append({"gap_symbols": kwargs["symbols"]})
        kwargs["checkpoints"]["BTCUSDT|1m"] = Checkpoint(last_open_time_ms=1719792000000, status="active")
        kwargs["checkpoint_callback"](kwargs["checkpoints"])

    class NullClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    monkeypatch.setattr(cli, "stream_closed_klines", fake_stream_closed_klines)
    monkeypatch.setattr(cli, "sync_missing_klines", fake_sync_missing_klines)
    monkeypatch.setattr(cli, "BinanceFuturesClient", NullClient)

    result = await cli._stream_klines(
        interval="1m",
        data_dir=data_dir,
        silver_dir=silver_dir,
        realtime_dir=realtime_dir,
        meta_dir=meta_dir,
        state_dir=state_dir,
        symbol=None,
        streams_per_connection=200,
        max_weight_per_minute=900,
        reconnect_delay_seconds=0,
        once=True,
    )
    assert result == StreamResult(connections_seen=1, klines_saved=2)
    assert stream_calls[0]["symbols"] == ["BTCUSDT"]
    assert stream_calls[0]["open_kline_store"] is not None
    assert stream_calls[1]["gap_symbols"] == ["BTCUSDT"]

    await gap_sync(["BTCUSDT"])

    checkpoint_payload = json.loads(
        (state_dir / "usdm_kline_checkpoints_1m.json").read_text(encoding="utf-8")
    )
    assert checkpoint_payload["BTCUSDT|1m"]["last_open_time_ms"] == 1719792000000
    assert stream_calls[2]["gap_symbols"] == ["BTCUSDT"]


@pytest.mark.asyncio
async def test_startup_gap_fill_uses_checkpoint_snapshot_after_websocket_starts(
    tmp_path, monkeypatch
) -> None:
    meta_dir = tmp_path / "meta"
    state_dir = tmp_path / "state"
    data_dir = tmp_path / "raw"
    silver_dir = tmp_path / "silver"
    realtime_dir = tmp_path / "realtime"
    meta_dir.mkdir()
    state_dir.mkdir()
    (meta_dir / "usdm_symbols_current.json").write_text(
        json.dumps(
            {
                "snapshot_time": "2024-07-01T00:00:00+00:00",
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "base_asset": "BTC",
                        "quote_asset": "USDT",
                        "contract_type": "PERPETUAL",
                        "status": "TRADING",
                        "onboard_date": 1,
                        "delivery_date": 2,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (state_dir / "usdm_kline_checkpoints_1m.json").write_text(
        json.dumps({"BTCUSDT|1m": {"last_open_time_ms": 1000, "status": "active"}}),
        encoding="utf-8",
    )

    calls = []

    async def fake_stream_closed_klines(**kwargs):
        calls.append({"stream_symbols": kwargs["symbols"]})
        kwargs["checkpoints"]["BTCUSDT|1m"] = Checkpoint(
            last_open_time_ms=2000,
            status="active",
        )
        return StreamResult(connections_seen=1, klines_saved=1)

    async def fake_sync_missing_klines(**kwargs):
        calls.append(
            {
                "gap_symbols": kwargs["symbols"],
                "gap_checkpoint": kwargs["checkpoints"]["BTCUSDT|1m"],
            }
        )

    class NullClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    monkeypatch.setattr(cli, "stream_closed_klines", fake_stream_closed_klines)
    monkeypatch.setattr(cli, "sync_missing_klines", fake_sync_missing_klines)
    monkeypatch.setattr(cli, "BinanceFuturesClient", NullClient)

    await cli._stream_klines(
        interval="1m",
        data_dir=data_dir,
        silver_dir=silver_dir,
        realtime_dir=realtime_dir,
        meta_dir=meta_dir,
        state_dir=state_dir,
        symbol=None,
        streams_per_connection=200,
        max_weight_per_minute=900,
        reconnect_delay_seconds=0,
        once=True,
    )

    assert calls == [
        {"stream_symbols": ["BTCUSDT"]},
        {
            "gap_symbols": ["BTCUSDT"],
            "gap_checkpoint": Checkpoint(last_open_time_ms=1000, status="active"),
        },
    ]


@pytest.mark.asyncio
async def test_stream_klines_can_skip_startup_gap_fill(tmp_path, monkeypatch) -> None:
    meta_dir = tmp_path / "meta"
    state_dir = tmp_path / "state"
    data_dir = tmp_path / "raw"
    silver_dir = tmp_path / "silver"
    realtime_dir = tmp_path / "realtime"
    meta_dir.mkdir()
    (meta_dir / "usdm_symbols_current.json").write_text(
        json.dumps(
            {
                "snapshot_time": "2024-07-01T00:00:00+00:00",
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "base_asset": "BTC",
                        "quote_asset": "USDT",
                        "contract_type": "PERPETUAL",
                        "status": "TRADING",
                        "onboard_date": 1,
                        "delivery_date": 2,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    stream_calls = []

    async def fake_stream_closed_klines(**kwargs):
        stream_calls.append(kwargs)
        return StreamResult(connections_seen=1, klines_saved=0)

    async def fake_sync_missing_klines(**kwargs):
        stream_calls.append({"gap_symbols": kwargs["symbols"]})

    class NullClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    monkeypatch.setattr(cli, "stream_closed_klines", fake_stream_closed_klines)
    monkeypatch.setattr(cli, "sync_missing_klines", fake_sync_missing_klines)
    monkeypatch.setattr(cli, "BinanceFuturesClient", NullClient)

    await cli._stream_klines(
        interval="1m",
        data_dir=data_dir,
        silver_dir=silver_dir,
        realtime_dir=realtime_dir,
        meta_dir=meta_dir,
        state_dir=state_dir,
        symbol=None,
        streams_per_connection=200,
        max_weight_per_minute=900,
        startup_gap_fill=False,
        reconnect_delay_seconds=0,
        once=True,
    )

    assert "symbols" in stream_calls[0]
    assert all("gap_symbols" not in call for call in stream_calls)


def test_merging_checkpoint_callback_never_moves_live_checkpoint_backward() -> None:
    live = {"BTCUSDT|1m": Checkpoint(last_open_time_ms=2000, status="active")}
    saved = []
    callback = cli.make_merging_checkpoint_callback(
        live_checkpoints=live,
        save=lambda checkpoints: saved.append(dict(checkpoints)),
    )

    callback({"BTCUSDT|1m": Checkpoint(last_open_time_ms=1000, status="active")})

    assert live["BTCUSDT|1m"] == Checkpoint(last_open_time_ms=2000, status="active")
    assert saved[-1]["BTCUSDT|1m"] == Checkpoint(last_open_time_ms=2000, status="active")


def test_throttled_checkpoint_callback_coalesces_saves_until_flush() -> None:
    now = 0.0
    saved = []
    callback = cli.ThrottledCheckpointCallback(
        save=lambda checkpoints: saved.append(dict(checkpoints)),
        monotonic=lambda: now,
        min_interval_seconds=10.0,
    )

    callback({"BTCUSDT|1m": Checkpoint(last_open_time_ms=1000, status="active")})
    callback({"BTCUSDT|1m": Checkpoint(last_open_time_ms=2000, status="active")})
    now = 5.0
    callback({"BTCUSDT|1m": Checkpoint(last_open_time_ms=3000, status="active")})

    assert saved == [
        {"BTCUSDT|1m": Checkpoint(last_open_time_ms=1000, status="active")},
    ]

    callback.flush()

    assert saved[-1] == {"BTCUSDT|1m": Checkpoint(last_open_time_ms=3000, status="active")}


def test_throttled_checkpoint_callback_saves_again_after_interval() -> None:
    now = 0.0
    saved = []
    callback = cli.ThrottledCheckpointCallback(
        save=lambda checkpoints: saved.append(dict(checkpoints)),
        monotonic=lambda: now,
        min_interval_seconds=10.0,
    )

    callback({"BTCUSDT|1m": Checkpoint(last_open_time_ms=1000, status="active")})
    now = 10.0
    callback({"BTCUSDT|1m": Checkpoint(last_open_time_ms=2000, status="active")})

    assert saved == [
        {"BTCUSDT|1m": Checkpoint(last_open_time_ms=1000, status="active")},
        {"BTCUSDT|1m": Checkpoint(last_open_time_ms=2000, status="active")},
    ]


def test_make_stream_kline_store_uses_normalized_store_when_silver_dir_is_set(tmp_path) -> None:
    store = cli.make_stream_kline_store(
        data_dir=tmp_path / "raw",
        silver_dir=tmp_path / "silver",
        quarantine_dir=tmp_path / "quarantine",
        gap_report_path=tmp_path / "gap_report.parquet",
        interval="1m",
    )

    assert isinstance(store, NormalizedKlineStore)


def test_normalize_klines_command_calls_existing_raw_normalizer(tmp_path, monkeypatch) -> None:
    calls = []

    def fake_normalize_existing_klines(**kwargs):
        calls.append(kwargs)
        return NormalizeExistingResult(
            files_seen=2,
            raw_klines_seen=3,
            accepted_klines=2,
            rejected_klines=1,
            conflict_klines=0,
            gaps_seen=1,
        )

    monkeypatch.setattr(cli, "normalize_existing_klines", fake_normalize_existing_klines)

    cli.normalize_klines(
        interval="1m",
        data_dir=tmp_path / "raw",
        silver_dir=tmp_path / "silver",
        quarantine_dir=tmp_path / "quarantine",
        gap_report_path=tmp_path / "gaps.parquet",
        symbol=["BTCUSDT"],
        start_date=None,
        end_date=None,
        overwrite=True,
        progress=False,
    )

    assert calls == [
        {
            "raw_dir": tmp_path / "raw",
            "silver_dir": tmp_path / "silver",
            "quarantine_dir": tmp_path / "quarantine",
            "gap_report_path": tmp_path / "gaps.parquet",
            "interval": "1m",
            "symbol": ["BTCUSDT"],
            "start_date": None,
            "end_date": None,
            "overwrite": True,
            "progress_callback": None,
        }
    ]


def test_normalize_klines_command_wires_progress_callback(tmp_path, monkeypatch) -> None:
    calls = []

    def fake_normalize_existing_klines(**kwargs):
        calls.append(kwargs)
        return NormalizeExistingResult(
            files_seen=0,
            raw_klines_seen=0,
            accepted_klines=0,
            rejected_klines=0,
            conflict_klines=0,
            gaps_seen=0,
        )

    monkeypatch.setattr(cli, "normalize_existing_klines", fake_normalize_existing_klines)

    cli.normalize_klines(
        interval="1m",
        data_dir=tmp_path / "raw",
        silver_dir=tmp_path / "silver",
        quarantine_dir=tmp_path / "quarantine",
        gap_report_path=tmp_path / "gaps.parquet",
        symbol=None,
        start_date=None,
        end_date=None,
        overwrite=True,
        progress=True,
    )

    assert calls[0]["progress_callback"] is not None


def test_normalize_klines_help_renders() -> None:
    result = runner.invoke(cli.app, ["normalize-klines", "--help"])

    assert result.exit_code == 0
    assert "normalize-klines" in result.output


def test_build_features_command_wires_feature_builder(tmp_path, monkeypatch) -> None:
    calls = []

    def fake_build_features(**kwargs):
        calls.append(kwargs)
        return FeatureBuildResult(rows_written=12, files_written=2)

    monkeypatch.setattr(cli, "build_features", fake_build_features)

    result = runner.invoke(
        cli.app,
        [
            "build-features",
            "--silver-dir",
            str(tmp_path / "silver"),
            "--output-dir",
            str(tmp_path / "gold"),
            "--base-interval",
            "1m",
            "--feature-interval",
            "1h",
            "--symbol",
            "BTCUSDT",
        ],
    )

    assert result.exit_code == 0
    assert "rows_written=12 files_written=2" in result.output
    assert calls == [
        {
            "silver_dir": tmp_path / "silver",
            "output_dir": tmp_path / "gold",
            "base_interval": "1m",
            "feature_interval": "1h",
            "symbol": ["BTCUSDT"],
        }
    ]


def test_build_signals_command_wires_signal_builder(tmp_path, monkeypatch) -> None:
    calls = []

    def fake_build_signals(**kwargs):
        calls.append(kwargs)
        return SignalBuildResult(rows_written=20, files_written=1)

    monkeypatch.setattr(cli, "build_signals", fake_build_signals)

    result = runner.invoke(
        cli.app,
        [
            "build-signals",
            "--features-dir",
            str(tmp_path / "features"),
            "--output-dir",
            str(tmp_path / "signals"),
            "--feature-interval",
            "1h",
            "--top-n",
            "5",
        ],
    )

    assert result.exit_code == 0
    assert "rows_written=20 files_written=1" in result.output
    assert calls == [
        {
            "features_dir": tmp_path / "features",
            "output_dir": tmp_path / "signals",
            "feature_interval": "1h",
            "top_n": 5,
        }
    ]


def test_backtest_signals_command_wires_backtest_runner(tmp_path, monkeypatch) -> None:
    calls = []

    def fake_run_backtest(**kwargs):
        calls.append(kwargs)
        return BacktestResult(rows_written=3, final_equity=1.23)

    monkeypatch.setattr(cli, "run_backtest", fake_run_backtest)

    result = runner.invoke(
        cli.app,
        [
            "backtest-signals",
            "--signals-path",
            str(tmp_path / "signals.parquet"),
            "--features-dir",
            str(tmp_path / "features"),
            "--output-path",
            str(tmp_path / "equity.parquet"),
            "--feature-interval",
            "1h",
        ],
    )

    assert result.exit_code == 0
    assert "rows_written=3 final_equity=1.23" in result.output
    assert calls == [
        {
            "signals_path": tmp_path / "signals.parquet",
            "features_dir": tmp_path / "features",
            "output_path": tmp_path / "equity.parquet",
            "feature_interval": "1h",
            "fee_rate": 0.0,
            "slippage_rate": 0.0,
        }
    ]


def test_show_signals_command_prints_latest_ranked_signals(tmp_path) -> None:
    import polars as pl

    signals_path = tmp_path / "signals.parquet"
    pl.DataFrame(
        {
            "feature_available_time_ms": [2000, 2000, 3000],
            "symbol": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
            "score": [0.9, 0.8, 0.7],
            "rank": [1, 2, 1],
            "target_weight": [0.5, 0.5, 1.0],
        }
    ).write_parquet(signals_path)

    result = runner.invoke(cli.app, ["show-signals", "--signals-path", str(signals_path)])

    assert result.exit_code == 0
    assert "SOLUSDT" in result.output
    assert "BTCUSDT" not in result.output


def test_backtest_report_command_prints_summary(tmp_path, monkeypatch) -> None:
    import polars as pl

    equity_path = tmp_path / "equity.parquet"
    pl.DataFrame(
        {
            "time_ms": [1000, 2000],
            "period_return": [0.0, 0.1],
            "turnover": [0.0, 1.0],
            "equity": [1.0, 1.1],
        }
    ).write_parquet(equity_path)

    result = runner.invoke(
        cli.app,
        ["backtest-report", "--equity-path", str(equity_path), "--periods-per-year", "24"],
    )

    assert result.exit_code == 0
    assert "final_equity=1.1" in result.output
    assert "total_return=" in result.output
    assert "max_drawdown=" in result.output


def test_build_relative_strength_command_wires_builder(tmp_path, monkeypatch) -> None:
    calls = []

    def fake_build_relative_strength(**kwargs):
        calls.append(kwargs)
        return RelativeStrengthBuildResult(rows_written=7, files_written=1)

    monkeypatch.setattr(cli, "build_relative_strength", fake_build_relative_strength)

    result = runner.invoke(
        cli.app,
        [
            "build-relative-strength",
            "--silver-dir",
            str(tmp_path / "silver"),
            "--output-dir",
            str(tmp_path / "rs"),
            "--tf",
            "60",
            "--btc-symbol",
            "BTCUSDT",
            "--max-abs-gap-atr",
            "2.5",
            "--max-ret",
            "0.25",
            "--tail-bars",
            "100",
            "--warmup-bars",
            "50",
            "--liquidity-top-n",
            "100",
            "--liquidity-lookback-bars",
            "20",
        ],
    )

    assert result.exit_code == 0
    assert "rows_written=7 files_written=1" in result.output
    assert calls == [
        {
            "silver_dir": tmp_path / "silver",
            "output_dir": tmp_path / "rs",
            "tf": "60",
            "btc_symbol": "BTCUSDT",
            "max_abs_gap_atr": 2.5,
            "max_ret": 0.25,
            "tail_bars": 100,
            "warmup_bars": 50,
            "liquidity_top_n": 100,
            "liquidity_lookback_bars": 20,
        }
    ]


def test_build_relative_strength_presets_command_runs_each_preset_tf(tmp_path, monkeypatch) -> None:
    calls = []

    def fake_build_relative_strength(**kwargs):
        calls.append(kwargs)
        return RelativeStrengthBuildResult(rows_written=7, files_written=1)

    monkeypatch.setattr(cli, "build_relative_strength", fake_build_relative_strength)

    result = runner.invoke(
        cli.app,
        [
            "build-relative-strength-presets",
            "--preset",
            "scalp",
            "--silver-dir",
            str(tmp_path / "silver"),
            "--output-dir",
            str(tmp_path / "rs"),
            "--btc-symbol",
            "BTCUSDT",
        ],
    )

    assert result.exit_code == 0
    assert "tf=1 rows_written=7 files_written=1" in result.output
    assert "tf=15 rows_written=7 files_written=1" in result.output
    assert [call["tf"] for call in calls] == ["1", "3", "5", "15"]
    assert calls[0] == {
        "silver_dir": tmp_path / "silver",
        "output_dir": tmp_path / "rs",
        "tf": "1",
        "btc_symbol": "BTCUSDT",
        "max_abs_gap_atr": 1.2,
        "max_ret": 0.03,
        "tail_bars": 240,
        "warmup_bars": 80,
        "liquidity_top_n": 100,
        "liquidity_lookback_bars": 240,
    }


def test_build_relative_strength_presets_command_uses_config_file(tmp_path, monkeypatch) -> None:
    calls = []
    config_path = tmp_path / "relative_strength_presets.toml"
    config_path.write_text(
        """
[groups]
custom = ["15"]

[timeframes."15"]
tail_bars = 88
warmup_bars = 34
liquidity_top_n = 55
liquidity_lookback_bars = 21
max_abs_gap_atr = 1.6
max_ret = 0.07
""",
        encoding="utf-8",
    )

    def fake_build_relative_strength(**kwargs):
        calls.append(kwargs)
        return RelativeStrengthBuildResult(rows_written=7, files_written=1)

    monkeypatch.setattr(cli, "build_relative_strength", fake_build_relative_strength)

    result = runner.invoke(
        cli.app,
        [
            "build-relative-strength-presets",
            "--preset",
            "custom",
            "--config",
            str(config_path),
            "--silver-dir",
            str(tmp_path / "silver"),
            "--output-dir",
            str(tmp_path / "rs"),
        ],
    )

    assert result.exit_code == 0
    assert calls[0]["tf"] == "15"
    assert calls[0]["tail_bars"] == 88
    assert calls[0]["warmup_bars"] == 34
    assert calls[0]["liquidity_top_n"] == 55
    assert calls[0]["liquidity_lookback_bars"] == 21
    assert calls[0]["max_abs_gap_atr"] == 1.6
    assert calls[0]["max_ret"] == 0.07


def test_build_relative_strength_presets_command_can_build_single_configured_tf(
    tmp_path,
    monkeypatch,
) -> None:
    calls = []
    config_path = tmp_path / "relative_strength_presets.toml"
    config_path.write_text(
        """
[timeframes."60"]
tail_bars = 88
warmup_bars = 34
liquidity_top_n = 55
liquidity_lookback_bars = 21
max_abs_gap_atr = 1.6
max_ret = 0.07
""",
        encoding="utf-8",
    )

    def fake_build_relative_strength(**kwargs):
        calls.append(kwargs)
        return RelativeStrengthBuildResult(rows_written=7, files_written=1)

    monkeypatch.setattr(cli, "build_relative_strength", fake_build_relative_strength)

    result = runner.invoke(
        cli.app,
        [
            "build-relative-strength-presets",
            "--preset",
            "intraday",
            "--tf",
            "60",
            "--config",
            str(config_path),
            "--silver-dir",
            str(tmp_path / "silver"),
            "--output-dir",
            str(tmp_path / "rs"),
        ],
    )

    assert result.exit_code == 0
    assert [call["tf"] for call in calls] == ["60"]
    assert calls[0]["tail_bars"] == 88
    assert calls[0]["warmup_bars"] == 34
    assert calls[0]["liquidity_top_n"] == 55
    assert calls[0]["liquidity_lookback_bars"] == 21
    assert calls[0]["max_abs_gap_atr"] == 1.6
    assert calls[0]["max_ret"] == 0.07


def test_build_relative_strength_presets_command_dry_run_does_not_build(tmp_path, monkeypatch) -> None:
    calls = []

    def fake_build_relative_strength(**kwargs):
        calls.append(kwargs)
        return RelativeStrengthBuildResult(rows_written=7, files_written=1)

    monkeypatch.setattr(cli, "build_relative_strength", fake_build_relative_strength)

    result = runner.invoke(
        cli.app,
        [
            "build-relative-strength-presets",
            "--preset",
            "scalp",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    assert calls == []
    assert "tf=1 tail_bars=240 warmup_bars=80" in result.output
    assert "max_abs_gap_atr=1.2 max_ret=0.03" in result.output


def test_show_relative_strength_command_prints_requested_side(tmp_path) -> None:
    import polars as pl

    rs_path = tmp_path / "relative_strength.parquet"
    pl.DataFrame(
        {
            "ts_ms": [1000, 1000, 1000],
            "symbol": ["ETHUSDT", "SOLUSDT", "DOGEUSDT"],
            "rs_ret": [0.02, -0.01, 0.08],
            "rs_gap": [0.5, -0.4, 2.0],
            "strong_rank": [1, None, None],
            "weak_rank": [None, 1, None],
            "is_overheated": [False, False, True],
        }
    ).write_parquet(rs_path)

    result = runner.invoke(
        cli.app,
        ["show-relative-strength", "--path", str(rs_path), "--side", "strong"],
    )

    assert result.exit_code == 0
    assert "ETHUSDT" in result.output
    assert "SOLUSDT" not in result.output


def test_show_relative_strength_command_supports_tf_rank_pct_and_top_n(tmp_path) -> None:
    import polars as pl

    rs_dir = tmp_path / "relative_strength"
    rs_path = rs_dir / "tf=360" / "relative_strength.parquet"
    rs_path.parent.mkdir(parents=True)
    pl.DataFrame(
        {
            "ts_ms": [1000, 1000, 1000, 1000],
            "symbol": ["AUSDT", "BUSDT", "CUSDT", "DUSDT"],
            "rs_ret": [0.04, 0.03, 0.02, 0.01],
            "rs_gap": [0.4, 0.3, 0.2, 0.1],
            "strong_rank": [1, 2, 3, 4],
            "weak_rank": [None, None, None, None],
            "is_overheated": [False, False, False, False],
        }
    ).write_parquet(rs_path)

    result = runner.invoke(
        cli.app,
        [
            "show-relative-strength",
            "--relative-strength-dir",
            str(rs_dir),
            "--tf",
            "360",
            "--side",
            "strong",
            "--rank-pct",
            "0.5",
            "--top-n",
            "1",
        ],
    )

    assert result.exit_code == 0
    assert "AUSDT" in result.output
    assert "BUSDT" not in result.output


def test_show_relative_strength_command_uses_total_universe_for_rank_pct(tmp_path) -> None:
    import polars as pl

    rs_path = tmp_path / "relative_strength.parquet"
    symbols = [f"S{i:02d}USDT" for i in range(100)]
    strong_ranks = list(range(1, 11)) + [None] * 90
    pl.DataFrame(
        {
            "ts_ms": [1000] * 100,
            "symbol": symbols,
            "rs_ret": [0.04] * 10 + [-0.01] * 90,
            "rs_gap": [0.4] * 10 + [-0.2] * 90,
            "strong_rank": strong_ranks,
            "weak_rank": [None] * 100,
            "is_overheated": [False] * 100,
        }
    ).write_parquet(rs_path)

    result = runner.invoke(
        cli.app,
        [
            "show-relative-strength",
            "--path",
            str(rs_path),
            "--side",
            "strong",
            "--rank-pct",
            "0.2",
            "--top-n",
            "10",
        ],
    )

    assert result.exit_code == 0
    assert "shape: (10, 6)" in result.output


def test_show_relative_strength_command_filters_overheated_by_default(tmp_path) -> None:
    import polars as pl

    rs_path = tmp_path / "relative_strength.parquet"
    pl.DataFrame(
        {
            "ts_ms": [1000, 1000],
            "symbol": ["AUSDT", "BUSDT"],
            "rs_ret": [0.04, 0.03],
            "rs_gap": [0.4, 0.3],
            "strong_rank": [1, 2],
            "weak_rank": [None, None],
            "is_overheated": [True, False],
        }
    ).write_parquet(rs_path)

    default_result = runner.invoke(
        cli.app,
        ["show-relative-strength", "--path", str(rs_path), "--side", "strong"],
    )
    include_result = runner.invoke(
        cli.app,
        [
            "show-relative-strength",
            "--path",
            str(rs_path),
            "--side",
            "strong",
            "--include-overheated",
        ],
    )

    assert default_result.exit_code == 0
    assert "AUSDT" not in default_result.output
    assert "BUSDT" in default_result.output
    assert include_result.exit_code == 0
    assert "AUSDT" in include_result.output


def test_normalize_progress_callback_updates_total_completed_and_counts() -> None:
    class FakeProgress:
        def __init__(self) -> None:
            self.add_calls = []
            self.update_calls = []

        def add_task(self, *args, **kwargs):
            self.add_calls.append((args, kwargs))
            return 123

        def update(self, *args, **kwargs) -> None:
            self.update_calls.append((args, kwargs))

    progress = FakeProgress()
    callback = cli.make_normalize_progress_callback(progress)

    callback(
        NormalizeExistingProgress(
            current="BTCUSDT 2024-07-01",
            files_seen=2,
            total_files=10,
            raw_klines_seen=100,
            accepted_klines=98,
            rejected_klines=1,
            conflict_klines=0,
            gaps_seen=1,
        )
    )

    assert progress.add_calls[0][1]["total"] is None
    assert progress.update_calls == [
        (
            (123,),
            {
                "total": 10,
                "completed": 2,
                "files": "2/10",
                "raw": 100,
                "accepted": 98,
                "rejected": 1,
                "conflicts": 0,
                "gaps": 1,
                "current": "BTCUSDT 2024-07-01",
            },
        )
    ]


@pytest.mark.asyncio
async def test_sync_klines_uses_normalized_store(tmp_path, monkeypatch) -> None:
    meta_dir = tmp_path / "meta"
    state_dir = tmp_path / "state"
    meta_dir.mkdir()
    (meta_dir / "usdm_symbols_current.json").write_text(
        json.dumps(
            {
                "snapshot_time": "2024-07-01T00:00:00+00:00",
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "base_asset": "BTC",
                        "quote_asset": "USDT",
                        "contract_type": "PERPETUAL",
                        "status": "TRADING",
                        "onboard_date": 1,
                        "delivery_date": 2,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    stores = []

    async def fake_sync_missing_klines(**kwargs):
        stores.append(kwargs["store"])
        return SyncResult(symbols_seen=1, klines_saved=0)

    class NullClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    monkeypatch.setattr(cli, "sync_missing_klines", fake_sync_missing_klines)
    monkeypatch.setattr(cli, "BinanceFuturesClient", NullClient)

    await cli._sync_klines(
        interval="1m",
        bootstrap_days=1,
        limit=499,
        data_dir=tmp_path / "raw",
        silver_dir=tmp_path / "silver",
        quarantine_dir=tmp_path / "quarantine",
        gap_report_path=tmp_path / "gap_report.parquet",
        meta_dir=meta_dir,
        state_dir=state_dir,
        symbols=None,
        concurrency=1,
        max_weight_per_minute=900,
        show_progress=False,
        use_archives=False,
        archive_threshold_days=2,
        archive_cache_dir=tmp_path / "cache",
        archive_concurrency=1,
    )

    assert isinstance(stores[0], NormalizedKlineStore)


@pytest.mark.asyncio
async def test_stream_klines_wires_progress_callbacks(tmp_path, monkeypatch) -> None:
    meta_dir = tmp_path / "meta"
    state_dir = tmp_path / "state"
    data_dir = tmp_path / "raw"
    silver_dir = tmp_path / "silver"
    realtime_dir = tmp_path / "realtime"
    meta_dir.mkdir()
    (meta_dir / "usdm_symbols_current.json").write_text(
        json.dumps(
            {
                "snapshot_time": "2024-07-01T00:00:00+00:00",
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "base_asset": "BTC",
                        "quote_asset": "USDT",
                        "contract_type": "PERPETUAL",
                        "status": "TRADING",
                        "onboard_date": 1,
                        "delivery_date": 2,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    progress_events = []

    async def fake_stream_closed_klines(**kwargs):
        kwargs["progress_callback"](
            StreamUpdate(symbol="BTCUSDT", open_time_ms=1719792000000, kline_saved=True)
        )
        return StreamResult(connections_seen=1, klines_saved=1)

    async def fake_sync_missing_klines(**kwargs):
        kwargs["progress_callback"](
            cli.ProgressUpdate(symbol="BTCUSDT", klines_saved=3, request_completed=True)
        )

    class NullClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    monkeypatch.setattr(cli, "stream_closed_klines", fake_stream_closed_klines)
    monkeypatch.setattr(cli, "sync_missing_klines", fake_sync_missing_klines)
    monkeypatch.setattr(cli, "BinanceFuturesClient", NullClient)

    await cli._stream_klines(
        interval="1m",
        data_dir=data_dir,
        silver_dir=silver_dir,
        realtime_dir=realtime_dir,
        meta_dir=meta_dir,
        state_dir=state_dir,
        symbol=None,
        streams_per_connection=200,
        max_weight_per_minute=900,
        reconnect_delay_seconds=0,
        once=True,
        progress_callback=progress_events.append,
    )

    assert progress_events == [
        cli.StreamProgressEvent(
            kind="metadata",
            symbol="-",
            klines=0,
            requests=0,
            symbols=1,
        ),
        cli.StreamProgressEvent(
            kind="connection_batch",
            symbol="-",
            klines=0,
            requests=0,
            connections=1,
        ),
        cli.StreamProgressEvent(kind="ws_kline", symbol="BTCUSDT", klines=1, requests=0),
        cli.StreamProgressEvent(kind="rest_batch", symbol="BTCUSDT", klines=3, requests=1),
    ]
