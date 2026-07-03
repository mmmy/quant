import json

import pytest

from quant_binance_sync import cli
from quant_binance_sync.checkpoints import Checkpoint
from quant_binance_sync.stream import StreamResult, StreamUpdate


@pytest.mark.asyncio
async def test_stream_klines_loads_symbols_and_wires_gap_sync(tmp_path, monkeypatch) -> None:
    meta_dir = tmp_path / "meta"
    state_dir = tmp_path / "state"
    data_dir = tmp_path / "raw"
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
    assert stream_calls[1]["gap_symbols"] == ["BTCUSDT"]

    await gap_sync(["BTCUSDT"])

    checkpoint_payload = json.loads(
        (state_dir / "usdm_kline_checkpoints.json").read_text(encoding="utf-8")
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
    (state_dir / "usdm_kline_checkpoints.json").write_text(
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


@pytest.mark.asyncio
async def test_stream_klines_wires_progress_callbacks(tmp_path, monkeypatch) -> None:
    meta_dir = tmp_path / "meta"
    state_dir = tmp_path / "state"
    data_dir = tmp_path / "raw"
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
