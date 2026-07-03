from datetime import UTC, datetime

import pytest

from quant_binance_sync.checkpoints import Checkpoint
from quant_binance_sync.normalizer import NormalizeResult
from quant_binance_sync.stream import (
    StreamMessage,
    StreamUpdate,
    build_combined_stream_urls,
    parse_closed_kline_message,
    stream_closed_klines,
)


class MemoryStore:
    def __init__(self) -> None:
        self.saved = []

    def upsert_klines(self, klines) -> None:
        self.saved.extend(klines)


class MemoryOpenKlineStore:
    def __init__(self) -> None:
        self.saved = []

    def upsert_open_kline(self, kline) -> None:
        self.saved.append(kline)


class NormalizingMemoryStore(MemoryStore):
    def __init__(self, result: NormalizeResult) -> None:
        super().__init__()
        self.result = result

    def upsert_klines(self, klines):
        super().upsert_klines(klines)
        return self.result


class MessageSource:
    def __init__(self, batches):
        self.batches = batches
        self.urls = []

    async def __call__(self, url: str):
        self.urls.append(url)
        for message in self.batches.pop(0):
            yield message


def closed_message(symbol: str = "BTCUSDT", open_time_ms: int = 1719792000000) -> StreamMessage:
    return {
        "stream": f"{symbol.lower()}@kline_1m",
        "data": {
            "e": "kline",
            "s": symbol,
            "k": {
                "t": open_time_ms,
                "T": open_time_ms + 59999,
                "s": symbol,
                "i": "1m",
                "o": "100",
                "c": "101",
                "h": "102",
                "l": "99",
                "v": "12.5",
                "n": 42,
                "x": True,
                "q": "1262.5",
                "V": "6",
                "Q": "606",
            },
        },
    }


def test_build_combined_stream_urls_chunks_symbols_and_lowercases_stream_names() -> None:
    urls = build_combined_stream_urls(
        symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
        interval="1m",
        streams_per_connection=2,
    )

    assert urls == [
        "wss://fstream.binance.com/market/stream?streams=btcusdt@kline_1m/ethusdt@kline_1m",
        "wss://fstream.binance.com/market/stream?streams=solusdt@kline_1m",
    ]


def test_parse_closed_kline_message_ignores_open_kline() -> None:
    message = closed_message()
    message["data"]["k"]["x"] = False

    assert parse_closed_kline_message(message) is None


def test_parse_closed_kline_message_returns_kline_for_closed_stream_event() -> None:
    kline = parse_closed_kline_message(closed_message())

    assert kline is not None
    assert kline.symbol == "BTCUSDT"
    assert kline.interval == "1m"
    assert kline.open_time == datetime(2024, 7, 1, 0, 0, tzinfo=UTC)
    assert kline.close == 101.0
    assert kline.trade_count == 42


@pytest.mark.asyncio
async def test_stream_closed_klines_saves_closed_events_and_updates_checkpoint() -> None:
    store = MemoryStore()
    checkpoints = {}
    saved_checkpoints = []
    source = MessageSource([[closed_message("BTCUSDT")]])

    result = await stream_closed_klines(
        symbols=["BTCUSDT"],
        interval="1m",
        store=store,
        checkpoints=checkpoints,
        message_source=source,
        checkpoint_callback=lambda current: saved_checkpoints.append(dict(current)),
    )

    assert result.klines_saved == 1
    assert result.connections_seen == 1
    assert store.saved[0].symbol == "BTCUSDT"
    assert checkpoints["BTCUSDT|1m"] == Checkpoint(
        last_open_time_ms=1719792000000,
        status="active",
    )
    assert saved_checkpoints[-1] == checkpoints


@pytest.mark.asyncio
async def test_stream_closed_klines_calls_gap_sync_after_disconnected_batch() -> None:
    store = MemoryStore()
    checkpoints = {}
    gap_sync_calls = []
    source = MessageSource([[closed_message("BTCUSDT")]])

    await stream_closed_klines(
        symbols=["BTCUSDT"],
        interval="1m",
        store=store,
        checkpoints=checkpoints,
        message_source=source,
        gap_sync_callback=lambda symbols: gap_sync_calls.append(list(symbols)),
    )

    assert gap_sync_calls == [["BTCUSDT"]]


@pytest.mark.asyncio
async def test_stream_closed_klines_reports_progress_for_closed_events() -> None:
    store = MemoryStore()
    updates = []

    await stream_closed_klines(
        symbols=["BTCUSDT"],
        interval="1m",
        store=store,
        checkpoints={},
        message_source=MessageSource([[closed_message("BTCUSDT")]]),
        progress_callback=updates.append,
    )

    assert updates == [
        StreamUpdate(
            symbol="BTCUSDT",
            open_time_ms=1719792000000,
            kline_saved=True,
        )
    ]


@pytest.mark.asyncio
async def test_stream_closed_klines_updates_open_cache_without_saving_unclosed_events() -> None:
    store = MemoryStore()
    open_store = MemoryOpenKlineStore()
    message = closed_message("BTCUSDT")
    message["data"]["k"]["x"] = False
    message["data"]["k"]["c"] = "100.5"

    result = await stream_closed_klines(
        symbols=["BTCUSDT"],
        interval="1m",
        store=store,
        checkpoints={},
        message_source=MessageSource([[message]]),
        open_kline_store=open_store,
    )

    assert result.klines_saved == 0
    assert store.saved == []
    assert open_store.saved[0].symbol == "BTCUSDT"
    assert open_store.saved[0].close == 100.5


@pytest.mark.asyncio
async def test_stream_closed_klines_checkpoint_uses_normalized_contiguous_last_open_time() -> None:
    message = closed_message("BTCUSDT")
    kline = parse_closed_kline_message(message)
    assert kline is not None
    store = NormalizingMemoryStore(
        NormalizeResult(
            accepted=[kline],
            rejected=[],
            conflicts=[],
            gaps=[],
            contiguous_last_open_time_ms=kline.open_time_ms - 60_000,
        )
    )
    checkpoints = {}

    await stream_closed_klines(
        symbols=["BTCUSDT"],
        interval="1m",
        store=store,
        checkpoints=checkpoints,
        message_source=MessageSource([[message]]),
    )

    assert checkpoints["BTCUSDT|1m"] == Checkpoint(
        last_open_time_ms=kline.open_time_ms - 60_000,
        status="active",
    )
