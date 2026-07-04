from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol, TypedDict

from quant_binance_sync.checkpoints import Checkpoint
from quant_binance_sync.models import Kline
from quant_binance_sync.normalizer import NormalizeResult
from quant_binance_sync.sync import checkpoint_key

DEFAULT_STREAM_BASE_URL = "wss://fstream.binance.com/market"


class KlineStore(Protocol):
    def upsert_klines(self, klines: list[Kline]) -> NormalizeResult | None: ...


class OpenKlineStore(Protocol):
    def upsert_open_kline(self, kline: Kline) -> None: ...


class StreamMessage(TypedDict, total=False):
    stream: str
    data: dict[str, Any]


MessageSource = Callable[[str], AsyncIterator[StreamMessage]]
CheckpointCallback = Callable[[dict[str, Checkpoint]], None]
GapSyncCallback = Callable[[list[str]], Awaitable[None] | None]


@dataclass(frozen=True)
class StreamResult:
    connections_seen: int
    klines_saved: int


@dataclass(frozen=True)
class StreamUpdate:
    symbol: str
    open_time_ms: int
    kline_saved: bool = False


def build_combined_stream_urls(
    *,
    symbols: list[str],
    interval: str,
    streams_per_connection: int = 200,
    base_url: str = DEFAULT_STREAM_BASE_URL,
) -> list[str]:
    if streams_per_connection < 1:
        raise ValueError("streams_per_connection must be at least 1")

    urls = []
    for start in range(0, len(symbols), streams_per_connection):
        chunk = symbols[start : start + streams_per_connection]
        streams = "/".join(f"{symbol.lower()}@kline_{interval}" for symbol in chunk)
        urls.append(f"{base_url.rstrip('/')}/stream?streams={streams}")
    return urls


def parse_closed_kline_message(message: StreamMessage) -> Kline | None:
    parsed = parse_kline_message(message)
    if parsed is None:
        return None

    kline, is_closed = parsed
    if not is_closed:
        return None
    return kline


def parse_kline_message(message: StreamMessage) -> tuple[Kline, bool] | None:
    data = message.get("data", {})
    if data.get("e") != "kline":
        return None

    kline = data.get("k", {})
    symbol = str(data.get("s") or kline["s"])
    interval = str(kline["i"])
    open_time_ms = int(kline["t"])
    return (
        Kline(
            symbol=symbol,
            interval=interval,
            open_time=datetime.fromtimestamp(open_time_ms / 1000, tz=UTC),
            open_time_ms=open_time_ms,
            open=float(kline["o"]),
            high=float(kline["h"]),
            low=float(kline["l"]),
            close=float(kline["c"]),
            volume=float(kline["v"]),
            close_time_ms=int(kline["T"]),
            quote_volume=float(kline["q"]),
            trade_count=int(kline["n"]),
            taker_buy_base_volume=float(kline["V"]),
            taker_buy_quote_volume=float(kline["Q"]),
        ),
        bool(kline.get("x")),
    )


async def stream_closed_klines(
    *,
    symbols: list[str],
    interval: str,
    store: KlineStore,
    checkpoints: dict[str, Checkpoint],
    message_source: MessageSource | None = None,
    checkpoint_callback: CheckpointCallback | None = None,
    gap_sync_callback: GapSyncCallback | None = None,
    progress_callback: Callable[[StreamUpdate], None] | None = None,
    open_kline_store: OpenKlineStore | None = None,
    streams_per_connection: int = 200,
    base_url: str = DEFAULT_STREAM_BASE_URL,
) -> StreamResult:
    source = message_source or websocket_message_source
    chunks = [
        symbols[start : start + streams_per_connection]
        for start in range(0, len(symbols), streams_per_connection)
    ]
    urls = build_combined_stream_urls(
        symbols=symbols,
        interval=interval,
        streams_per_connection=streams_per_connection,
        base_url=base_url,
    )

    async def consume_connection(url: str, chunk_symbols: list[str]) -> int:
        saved = 0
        cancelled = False
        try:
            async for message in source(url):
                parsed = parse_kline_message(message)
                if parsed is None:
                    continue
                kline, is_closed = parsed
                if not is_closed:
                    if open_kline_store is not None:
                        open_kline_store.upsert_open_kline(kline)
                    continue
                store_result = store.upsert_klines([kline])
                checkpoint_open_time_ms = stream_checkpoint_open_time_ms(kline, store_result)
                saved_count = stream_saved_count(store_result)
                if checkpoint_open_time_ms is not None:
                    checkpoints[checkpoint_key(kline.symbol, kline.interval)] = Checkpoint(
                        last_open_time_ms=checkpoint_open_time_ms,
                        status="active",
                    )
                    if checkpoint_callback is not None:
                        checkpoint_callback(checkpoints)
                if saved_count:
                    if progress_callback is not None:
                        progress_callback(
                            StreamUpdate(
                                symbol=kline.symbol,
                                open_time_ms=checkpoint_open_time_ms or kline.open_time_ms,
                                kline_saved=True,
                            )
                        )
                    saved += saved_count
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            if gap_sync_callback is not None and not cancelled:
                result = gap_sync_callback(chunk_symbols)
                if result is not None:
                    await result
        return saved

    results = await asyncio.gather(
        *(consume_connection(url, chunk) for url, chunk in zip(urls, chunks, strict=True))
    )
    return StreamResult(connections_seen=len(urls), klines_saved=sum(results))


async def websocket_message_source(url: str) -> AsyncIterator[StreamMessage]:
    import websockets

    async with websockets.connect(url, ping_interval=20, ping_timeout=20) as websocket:
        async for raw_message in websocket:
            yield json.loads(raw_message)


def stream_checkpoint_open_time_ms(
    kline: Kline,
    store_result: NormalizeResult | None,
) -> int | None:
    if store_result is None:
        return kline.open_time_ms
    return store_result.contiguous_last_open_time_ms


def stream_saved_count(store_result: NormalizeResult | None) -> int:
    if store_result is None:
        return 1
    return store_result.accepted_count
