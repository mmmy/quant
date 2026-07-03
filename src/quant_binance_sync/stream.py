from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol, TypedDict

from quant_binance_sync.checkpoints import Checkpoint
from quant_binance_sync.models import Kline
from quant_binance_sync.sync import checkpoint_key

DEFAULT_STREAM_BASE_URL = "wss://fstream.binance.com/market"


class KlineStore(Protocol):
    def upsert_klines(self, klines: list[Kline]) -> None: ...


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
    data = message.get("data", {})
    if data.get("e") != "kline":
        return None

    kline = data.get("k", {})
    if not kline.get("x"):
        return None

    symbol = str(data.get("s") or kline["s"])
    interval = str(kline["i"])
    open_time_ms = int(kline["t"])
    return Kline(
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
        try:
            async for message in source(url):
                kline = parse_closed_kline_message(message)
                if kline is None:
                    continue
                store.upsert_klines([kline])
                checkpoints[checkpoint_key(kline.symbol, kline.interval)] = Checkpoint(
                    last_open_time_ms=kline.open_time_ms,
                    status="active",
                )
                if checkpoint_callback is not None:
                    checkpoint_callback(checkpoints)
                if progress_callback is not None:
                    progress_callback(
                        StreamUpdate(
                            symbol=kline.symbol,
                            open_time_ms=kline.open_time_ms,
                            kline_saved=True,
                        )
                    )
                saved += 1
        finally:
            if gap_sync_callback is not None:
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
