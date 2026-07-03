from __future__ import annotations

import csv
import asyncio
from datetime import date
from io import BytesIO, StringIO
from pathlib import Path
from collections.abc import Awaitable, Callable
from zipfile import ZipFile

import httpx

from quant_binance_sync.models import Kline, parse_kline

BINANCE_VISION_BASE_URL = "https://data.binance.vision"


class ArchiveMissing(Exception):
    pass


ArchiveTransientError = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.PoolTimeout,
    httpx.RemoteProtocolError,
    httpx.ReadError,
    httpx.WriteError,
)


def daily_kline_url(symbol: str, interval: str, day: date) -> str:
    day_text = day.isoformat()
    return (
        f"{BINANCE_VISION_BASE_URL}/data/futures/um/daily/klines/"
        f"{symbol}/{interval}/{symbol}-{interval}-{day_text}.zip"
    )


def parse_daily_kline_zip(symbol: str, interval: str, payload: bytes) -> list[Kline]:
    with ZipFile(BytesIO(payload)) as archive:
        csv_name = next(name for name in archive.namelist() if name.endswith(".csv"))
        csv_text = archive.read(csv_name).decode("utf-8")

    rows = []
    reader = csv.reader(StringIO(csv_text))
    for row in reader:
        if not row:
            continue
        if row[0] == "open_time":
            continue
        rows.append(parse_kline(symbol, interval, row))
    return rows


class BinanceVisionArchiveClient:
    def __init__(
        self,
        *,
        cache_dir: Path | str,
        base_url: str = BINANCE_VISION_BASE_URL,
        timeout: float = 30.0,
        max_retries: int = 3,
        sleep: Callable[[float], Awaitable[None] | None] | None = None,
    ) -> None:
        self.cache_dir = Path(cache_dir)
        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(timeout=timeout)
        self._max_retries = max_retries
        self._sleep = sleep

    async def __aenter__(self) -> BinanceVisionArchiveClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def daily_klines(self, *, symbol: str, interval: str, day: date) -> list[Kline]:
        path = self._cache_path(symbol=symbol, interval=interval, day=day)
        if path.exists():
            payload = path.read_bytes()
        else:
            url = daily_kline_url(symbol, interval, day).replace(BINANCE_VISION_BASE_URL, self.base_url)
            response = await self._get_with_retries(url)
            if response.status_code == 404:
                raise ArchiveMissing(f"archive not found: {url}")
            response.raise_for_status()
            payload = response.content
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
        return parse_daily_kline_zip(symbol, interval, payload)

    def _cache_path(self, *, symbol: str, interval: str, day: date) -> Path:
        return self.cache_dir / "futures" / "um" / "daily" / "klines" / symbol / interval / (
            f"{symbol}-{interval}-{day.isoformat()}.zip"
        )

    async def _get_with_retries(self, url: str) -> httpx.Response:
        for attempt in range(self._max_retries + 1):
            try:
                return await self._client.get(url)
            except ArchiveTransientError:
                if attempt >= self._max_retries:
                    raise
                await self._sleep_for(min(30.0, 2.0**attempt))
        raise RuntimeError("unreachable archive retry state")

    async def _sleep_for(self, seconds: float) -> None:
        if self._sleep is None:
            await asyncio.sleep(seconds)
            return
        result = self._sleep(seconds)
        if result is not None:
            await result
