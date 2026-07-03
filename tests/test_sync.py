import asyncio
from datetime import UTC, datetime
from datetime import date

import httpx
import pytest
import respx

from quant_binance_sync.archive import ArchiveMissing
from quant_binance_sync.client import BinanceFuturesClient
from quant_binance_sync.checkpoints import Checkpoint
from quant_binance_sync.models import Kline
from quant_binance_sync.sync import ProgressUpdate
from quant_binance_sync.sync import estimate_sync_plan
from quant_binance_sync.sync import SyncResult, sync_recent_closed_klines
from quant_binance_sync.sync import sync_missing_klines


class MemoryStore:
    def __init__(self) -> None:
        self.saved = []

    def upsert_klines(self, klines) -> None:
        self.saved.extend(klines)


class KlineClient:
    def __init__(self):
        self.calls = []

    async def klines(
        self,
        *,
        symbol: str,
        interval: str,
        limit: int,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ):
        self.calls.append(
            {
                "symbol": symbol,
                "interval": interval,
                "limit": limit,
                "start_time_ms": start_time_ms,
                "end_time_ms": end_time_ms,
            }
        )
        return [
            [
                start_time_ms,
                "100",
                "101",
                "99",
                "100.5",
                "1",
                start_time_ms + 59999,
                "100.5",
                7,
                "0.5",
                "50.25",
                "0",
            ]
        ]


class PartiallyFailingKlineClient(KlineClient):
    async def klines(
        self,
        *,
        symbol: str,
        interval: str,
        limit: int,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ):
        if symbol == "ETHUSDT":
            raise httpx.ConnectError("temporary failure")
        return await super().klines(
            symbol=symbol,
            interval=interval,
            limit=limit,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )


class ArchiveClient:
    def __init__(self, missing: set[date] | None = None):
        self.calls = []
        self.missing = missing or set()

    async def daily_klines(self, *, symbol: str, interval: str, day: date):
        self.calls.append({"symbol": symbol, "interval": interval, "day": day})
        if day in self.missing:
            raise ArchiveMissing("missing")
        open_time = datetime(day.year, day.month, day.day, 0, 1, tzinfo=UTC)
        open_time_ms = int(open_time.timestamp() * 1000)
        return [
            Kline(
                symbol=symbol,
                interval=interval,
                open_time=open_time,
                open_time_ms=open_time_ms,
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=1.0,
                close_time_ms=open_time_ms + 59999,
                quote_volume=100.5,
                trade_count=7,
                taker_buy_base_volume=0.5,
                taker_buy_quote_volume=50.25,
            )
        ]


class MultiDayArchiveClient(ArchiveClient):
    async def daily_klines(self, *, symbol: str, interval: str, day: date):
        self.calls.append({"symbol": symbol, "interval": interval, "day": day})
        open_time = datetime(day.year, day.month, day.day, 0, 1, tzinfo=UTC)
        open_time_ms = int(open_time.timestamp() * 1000)
        return [
            Kline(
                symbol=symbol,
                interval=interval,
                open_time=open_time,
                open_time_ms=open_time_ms,
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=1.0,
                close_time_ms=open_time_ms + 59999,
                quote_volume=100.5,
                trade_count=7,
                taker_buy_base_volume=0.5,
                taker_buy_quote_volume=50.25,
            )
        ]


class FailingArchiveClient:
    async def daily_klines(self, *, symbol: str, interval: str, day: date):
        raise httpx.ConnectError("temporary failure")


class ConcurrentArchiveClient(ArchiveClient):
    def __init__(self, release: asyncio.Event):
        super().__init__()
        self.release = release
        self.in_flight = 0
        self.max_in_flight = 0

    async def daily_klines(self, *, symbol: str, interval: str, day: date):
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        await self.release.wait()
        self.in_flight -= 1
        return await super().daily_klines(symbol=symbol, interval=interval, day=day)


class BatchTrackingStore(MemoryStore):
    def __init__(self) -> None:
        super().__init__()
        self.batch_sizes = []

    def upsert_klines(self, klines) -> None:
        self.batch_sizes.append(len(klines))
        super().upsert_klines(klines)


@pytest.mark.asyncio
@respx.mock
async def test_sync_fetches_active_symbols_and_skips_open_kline() -> None:
    respx.get("https://fapi.binance.com/fapi/v1/exchangeInfo").mock(
        return_value=httpx.Response(
            200,
            json={
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "contractType": "PERPETUAL",
                        "quoteAsset": "USDT",
                        "status": "TRADING",
                    }
                ]
            },
        )
    )
    respx.get("https://fapi.binance.com/fapi/v1/klines").mock(
        return_value=httpx.Response(
            200,
            json=[
                [
                    1719792000000,
                    "100",
                    "101",
                    "99",
                    "100.5",
                    "1",
                    1719792059999,
                    "100.5",
                    7,
                    "0.5",
                    "50.25",
                    "0",
                ],
                [
                    1719792060000,
                    "100.5",
                    "102",
                    "100",
                    "101.5",
                    "2",
                    1719792119999,
                    "203",
                    8,
                    "1",
                    "101.5",
                    "0",
                ],
            ],
        )
    )
    store = MemoryStore()

    async with BinanceFuturesClient() as client:
        result = await sync_recent_closed_klines(
            client=client,
            store=store,
            interval="1m",
            limit=2,
            now=datetime(2024, 7, 1, 0, 1, 30, tzinfo=UTC),
        )

    assert result == SyncResult(symbols_seen=1, klines_saved=1)
    assert [k.open_time_ms for k in store.saved] == [1719792000000]


@pytest.mark.asyncio
async def test_sync_missing_bootstraps_from_days_when_checkpoint_is_missing() -> None:
    client = KlineClient()
    store = MemoryStore()
    checkpoints = {}

    result = await sync_missing_klines(
        client=client,
        store=store,
        checkpoints=checkpoints,
        symbols=["BTCUSDT"],
        interval="1m",
        bootstrap_days=1,
        limit=1500,
        now=datetime(2024, 7, 2, 0, 0, 30, tzinfo=UTC),
    )

    assert result == SyncResult(symbols_seen=1, klines_saved=1)
    assert client.calls == [
        {
            "symbol": "BTCUSDT",
            "interval": "1m",
            "limit": 1500,
            "start_time_ms": 1719792030000,
            "end_time_ms": 1719878340000,
        }
    ]
    assert checkpoints["BTCUSDT|1m"] == Checkpoint(
        last_open_time_ms=1719792030000,
        status="active",
    )


@pytest.mark.asyncio
async def test_sync_missing_resumes_after_checkpoint() -> None:
    client = KlineClient()
    store = MemoryStore()
    checkpoints = {"BTCUSDT|1m": Checkpoint(last_open_time_ms=1719878340000, status="active")}

    await sync_missing_klines(
        client=client,
        store=store,
        checkpoints=checkpoints,
        symbols=["BTCUSDT"],
        interval="1m",
        bootstrap_days=1,
        limit=1500,
        now=datetime(2024, 7, 2, 0, 0, 30, tzinfo=UTC),
    )

    assert client.calls == []
    assert checkpoints["BTCUSDT|1m"] == Checkpoint(last_open_time_ms=1719878340000, status="active")


@pytest.mark.asyncio
async def test_sync_missing_reports_progress_for_batches_and_completed_symbols() -> None:
    client = KlineClient()
    store = MemoryStore()
    checkpoints = {}
    updates: list[ProgressUpdate] = []

    await sync_missing_klines(
        client=client,
        store=store,
        checkpoints=checkpoints,
        symbols=["BTCUSDT"],
        interval="1m",
        bootstrap_days=1,
        limit=1500,
        now=datetime(2024, 7, 2, 0, 0, 30, tzinfo=UTC),
        progress_callback=updates.append,
    )

    assert updates == [
        ProgressUpdate(symbol="BTCUSDT", klines_saved=1, request_completed=True),
        ProgressUpdate(symbol="BTCUSDT", symbol_completed=True),
    ]


@pytest.mark.asyncio
async def test_sync_missing_uses_archive_for_days_before_threshold_then_rest() -> None:
    client = KlineClient()
    archive_client = ArchiveClient()
    store = MemoryStore()
    checkpoints = {}

    result = await sync_missing_klines(
        client=client,
        store=store,
        checkpoints=checkpoints,
        symbols=["BTCUSDT"],
        interval="1m",
        bootstrap_days=2,
        limit=1500,
        now=datetime(2024, 7, 3, 0, 0, 30, tzinfo=UTC),
        archive_client=archive_client,
        archive_threshold_days=2,
    )

    assert result == SyncResult(symbols_seen=1, klines_saved=2)
    assert archive_client.calls == [{"symbol": "BTCUSDT", "interval": "1m", "day": date(2024, 7, 1)}]
    assert client.calls[0]["start_time_ms"] == 1719878400000


@pytest.mark.asyncio
async def test_sync_missing_falls_back_to_rest_when_archive_day_is_missing() -> None:
    client = KlineClient()
    archive_client = ArchiveClient(missing={date(2024, 7, 1)})
    store = MemoryStore()
    checkpoints = {}

    await sync_missing_klines(
        client=client,
        store=store,
        checkpoints=checkpoints,
        symbols=["BTCUSDT"],
        interval="1m",
        bootstrap_days=2,
        limit=1500,
        now=datetime(2024, 7, 3, 0, 0, 30, tzinfo=UTC),
        archive_client=archive_client,
        archive_threshold_days=2,
    )

    assert client.calls[0]["start_time_ms"] == 1719792030000


@pytest.mark.asyncio
async def test_sync_missing_falls_back_to_rest_when_archive_network_fails() -> None:
    client = KlineClient()
    store = MemoryStore()
    checkpoints = {}

    result = await sync_missing_klines(
        client=client,
        store=store,
        checkpoints=checkpoints,
        symbols=["BTCUSDT"],
        interval="1m",
        bootstrap_days=2,
        limit=1500,
        now=datetime(2024, 7, 3, 0, 0, 30, tzinfo=UTC),
        archive_client=FailingArchiveClient(),
        archive_threshold_days=2,
    )

    assert result == SyncResult(symbols_seen=1, klines_saved=1)
    assert client.calls[0]["start_time_ms"] == 1719792030000


@pytest.mark.asyncio
async def test_sync_missing_limits_archive_concurrency() -> None:
    client = KlineClient()
    store = MemoryStore()
    checkpoints = {}
    release = asyncio.Event()
    archive_client = ConcurrentArchiveClient(release)

    task = asyncio.create_task(
        sync_missing_klines(
            client=client,
            store=store,
            checkpoints=checkpoints,
            symbols=["BTCUSDT", "ETHUSDT"],
            interval="1m",
            bootstrap_days=2,
            limit=1500,
            now=datetime(2024, 7, 3, 0, 0, 30, tzinfo=UTC),
            archive_client=archive_client,
            archive_threshold_days=2,
            archive_concurrency=1,
        )
    )
    while archive_client.max_in_flight == 0:
        await asyncio.sleep(0)

    await asyncio.sleep(0)
    assert archive_client.max_in_flight == 1

    release.set()
    await task


@pytest.mark.asyncio
async def test_sync_missing_limits_symbol_concurrency_not_just_request_concurrency() -> None:
    client = KlineClient()
    store = MemoryStore()
    checkpoints = {}
    release = asyncio.Event()
    archive_client = ConcurrentArchiveClient(release)

    task = asyncio.create_task(
        sync_missing_klines(
            client=client,
            store=store,
            checkpoints=checkpoints,
            symbols=["BTCUSDT", "ETHUSDT"],
            interval="1m",
            bootstrap_days=2,
            limit=1500,
            now=datetime(2024, 7, 3, 0, 0, 30, tzinfo=UTC),
            archive_client=archive_client,
            archive_threshold_days=2,
            concurrency=1,
            archive_concurrency=10,
        )
    )
    while archive_client.max_in_flight == 0:
        await asyncio.sleep(0)

    await asyncio.sleep(0)
    assert archive_client.max_in_flight == 1

    release.set()
    await task


@pytest.mark.asyncio
async def test_sync_missing_persists_completed_symbol_before_later_symbol_fails() -> None:
    client = PartiallyFailingKlineClient()
    store = MemoryStore()
    checkpoints = {}
    saved_checkpoints = []

    with pytest.raises(httpx.ConnectError):
        await sync_missing_klines(
            client=client,
            store=store,
            checkpoints=checkpoints,
            symbols=["BTCUSDT", "ETHUSDT"],
            interval="1m",
            bootstrap_days=1,
            limit=1500,
            concurrency=1,
            now=datetime(2024, 7, 2, 0, 0, 30, tzinfo=UTC),
            checkpoint_callback=lambda current: saved_checkpoints.append(dict(current)),
        )

    assert [kline.symbol for kline in store.saved] == ["BTCUSDT"]
    assert saved_checkpoints[-1]["BTCUSDT|1m"] == Checkpoint(
        last_open_time_ms=1719792030000,
        status="active",
    )


@pytest.mark.asyncio
async def test_sync_missing_persists_each_archive_day_without_waiting_for_symbol_end() -> None:
    client = KlineClient()
    store = BatchTrackingStore()
    checkpoints = {}
    saved_checkpoints = []

    result = await sync_missing_klines(
        client=client,
        store=store,
        checkpoints=checkpoints,
        symbols=["BTCUSDT"],
        interval="1m",
        bootstrap_days=3,
        limit=1500,
        now=datetime(2024, 7, 4, 0, 0, 30, tzinfo=UTC),
        archive_client=MultiDayArchiveClient(),
        archive_threshold_days=2,
        checkpoint_callback=lambda current: saved_checkpoints.append(dict(current)),
    )

    assert result == SyncResult(symbols_seen=1, klines_saved=3)
    assert store.batch_sizes == [1, 1, 1]
    assert len(saved_checkpoints) >= 3


def test_estimate_sync_plan_counts_archive_days_and_rest_batches() -> None:
    plan = estimate_sync_plan(
        symbols=["BTCUSDT"],
        checkpoints={},
        interval="1m",
        bootstrap_days=3,
        limit=499,
        now=datetime(2024, 7, 4, 0, 0, 30, tzinfo=UTC),
        use_archives=True,
        archive_threshold_days=2,
    )

    assert plan.total_batches == 5
    assert plan.total_symbols == 1


def test_estimate_sync_plan_skips_completed_symbols() -> None:
    plan = estimate_sync_plan(
        symbols=["BTCUSDT"],
        checkpoints={"BTCUSDT|1m": Checkpoint(last_open_time_ms=1720051140000, status="active")},
        interval="1m",
        bootstrap_days=3,
        limit=499,
        now=datetime(2024, 7, 4, 0, 0, 30, tzinfo=UTC),
        use_archives=True,
        archive_threshold_days=2,
    )

    assert plan.total_batches == 0
