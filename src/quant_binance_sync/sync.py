from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Protocol

from quant_binance_sync.archive import ArchiveMissing, ArchiveTransientError
from quant_binance_sync.client import BinanceFuturesClient
from quant_binance_sync.checkpoints import Checkpoint
from quant_binance_sync.models import Kline, active_usdm_perpetual_symbols, parse_kline


class KlineStore(Protocol):
    def upsert_klines(self, klines: list[Kline]) -> None: ...


class ArchiveKlineClient(Protocol):
    async def daily_klines(self, *, symbol: str, interval: str, day: date) -> list[Kline]: ...


@dataclass(frozen=True)
class SyncResult:
    symbols_seen: int
    klines_saved: int


@dataclass(frozen=True)
class SyncPlan:
    total_symbols: int
    total_batches: int


@dataclass(frozen=True)
class ProgressUpdate:
    symbol: str
    klines_saved: int = 0
    request_completed: bool = False
    symbol_completed: bool = False


def estimate_sync_plan(
    *,
    symbols: list[str],
    checkpoints: dict[str, Checkpoint],
    interval: str,
    bootstrap_days: int,
    limit: int,
    now: datetime | None = None,
    use_archives: bool,
    archive_threshold_days: int,
) -> SyncPlan:
    current_time = now or datetime.now(tz=UTC)
    interval_ms = interval_to_milliseconds(interval)
    end_time_ms = latest_closed_open_time_ms(current_time, interval_ms)
    archive_cutoff = current_time.date() - timedelta(days=archive_threshold_days)
    total_batches = 0

    for symbol in symbols:
        checkpoint = checkpoints.get(checkpoint_key(symbol, interval))
        if checkpoint and checkpoint.last_open_time_ms is not None:
            start_time_ms = checkpoint.last_open_time_ms + interval_ms
        else:
            start_time_ms = int((current_time - timedelta(days=bootstrap_days)).timestamp() * 1000)

        if start_time_ms > end_time_ms:
            continue

        rest_start_ms = start_time_ms
        if use_archives:
            archive_days = estimate_archive_days(
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
                archive_cutoff=archive_cutoff,
            )
            total_batches += archive_days
            if archive_days:
                first_day = datetime.fromtimestamp(start_time_ms / 1000, tz=UTC).date()
                rest_start_ms = utc_day_start_ms(first_day + timedelta(days=archive_days))

        if rest_start_ms <= end_time_ms:
            total_batches += estimate_rest_batches(
                start_time_ms=rest_start_ms,
                end_time_ms=end_time_ms,
                interval_ms=interval_ms,
                limit=limit,
            )

    return SyncPlan(total_symbols=len(symbols), total_batches=total_batches)


async def sync_recent_closed_klines(
    *,
    client: BinanceFuturesClient,
    store: KlineStore,
    interval: str,
    limit: int,
    symbols: list[str] | None = None,
    concurrency: int = 8,
    now: datetime | None = None,
    progress_callback: Callable[[ProgressUpdate], None] | None = None,
) -> SyncResult:
    current_time = now or datetime.now(tz=UTC)
    current_time_ms = int(current_time.timestamp() * 1000)

    selected_symbols = symbols
    if selected_symbols is None:
        selected_symbols = active_usdm_perpetual_symbols(await client.exchange_info())

    semaphore = asyncio.Semaphore(concurrency)

    async def fetch_symbol(symbol: str) -> list[Kline]:
        async with semaphore:
            rows = await client.klines(symbol=symbol, interval=interval, limit=limit)
        klines = [
            parse_kline(symbol, interval, row)
            for row in rows
            if int(row[6]) < current_time_ms
        ]
        if progress_callback is not None:
            progress_callback(
                ProgressUpdate(symbol=symbol, klines_saved=len(klines), request_completed=True)
            )
            progress_callback(ProgressUpdate(symbol=symbol, symbol_completed=True))
        return klines

    batches = await asyncio.gather(*(fetch_symbol(symbol) for symbol in selected_symbols))
    klines = [kline for batch in batches for kline in batch]
    if klines:
        store.upsert_klines(klines)
    return SyncResult(symbols_seen=len(selected_symbols), klines_saved=len(klines))


async def sync_missing_klines(
    *,
    client: BinanceFuturesClient,
    store: KlineStore,
    checkpoints: dict[str, Checkpoint],
    symbols: list[str],
    interval: str,
    bootstrap_days: int,
    limit: int,
    concurrency: int = 8,
    now: datetime | None = None,
    progress_callback: Callable[[ProgressUpdate], None] | None = None,
    archive_client: ArchiveKlineClient | None = None,
    archive_threshold_days: int = 2,
    archive_concurrency: int = 4,
    checkpoint_callback: Callable[[dict[str, Checkpoint]], None] | None = None,
) -> SyncResult:
    current_time = now or datetime.now(tz=UTC)
    interval_ms = interval_to_milliseconds(interval)
    end_time_ms = latest_closed_open_time_ms(current_time, interval_ms)
    semaphore = asyncio.Semaphore(concurrency)
    archive_semaphore = asyncio.Semaphore(archive_concurrency)

    def persist_batch(key: str, klines: list[Kline]) -> int:
        if not klines:
            return 0
        store.upsert_klines(klines)
        checkpoints[key] = Checkpoint(
            last_open_time_ms=max(kline.open_time_ms for kline in klines),
            status="active",
        )
        if checkpoint_callback is not None:
            checkpoint_callback(checkpoints)
        return len(klines)

    async def sync_symbol(symbol: str) -> int:
        key = checkpoint_key(symbol, interval)
        checkpoint = checkpoints.get(key)
        if checkpoint and checkpoint.last_open_time_ms is not None:
            start_time_ms = checkpoint.last_open_time_ms + interval_ms
        else:
            start_time_ms = int((current_time - timedelta(days=bootstrap_days)).timestamp() * 1000)

        if start_time_ms > end_time_ms:
            checkpoints[key] = Checkpoint(
                last_open_time_ms=checkpoint.last_open_time_ms if checkpoint else None,
                status="active",
            )
            if progress_callback is not None:
                progress_callback(ProgressUpdate(symbol=symbol, symbol_completed=True))
            if checkpoint_callback is not None:
                checkpoint_callback(checkpoints)
            return 0

        klines_saved = 0
        cursor = start_time_ms

        if archive_client is not None:
            archive_cutoff = current_time.date() - timedelta(days=archive_threshold_days)
            archive_saved, cursor = await sync_archive_days(
                archive_client=archive_client,
                persist_batch=lambda batch: persist_batch(key, batch),
                symbol=symbol,
                interval=interval,
                start_time_ms=cursor,
                end_time_ms=end_time_ms,
                archive_cutoff=archive_cutoff,
                archive_semaphore=archive_semaphore,
                progress_callback=progress_callback,
            )
            klines_saved += archive_saved

        rest_saved = await fetch_rest_klines(
            client=client,
            semaphore=semaphore,
            persist_batch=lambda batch: persist_batch(key, batch),
            symbol=symbol,
            interval=interval,
            limit=limit,
            start_time_ms=cursor,
            end_time_ms=end_time_ms,
            interval_ms=interval_ms,
            progress_callback=progress_callback,
        )
        klines_saved += rest_saved

        if progress_callback is not None:
            progress_callback(ProgressUpdate(symbol=symbol, symbol_completed=True))
        if checkpoint_callback is not None:
            checkpoint_callback(checkpoints)
        return klines_saved

    symbol_results: list[int] = []
    for start in range(0, len(symbols), concurrency):
        batch_symbols = symbols[start : start + concurrency]
        symbol_results.extend(await asyncio.gather(*(sync_symbol(symbol) for symbol in batch_symbols)))
    return SyncResult(symbols_seen=len(symbols), klines_saved=sum(symbol_results))


async def fetch_rest_klines(
    *,
    client: BinanceFuturesClient,
    semaphore: asyncio.Semaphore,
    persist_batch: Callable[[list[Kline]], int],
    symbol: str,
    interval: str,
    limit: int,
    start_time_ms: int,
    end_time_ms: int,
    interval_ms: int,
    progress_callback: Callable[[ProgressUpdate], None] | None,
) -> int:
    klines_saved = 0
    cursor = start_time_ms
    while cursor <= end_time_ms:
            async with semaphore:
                rows = await client.klines(
                    symbol=symbol,
                    interval=interval,
                    limit=limit,
                    start_time_ms=cursor,
                    end_time_ms=end_time_ms,
                )
            if not rows:
                break

            batch = [parse_kline(symbol, interval, row) for row in rows if int(row[0]) <= end_time_ms]
            saved = persist_batch(batch)
            klines_saved += saved
            if progress_callback is not None:
                progress_callback(
                    ProgressUpdate(
                        symbol=symbol,
                        klines_saved=saved,
                        request_completed=True,
                    )
                )
            last_open_time_ms = max(kline.open_time_ms for kline in batch)

            next_cursor = last_open_time_ms + interval_ms
            if next_cursor <= cursor:
                break
            cursor = next_cursor
            if len(rows) < limit:
                break

    return klines_saved


async def sync_archive_days(
    *,
    archive_client: ArchiveKlineClient,
    persist_batch: Callable[[list[Kline]], int],
    symbol: str,
    interval: str,
    start_time_ms: int,
    end_time_ms: int,
    archive_cutoff: date,
    archive_semaphore: asyncio.Semaphore,
    progress_callback: Callable[[ProgressUpdate], None] | None,
) -> tuple[int, int]:
    start_day = datetime.fromtimestamp(start_time_ms / 1000, tz=UTC).date()
    cursor = start_time_ms
    klines_saved = 0

    day = start_day
    while day <= archive_cutoff:
        day_start_ms = utc_day_start_ms(day)
        day_end_ms = utc_day_start_ms(day + timedelta(days=1)) - 1
        if day_start_ms > end_time_ms:
            break

        try:
            async with archive_semaphore:
                batch = await archive_client.daily_klines(symbol=symbol, interval=interval, day=day)
        except (ArchiveMissing, *ArchiveTransientError):
            break

        batch = [
            kline
            for kline in batch
            if start_time_ms <= kline.open_time_ms <= end_time_ms
        ]
        saved = persist_batch(batch)
        klines_saved += saved
        if progress_callback is not None:
            progress_callback(
                ProgressUpdate(symbol=symbol, klines_saved=saved, request_completed=True)
            )
        cursor = day_end_ms + 1
        day = day + timedelta(days=1)

    return klines_saved, cursor


def checkpoint_key(symbol: str, interval: str) -> str:
    return f"{symbol}|{interval}"


def latest_closed_open_time_ms(now: datetime, interval_ms: int) -> int:
    now_ms = int(now.timestamp() * 1000)
    return (now_ms // interval_ms) * interval_ms - interval_ms


def utc_day_start_ms(day: date) -> int:
    return int(datetime.combine(day, time.min, tzinfo=UTC).timestamp() * 1000)


def estimate_archive_days(
    *,
    start_time_ms: int,
    end_time_ms: int,
    archive_cutoff: date,
) -> int:
    start_day = datetime.fromtimestamp(start_time_ms / 1000, tz=UTC).date()
    count = 0
    day = start_day
    while day <= archive_cutoff:
        if utc_day_start_ms(day) > end_time_ms:
            break
        count += 1
        day = day + timedelta(days=1)
    return count


def estimate_rest_batches(
    *,
    start_time_ms: int,
    end_time_ms: int,
    interval_ms: int,
    limit: int,
) -> int:
    missing_candles = ((end_time_ms - start_time_ms) // interval_ms) + 1
    return max(0, (missing_candles + limit - 1) // limit)


def interval_to_milliseconds(interval: str) -> int:
    unit = interval[-1]
    value = int(interval[:-1])
    multipliers = {
        "m": 60_000,
        "h": 3_600_000,
        "d": 86_400_000,
        "w": 604_800_000,
    }
    if unit not in multipliers:
        raise ValueError(f"Unsupported interval: {interval}")
    return value * multipliers[unit]
