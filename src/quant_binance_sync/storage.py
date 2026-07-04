from __future__ import annotations

import sqlite3
from collections import defaultdict
from dataclasses import asdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable

import polars as pl

from quant_binance_sync.models import Kline
from quant_binance_sync.normalizer import NormalizeResult, normalize_klines


KLINE_COLUMNS = [
    "symbol",
    "interval",
    "open_time_ms",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time_ms",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
]


@dataclass(frozen=True)
class BufferedKlineStoreStats:
    sql_buffer_klines: int
    sqlite_flushes: int
    pending_klines: int
    parquet_flushes: int
    flush_size: int


class ParquetKlineStore:
    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)

    def upsert_klines(self, klines: list[Kline]) -> None:
        partitions: dict[tuple[str, str, str], list[Kline]] = defaultdict(list)
        for kline in klines:
            partitions[(kline.interval, kline.symbol, kline.date)].append(kline)

        for (interval, symbol, date), partition_klines in partitions.items():
            path = self._partition_path(interval=interval, symbol=symbol, date=date)
            path.parent.mkdir(parents=True, exist_ok=True)

            incoming = pl.DataFrame([kline.to_record() for kline in partition_klines])
            if path.exists():
                existing = pl.read_parquet(path)
                frame = pl.concat([existing, incoming], how="vertical_relaxed")
            else:
                frame = incoming

            frame = frame.sort("open_time_ms").unique(
                subset=["symbol", "interval", "open_time_ms"],
                keep="last",
                maintain_order=True,
            )
            frame.write_parquet(path)

    def _partition_path(self, *, interval: str, symbol: str, date: str) -> Path:
        return self.root / f"interval={interval}" / f"symbol={symbol}" / f"date={date}" / "klines.parquet"


class DualKlineStore:
    def __init__(self, *, raw: ParquetKlineStore, silver: ParquetKlineStore) -> None:
        self.raw = raw
        self.silver = silver

    def upsert_klines(self, klines: list[Kline]) -> None:
        self.raw.upsert_klines(klines)
        self.silver.upsert_klines(klines)


class NormalizedKlineStore:
    def __init__(
        self,
        *,
        raw: ParquetKlineStore,
        silver: ParquetKlineStore,
        quarantine: ParquetKlineStore,
        gap_report_path: Path | str,
        interval_ms: int,
    ) -> None:
        self.raw = raw
        self.silver = silver
        self.quarantine = quarantine
        self.gap_report_path = Path(gap_report_path)
        self.interval_ms = interval_ms

    def upsert_klines(self, klines: list[Kline]) -> NormalizeResult:
        self.raw.upsert_klines(klines)
        result = normalize_klines(klines, interval_ms=self.interval_ms)
        if result.accepted:
            self.silver.upsert_klines(result.accepted)
        quarantined = [item.kline for item in result.rejected]
        quarantined.extend(item.incoming for item in result.conflicts)
        if quarantined:
            self.quarantine.upsert_klines(quarantined)
        if result.gaps:
            self._append_gap_report(result)
        return result

    def _append_gap_report(self, result: NormalizeResult) -> None:
        detected_at = datetime.now(tz=UTC)
        records = [
            {
                **asdict(gap),
                "detected_at": detected_at,
                "status": "open",
                "reason": "missing_kline",
            }
            for gap in result.gaps
        ]
        incoming = pl.DataFrame(records)
        self.gap_report_path.parent.mkdir(parents=True, exist_ok=True)
        if self.gap_report_path.exists():
            existing = pl.read_parquet(self.gap_report_path)
            frame = pl.concat([existing, incoming], how="vertical_relaxed")
        else:
            frame = incoming
        frame = frame.unique(
            subset=["symbol", "interval", "missing_open_time_ms"],
            keep="last",
            maintain_order=True,
        ).sort(["symbol", "interval", "missing_open_time_ms"])
        frame.write_parquet(self.gap_report_path)


class SqliteKlineHotStore:
    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def upsert_klines(self, klines: list[Kline]) -> None:
        if not klines:
            return
        connection = self._connect()
        try:
            connection.executemany(
                """
                INSERT INTO closed_klines (
                    symbol,
                    interval,
                    open_time_ms,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    close_time_ms,
                    quote_volume,
                    trade_count,
                    taker_buy_base_volume,
                    taker_buy_quote_volume
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, interval, open_time_ms) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume,
                    close_time_ms = excluded.close_time_ms,
                    quote_volume = excluded.quote_volume,
                    trade_count = excluded.trade_count,
                    taker_buy_base_volume = excluded.taker_buy_base_volume,
                    taker_buy_quote_volume = excluded.taker_buy_quote_volume
                """,
                [self._to_row(kline) for kline in klines],
            )
            connection.commit()
        finally:
            connection.close()

    def load_klines(
        self,
        *,
        interval: str,
        symbols: list[str] | None = None,
    ) -> list[Kline]:
        query = f"SELECT {', '.join(KLINE_COLUMNS)} FROM closed_klines WHERE interval = ?"
        params: list[str] = [interval]
        if symbols is not None:
            placeholders = ", ".join("?" for _ in symbols)
            query += f" AND symbol IN ({placeholders})"
            params.extend(symbols)
        query += " ORDER BY symbol, open_time_ms"
        connection = self._connect()
        try:
            rows = connection.execute(query, params).fetchall()
        finally:
            connection.close()
        return [self._from_row(row) for row in rows]

    def delete_klines(self, klines: list[Kline]) -> None:
        if not klines:
            return
        connection = self._connect()
        try:
            connection.executemany(
                """
                DELETE FROM closed_klines
                WHERE symbol = ? AND interval = ? AND open_time_ms = ?
                """,
                [
                    (kline.symbol, kline.interval, kline.open_time_ms)
                    for kline in klines
                ],
            )
            connection.commit()
        finally:
            connection.close()

    def _initialize(self) -> None:
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS closed_klines (
                    symbol TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    open_time_ms INTEGER NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL,
                    close_time_ms INTEGER NOT NULL,
                    quote_volume REAL NOT NULL,
                    trade_count INTEGER NOT NULL,
                    taker_buy_base_volume REAL NOT NULL,
                    taker_buy_quote_volume REAL NOT NULL,
                    PRIMARY KEY (symbol, interval, open_time_ms)
                )
                """
            )
            connection.commit()
        finally:
            connection.close()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path)

    def _to_row(
        self,
        kline: Kline,
    ) -> tuple[str, str, int, float, float, float, float, float, int, float, int, float, float]:
        return (
            kline.symbol,
            kline.interval,
            kline.open_time_ms,
            kline.open,
            kline.high,
            kline.low,
            kline.close,
            kline.volume,
            kline.close_time_ms,
            kline.quote_volume,
            kline.trade_count,
            kline.taker_buy_base_volume,
            kline.taker_buy_quote_volume,
        )

    def _from_row(self, row: tuple) -> Kline:
        open_time_ms = int(row[2])
        return Kline(
            symbol=str(row[0]),
            interval=str(row[1]),
            open_time=datetime.fromtimestamp(open_time_ms / 1000, tz=UTC),
            open_time_ms=open_time_ms,
            open=float(row[3]),
            high=float(row[4]),
            low=float(row[5]),
            close=float(row[6]),
            volume=float(row[7]),
            close_time_ms=int(row[8]),
            quote_volume=float(row[9]),
            trade_count=int(row[10]),
            taker_buy_base_volume=float(row[11]),
            taker_buy_quote_volume=float(row[12]),
        )


class BufferedKlineStore:
    def __init__(
        self,
        *,
        hot: SqliteKlineHotStore,
        cold,
        flush_size: int,
        hot_flush_size: int = 100,
        stats_callback: Callable[[BufferedKlineStoreStats], None] | None = None,
    ) -> None:
        self.hot = hot
        self.cold = cold
        self.flush_size = flush_size
        self.hot_flush_size = hot_flush_size
        self.stats_callback = stats_callback
        self._sql_buffer: list[Kline] = []
        self._pending: list[Kline] = []
        self._sqlite_flushes = 0
        self._parquet_flushes = 0

    def upsert_klines(self, klines: list[Kline]) -> NormalizeResult | None:
        self._sql_buffer.extend(klines)
        if len(self._sql_buffer) >= self.hot_flush_size:
            self.flush_hot()
        if len(self._pending) >= self.flush_size:
            return self.flush()
        self._emit_stats()
        return None

    def flush_hot(self) -> None:
        if not self._sql_buffer:
            return
        buffered = self._sql_buffer
        self._sql_buffer = []
        self.hot.upsert_klines(buffered)
        self._pending.extend(buffered)
        self._sqlite_flushes += 1
        self._emit_stats()

    def flush(self) -> NormalizeResult | None:
        self.flush_hot()
        if not self._pending:
            return None
        pending = self._pending
        self._pending = []
        result = self.cold.upsert_klines(pending)
        self.hot.delete_klines(pending)
        self._parquet_flushes += 1
        self._emit_stats()
        return result

    def _emit_stats(self) -> None:
        if self.stats_callback is None:
            return
        self.stats_callback(self.stats())

    def stats(self) -> BufferedKlineStoreStats:
        return BufferedKlineStoreStats(
            sql_buffer_klines=len(self._sql_buffer),
            sqlite_flushes=self._sqlite_flushes,
            pending_klines=len(self._pending),
            parquet_flushes=self._parquet_flushes,
            flush_size=self.flush_size,
        )


class LatestOpenKlineStore:
    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)

    def upsert_open_kline(self, kline: Kline) -> None:
        path = self._path(interval=kline.interval, symbol=kline.symbol)
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame([kline.to_record()]).write_parquet(path)

    def _path(self, *, interval: str, symbol: str) -> Path:
        return self.root / f"interval={interval}" / f"symbol={symbol}" / "open_kline.parquet"


class InMemoryOpenKlineStore:
    def __init__(self) -> None:
        self.latest: dict[tuple[str, str], Kline] = {}

    def upsert_open_kline(self, kline: Kline) -> None:
        self.latest[(kline.symbol, kline.interval)] = kline
