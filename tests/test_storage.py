from datetime import UTC, datetime

import polars as pl

from quant_binance_sync.models import Kline
from quant_binance_sync.storage import (
    BufferedKlineStore,
    NormalizedKlineStore,
    ParquetKlineStore,
    SqliteKlineHotStore,
)


def make_kline(symbol: str, close: float) -> Kline:
    return Kline(
        symbol=symbol,
        interval="1m",
        open_time=datetime(2024, 7, 1, 0, 0, tzinfo=UTC),
        open_time_ms=1719792000000,
        open=close - 1,
        high=close + 1,
        low=close - 2,
        close=close,
        volume=100.0,
        close_time_ms=1719792059999,
        quote_volume=1000.0,
        trade_count=10,
        taker_buy_base_volume=50.0,
        taker_buy_quote_volume=500.0,
    )


def test_store_partitions_by_interval_symbol_and_date_then_deduplicates(tmp_path) -> None:
    store = ParquetKlineStore(tmp_path)

    store.upsert_klines([make_kline("BTCUSDT", 100.0)])
    store.upsert_klines([make_kline("BTCUSDT", 101.0)])

    path = tmp_path / "interval=1m" / "symbol=BTCUSDT" / "date=2024-07-01" / "klines.parquet"
    frame = pl.read_parquet(path)

    assert frame.height == 1
    assert frame.item(0, "close") == 101.0


def test_normalized_store_writes_valid_klines_to_raw_and_silver(tmp_path) -> None:
    raw = ParquetKlineStore(tmp_path / "raw")
    silver = ParquetKlineStore(tmp_path / "silver")
    quarantine = ParquetKlineStore(tmp_path / "quarantine")
    gap_report = tmp_path / "gap_report.parquet"
    store = NormalizedKlineStore(
        raw=raw,
        silver=silver,
        quarantine=quarantine,
        gap_report_path=gap_report,
        interval_ms=60_000,
    )

    result = store.upsert_klines([make_kline("BTCUSDT", 100.0)])

    relative_path = "interval=1m/symbol=BTCUSDT/date=2024-07-01/klines.parquet"
    assert (tmp_path / "raw" / relative_path).exists()
    assert (tmp_path / "silver" / relative_path).exists()
    assert result.accepted_count == 1
    assert result.contiguous_last_open_time_ms == 1719792000000


def test_normalized_store_quarantines_invalid_klines_without_writing_silver(tmp_path) -> None:
    raw = ParquetKlineStore(tmp_path / "raw")
    silver = ParquetKlineStore(tmp_path / "silver")
    quarantine = ParquetKlineStore(tmp_path / "quarantine")
    store = NormalizedKlineStore(
        raw=raw,
        silver=silver,
        quarantine=quarantine,
        gap_report_path=tmp_path / "gap_report.parquet",
        interval_ms=60_000,
    )
    kline = make_kline("BTCUSDT", 100.0)
    invalid = Kline(**{**kline.__dict__, "close_time_ms": kline.open_time_ms + 60_000})

    result = store.upsert_klines([invalid])

    relative_path = "interval=1m/symbol=BTCUSDT/date=2024-07-01/klines.parquet"
    assert (tmp_path / "raw" / relative_path).exists()
    assert not (tmp_path / "silver" / relative_path).exists()
    assert (tmp_path / "quarantine" / relative_path).exists()
    assert result.accepted_count == 0
    assert result.rejected_count == 1


def test_normalized_store_writes_gap_report(tmp_path) -> None:
    raw = ParquetKlineStore(tmp_path / "raw")
    silver = ParquetKlineStore(tmp_path / "silver")
    quarantine = ParquetKlineStore(tmp_path / "quarantine")
    store = NormalizedKlineStore(
        raw=raw,
        silver=silver,
        quarantine=quarantine,
        gap_report_path=tmp_path / "gap_report.parquet",
        interval_ms=60_000,
    )
    first = make_kline("BTCUSDT", 100.0)
    third = Kline(
        **{
            **make_kline("BTCUSDT", 101.0).__dict__,
            "open_time": datetime(2024, 7, 1, 0, 2, tzinfo=UTC),
            "open_time_ms": 1719792120000,
            "close_time_ms": 1719792179999,
        }
    )

    result = store.upsert_klines([first, third])

    gap_frame = pl.read_parquet(tmp_path / "gap_report.parquet")
    assert gap_frame.height == 1
    assert gap_frame.item(0, "missing_open_time_ms") == 1719792060000
    assert result.gap_count == 1


def test_sqlite_hot_store_upserts_closed_klines_immediately(tmp_path) -> None:
    store = SqliteKlineHotStore(tmp_path / "realtime.db")
    first = make_kline("BTCUSDT", 100.0)
    replacement = make_kline("BTCUSDT", 101.0)

    store.upsert_klines([first])
    store.upsert_klines([replacement])

    rows = store.load_klines(interval="1m", symbols=["BTCUSDT"])
    assert rows == [replacement]


def test_buffered_store_defers_parquet_until_flush(tmp_path) -> None:
    stats = []
    hot = SqliteKlineHotStore(tmp_path / "realtime.db")
    parquet = ParquetKlineStore(tmp_path / "raw")
    store = BufferedKlineStore(
        hot=hot,
        cold=parquet,
        flush_size=2,
        hot_flush_size=100,
        stats_callback=stats.append,
    )
    first = make_kline("BTCUSDT", 100.0)

    store.upsert_klines([first])

    relative_path = "interval=1m/symbol=BTCUSDT/date=2024-07-01/klines.parquet"
    assert hot.load_klines(interval="1m", symbols=["BTCUSDT"]) == []
    assert not (tmp_path / "raw" / relative_path).exists()
    assert stats[-1].sql_buffer_klines == 1
    assert stats[-1].sqlite_flushes == 0
    assert stats[-1].pending_klines == 0
    assert stats[-1].parquet_flushes == 0

    store.flush()

    frame = pl.read_parquet(tmp_path / "raw" / relative_path)
    assert frame.height == 1
    assert frame.item(0, "close") == 100.0
    assert hot.load_klines(interval="1m", symbols=["BTCUSDT"]) == []
    assert stats[-1].sql_buffer_klines == 0
    assert stats[-1].sqlite_flushes == 1
    assert stats[-1].pending_klines == 0
    assert stats[-1].parquet_flushes == 1


def test_buffered_store_flushes_sqlite_when_hot_buffer_reaches_threshold(tmp_path) -> None:
    stats = []
    hot = SqliteKlineHotStore(tmp_path / "realtime.db")
    parquet = ParquetKlineStore(tmp_path / "raw")
    store = BufferedKlineStore(
        hot=hot,
        cold=parquet,
        flush_size=10,
        hot_flush_size=2,
        stats_callback=stats.append,
    )
    first = make_kline("BTCUSDT", 100.0)
    second = make_kline("ETHUSDT", 200.0)

    store.upsert_klines([first])
    store.upsert_klines([second])

    assert hot.load_klines(interval="1m", symbols=["BTCUSDT", "ETHUSDT"]) == [
        first,
        second,
    ]
    assert stats[-1].sql_buffer_klines == 0
    assert stats[-1].sqlite_flushes == 1
    assert stats[-1].pending_klines == 2
    assert stats[-1].parquet_flushes == 0
