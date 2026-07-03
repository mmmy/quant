from datetime import UTC, datetime

import polars as pl

from quant_binance_sync.models import Kline
from quant_binance_sync.storage import ParquetKlineStore


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
