from datetime import UTC, datetime, timedelta

import polars as pl

from quant_binance_sync.features import build_feature_frame, build_features
from quant_binance_sync.models import Kline
from quant_binance_sync.storage import ParquetKlineStore


def make_minute_rows(symbol: str, start: datetime, closes: list[float]) -> list[dict]:
    rows = []
    for offset, close in enumerate(closes):
        open_time = start + timedelta(minutes=offset)
        open_time_ms = int(open_time.timestamp() * 1000)
        rows.append(
            {
                "symbol": symbol,
                "interval": "1m",
                "open_time": open_time,
                "open_time_ms": open_time_ms,
                "open": close - 0.5,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "volume": 10.0,
                "close_time_ms": open_time_ms + 59_999,
                "quote_volume": close * 10.0,
                "trade_count": 5,
                "taker_buy_base_volume": 4.0,
                "taker_buy_quote_volume": close * 4.0,
                "date": open_time.date().isoformat(),
            }
        )
    return rows


def test_build_feature_frame_aggregates_1m_klines_to_hourly_features() -> None:
    start = datetime(2024, 7, 1, 0, 0, tzinfo=UTC)
    btc_closes = [100.0] * 59 + [120.0] + [120.0] * 59 + [132.0]
    eth_closes = [100.0] * 59 + [110.0] + [110.0] * 59 + [121.0]
    frame = pl.DataFrame(
        [
            *make_minute_rows("BTCUSDT", start, btc_closes),
            *make_minute_rows("ETHUSDT", start, eth_closes),
        ]
    )

    features = build_feature_frame(frame, base_interval="1m", feature_interval="1h")

    second_hour = features.filter(pl.col("ts_ms") == int((start + timedelta(hours=1)).timestamp() * 1000))
    btc = second_hour.filter(pl.col("symbol") == "BTCUSDT").row(0, named=True)
    eth = second_hour.filter(pl.col("symbol") == "ETHUSDT").row(0, named=True)
    assert btc["ret_1h"] == 0.1
    assert eth["ret_1h"] == 0.1
    assert btc["quote_volume_24h"] > eth["quote_volume_24h"]
    assert btc["liquidity_rank"] == 1
    assert eth["liquidity_rank"] == 2
    assert btc["feature_available_time_ms"] == btc["ts_ms"] + 3_600_000


def test_build_feature_frame_marks_incomplete_hour_untradable() -> None:
    start = datetime(2024, 7, 1, 0, 0, tzinfo=UTC)
    frame = pl.DataFrame(make_minute_rows("BTCUSDT", start, [100.0] * 59))

    features = build_feature_frame(frame, base_interval="1m", feature_interval="1h")

    assert features.item(0, "minute_count") == 59
    assert features.item(0, "is_tradable") is False
    assert features.item(0, "score") is None


def test_build_feature_frame_does_not_let_future_prices_change_past_features() -> None:
    start = datetime(2024, 7, 1, 0, 0, tzinfo=UTC)
    original = pl.DataFrame(make_minute_rows("BTCUSDT", start, [100.0] * 59 + [110.0] + [120.0] * 60))
    mutated = pl.DataFrame(make_minute_rows("BTCUSDT", start, [100.0] * 59 + [110.0] + [999.0] * 60))

    original_features = build_feature_frame(original, base_interval="1m", feature_interval="1h")
    mutated_features = build_feature_frame(mutated, base_interval="1m", feature_interval="1h")

    first_ts_ms = int(start.timestamp() * 1000)
    original_first = original_features.filter(pl.col("ts_ms") == first_ts_ms).row(0, named=True)
    mutated_first = mutated_features.filter(pl.col("ts_ms") == first_ts_ms).row(0, named=True)
    assert original_first["close"] == mutated_first["close"]
    assert original_first["quote_volume_24h"] == mutated_first["quote_volume_24h"]
    assert original_first["score"] == mutated_first["score"]


def test_build_features_reads_silver_partitions_and_writes_gold_features(tmp_path) -> None:
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    store = ParquetKlineStore(silver_dir)
    start = datetime(2024, 7, 1, 0, 0, tzinfo=UTC)
    klines = []
    for row in make_minute_rows("BTCUSDT", start, [100.0] * 59 + [110.0]):
        klines.append(
            Kline(
                symbol=row["symbol"],
                interval=row["interval"],
                open_time=row["open_time"],
                open_time_ms=row["open_time_ms"],
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
                volume=row["volume"],
                close_time_ms=row["close_time_ms"],
                quote_volume=row["quote_volume"],
                trade_count=row["trade_count"],
                taker_buy_base_volume=row["taker_buy_base_volume"],
                taker_buy_quote_volume=row["taker_buy_quote_volume"],
            )
        )
    store.upsert_klines(klines)

    result = build_features(
        silver_dir=silver_dir,
        output_dir=gold_dir,
        base_interval="1m",
        feature_interval="1h",
    )

    output_path = gold_dir / "interval=1h" / "date=2024-07-01" / "features.parquet"
    written = pl.read_parquet(output_path)
    assert result.rows_written == 1
    assert written.item(0, "symbol") == "BTCUSDT"
