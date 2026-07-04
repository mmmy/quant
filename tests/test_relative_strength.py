from datetime import UTC, datetime, timedelta

import polars as pl

from quant_binance_sync.relative_strength import (
    build_relative_strength_frame,
    relative_strength_source_interval,
)
from quant_binance_sync.storage import ParquetKlineStore
from quant_binance_sync.models import Kline


def make_rows(symbol: str, start: datetime, closes: list[float], interval: str = "1m") -> list[dict]:
    interval_minutes = 1 if interval == "1m" else 15
    rows = []
    for offset, close in enumerate(closes):
        open_time = start + timedelta(minutes=offset * interval_minutes)
        open_time_ms = int(open_time.timestamp() * 1000)
        close_time_ms = open_time_ms + interval_minutes * 60_000 - 1
        rows.append(
            {
                "symbol": symbol,
                "interval": interval,
                "open_time": open_time,
                "open_time_ms": open_time_ms,
                "open": close - 1,
                "high": close + 1,
                "low": close - 1,
                "close": close,
                "volume": 10.0,
                "close_time_ms": close_time_ms,
                "quote_volume": close * 10,
                "trade_count": 5,
                "taker_buy_base_volume": 4.0,
                "taker_buy_quote_volume": close * 4,
                "date": open_time.date().isoformat(),
            }
        )
    return rows


def test_relative_strength_source_interval_prefers_15m_when_target_is_divisible_by_15() -> None:
    assert relative_strength_source_interval("1") == "1m"
    assert relative_strength_source_interval("10") == "1m"
    assert relative_strength_source_interval("20") == "1m"
    assert relative_strength_source_interval("15") == "15m"
    assert relative_strength_source_interval("30") == "15m"
    assert relative_strength_source_interval("720") == "15m"
    assert relative_strength_source_interval("D") == "15m"


def test_build_relative_strength_frame_ranks_symbols_against_btc_without_overheated_names() -> None:
    start = datetime(2024, 7, 1, 0, 0, tzinfo=UTC)
    frame = pl.DataFrame(
        [
            *make_rows("BTCUSDT", start, [100.0, 102.0, 104.0]),
            *make_rows("ETHUSDT", start, [100.0, 103.0, 108.0]),
            *make_rows("SOLUSDT", start, [100.0, 101.0, 102.0]),
            *make_rows("DOGEUSDT", start, [100.0, 120.0, 160.0]),
        ]
    )

    result = build_relative_strength_frame(frame, tf="1", max_abs_gap_atr=100.0, max_ret=0.25)
    latest = result.filter(pl.col("ts_ms") == result.select(pl.col("ts_ms").max()).item())

    eth = latest.filter(pl.col("symbol") == "ETHUSDT").row(0, named=True)
    sol = latest.filter(pl.col("symbol") == "SOLUSDT").row(0, named=True)
    doge = latest.filter(pl.col("symbol") == "DOGEUSDT").row(0, named=True)
    assert eth["rs_ret"] > 0
    assert sol["rs_ret"] < 0
    assert eth["strong_rank"] == 1
    assert sol["weak_rank"] == 1
    assert doge["is_overheated"] is True
    assert doge["strong_rank"] is None


def test_build_relative_strength_frame_can_keep_only_recent_target_bars() -> None:
    start = datetime(2024, 7, 1, 0, 0, tzinfo=UTC)
    frame = pl.DataFrame(
        [
            *make_rows("BTCUSDT", start, [100.0, 101.0, 102.0, 103.0, 104.0]),
            *make_rows("ETHUSDT", start, [100.0, 102.0, 104.0, 106.0, 108.0]),
            *make_rows("SOLUSDT", start, [100.0, 100.5, 101.0, 101.5, 102.0]),
        ]
    )

    result = build_relative_strength_frame(
        frame,
        tf="1",
        max_abs_gap_atr=100.0,
        tail_bars=2,
    )

    assert result.select("ts_ms").unique().height == 2
    assert result.select("ts_ms").min().item() == int((start + timedelta(minutes=3)).timestamp() * 1000)


def test_build_relative_strength_frame_prefilters_by_liquidity_top_n() -> None:
    start = datetime(2024, 7, 1, 0, 0, tzinfo=UTC)
    rows = [
        *make_rows("BTCUSDT", start, [100.0, 101.0, 102.0]),
        *make_rows("HIGHUSDT", start, [100.0, 103.0, 106.0]),
        *make_rows("LOWUSDT", start, [100.0, 104.0, 108.0]),
    ]
    for row in rows:
        if row["symbol"] == "LOWUSDT":
            row["quote_volume"] = 1.0
        elif row["symbol"] == "HIGHUSDT":
            row["quote_volume"] = 1_000_000.0
    frame = pl.DataFrame(rows)

    result = build_relative_strength_frame(
        frame,
        tf="1",
        max_abs_gap_atr=100.0,
        liquidity_top_n=1,
        liquidity_lookback_bars=2,
    )

    assert set(result["symbol"].to_list()) == {"HIGHUSDT"}


def test_build_relative_strength_uses_15m_silver_for_hourly_targets(tmp_path) -> None:
    silver_dir = tmp_path / "silver"
    store = ParquetKlineStore(silver_dir)
    start = datetime(2024, 7, 1, 0, 0, tzinfo=UTC)
    klines = []
    for row in [
        *make_rows("BTCUSDT", start, [100.0, 101.0, 102.0, 103.0], interval="15m"),
        *make_rows("ETHUSDT", start, [100.0, 102.0, 104.0, 106.0], interval="15m"),
    ]:
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

    from quant_binance_sync.relative_strength import build_relative_strength

    build_result = build_relative_strength(
        silver_dir=silver_dir,
        output_dir=tmp_path / "gold",
        tf="60",
        max_abs_gap_atr=100.0,
    )

    output_path = tmp_path / "gold" / "tf=60" / "relative_strength.parquet"
    written = pl.read_parquet(output_path)
    assert build_result.rows_written == 1
    assert set(written["source_interval"].to_list()) == {"15m"}


def test_build_relative_strength_tail_bars_writes_only_recent_target_bars(tmp_path) -> None:
    silver_dir = tmp_path / "silver"
    store = ParquetKlineStore(silver_dir)
    start = datetime(2024, 7, 1, 0, 0, tzinfo=UTC)
    klines = []
    for row in [
        *make_rows("BTCUSDT", start, [100.0, 101.0, 102.0, 103.0, 104.0], interval="15m"),
        *make_rows("ETHUSDT", start, [100.0, 102.0, 104.0, 106.0, 108.0], interval="15m"),
        *make_rows("SOLUSDT", start, [100.0, 100.5, 101.0, 101.5, 102.0], interval="15m"),
    ]:
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

    from quant_binance_sync.relative_strength import build_relative_strength

    build_result = build_relative_strength(
        silver_dir=silver_dir,
        output_dir=tmp_path / "gold",
        tf="15",
        max_abs_gap_atr=100.0,
        tail_bars=2,
        warmup_bars=2,
    )

    written = pl.read_parquet(tmp_path / "gold" / "tf=15" / "relative_strength.parquet")
    assert build_result.rows_written == 4
    assert written.select("ts_ms").unique().height == 2
