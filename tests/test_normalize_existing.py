from datetime import UTC, datetime

import polars as pl

from quant_binance_sync.models import Kline
from quant_binance_sync.normalize_existing import NormalizeExistingProgress, normalize_existing_klines


def make_kline(open_time_ms: int, *, close: float = 100.5) -> Kline:
    return Kline(
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime.fromtimestamp(open_time_ms / 1000, tz=UTC),
        open_time_ms=open_time_ms,
        open=100.0,
        high=max(101.0, close),
        low=99.0,
        close=close,
        volume=1.0,
        close_time_ms=open_time_ms + 59_999,
        quote_volume=100.5,
        trade_count=7,
        taker_buy_base_volume=0.5,
        taker_buy_quote_volume=50.25,
    )


def write_raw_partition(root, *klines: Kline) -> None:
    path = root / "interval=1m" / "symbol=BTCUSDT" / "date=2024-07-01" / "klines.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([kline.to_record() for kline in klines]).write_parquet(path)


def write_symbol_partition(root, symbol: str, *klines: Kline) -> None:
    path = root / "interval=1m" / f"symbol={symbol}" / "date=2024-07-01" / "klines.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    records = [{**kline.to_record(), "symbol": symbol} for kline in klines]
    pl.DataFrame(records).write_parquet(path)


def test_normalize_existing_klines_rebuilds_silver_quarantine_and_gap_report(tmp_path) -> None:
    raw_dir = tmp_path / "raw"
    silver_dir = tmp_path / "silver"
    quarantine_dir = tmp_path / "quarantine"
    gap_report_path = tmp_path / "reports" / "gaps.parquet"
    first = make_kline(1719792000000)
    third = make_kline(1719792120000)
    invalid = Kline(**{**make_kline(1719792180000).__dict__, "close_time_ms": 1719792240000})
    write_raw_partition(raw_dir, first, third, invalid)

    result = normalize_existing_klines(
        raw_dir=raw_dir,
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir,
        gap_report_path=gap_report_path,
        interval="1m",
        symbol=None,
        start_date=None,
        end_date=None,
        overwrite=True,
    )

    silver_frame = pl.read_parquet(
        silver_dir / "interval=1m" / "symbol=BTCUSDT" / "date=2024-07-01" / "klines.parquet"
    )
    quarantine_frame = pl.read_parquet(
        quarantine_dir / "interval=1m" / "symbol=BTCUSDT" / "date=2024-07-01" / "klines.parquet"
    )
    gap_frame = pl.read_parquet(gap_report_path)

    assert result.files_seen == 1
    assert result.raw_klines_seen == 3
    assert result.accepted_klines == 2
    assert result.rejected_klines == 1
    assert result.gaps_seen == 1
    assert silver_frame.height == 2
    assert quarantine_frame.height == 1
    assert gap_frame.item(0, "missing_open_time_ms") == 1719792060000


def test_normalize_existing_klines_reports_file_progress(tmp_path) -> None:
    raw_dir = tmp_path / "raw"
    write_raw_partition(raw_dir, make_kline(1719792000000))
    events: list[NormalizeExistingProgress] = []

    normalize_existing_klines(
        raw_dir=raw_dir,
        silver_dir=tmp_path / "silver",
        quarantine_dir=tmp_path / "quarantine",
        gap_report_path=tmp_path / "reports" / "gaps.parquet",
        interval="1m",
        symbol=None,
        start_date=None,
        end_date=None,
        overwrite=True,
        progress_callback=events.append,
    )

    assert events == [
        NormalizeExistingProgress(
            current="BTCUSDT 2024-07-01",
            files_seen=1,
            total_files=1,
            raw_klines_seen=1,
            accepted_klines=1,
            rejected_klines=0,
            conflict_klines=0,
            gaps_seen=0,
        )
    ]


def test_normalize_existing_filtered_overwrite_preserves_unmatched_silver_partitions(tmp_path) -> None:
    raw_dir = tmp_path / "raw"
    silver_dir = tmp_path / "silver"
    write_symbol_partition(raw_dir, "BTCUSDT", make_kline(1719792000000))
    write_symbol_partition(raw_dir, "ETHUSDT", make_kline(1719792000000))
    write_symbol_partition(silver_dir, "ETHUSDT", make_kline(1719792000000, close=200.0))

    normalize_existing_klines(
        raw_dir=raw_dir,
        silver_dir=silver_dir,
        quarantine_dir=tmp_path / "quarantine",
        gap_report_path=tmp_path / "reports" / "gaps.parquet",
        interval="1m",
        symbol=["BTCUSDT"],
        start_date=None,
        end_date=None,
        overwrite=True,
    )

    eth_frame = pl.read_parquet(
        silver_dir / "interval=1m" / "symbol=ETHUSDT" / "date=2024-07-01" / "klines.parquet"
    )
    assert eth_frame.item(0, "close") == 200.0


def test_normalize_existing_does_not_rewrite_raw_duplicates(tmp_path) -> None:
    raw_dir = tmp_path / "raw"
    first = make_kline(1719792000000, close=100.5)
    conflicting = make_kline(1719792000000, close=101.5)
    write_raw_partition(raw_dir, first, conflicting)

    normalize_existing_klines(
        raw_dir=raw_dir,
        silver_dir=tmp_path / "silver",
        quarantine_dir=tmp_path / "quarantine",
        gap_report_path=tmp_path / "reports" / "gaps.parquet",
        interval="1m",
        symbol=None,
        start_date=None,
        end_date=None,
        overwrite=True,
    )

    raw_frame = pl.read_parquet(
        raw_dir / "interval=1m" / "symbol=BTCUSDT" / "date=2024-07-01" / "klines.parquet"
    )
    assert raw_frame.height == 2
    assert raw_frame["close"].to_list() == [100.5, 101.5]
