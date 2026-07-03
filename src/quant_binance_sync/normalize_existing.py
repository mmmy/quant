from __future__ import annotations

import shutil
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from quant_binance_sync.models import Kline
from quant_binance_sync.normalizer import NormalizeResult, normalize_klines
from quant_binance_sync.storage import ParquetKlineStore
from quant_binance_sync.sync import interval_to_milliseconds


@dataclass(frozen=True)
class NormalizeExistingResult:
    files_seen: int
    raw_klines_seen: int
    accepted_klines: int
    rejected_klines: int
    conflict_klines: int
    gaps_seen: int


@dataclass(frozen=True)
class NormalizeExistingProgress:
    current: str
    files_seen: int
    total_files: int
    raw_klines_seen: int
    accepted_klines: int
    rejected_klines: int
    conflict_klines: int
    gaps_seen: int


def normalize_existing_klines(
    *,
    raw_dir: Path | str,
    silver_dir: Path | str,
    quarantine_dir: Path | str,
    gap_report_path: Path | str,
    interval: str,
    symbol: list[str] | None,
    start_date: date | None,
    end_date: date | None,
    overwrite: bool,
    progress_callback: Callable[[NormalizeExistingProgress], None] | None = None,
) -> NormalizeExistingResult:
    raw_root = Path(raw_dir)
    silver_root = Path(silver_dir)
    quarantine_root = Path(quarantine_dir)
    gap_report = Path(gap_report_path)
    selected_symbols = set(symbol) if symbol else None
    raw_paths = [
        path
        for path in sorted(raw_root.glob(f"interval={interval}/symbol=*/date=*/klines.parquet"))
        if should_process_partition(
            parse_partition_path(path),
            selected_symbols=selected_symbols,
            start_date=start_date,
            end_date=end_date,
        )
    ]
    if overwrite:
        remove_outputs_for_paths(
            raw_paths=raw_paths,
            silver_root=silver_root,
            quarantine_root=quarantine_root,
            gap_report=gap_report,
            interval=interval,
            full_overwrite=selected_symbols is None and start_date is None and end_date is None,
        )

    silver_store = ParquetKlineStore(silver_root)
    quarantine_store = ParquetKlineStore(quarantine_root)
    interval_ms = interval_to_milliseconds(interval)

    totals = {
        "files_seen": 0,
        "raw_klines_seen": 0,
        "accepted_klines": 0,
        "rejected_klines": 0,
        "conflict_klines": 0,
        "gaps_seen": 0,
    }
    total_files = len(raw_paths)
    for path in raw_paths:
        partition = parse_partition_path(path)

        klines = read_klines(path)
        result = normalize_klines(klines, interval_ms=interval_ms)
        write_normalized_outputs(
            result=result,
            silver_store=silver_store,
            quarantine_store=quarantine_store,
            gap_report=gap_report,
        )
        totals["files_seen"] += 1
        totals["raw_klines_seen"] += len(klines)
        totals["accepted_klines"] += result.accepted_count
        totals["rejected_klines"] += result.rejected_count
        totals["conflict_klines"] += result.conflict_count
        totals["gaps_seen"] += result.gap_count
        if progress_callback is not None:
            progress_callback(
                NormalizeExistingProgress(
                    current=f"{partition['symbol']} {partition['date']}",
                    total_files=total_files,
                    **totals,
                )
            )

    return NormalizeExistingResult(**totals)


def read_klines(path: Path) -> list[Kline]:
    rows = pl.read_parquet(path).drop("open_time", strict=False).to_dicts()
    return [kline_from_record(row) for row in rows]


def kline_from_record(record: dict[str, Any]) -> Kline:
    return Kline(
        symbol=str(record["symbol"]),
        interval=str(record["interval"]),
        open_time_ms=int(record["open_time_ms"]),
        open_time=datetime.fromtimestamp(int(record["open_time_ms"]) / 1000, tz=UTC),
        open=float(record["open"]),
        high=float(record["high"]),
        low=float(record["low"]),
        close=float(record["close"]),
        volume=float(record["volume"]),
        close_time_ms=int(record["close_time_ms"]),
        quote_volume=float(record["quote_volume"]),
        trade_count=int(record["trade_count"]),
        taker_buy_base_volume=float(record["taker_buy_base_volume"]),
        taker_buy_quote_volume=float(record["taker_buy_quote_volume"]),
    )


def parse_partition_path(path: Path) -> dict[str, str]:
    return {
        "date": path.parent.name.removeprefix("date="),
        "symbol": path.parent.parent.name.removeprefix("symbol="),
        "interval": path.parent.parent.parent.name.removeprefix("interval="),
    }


def should_process_partition(
    partition: dict[str, str],
    *,
    selected_symbols: set[str] | None,
    start_date: date | None,
    end_date: date | None,
) -> bool:
    if selected_symbols is not None and partition["symbol"] not in selected_symbols:
        return False
    partition_date = date.fromisoformat(partition["date"])
    if start_date is not None and partition_date < start_date:
        return False
    if end_date is not None and partition_date > end_date:
        return False
    return True


def write_normalized_outputs(
    *,
    result: NormalizeResult,
    silver_store: ParquetKlineStore,
    quarantine_store: ParquetKlineStore,
    gap_report: Path,
) -> None:
    if result.accepted:
        silver_store.upsert_klines(result.accepted)
    quarantined = [item.kline for item in result.rejected]
    quarantined.extend(item.incoming for item in result.conflicts)
    if quarantined:
        quarantine_store.upsert_klines(quarantined)
    if result.gaps:
        append_gap_report(gap_report, result)


def append_gap_report(path: Path, result: NormalizeResult) -> None:
    from dataclasses import asdict

    records = [
        {
            **asdict(gap),
            "detected_at": datetime.now(tz=UTC),
            "status": "open",
            "reason": "missing_kline",
        }
        for gap in result.gaps
    ]
    incoming = pl.DataFrame(records)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = pl.read_parquet(path)
        frame = pl.concat([existing, incoming], how="vertical_relaxed")
    else:
        frame = incoming
    frame = frame.unique(
        subset=["symbol", "interval", "missing_open_time_ms"],
        keep="last",
        maintain_order=True,
    ).sort(["symbol", "interval", "missing_open_time_ms"])
    frame.write_parquet(path)


def remove_outputs_for_paths(
    *,
    raw_paths: list[Path],
    silver_root: Path,
    quarantine_root: Path,
    gap_report: Path,
    interval: str,
    full_overwrite: bool,
) -> None:
    if full_overwrite:
        remove_path(silver_root)
        remove_path(quarantine_root)
        remove_path(gap_report)
        return

    for raw_path in raw_paths:
        partition = parse_partition_path(raw_path)
        relative = (
            Path(f"interval={partition['interval']}")
            / f"symbol={partition['symbol']}"
            / f"date={partition['date']}"
            / "klines.parquet"
        )
        remove_path(silver_root / relative)
        remove_path(quarantine_root / relative)
    remove_gap_rows_for_paths(gap_report, raw_paths=raw_paths, interval=interval)


def remove_gap_rows_for_paths(path: Path, *, raw_paths: list[Path], interval: str) -> None:
    if not path.exists() or not raw_paths:
        return
    partitions = [parse_partition_path(raw_path) for raw_path in raw_paths]
    frame = pl.read_parquet(path)
    keep_expr = pl.lit(True)
    for partition in partitions:
        day_start = int(
            datetime.fromisoformat(partition["date"]).replace(tzinfo=UTC).timestamp() * 1000
        )
        day_end = day_start + 86_400_000 - 1
        keep_expr = keep_expr & ~(
            (pl.col("symbol") == partition["symbol"])
            & (pl.col("interval") == interval)
            & (pl.col("missing_open_time_ms") >= day_start)
            & (pl.col("missing_open_time_ms") <= day_end)
        )
    filtered = frame.filter(keep_expr)
    if filtered.is_empty():
        remove_path(path)
    else:
        filtered.write_parquet(path)


def remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()
