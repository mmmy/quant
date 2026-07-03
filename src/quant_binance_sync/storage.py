from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from quant_binance_sync.models import Kline
from quant_binance_sync.normalizer import NormalizeResult, normalize_klines


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
