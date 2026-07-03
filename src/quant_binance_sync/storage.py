from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import polars as pl

from quant_binance_sync.models import Kline


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
