from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any


@dataclass(frozen=True)
class Kline:
    symbol: str
    interval: str
    open_time: datetime
    open_time_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time_ms: int
    quote_volume: float
    trade_count: int
    taker_buy_base_volume: float
    taker_buy_quote_volume: float

    @property
    def date(self) -> str:
        return self.open_time.date().isoformat()

    def to_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["open_time"] = self.open_time
        record["date"] = self.date
        return record


def active_usdm_perpetual_symbols(exchange_info: dict[str, Any]) -> list[str]:
    symbols = []
    for item in exchange_info.get("symbols", []):
        if (
            item.get("status") == "TRADING"
            and item.get("contractType") == "PERPETUAL"
            and item.get("quoteAsset") == "USDT"
        ):
            symbols.append(item["symbol"])
    return sorted(symbols)


def parse_kline(symbol: str, interval: str, row: list[Any]) -> Kline:
    open_time_ms = int(row[0])
    return Kline(
        symbol=symbol,
        interval=interval,
        open_time=datetime.fromtimestamp(open_time_ms / 1000, tz=UTC),
        open_time_ms=open_time_ms,
        open=float(row[1]),
        high=float(row[2]),
        low=float(row[3]),
        close=float(row[4]),
        volume=float(row[5]),
        close_time_ms=int(row[6]),
        quote_volume=float(row[7]),
        trade_count=int(row[8]),
        taker_buy_base_volume=float(row[9]),
        taker_buy_quote_volume=float(row[10]),
    )
