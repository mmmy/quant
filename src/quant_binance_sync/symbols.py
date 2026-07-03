from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol


@dataclass(frozen=True)
class ContractSymbol:
    symbol: str
    base_asset: str
    quote_asset: str
    contract_type: str
    status: str
    onboard_date: int | None
    delivery_date: int | None

    @classmethod
    def from_exchange_info(cls, item: dict[str, Any]) -> ContractSymbol:
        return cls(
            symbol=item["symbol"],
            base_asset=item.get("baseAsset", ""),
            quote_asset=item.get("quoteAsset", ""),
            contract_type=item.get("contractType", ""),
            status=item.get("status", ""),
            onboard_date=item.get("onboardDate"),
            delivery_date=item.get("deliveryDate"),
        )

    @property
    def is_active_usdt_perpetual(self) -> bool:
        return (
            self.quote_asset == "USDT"
            and self.contract_type == "PERPETUAL"
            and self.status == "TRADING"
        )

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


class ExchangeInfoProvider(Protocol):
    async def exchange_info(self) -> dict[str, Any]: ...


class SymbolMetadataStore:
    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)
        self.snapshots_path = self.root / "usdm_symbols_snapshots.jsonl"
        self.current_path = self.root / "usdm_symbols_current.json"

    def save_snapshot(
        self,
        *,
        symbols: list[ContractSymbol],
        active_symbols: list[ContractSymbol],
        snapshot_time: datetime,
    ) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        snapshot = {
            "snapshot_time": snapshot_time.isoformat(),
            "symbols": [symbol.to_record() for symbol in symbols],
        }
        with self.snapshots_path.open("a", encoding="utf-8") as file:
            file.write(json.dumps(snapshot, sort_keys=True) + "\n")

        current = {
            "snapshot_time": snapshot_time.isoformat(),
            "symbols": [symbol.to_record() for symbol in active_symbols],
        }
        self.current_path.write_text(
            json.dumps(current, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def load_current_symbols(self) -> list[ContractSymbol]:
        payload = json.loads(self.current_path.read_text(encoding="utf-8"))
        return [ContractSymbol(**item) for item in payload.get("symbols", [])]


async def refresh_symbol_metadata(
    *,
    client: ExchangeInfoProvider,
    store: SymbolMetadataStore,
    now: datetime | None = None,
) -> list[ContractSymbol]:
    snapshot_time = now or datetime.now(tz=UTC)
    payload = await client.exchange_info()
    symbols = sorted(
        [ContractSymbol.from_exchange_info(item) for item in payload.get("symbols", [])],
        key=lambda item: item.symbol,
    )
    active_symbols = [symbol for symbol in symbols if symbol.is_active_usdt_perpetual]
    store.save_snapshot(
        symbols=symbols,
        active_symbols=active_symbols,
        snapshot_time=snapshot_time,
    )
    return active_symbols
