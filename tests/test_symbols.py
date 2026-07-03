import json
from datetime import UTC, datetime

import pytest

from quant_binance_sync.symbols import SymbolMetadataStore, refresh_symbol_metadata


class ExchangeInfoClient:
    async def exchange_info(self):
        return {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                    "onboardDate": 1569398400000,
                    "deliveryDate": 4133404800000,
                },
                {
                    "symbol": "ETHUSDC",
                    "baseAsset": "ETH",
                    "quoteAsset": "USDC",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                    "onboardDate": 1569398400000,
                    "deliveryDate": 4133404800000,
                },
                {
                    "symbol": "OLDUSDT",
                    "baseAsset": "OLD",
                    "quoteAsset": "USDT",
                    "contractType": "PERPETUAL",
                    "status": "SETTLING",
                    "onboardDate": 1569398400000,
                    "deliveryDate": 1719792000000,
                },
            ]
        }


@pytest.mark.asyncio
async def test_refresh_symbol_metadata_writes_snapshot_and_current_universe(tmp_path) -> None:
    store = SymbolMetadataStore(tmp_path)

    current = await refresh_symbol_metadata(
        client=ExchangeInfoClient(),
        store=store,
        now=datetime(2024, 7, 1, 0, 0, tzinfo=UTC),
    )

    assert [symbol.symbol for symbol in current] == ["BTCUSDT"]

    current_file = tmp_path / "usdm_symbols_current.json"
    current_payload = json.loads(current_file.read_text(encoding="utf-8"))
    assert current_payload["symbols"][0]["symbol"] == "BTCUSDT"

    snapshot_file = tmp_path / "usdm_symbols_snapshots.jsonl"
    snapshot_lines = snapshot_file.read_text(encoding="utf-8").splitlines()
    assert len(snapshot_lines) == 1
    snapshot = json.loads(snapshot_lines[0])
    assert snapshot["snapshot_time"] == "2024-07-01T00:00:00+00:00"
    assert {item["symbol"] for item in snapshot["symbols"]} == {"BTCUSDT", "ETHUSDC", "OLDUSDT"}
