from datetime import UTC, datetime

from quant_binance_sync.models import Kline, active_usdm_perpetual_symbols, parse_kline


def test_filters_active_usdt_perpetual_symbols() -> None:
    payload = {
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "contractType": "PERPETUAL",
                "quoteAsset": "USDT",
                "status": "TRADING",
            },
            {
                "symbol": "ETHUSDC",
                "contractType": "PERPETUAL",
                "quoteAsset": "USDC",
                "status": "TRADING",
            },
            {
                "symbol": "BTCUSDT_260327",
                "contractType": "CURRENT_QUARTER",
                "quoteAsset": "USDT",
                "status": "TRADING",
            },
            {
                "symbol": "OLDUSDT",
                "contractType": "PERPETUAL",
                "quoteAsset": "USDT",
                "status": "SETTLING",
            },
        ]
    }

    assert active_usdm_perpetual_symbols(payload) == ["BTCUSDT"]


def test_parses_binance_kline_array() -> None:
    row = [
        1719792000000,
        "62768.10",
        "62801.00",
        "62700.50",
        "62742.90",
        "123.456",
        1719792059999,
        "7743210.12",
        1001,
        "61.234",
        "3844000.77",
        "0",
    ]

    kline = parse_kline("BTCUSDT", "1m", row)

    assert kline == Kline(
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2024, 7, 1, 0, 0, tzinfo=UTC),
        open_time_ms=1719792000000,
        open=62768.10,
        high=62801.00,
        low=62700.50,
        close=62742.90,
        volume=123.456,
        close_time_ms=1719792059999,
        quote_volume=7743210.12,
        trade_count=1001,
        taker_buy_base_volume=61.234,
        taker_buy_quote_volume=3844000.77,
    )
