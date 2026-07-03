import httpx
import pytest
import respx

from quant_binance_sync.client import BinanceFuturesClient
from quant_binance_sync.rate_limit import AsyncWeightRateLimiter, kline_request_weight


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def test_kline_request_weight_matches_binance_tiers() -> None:
    assert kline_request_weight(99) == 1
    assert kline_request_weight(100) == 2
    assert kline_request_weight(499) == 2
    assert kline_request_weight(500) == 5
    assert kline_request_weight(1000) == 5
    assert kline_request_weight(1001) == 10
    assert kline_request_weight(1500) == 10


@pytest.mark.asyncio
async def test_rate_limiter_waits_until_minute_window_has_capacity() -> None:
    clock = FakeClock()
    limiter = AsyncWeightRateLimiter(
        max_weight_per_minute=10,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    await limiter.acquire(7)
    await limiter.acquire(3)
    await limiter.acquire(1)

    assert clock.sleeps == [60.0]


@pytest.mark.asyncio
@respx.mock
async def test_client_retries_429_using_retry_after_header() -> None:
    clock = FakeClock()
    limiter = AsyncWeightRateLimiter(
        max_weight_per_minute=100,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )
    route = respx.get("https://fapi.binance.com/fapi/v1/klines").mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "2"}, json={"msg": "too many requests"}),
            httpx.Response(
                200,
                json=[
                    [
                        1719792000000,
                        "100",
                        "101",
                        "99",
                        "100.5",
                        "1",
                        1719792059999,
                        "100.5",
                        7,
                        "0.5",
                        "50.25",
                        "0",
                    ]
                ],
            ),
        ]
    )

    async with BinanceFuturesClient(rate_limiter=limiter, sleep=clock.sleep) as client:
        rows = await client.klines(symbol="BTCUSDT", interval="1m", limit=1500)

    assert len(route.calls) == 2
    assert clock.sleeps == [2.0]
    assert rows[0][0] == 1719792000000
