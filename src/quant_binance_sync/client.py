from __future__ import annotations

from typing import Any

import httpx

from quant_binance_sync.rate_limit import AsyncWeightRateLimiter, kline_request_weight


class BinanceFuturesClient:
    def __init__(
        self,
        *,
        base_url: str = "https://fapi.binance.com",
        timeout: float = 20.0,
        rate_limiter: AsyncWeightRateLimiter | None = None,
        max_retries: int = 5,
        sleep=None,
    ) -> None:
        self._client = httpx.AsyncClient(base_url=base_url, timeout=timeout)
        self._rate_limiter = rate_limiter
        self._max_retries = max_retries
        self._sleep = sleep

    async def __aenter__(self) -> BinanceFuturesClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def exchange_info(self) -> dict[str, Any]:
        response = await self._request("GET", "/fapi/v1/exchangeInfo", weight=1)
        response.raise_for_status()
        return response.json()

    async def klines(
        self,
        *,
        symbol: str,
        interval: str,
        limit: int,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[list[Any]]:
        params: dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_time_ms is not None:
            params["startTime"] = start_time_ms
        if end_time_ms is not None:
            params["endTime"] = end_time_ms
        response = await self._request(
            "GET",
            "/fapi/v1/klines",
            weight=kline_request_weight(limit),
            params=params,
        )
        response.raise_for_status()
        return response.json()

    async def _request(self, method: str, path: str, *, weight: int, **kwargs: Any) -> httpx.Response:
        for attempt in range(self._max_retries + 1):
            if self._rate_limiter is not None:
                await self._rate_limiter.acquire(weight)

            response = await self._client.request(method, path, **kwargs)
            if response.status_code not in {429, 418}:
                return response
            if attempt >= self._max_retries:
                response.raise_for_status()

            retry_after = _retry_after_seconds(response)
            if retry_after is None:
                retry_after = min(60.0, 2.0**attempt)
            await self._sleep_for(retry_after)

        raise RuntimeError("unreachable retry state")

    async def _sleep_for(self, seconds: float) -> None:
        if self._sleep is None:
            import asyncio

            await asyncio.sleep(seconds)
        else:
            await self._sleep(seconds)


def _retry_after_seconds(response: httpx.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None
