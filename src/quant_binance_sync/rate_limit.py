from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Awaitable, Callable


class AsyncWeightRateLimiter:
    def __init__(
        self,
        *,
        max_weight_per_minute: int,
        monotonic: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        if max_weight_per_minute <= 0:
            raise ValueError("max_weight_per_minute must be positive")
        self.max_weight_per_minute = max_weight_per_minute
        self._monotonic = monotonic or time.monotonic
        self._sleep = sleep or asyncio.sleep
        self._lock = asyncio.Lock()
        self._events: deque[tuple[float, int]] = deque()

    async def acquire(self, weight: int) -> None:
        if weight <= 0:
            raise ValueError("weight must be positive")

        while True:
            async with self._lock:
                now = self._monotonic()
                self._drop_expired(now)
                used = sum(event_weight for _, event_weight in self._events)
                if used + weight <= self.max_weight_per_minute:
                    self._events.append((now, weight))
                    return

                oldest_time = self._events[0][0]
                wait_seconds = max(0.0, 60.0 - (now - oldest_time))

            await self._sleep(wait_seconds)

    def _drop_expired(self, now: float) -> None:
        while self._events and now - self._events[0][0] >= 60.0:
            self._events.popleft()


def kline_request_weight(limit: int) -> int:
    if limit < 100:
        return 1
    if limit < 500:
        return 2
    if limit <= 1000:
        return 5
    return 10
