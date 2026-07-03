from __future__ import annotations

from dataclasses import dataclass

from quant_binance_sync.models import Kline


@dataclass(frozen=True)
class RejectedKline:
    kline: Kline
    reason: str


@dataclass(frozen=True)
class KlineConflict:
    existing: Kline
    incoming: Kline
    reason: str


@dataclass(frozen=True)
class KlineGap:
    symbol: str
    interval: str
    missing_open_time_ms: int
    previous_open_time_ms: int
    next_open_time_ms: int


@dataclass(frozen=True)
class NormalizeResult:
    accepted: list[Kline]
    rejected: list[RejectedKline]
    conflicts: list[KlineConflict]
    gaps: list[KlineGap]
    contiguous_last_open_time_ms: int | None

    @property
    def accepted_count(self) -> int:
        return len(self.accepted)

    @property
    def rejected_count(self) -> int:
        return len(self.rejected)

    @property
    def conflict_count(self) -> int:
        return len(self.conflicts)

    @property
    def gap_count(self) -> int:
        return len(self.gaps)


def normalize_klines(klines: list[Kline], *, interval_ms: int) -> NormalizeResult:
    accepted_by_key: dict[tuple[str, str, int], Kline] = {}
    rejected: list[RejectedKline] = []
    conflicts: list[KlineConflict] = []

    for kline in klines:
        invalid_reason = validate_kline(kline, interval_ms=interval_ms)
        if invalid_reason is not None:
            rejected.append(RejectedKline(kline=kline, reason=invalid_reason))
            continue

        key = (kline.symbol, kline.interval, kline.open_time_ms)
        existing = accepted_by_key.get(key)
        if existing is None:
            accepted_by_key[key] = kline
            continue
        if existing == kline:
            continue
        conflicts.append(
            KlineConflict(existing=existing, incoming=kline, reason="duplicate_conflict")
        )

    accepted = sorted(accepted_by_key.values(), key=lambda item: item.open_time_ms)
    gaps = find_gaps(accepted, interval_ms=interval_ms)
    return NormalizeResult(
        accepted=accepted,
        rejected=rejected,
        conflicts=conflicts,
        gaps=gaps,
        contiguous_last_open_time_ms=contiguous_last_open_time_ms(
            accepted,
            gaps,
            interval_ms=interval_ms,
        ),
    )


def validate_kline(kline: Kline, *, interval_ms: int) -> str | None:
    if kline.close_time_ms != kline.open_time_ms + interval_ms - 1:
        return "invalid_close_time"
    if kline.open_time_ms % interval_ms != 0:
        return "misaligned_open_time"
    if (
        kline.high < kline.open
        or kline.high < kline.close
        or kline.high < kline.low
        or kline.low > kline.open
        or kline.low > kline.close
        or kline.low > kline.high
    ):
        return "invalid_ohlc"
    if kline.volume < 0 or kline.quote_volume < 0:
        return "negative_volume"
    if kline.trade_count < 0:
        return "negative_trade_count"
    if kline.taker_buy_base_volume < 0 or kline.taker_buy_quote_volume < 0:
        return "negative_taker_buy_volume"
    return None


def find_gaps(klines: list[Kline], *, interval_ms: int) -> list[KlineGap]:
    gaps: list[KlineGap] = []
    by_series: dict[tuple[str, str], list[Kline]] = {}
    for kline in klines:
        by_series.setdefault((kline.symbol, kline.interval), []).append(kline)

    for (symbol, interval), series in by_series.items():
        ordered = sorted(series, key=lambda item: item.open_time_ms)
        for previous, current in zip(ordered, ordered[1:], strict=False):
            missing_open_time_ms = previous.open_time_ms + interval_ms
            while missing_open_time_ms < current.open_time_ms:
                gaps.append(
                    KlineGap(
                        symbol=symbol,
                        interval=interval,
                        missing_open_time_ms=missing_open_time_ms,
                        previous_open_time_ms=previous.open_time_ms,
                        next_open_time_ms=current.open_time_ms,
                    )
                )
                missing_open_time_ms += interval_ms
    return gaps


def contiguous_last_open_time_ms(
    accepted: list[Kline],
    gaps: list[KlineGap],
    *,
    interval_ms: int,
) -> int | None:
    if not accepted:
        return None
    first_gap_by_series = {
        (gap.symbol, gap.interval): gap.missing_open_time_ms
        for gap in sorted(gaps, key=lambda item: item.missing_open_time_ms)
    }
    last_by_series: list[int] = []
    by_series: dict[tuple[str, str], list[Kline]] = {}
    for kline in accepted:
        by_series.setdefault((kline.symbol, kline.interval), []).append(kline)
    for key, series in by_series.items():
        ordered = sorted(series, key=lambda item: item.open_time_ms)
        first_gap = first_gap_by_series.get(key)
        if first_gap is None:
            last_by_series.append(ordered[-1].open_time_ms)
        else:
            last_by_series.append(first_gap - interval_ms)
    return min(last_by_series)
