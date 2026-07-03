from datetime import UTC, datetime

from quant_binance_sync.models import Kline
from quant_binance_sync.normalizer import normalize_klines


def make_kline(open_time_ms: int, *, close: float = 100.5) -> Kline:
    return Kline(
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime.fromtimestamp(open_time_ms / 1000, tz=UTC),
        open_time_ms=open_time_ms,
        open=100.0,
        high=max(101.0, close),
        low=99.0,
        close=close,
        volume=1.0,
        close_time_ms=open_time_ms + 59_999,
        quote_volume=100.5,
        trade_count=7,
        taker_buy_base_volume=0.5,
        taker_buy_quote_volume=50.25,
    )


def test_normalize_rejects_invalid_close_time() -> None:
    kline = make_kline(1719792000000)
    invalid = Kline(
        **{
            **kline.__dict__,
            "close_time_ms": kline.open_time_ms + 60_000,
        }
    )

    result = normalize_klines([invalid], interval_ms=60_000)

    assert result.accepted == []
    assert len(result.rejected) == 1
    assert result.rejected[0].reason == "invalid_close_time"


def test_normalize_rejects_invalid_ohlc() -> None:
    kline = make_kline(1719792000000)
    invalid = Kline(
        **{
            **kline.__dict__,
            "high": 100.0,
            "close": 101.0,
        }
    )

    result = normalize_klines([invalid], interval_ms=60_000)

    assert result.accepted == []
    assert result.rejected[0].reason == "invalid_ohlc"


def test_normalize_deduplicates_identical_klines() -> None:
    kline = make_kline(1719792000000)

    result = normalize_klines([kline, kline], interval_ms=60_000)

    assert result.accepted == [kline]
    assert result.conflicts == []


def test_normalize_quarantines_conflicting_duplicate() -> None:
    first = make_kline(1719792000000, close=100.5)
    second = make_kline(1719792000000, close=101.5)

    result = normalize_klines([first, second], interval_ms=60_000)

    assert result.accepted == [first]
    assert len(result.conflicts) == 1
    assert result.conflicts[0].reason == "duplicate_conflict"
    assert result.conflicts[0].incoming.close == 101.5


def test_normalize_reports_missing_open_times_between_accepted_klines() -> None:
    first = make_kline(1719792000000)
    third = make_kline(1719792120000)

    result = normalize_klines([first, third], interval_ms=60_000)

    assert [gap.missing_open_time_ms for gap in result.gaps] == [1719792060000]
    assert result.contiguous_last_open_time_ms == 1719792000000


def test_normalize_reports_contiguous_last_open_time_for_clean_batch() -> None:
    first = make_kline(1719792000000)
    second = make_kline(1719792060000)

    result = normalize_klines([second, first], interval_ms=60_000)

    assert result.accepted == [first, second]
    assert result.gaps == []
    assert result.contiguous_last_open_time_ms == second.open_time_ms
