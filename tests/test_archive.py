from datetime import UTC, date, datetime
from io import BytesIO
from zipfile import ZipFile

import httpx
import pytest
import respx

from quant_binance_sync.archive import (
    ArchiveMissing,
    BinanceVisionArchiveClient,
    daily_kline_url,
    parse_daily_kline_zip,
)


def make_zip(csv_text: str, name: str = "BTCUSDT-1m-2024-07-01.csv") -> bytes:
    buffer = BytesIO()
    with ZipFile(buffer, "w") as archive:
        archive.writestr(name, csv_text)
    return buffer.getvalue()


def test_daily_kline_url_uses_usdm_futures_public_data_path() -> None:
    assert daily_kline_url("BTCUSDT", "1m", date(2024, 7, 1)) == (
        "https://data.binance.vision/data/futures/um/daily/klines/"
        "BTCUSDT/1m/BTCUSDT-1m-2024-07-01.zip"
    )


def test_parse_daily_kline_zip_with_header() -> None:
    payload = make_zip(
        "open_time,open,high,low,close,volume,close_time,quote_volume,count,"
        "taker_buy_volume,taker_buy_quote_volume,ignore\n"
        "1719792000000,100,101,99,100.5,1,1719792059999,100.5,7,0.5,50.25,0\n"
    )

    klines = parse_daily_kline_zip("BTCUSDT", "1m", payload)

    assert len(klines) == 1
    assert klines[0].symbol == "BTCUSDT"
    assert klines[0].open_time == datetime(2024, 7, 1, 0, 0, tzinfo=UTC)
    assert klines[0].close == 100.5


@pytest.mark.asyncio
@respx.mock
async def test_archive_client_downloads_daily_zip_and_raises_on_404(tmp_path) -> None:
    url = daily_kline_url("BTCUSDT", "1m", date(2024, 7, 1))
    respx.get(url).mock(return_value=httpx.Response(404))
    client = BinanceVisionArchiveClient(cache_dir=tmp_path)

    with pytest.raises(ArchiveMissing):
        await client.daily_klines(symbol="BTCUSDT", interval="1m", day=date(2024, 7, 1))


@pytest.mark.asyncio
@respx.mock
async def test_archive_client_retries_transient_connect_errors(tmp_path) -> None:
    url = daily_kline_url("BTCUSDT", "1m", date(2024, 7, 1))
    route = respx.get(url).mock(
        side_effect=[
            httpx.ConnectError("temporary failure"),
            httpx.Response(
                200,
                content=make_zip(
                    "open_time,open,high,low,close,volume,close_time,quote_volume,count,"
                    "taker_buy_volume,taker_buy_quote_volume,ignore\n"
                    "1719792000000,100,101,99,100.5,1,1719792059999,100.5,7,0.5,50.25,0\n"
                ),
            ),
        ]
    )
    sleeps: list[float] = []
    client = BinanceVisionArchiveClient(
        cache_dir=tmp_path,
        max_retries=1,
        sleep=lambda seconds: sleeps.append(seconds),
    )

    klines = await client.daily_klines(symbol="BTCUSDT", interval="1m", day=date(2024, 7, 1))

    assert route.call_count == 2
    assert sleeps == [1.0]
    assert len(klines) == 1
