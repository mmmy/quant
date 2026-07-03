from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import typer
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from quant_binance_sync.archive import BinanceVisionArchiveClient
from quant_binance_sync.client import BinanceFuturesClient
from quant_binance_sync.checkpoints import Checkpoint
from quant_binance_sync.checkpoints import CheckpointStore, mark_inactive_checkpoints
from quant_binance_sync.normalize_existing import NormalizeExistingProgress, normalize_existing_klines
from quant_binance_sync.rate_limit import AsyncWeightRateLimiter
from quant_binance_sync.storage import LatestOpenKlineStore, NormalizedKlineStore, ParquetKlineStore
from quant_binance_sync.stream import StreamResult, StreamUpdate, stream_closed_klines
from quant_binance_sync.symbols import SymbolMetadataStore, refresh_symbol_metadata
from quant_binance_sync.sync import (
    ProgressUpdate,
    estimate_sync_plan,
    interval_to_milliseconds,
    sync_missing_klines,
)

app = typer.Typer(help="Sync Binance USD-M perpetual futures klines with HTTP polling.")


@dataclass(frozen=True)
class StreamProgressEvent:
    kind: str
    symbol: str
    klines: int
    requests: int
    connections: int = 0
    symbols: int | None = None


@app.command("refresh-symbols")
def refresh_symbols(
    meta_dir: Path = typer.Option(Path("data/meta/binance"), help="Symbol metadata directory."),
    max_weight_per_minute: int = typer.Option(900, min=1, help="Binance request weight budget."),
) -> None:
    count = asyncio.run(
        _refresh_symbols(meta_dir=meta_dir, max_weight_per_minute=max_weight_per_minute)
    )
    typer.echo(f"active_symbols={count}")


@app.command("sync-klines")
def sync_klines(
    interval: str = typer.Option("1m", help="Binance kline interval, e.g. 1m, 5m, 1h."),
    bootstrap_days: int = typer.Option(30, min=1, help="Days to sync when no checkpoint exists."),
    limit: int = typer.Option(499, min=1, max=1500, help="Candles per Binance request."),
    data_dir: Path = typer.Option(Path("data/raw/binance/usdm_futures/klines")),
    silver_dir: Path = typer.Option(Path("data/silver/binance/usdm_futures/klines")),
    quarantine_dir: Path = typer.Option(Path("data/quarantine/binance/usdm_futures/klines")),
    gap_report_path: Path = typer.Option(Path("data/reports/binance/usdm_futures/kline_gaps.parquet")),
    meta_dir: Path = typer.Option(Path("data/meta/binance"), help="Symbol metadata directory."),
    state_dir: Path = typer.Option(Path("data/state/binance"), help="Checkpoint state directory."),
    symbol: list[str] | None = typer.Option(None, help="Optional symbol filter, repeatable."),
    concurrency: int = typer.Option(8, min=1, max=50),
    max_weight_per_minute: int = typer.Option(900, min=1, help="Binance request weight budget."),
    progress: bool = typer.Option(True, "--progress/--no-progress", help="Show progress bar."),
    use_archives: bool = typer.Option(
        True,
        "--use-archives/--no-use-archives",
        help="Use data.binance.vision daily ZIP files for older missing days.",
    ),
    archive_threshold_days: int = typer.Option(
        2,
        min=1,
        help="Use archives for UTC days at least this many days before today.",
    ),
    archive_cache_dir: Path = typer.Option(
        Path("data/cache/binance-vision"),
        help="Downloaded Binance public data ZIP cache directory.",
    ),
    archive_concurrency: int = typer.Option(
        4,
        min=1,
        max=50,
        help="Maximum concurrent Binance public data ZIP downloads.",
    ),
) -> None:
    result = asyncio.run(
        _sync_klines(
            interval=interval,
            bootstrap_days=bootstrap_days,
            limit=limit,
            data_dir=data_dir,
            silver_dir=silver_dir,
            quarantine_dir=quarantine_dir,
            gap_report_path=gap_report_path,
            meta_dir=meta_dir,
            state_dir=state_dir,
            symbols=symbol,
            concurrency=concurrency,
            max_weight_per_minute=max_weight_per_minute,
            show_progress=progress,
            use_archives=use_archives,
            archive_threshold_days=archive_threshold_days,
            archive_cache_dir=archive_cache_dir,
            archive_concurrency=archive_concurrency,
        )
    )
    typer.echo(f"symbols_seen={result.symbols_seen} klines_saved={result.klines_saved}")


@app.command("sync-all")
def sync_all(
    interval: str = typer.Option("1m", help="Binance kline interval, e.g. 1m, 5m, 1h."),
    bootstrap_days: int = typer.Option(30, min=1, help="Days to sync when no checkpoint exists."),
    limit: int = typer.Option(499, min=1, max=1500, help="Candles per Binance request."),
    data_dir: Path = typer.Option(Path("data/raw/binance/usdm_futures/klines")),
    silver_dir: Path = typer.Option(Path("data/silver/binance/usdm_futures/klines")),
    quarantine_dir: Path = typer.Option(Path("data/quarantine/binance/usdm_futures/klines")),
    gap_report_path: Path = typer.Option(Path("data/reports/binance/usdm_futures/kline_gaps.parquet")),
    meta_dir: Path = typer.Option(Path("data/meta/binance"), help="Symbol metadata directory."),
    state_dir: Path = typer.Option(Path("data/state/binance"), help="Checkpoint state directory."),
    concurrency: int = typer.Option(8, min=1, max=50),
    max_weight_per_minute: int = typer.Option(900, min=1, help="Binance request weight budget."),
    progress: bool = typer.Option(True, "--progress/--no-progress", help="Show progress bar."),
    use_archives: bool = typer.Option(
        True,
        "--use-archives/--no-use-archives",
        help="Use data.binance.vision daily ZIP files for older missing days.",
    ),
    archive_threshold_days: int = typer.Option(
        2,
        min=1,
        help="Use archives for UTC days at least this many days before today.",
    ),
    archive_cache_dir: Path = typer.Option(
        Path("data/cache/binance-vision"),
        help="Downloaded Binance public data ZIP cache directory.",
    ),
    archive_concurrency: int = typer.Option(
        4,
        min=1,
        max=50,
        help="Maximum concurrent Binance public data ZIP downloads.",
    ),
) -> None:
    active_count, result = asyncio.run(
        _sync_all(
            interval=interval,
            bootstrap_days=bootstrap_days,
            limit=limit,
            data_dir=data_dir,
            silver_dir=silver_dir,
            quarantine_dir=quarantine_dir,
            gap_report_path=gap_report_path,
            meta_dir=meta_dir,
            state_dir=state_dir,
            concurrency=concurrency,
            max_weight_per_minute=max_weight_per_minute,
            show_progress=progress,
            use_archives=use_archives,
            archive_threshold_days=archive_threshold_days,
            archive_cache_dir=archive_cache_dir,
            archive_concurrency=archive_concurrency,
        )
    )
    typer.echo(
        f"active_symbols={active_count} symbols_seen={result.symbols_seen} "
        f"klines_saved={result.klines_saved}"
    )


@app.command("normalize-klines")
def normalize_klines(
    interval: str = typer.Option("1m", help="Binance kline interval, e.g. 1m, 5m, 1h."),
    data_dir: Path = typer.Option(Path("data/raw/binance/usdm_futures/klines")),
    silver_dir: Path = typer.Option(Path("data/silver/binance/usdm_futures/klines")),
    quarantine_dir: Path = typer.Option(Path("data/quarantine/binance/usdm_futures/klines")),
    gap_report_path: Path = typer.Option(Path("data/reports/binance/usdm_futures/kline_gaps.parquet")),
    symbol: list[str] | None = typer.Option(None, help="Optional symbol filter, repeatable."),
    start_date: datetime | None = typer.Option(None, help="Optional UTC date lower bound."),
    end_date: datetime | None = typer.Option(None, help="Optional UTC date upper bound."),
    overwrite: bool = typer.Option(
        True,
        "--overwrite/--append",
        help="Rebuild silver/quarantine/gap report before normalizing.",
    ),
    progress: bool = typer.Option(True, "--progress/--no-progress", help="Show progress bar."),
) -> None:
    progress_context = normalize_progress_bar() if progress else nullcontext(None)
    with progress_context as progress_view:
        callback = (
            make_normalize_progress_callback(progress_view)
            if progress_view is not None
            else None
        )
        result = normalize_existing_klines(
            raw_dir=data_dir,
            silver_dir=silver_dir,
            quarantine_dir=quarantine_dir,
            gap_report_path=gap_report_path,
            interval=interval,
            symbol=symbol,
            start_date=start_date.date() if start_date is not None else None,
            end_date=end_date.date() if end_date is not None else None,
            overwrite=overwrite,
            progress_callback=callback,
        )
    typer.echo(
        f"files_seen={result.files_seen} raw_klines_seen={result.raw_klines_seen} "
        f"accepted_klines={result.accepted_klines} rejected_klines={result.rejected_klines} "
        f"conflict_klines={result.conflict_klines} gaps_seen={result.gaps_seen}"
    )


def normalize_progress_bar() -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TextColumn("files={task.fields[files]}"),
        TextColumn("raw={task.fields[raw]}"),
        TextColumn("accepted={task.fields[accepted]}"),
        TextColumn("rejected={task.fields[rejected]}"),
        TextColumn("conflicts={task.fields[conflicts]}"),
        TextColumn("gaps={task.fields[gaps]}"),
        TextColumn("current={task.fields[current]}"),
        TimeRemainingColumn(),
        TimeElapsedColumn(),
    )


def make_normalize_progress_callback(
    progress: Progress,
) -> Callable[[NormalizeExistingProgress], None]:
    task_id = progress.add_task(
        "normalize klines",
        total=None,
        files="0/0",
        raw=0,
        accepted=0,
        rejected=0,
        conflicts=0,
        gaps=0,
        current="-",
    )

    def callback(event: NormalizeExistingProgress) -> None:
        progress.update(
            task_id,
            total=event.total_files,
            completed=event.files_seen,
            files=f"{event.files_seen}/{event.total_files}",
            raw=event.raw_klines_seen,
            accepted=event.accepted_klines,
            rejected=event.rejected_klines,
            conflicts=event.conflict_klines,
            gaps=event.gaps_seen,
            current=event.current,
        )

    return callback


@app.command("stream-klines")
def stream_klines(
    interval: str = typer.Option("1m", help="Binance kline interval, e.g. 1m, 5m, 1h."),
    data_dir: Path = typer.Option(Path("data/raw/binance/usdm_futures/klines")),
    silver_dir: Path = typer.Option(Path("data/silver/binance/usdm_futures/klines")),
    quarantine_dir: Path = typer.Option(Path("data/quarantine/binance/usdm_futures/klines")),
    gap_report_path: Path = typer.Option(Path("data/reports/binance/usdm_futures/kline_gaps.parquet")),
    realtime_dir: Path = typer.Option(Path("data/realtime/binance/usdm_futures/open_klines")),
    meta_dir: Path = typer.Option(Path("data/meta/binance"), help="Symbol metadata directory."),
    state_dir: Path = typer.Option(Path("data/state/binance"), help="Checkpoint state directory."),
    symbol: list[str] | None = typer.Option(None, help="Optional symbol filter, repeatable."),
    streams_per_connection: int = typer.Option(
        200,
        min=1,
        max=1024,
        help="Maximum kline streams per websocket connection.",
    ),
    bootstrap_days: int = typer.Option(
        1,
        min=1,
        help="Days to REST-sync when a streamed symbol has no checkpoint.",
    ),
    limit: int = typer.Option(499, min=1, max=1500, help="Candles per REST gap-fill request."),
    concurrency: int = typer.Option(8, min=1, max=50),
    max_weight_per_minute: int = typer.Option(900, min=1, help="Binance REST request weight budget."),
    startup_gap_fill: bool = typer.Option(
        True,
        "--startup-gap-fill/--no-startup-gap-fill",
        help="REST-sync missing closed klines in the background after websocket streams open.",
    ),
    progress: bool = typer.Option(True, "--progress/--no-progress", help="Show stream stats."),
    reconnect_delay_seconds: float = typer.Option(5.0, min=0.0, help="Delay before reconnecting."),
    once: bool = typer.Option(False, "--once", help="Exit after one websocket disconnect."),
) -> None:
    progress_context = stream_progress_bar() if progress else nullcontext(None)
    with progress_context as progress_view:
        callback = (
            make_stream_progress_callback(
                progress_view,
                symbol_count=len(symbol) if symbol is not None else None,
            )
            if progress_view is not None
            else None
        )
        result = asyncio.run(
            _stream_klines(
                interval=interval,
                data_dir=data_dir,
                silver_dir=silver_dir,
                quarantine_dir=quarantine_dir,
                gap_report_path=gap_report_path,
                realtime_dir=realtime_dir,
                meta_dir=meta_dir,
                state_dir=state_dir,
                symbol=symbol,
                streams_per_connection=streams_per_connection,
                bootstrap_days=bootstrap_days,
                limit=limit,
                concurrency=concurrency,
                max_weight_per_minute=max_weight_per_minute,
                startup_gap_fill=startup_gap_fill,
                reconnect_delay_seconds=reconnect_delay_seconds,
                once=once,
                progress_callback=callback,
            )
        )
    typer.echo(f"connections_seen={result.connections_seen} klines_saved={result.klines_saved}")


async def _refresh_symbols(*, meta_dir: Path, max_weight_per_minute: int) -> int:
    store = SymbolMetadataStore(meta_dir)
    rate_limiter = AsyncWeightRateLimiter(max_weight_per_minute=max_weight_per_minute)
    async with BinanceFuturesClient(rate_limiter=rate_limiter) as client:
        active_symbols = await refresh_symbol_metadata(client=client, store=store)
    return len(active_symbols)


async def _sync_klines(
    *,
    interval: str,
    bootstrap_days: int,
    limit: int,
    data_dir: Path,
    silver_dir: Path | None,
    quarantine_dir: Path,
    gap_report_path: Path,
    meta_dir: Path,
    state_dir: Path,
    symbols: list[str] | None,
    concurrency: int,
    max_weight_per_minute: int,
    show_progress: bool,
    use_archives: bool,
    archive_threshold_days: int,
    archive_cache_dir: Path,
    archive_concurrency: int,
):
    metadata_store = SymbolMetadataStore(meta_dir)
    active_symbols = [symbol.symbol for symbol in metadata_store.load_current_symbols()]
    selected_symbols = symbols or active_symbols

    checkpoint_store = CheckpointStore(state_dir / "usdm_kline_checkpoints.json")
    checkpoints = checkpoint_store.load()
    mark_inactive_checkpoints(checkpoints, active_symbols=active_symbols, interval=interval)

    kline_store = make_normalized_kline_store(
        data_dir=data_dir,
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir,
        gap_report_path=gap_report_path,
        interval=interval,
    )
    rate_limiter = AsyncWeightRateLimiter(max_weight_per_minute=max_weight_per_minute)
    plan = estimate_sync_plan(
        symbols=selected_symbols,
        checkpoints=checkpoints,
        interval=interval,
        bootstrap_days=bootstrap_days,
        limit=limit,
        use_archives=use_archives,
        archive_threshold_days=archive_threshold_days,
    )
    progress_context = progress_bar(plan.total_batches) if show_progress else nullcontext(None)
    with progress_context as progress:
        callback = (
            make_progress_callback(progress, total_batches=plan.total_batches)
            if progress is not None
            else None
        )
        archive_context = (
            BinanceVisionArchiveClient(cache_dir=archive_cache_dir)
            if use_archives
            else nullcontext(None)
        )
        async with BinanceFuturesClient(rate_limiter=rate_limiter) as client:
            async with archive_context as archive_client:
                result = await sync_missing_klines(
                    client=client,
                    store=kline_store,
                    checkpoints=checkpoints,
                    interval=interval,
                    bootstrap_days=bootstrap_days,
                    limit=limit,
                    symbols=selected_symbols,
                    concurrency=concurrency,
                    progress_callback=callback,
                    archive_client=archive_client,
                    archive_threshold_days=archive_threshold_days,
                    archive_concurrency=archive_concurrency,
                    checkpoint_callback=checkpoint_store.save,
                )
    checkpoint_store.save(checkpoints)
    return result


async def _sync_all(
    *,
    interval: str,
    bootstrap_days: int,
    limit: int,
    data_dir: Path,
    silver_dir: Path | None,
    quarantine_dir: Path,
    gap_report_path: Path,
    meta_dir: Path,
    state_dir: Path,
    concurrency: int,
    max_weight_per_minute: int,
    show_progress: bool,
    use_archives: bool,
    archive_threshold_days: int,
    archive_cache_dir: Path,
    archive_concurrency: int,
):
    metadata_store = SymbolMetadataStore(meta_dir)
    rate_limiter = AsyncWeightRateLimiter(max_weight_per_minute=max_weight_per_minute)
    async with BinanceFuturesClient(rate_limiter=rate_limiter) as client:
        active = await refresh_symbol_metadata(client=client, store=metadata_store)
        active_symbols = [symbol.symbol for symbol in active]

        checkpoint_store = CheckpointStore(state_dir / "usdm_kline_checkpoints.json")
        checkpoints = checkpoint_store.load()
        mark_inactive_checkpoints(checkpoints, active_symbols=active_symbols, interval=interval)

        plan = estimate_sync_plan(
            symbols=active_symbols,
            checkpoints=checkpoints,
            interval=interval,
            bootstrap_days=bootstrap_days,
            limit=limit,
            use_archives=use_archives,
            archive_threshold_days=archive_threshold_days,
        )
        progress_context = progress_bar(plan.total_batches) if show_progress else nullcontext(None)
        with progress_context as progress:
            callback = (
                make_progress_callback(progress, total_batches=plan.total_batches)
                if progress is not None
                else None
            )
            archive_context = (
                BinanceVisionArchiveClient(cache_dir=archive_cache_dir)
                if use_archives
                else nullcontext(None)
            )
            async with archive_context as archive_client:
                result = await sync_missing_klines(
                    client=client,
                    store=make_normalized_kline_store(
                        data_dir=data_dir,
                        silver_dir=silver_dir,
                        quarantine_dir=quarantine_dir,
                        gap_report_path=gap_report_path,
                        interval=interval,
                    ),
                    checkpoints=checkpoints,
                    interval=interval,
                    bootstrap_days=bootstrap_days,
                    limit=limit,
                    symbols=active_symbols,
                    concurrency=concurrency,
                    progress_callback=callback,
                    archive_client=archive_client,
                    archive_threshold_days=archive_threshold_days,
                    archive_concurrency=archive_concurrency,
                    checkpoint_callback=checkpoint_store.save,
                )

    checkpoint_store.save(checkpoints)
    return len(active_symbols), result


async def _stream_klines(
    *,
    interval: str,
    data_dir: Path,
    silver_dir: Path | None = None,
    quarantine_dir: Path | None = None,
    gap_report_path: Path | None = None,
    realtime_dir: Path | None = None,
    meta_dir: Path,
    state_dir: Path,
    symbol: list[str] | None,
    streams_per_connection: int,
    max_weight_per_minute: int,
    startup_gap_fill: bool = True,
    reconnect_delay_seconds: float,
    once: bool,
    bootstrap_days: int = 1,
    limit: int = 499,
    concurrency: int = 8,
    progress_callback: Callable[[StreamProgressEvent], None] | None = None,
) -> StreamResult:
    metadata_store = SymbolMetadataStore(meta_dir)
    active_symbols = [item.symbol for item in metadata_store.load_current_symbols()]
    selected_symbols = symbol or active_symbols
    if progress_callback is not None:
        progress_callback(
            StreamProgressEvent(
                kind="metadata",
                symbol="-",
                klines=0,
                requests=0,
                symbols=len(selected_symbols),
            )
        )

    checkpoint_store = CheckpointStore(state_dir / "usdm_kline_checkpoints.json")
    checkpoints = checkpoint_store.load()
    mark_inactive_checkpoints(checkpoints, active_symbols=active_symbols, interval=interval)
    checkpoint_store.save(checkpoints)

    kline_store = make_stream_kline_store(
        data_dir=data_dir,
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir,
        gap_report_path=gap_report_path,
        interval=interval,
    )
    open_kline_store = (
        LatestOpenKlineStore(realtime_dir)
        if realtime_dir is not None
        else None
    )
    rate_limiter = AsyncWeightRateLimiter(max_weight_per_minute=max_weight_per_minute)

    async def gap_sync_with_checkpoints(
        symbols: list[str],
        gap_checkpoints: dict[str, Checkpoint],
        checkpoint_callback,
    ) -> None:
        async with BinanceFuturesClient(rate_limiter=rate_limiter) as client:
            await sync_missing_klines(
                client=client,
                store=kline_store,
                checkpoints=gap_checkpoints,
                symbols=symbols,
                interval=interval,
                bootstrap_days=bootstrap_days,
                limit=limit,
                concurrency=concurrency,
                progress_callback=make_rest_stream_progress_callback(progress_callback),
                checkpoint_callback=checkpoint_callback,
            )

    async def gap_sync(symbols: list[str]) -> None:
        await gap_sync_with_checkpoints(
            symbols=symbols,
            gap_checkpoints=checkpoints,
            checkpoint_callback=checkpoint_store.save,
        )

    startup_task: asyncio.Task[None] | None = None
    if startup_gap_fill:
        startup_checkpoints = dict(checkpoints)
        startup_task = asyncio.create_task(
            gap_sync_with_checkpoints(
                symbols=selected_symbols,
                gap_checkpoints=startup_checkpoints,
                checkpoint_callback=make_merging_checkpoint_callback(
                    live_checkpoints=checkpoints,
                    save=checkpoint_store.save,
                ),
            )
        )

    total = StreamResult(connections_seen=0, klines_saved=0)
    while True:
        connection_count = (len(selected_symbols) + streams_per_connection - 1) // streams_per_connection
        if progress_callback is not None:
            progress_callback(
                StreamProgressEvent(
                    kind="connection_batch",
                    symbol="-",
                    klines=0,
                    requests=0,
                    connections=connection_count,
                )
            )
        result = await stream_closed_klines(
            symbols=selected_symbols,
            interval=interval,
            store=kline_store,
            checkpoints=checkpoints,
            streams_per_connection=streams_per_connection,
            checkpoint_callback=checkpoint_store.save,
            gap_sync_callback=gap_sync,
            progress_callback=make_ws_stream_progress_callback(progress_callback),
            open_kline_store=open_kline_store,
        )
        total = StreamResult(
            connections_seen=total.connections_seen + result.connections_seen,
            klines_saved=total.klines_saved + result.klines_saved,
        )
        if once:
            if startup_task is not None:
                await startup_task
            return total
        await asyncio.sleep(reconnect_delay_seconds)


def make_stream_kline_store(
    *,
    data_dir: Path,
    silver_dir: Path | None,
    quarantine_dir: Path | None = None,
    gap_report_path: Path | None = None,
    interval: str,
):
    if silver_dir is None:
        return ParquetKlineStore(data_dir)
    return make_normalized_kline_store(
        data_dir=data_dir,
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir or data_dir.parent / "quarantine",
        gap_report_path=gap_report_path or data_dir.parent / "kline_gaps.parquet",
        interval=interval,
    )


def make_normalized_kline_store(
    *,
    data_dir: Path,
    silver_dir: Path | None,
    quarantine_dir: Path,
    gap_report_path: Path,
    interval: str,
):
    raw_store = ParquetKlineStore(data_dir)
    if silver_dir is None:
        return raw_store
    return NormalizedKlineStore(
        raw=raw_store,
        silver=ParquetKlineStore(silver_dir),
        quarantine=ParquetKlineStore(quarantine_dir),
        gap_report_path=gap_report_path,
        interval_ms=interval_to_milliseconds(interval),
    )


def make_merging_checkpoint_callback(
    *,
    live_checkpoints: dict[str, Checkpoint],
    save: Callable[[dict[str, Checkpoint]], None],
) -> Callable[[dict[str, Checkpoint]], None]:
    def callback(incoming: dict[str, Checkpoint]) -> None:
        for key, checkpoint in incoming.items():
            current = live_checkpoints.get(key)
            if (
                current is None
                or current.last_open_time_ms is None
                or (
                    checkpoint.last_open_time_ms is not None
                    and checkpoint.last_open_time_ms >= current.last_open_time_ms
                )
            ):
                live_checkpoints[key] = checkpoint
        save(live_checkpoints)

    return callback


def make_ws_stream_progress_callback(
    progress_callback: Callable[[StreamProgressEvent], None] | None,
) -> Callable[[StreamUpdate], None] | None:
    if progress_callback is None:
        return None

    def callback(update: StreamUpdate) -> None:
        if update.kline_saved:
            progress_callback(
                StreamProgressEvent(
                    kind="ws_kline",
                    symbol=update.symbol,
                    klines=1,
                    requests=0,
                )
            )

    return callback


def make_rest_stream_progress_callback(
    progress_callback: Callable[[StreamProgressEvent], None] | None,
) -> Callable[[ProgressUpdate], None] | None:
    if progress_callback is None:
        return None

    def callback(update: ProgressUpdate) -> None:
        if update.request_completed:
            progress_callback(
                StreamProgressEvent(
                    kind="rest_batch",
                    symbol=update.symbol,
                    klines=update.klines_saved,
                    requests=1,
                )
            )

    return callback


def stream_progress_bar() -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TextColumn("symbols={task.fields[symbols]}"),
        TextColumn("conns={task.fields[connections]}"),
        TextColumn("ws={task.fields[ws_klines]}"),
        TextColumn("rest={task.fields[rest_klines]}"),
        TextColumn("requests={task.fields[rest_requests]}"),
        TextColumn("current={task.fields[current]}"),
        TimeElapsedColumn(),
    )


def make_stream_progress_callback(
    progress: Progress,
    *,
    symbol_count: int | None,
) -> Callable[[StreamProgressEvent], None]:
    task_id = progress.add_task(
        "stream klines",
        total=None,
        symbols=symbol_count if symbol_count is not None else "-",
        connections="-",
        ws_klines=0,
        rest_klines=0,
        rest_requests=0,
        current="-",
    )
    state = {
        "ws_klines": 0,
        "rest_klines": 0,
        "rest_requests": 0,
        "current": "-",
        "connections": "-",
        "symbols": symbol_count if symbol_count is not None else "-",
    }

    def callback(event: StreamProgressEvent) -> None:
        if event.kind == "ws_kline":
            state["ws_klines"] += event.klines
        elif event.kind == "rest_batch":
            state["rest_klines"] += event.klines
            state["rest_requests"] += event.requests
        elif event.kind == "connection_batch":
            state["connections"] = event.connections
        elif event.kind == "metadata" and event.symbols is not None:
            state["symbols"] = event.symbols
        state["current"] = event.symbol
        progress.update(
            task_id,
            symbols=state["symbols"],
            connections=state["connections"],
            ws_klines=state["ws_klines"],
            rest_klines=state["rest_klines"],
            rest_requests=state["rest_requests"],
            current=state["current"],
        )

    return callback


def progress_bar(total_batches: int) -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn("requests={task.fields[requests]}"),
        TextColumn("klines={task.fields[klines]}"),
        TextColumn("current={task.fields[current]}"),
        TimeRemainingColumn(),
        TimeElapsedColumn(),
    )


def make_progress_callback(progress: Progress, *, total_batches: int):
    task_id = progress.add_task(
        "sync klines",
        total=max(1, total_batches),
        requests=0,
        klines=0,
        current="-",
    )
    state = {"requests": 0, "klines": 0}

    def callback(update: ProgressUpdate) -> None:
        advance = 0
        if update.request_completed:
            state["requests"] += 1
            state["klines"] += update.klines_saved
            advance = 1
        progress.update(
            task_id,
            advance=advance,
            requests=state["requests"],
            klines=state["klines"],
            current=update.symbol,
        )

    return callback
