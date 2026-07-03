from __future__ import annotations

import asyncio
from contextlib import nullcontext
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
from quant_binance_sync.checkpoints import CheckpointStore, mark_inactive_checkpoints
from quant_binance_sync.rate_limit import AsyncWeightRateLimiter
from quant_binance_sync.storage import ParquetKlineStore
from quant_binance_sync.symbols import SymbolMetadataStore, refresh_symbol_metadata
from quant_binance_sync.sync import ProgressUpdate, estimate_sync_plan, sync_missing_klines

app = typer.Typer(help="Sync Binance USD-M perpetual futures klines with HTTP polling.")


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

    kline_store = ParquetKlineStore(data_dir)
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
                    store=ParquetKlineStore(data_dir),
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
