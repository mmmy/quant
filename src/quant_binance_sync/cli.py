from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import nullcontext, suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from time import monotonic

import polars as pl
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
from quant_binance_sync.backtest import run_backtest, summarize_equity
from quant_binance_sync.client import BinanceFuturesClient
from quant_binance_sync.checkpoints import Checkpoint
from quant_binance_sync.checkpoints import CheckpointStore, mark_inactive_checkpoints
from quant_binance_sync.features import build_features
from quant_binance_sync.normalize_existing import NormalizeExistingProgress, normalize_existing_klines
from quant_binance_sync.rate_limit import AsyncWeightRateLimiter
from quant_binance_sync.relative_strength import build_relative_strength, relative_strength_source_interval
from quant_binance_sync.relative_strength_presets import (
    resolve_relative_strength_preset_timeframe,
    resolve_relative_strength_presets,
)
from quant_binance_sync.signals import build_signals
from quant_binance_sync.storage import (
    BufferedKlineStore,
    BufferedKlineStoreStats,
    InMemoryOpenKlineStore,
    LatestOpenKlineStore,
    NormalizedKlineStore,
    ParquetKlineStore,
    SqliteKlineHotStore,
)
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
    sql_buffer_klines: int = 0
    sqlite_flushes: int = 0
    pending_klines: int = 0
    parquet_flushes: int = 0
    flush_size: int = 0


class ThrottledCheckpointCallback:
    def __init__(
        self,
        *,
        save: Callable[[dict[str, Checkpoint]], None],
        min_interval_seconds: float = 30.0,
        monotonic: Callable[[], float] = monotonic,
    ) -> None:
        self._save = save
        self._min_interval_seconds = min_interval_seconds
        self._monotonic = monotonic
        self._last_save_at: float | None = None
        self._pending: dict[str, Checkpoint] | None = None
        self._dirty = False

    def __call__(self, checkpoints: dict[str, Checkpoint]) -> None:
        self._pending = dict(checkpoints)
        now = self._monotonic()
        if self._last_save_at is None or now - self._last_save_at >= self._min_interval_seconds:
            self._save_pending(now)
            return
        self._dirty = True

    def flush(self) -> None:
        if self._dirty and self._pending is not None:
            self._save_pending(self._monotonic())

    def _save_pending(self, now: float) -> None:
        if self._pending is None:
            return
        self._save(self._pending)
        self._last_save_at = now
        self._dirty = False


def load_interval_checkpoint_store(
    state_dir: Path,
    *,
    interval: str,
) -> tuple[CheckpointStore, dict[str, Checkpoint]]:
    store = CheckpointStore(state_dir / f"usdm_kline_checkpoints_{interval}.json")
    checkpoints = store.load()
    if checkpoints:
        return store, checkpoints

    legacy_checkpoints = CheckpointStore(state_dir / "usdm_kline_checkpoints.json").load()
    suffix = f"|{interval}"
    return store, {
        key: checkpoint
        for key, checkpoint in legacy_checkpoints.items()
        if key.endswith(suffix)
    }


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


@app.command("build-features")
def build_features_command(
    silver_dir: Path = typer.Option(Path("data/silver/binance/usdm_futures/klines")),
    output_dir: Path = typer.Option(Path("data/gold/binance/usdm_futures/features")),
    base_interval: str = typer.Option("1m", help="Source kline interval."),
    feature_interval: str = typer.Option("1h", help="Feature bar interval."),
    realtime_closed_db_path: Path | None = typer.Option(
        None,
        help="Optional SQLite closed-kline hot store to overlay before building features.",
    ),
    symbol: list[str] | None = typer.Option(None, help="Optional symbol filter, repeatable."),
) -> None:
    result = build_features(
        silver_dir=silver_dir,
        output_dir=output_dir,
        base_interval=base_interval,
        feature_interval=feature_interval,
        realtime_closed_db_path=realtime_closed_db_path,
        symbol=symbol,
    )
    typer.echo(f"rows_written={result.rows_written} files_written={result.files_written}")


@app.command("build-signals")
def build_signals_command(
    features_dir: Path = typer.Option(Path("data/gold/binance/usdm_futures/features")),
    output_dir: Path = typer.Option(Path("data/gold/binance/usdm_futures/signals")),
    feature_interval: str = typer.Option("1h", help="Feature bar interval."),
    top_n: int = typer.Option(10, min=1, help="Number of symbols to select per rebalance time."),
) -> None:
    result = build_signals(
        features_dir=features_dir,
        output_dir=output_dir,
        feature_interval=feature_interval,
        top_n=top_n,
    )
    typer.echo(f"rows_written={result.rows_written} files_written={result.files_written}")


@app.command("backtest-signals")
def backtest_signals_command(
    signals_path: Path = typer.Option(
        Path("data/gold/binance/usdm_futures/signals/interval=1h/signals.parquet")
    ),
    features_dir: Path = typer.Option(Path("data/gold/binance/usdm_futures/features")),
    output_path: Path = typer.Option(
        Path("data/gold/binance/usdm_futures/backtests/interval=1h/equity.parquet")
    ),
    feature_interval: str = typer.Option("1h", help="Feature bar interval."),
    fee_rate: float = typer.Option(0.0, min=0.0, help="One-way fee rate, e.g. 0.0004."),
    slippage_rate: float = typer.Option(0.0, min=0.0, help="One-way slippage rate."),
) -> None:
    result = run_backtest(
        signals_path=signals_path,
        features_dir=features_dir,
        output_path=output_path,
        feature_interval=feature_interval,
        fee_rate=fee_rate,
        slippage_rate=slippage_rate,
    )
    typer.echo(f"rows_written={result.rows_written} final_equity={result.final_equity}")


@app.command("show-signals")
def show_signals_command(
    signals_path: Path = typer.Option(
        Path("data/gold/binance/usdm_futures/signals/interval=1h/signals.parquet")
    ),
    latest: bool = typer.Option(True, "--latest/--all", help="Show only the latest rebalance time."),
    limit: int = typer.Option(20, min=1, help="Maximum rows to print."),
) -> None:
    if not signals_path.exists():
        typer.echo(f"missing_signals={signals_path}")
        return
    frame = pl.read_parquet(signals_path)
    if latest and not frame.is_empty():
        latest_time = frame.select(pl.col("feature_available_time_ms").max()).item()
        frame = frame.filter(pl.col("feature_available_time_ms") == latest_time)
    view = frame.sort(["feature_available_time_ms", "rank"]).head(limit)
    typer.echo(view)


@app.command("backtest-report")
def backtest_report_command(
    equity_path: Path = typer.Option(
        Path("data/gold/binance/usdm_futures/backtests/interval=1h/equity.parquet")
    ),
    periods_per_year: int = typer.Option(365 * 24, min=1),
) -> None:
    if not equity_path.exists():
        typer.echo(f"missing_equity={equity_path}")
        return
    report = summarize_equity(pl.read_parquet(equity_path), periods_per_year=periods_per_year)
    for key, value in report.items():
        typer.echo(f"{key}={value}")


@app.command("build-relative-strength")
def build_relative_strength_command(
    silver_dir: Path = typer.Option(Path("data/silver/binance/usdm_futures/klines")),
    output_dir: Path = typer.Option(Path("data/gold/binance/usdm_futures/relative_strength")),
    tf: str = typer.Option("15", help="Target timeframe: 1, 15, 60, 240, D, etc."),
    btc_symbol: str = typer.Option("BTCUSDT"),
    max_abs_gap_atr: float = typer.Option(2.5, min=0.0),
    max_ret: float | None = typer.Option(None, min=0.0),
    tail_bars: int | None = typer.Option(None, min=1, help="Only write the latest N target bars."),
    warmup_bars: int = typer.Option(50, min=0, help="Extra target bars used for EMA/ATR warmup."),
    liquidity_top_n: int | None = typer.Option(None, min=1),
    liquidity_lookback_bars: int = typer.Option(20, min=1),
    realtime_closed_db_path: Path | None = typer.Option(
        None,
        help="Optional SQLite closed-kline hot store to overlay before building relative strength.",
    ),
) -> None:
    result = build_relative_strength(
        silver_dir=silver_dir,
        output_dir=output_dir,
        tf=tf,
        btc_symbol=btc_symbol,
        max_abs_gap_atr=max_abs_gap_atr,
        max_ret=max_ret,
        tail_bars=tail_bars,
        warmup_bars=warmup_bars,
        liquidity_top_n=liquidity_top_n,
        liquidity_lookback_bars=liquidity_lookback_bars,
        realtime_closed_db_path=realtime_closed_db_path,
    )
    typer.echo(f"rows_written={result.rows_written} files_written={result.files_written}")


@app.command("build-relative-strength-presets")
def build_relative_strength_presets_command(
    preset: str = typer.Option("intraday", help="Preset group: scalp, intraday, swing, all."),
    tf: str | None = typer.Option(None, help="Build one configured timeframe instead of a group."),
    silver_dir: Path = typer.Option(Path("data/silver/binance/usdm_futures/klines")),
    output_dir: Path = typer.Option(Path("data/gold/binance/usdm_futures/relative_strength")),
    state_dir: Path = typer.Option(Path("data/state/binance"), help="Checkpoint and hot-store state directory."),
    realtime_closed_db_path: Path | None = typer.Option(
        None,
        help="Optional SQLite closed-kline hot store; defaults per source interval when omitted.",
    ),
    btc_symbol: str = typer.Option("BTCUSDT"),
    config: Path | None = typer.Option(None, help="Optional TOML preset config file."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print resolved params without writing."),
) -> None:
    try:
        presets = (
            [resolve_relative_strength_preset_timeframe(tf, config_path=config)]
            if tf is not None
            else resolve_relative_strength_presets(preset, config_path=config)
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--tf" if tf is not None else "--preset") from exc

    for item in presets:
        if dry_run:
            typer.echo(
                f"tf={item.tf} tail_bars={item.tail_bars} warmup_bars={item.warmup_bars} "
                f"liquidity_top_n={item.liquidity_top_n} "
                f"liquidity_lookback_bars={item.liquidity_lookback_bars} "
                f"max_abs_gap_atr={item.max_abs_gap_atr:g} max_ret={item.max_ret:g}"
            )
            continue
        result = build_relative_strength(
            silver_dir=silver_dir,
            output_dir=output_dir,
            tf=item.tf,
            btc_symbol=btc_symbol,
            max_abs_gap_atr=item.max_abs_gap_atr,
            max_ret=item.max_ret,
            tail_bars=item.tail_bars,
            warmup_bars=item.warmup_bars,
            liquidity_top_n=item.liquidity_top_n,
            liquidity_lookback_bars=item.liquidity_lookback_bars,
            realtime_closed_db_path=(
                realtime_closed_db_path
                or relative_strength_realtime_db_path(state_dir=state_dir, tf=item.tf)
            ),
        )
        typer.echo(
            f"tf={item.tf} rows_written={result.rows_written} files_written={result.files_written}"
        )


def relative_strength_realtime_db_path(*, state_dir: Path, tf: str) -> Path:
    source_interval = relative_strength_source_interval(tf)
    return state_dir / f"stream_closed_klines_{source_interval}.sqlite"


@app.command("show-relative-strength")
def show_relative_strength_command(
    path: Path | None = typer.Option(None),
    relative_strength_dir: Path = typer.Option(
        Path("data/gold/binance/usdm_futures/relative_strength")
    ),
    tf: str = typer.Option("15", help="Target timeframe used when --path is not supplied."),
    side: str = typer.Option("strong", help="strong or weak"),
    top_n: int = typer.Option(10, min=1),
    rank_pct: float = typer.Option(0.2, min=0.0, max=1.0),
    include_overheated: bool = typer.Option(False, "--include-overheated"),
    latest: bool = typer.Option(True, "--latest/--all"),
    limit: int = typer.Option(20, min=1),
) -> None:
    if path is None:
        path = relative_strength_dir / f"tf={tf}" / "relative_strength.parquet"
    if not path.exists():
        typer.echo(f"missing_relative_strength={path}")
        return
    frame = pl.read_parquet(path)
    if latest and not frame.is_empty():
        latest_time = frame.select(pl.col("ts_ms").max()).item()
        frame = frame.filter(pl.col("ts_ms") == latest_time)
    rank_col = "strong_rank" if side == "strong" else "weak_rank"
    if not include_overheated and "is_overheated" in frame.columns:
        frame = frame.filter(~pl.col("is_overheated"))
    frame = frame.with_columns(
        pl.col(rank_col)
        .rank(descending=False)
        .over("ts_ms")
        .cast(pl.Int64)
        .alias("_display_rank")
    )
    universe_count = frame.height
    max_rank = max(1, int(universe_count * rank_pct)) if rank_pct < 1.0 else universe_count
    row_limit = min(limit, top_n)
    view = (
        frame.filter(pl.col("_display_rank").is_not_null())
        .filter(pl.col("_display_rank") <= max_rank)
        .sort("_display_rank")
        .head(row_limit)
        .select(["ts_ms", "symbol", "rs_ret", "rs_gap", rank_col, "is_overheated"])
    )
    typer.echo(view)


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
    realtime_dir: Path | None = typer.Option(
        None,
        help="Optional directory for persisting unclosed latest-candle snapshots.",
    ),
    realtime_closed_db_path: Path | None = typer.Option(
        None,
        help="SQLite path for closed streamed klines before Parquet compaction.",
    ),
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
    stream_flush_size: int = typer.Option(
        1000,
        min=1,
        help="Closed websocket klines to batch before flushing to Parquet.",
    ),
    hot_flush_size: int = typer.Option(
        100,
        min=1,
        help="Closed websocket klines to batch before flushing to SQLite.",
    ),
    hot_flush_interval_seconds: float = typer.Option(
        0.5,
        min=0.01,
        help="Maximum seconds to keep closed websocket klines in memory before SQLite flush.",
    ),
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
                realtime_closed_db_path=(
                    realtime_closed_db_path
                    or state_dir / f"stream_closed_klines_{interval}.sqlite"
                ),
                meta_dir=meta_dir,
                state_dir=state_dir,
                symbol=symbol,
                streams_per_connection=streams_per_connection,
                bootstrap_days=bootstrap_days,
                limit=limit,
                concurrency=concurrency,
                stream_flush_size=stream_flush_size,
                hot_flush_size=hot_flush_size,
                hot_flush_interval_seconds=hot_flush_interval_seconds,
                max_weight_per_minute=max_weight_per_minute,
                startup_gap_fill=startup_gap_fill,
                reconnect_delay_seconds=reconnect_delay_seconds,
                once=once,
                progress_callback=callback,
                shutdown_callback=typer.echo,
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

    checkpoint_store, checkpoints = load_interval_checkpoint_store(state_dir, interval=interval)
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
    checkpoint_callback = ThrottledCheckpointCallback(save=checkpoint_store.save)
    try:
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
                        checkpoint_callback=checkpoint_callback,
                    )
    finally:
        checkpoint_callback.flush()
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

        checkpoint_store, checkpoints = load_interval_checkpoint_store(state_dir, interval=interval)
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
        checkpoint_callback = ThrottledCheckpointCallback(save=checkpoint_store.save)
        try:
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
                        checkpoint_callback=checkpoint_callback,
                    )
        finally:
            checkpoint_callback.flush()
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
    realtime_closed_db_path: Path | None = None,
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
    stream_flush_size: int = 1000,
    hot_flush_size: int = 100,
    hot_flush_interval_seconds: float = 0.5,
    progress_callback: Callable[[StreamProgressEvent], None] | None = None,
    shutdown_callback: Callable[[str], None] | None = None,
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

    checkpoint_store, checkpoints = load_interval_checkpoint_store(state_dir, interval=interval)
    mark_inactive_checkpoints(checkpoints, active_symbols=active_symbols, interval=interval)
    checkpoint_store.save(checkpoints)
    checkpoint_callback = ThrottledCheckpointCallback(save=checkpoint_store.save)

    kline_store = make_stream_kline_store(
        data_dir=data_dir,
        silver_dir=silver_dir,
        quarantine_dir=quarantine_dir,
        gap_report_path=gap_report_path,
        interval=interval,
    )
    live_kline_store = BufferedKlineStore(
        hot=SqliteKlineHotStore(
            realtime_closed_db_path
            or state_dir / f"stream_closed_klines_{interval}.sqlite"
        ),
        cold=kline_store,
        flush_size=stream_flush_size,
        hot_flush_size=hot_flush_size,
        stats_callback=make_hot_store_progress_callback(progress_callback),
    )
    open_kline_store = (
        LatestOpenKlineStore(realtime_dir)
        if realtime_dir is not None
        else InMemoryOpenKlineStore()
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
            checkpoint_callback=checkpoint_callback,
        )

    async def flush_hot_periodically() -> None:
        try:
            while True:
                await asyncio.sleep(hot_flush_interval_seconds)
                live_kline_store.flush_hot()
        except asyncio.CancelledError:
            live_kline_store.flush_hot()
            raise

    startup_task: asyncio.Task[None] | None = None
    hot_flush_task = asyncio.create_task(flush_hot_periodically())
    if startup_gap_fill:
        startup_checkpoints = dict(checkpoints)
        startup_task = asyncio.create_task(
            gap_sync_with_checkpoints(
                symbols=selected_symbols,
                gap_checkpoints=startup_checkpoints,
                checkpoint_callback=make_merging_checkpoint_callback(
                    live_checkpoints=checkpoints,
                    save=checkpoint_callback,
                ),
            )
        )

    total = StreamResult(connections_seen=0, klines_saved=0)
    try:
        while True:
            connection_count = (
                len(selected_symbols) + streams_per_connection - 1
            ) // streams_per_connection
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
                store=live_kline_store,
                checkpoints=checkpoints,
                streams_per_connection=streams_per_connection,
                checkpoint_callback=checkpoint_callback,
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
    finally:
        emit_stream_shutdown_message(
            shutdown_callback,
            status="requested",
            stats=live_kline_store.stats(),
        )
        if startup_task is not None and not startup_task.done():
            startup_task.cancel()
            with suppress(asyncio.CancelledError):
                await startup_task
        hot_flush_task.cancel()
        with suppress(asyncio.CancelledError):
            await hot_flush_task
        live_kline_store.flush()
        checkpoint_callback.flush()
        checkpoint_store.save(checkpoints)
        emit_stream_shutdown_message(
            shutdown_callback,
            status="complete",
            stats=live_kline_store.stats(),
            checkpoints_saved=True,
        )


def emit_stream_shutdown_message(
    callback: Callable[[str], None] | None,
    *,
    status: str,
    stats: BufferedKlineStoreStats,
    checkpoints_saved: bool = False,
) -> None:
    if callback is None:
        return
    message = (
        f"shutdown={status} "
        f"sql_buffer={stats.sql_buffer_klines} "
        f"pending={stats.pending_klines} "
        f"sqlite_flushes={stats.sqlite_flushes} "
        f"parquet_flushes={stats.parquet_flushes}"
    )
    if checkpoints_saved:
        message += " checkpoints_saved=1"
    callback(message)


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


def make_hot_store_progress_callback(
    progress_callback: Callable[[StreamProgressEvent], None] | None,
) -> Callable[[BufferedKlineStoreStats], None] | None:
    if progress_callback is None:
        return None

    def callback(stats: BufferedKlineStoreStats) -> None:
        progress_callback(
            StreamProgressEvent(
                kind="hot_store",
                symbol="-",
                klines=0,
                requests=0,
                sql_buffer_klines=stats.sql_buffer_klines,
                sqlite_flushes=stats.sqlite_flushes,
                pending_klines=stats.pending_klines,
                parquet_flushes=stats.parquet_flushes,
                flush_size=stats.flush_size,
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
        TextColumn("sqlbuf={task.fields[sql_buffer_klines]}"),
        TextColumn("sqlflush={task.fields[sqlite_flushes]}"),
        TextColumn("pending={task.fields[pending_klines]}"),
        TextColumn("pqflush={task.fields[parquet_flushes]}/{task.fields[flush_size]}"),
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
        sql_buffer_klines=0,
        sqlite_flushes=0,
        pending_klines=0,
        parquet_flushes=0,
        flush_size=0,
        current="-",
    )
    state = {
        "ws_klines": 0,
        "rest_klines": 0,
        "rest_requests": 0,
        "sql_buffer_klines": 0,
        "sqlite_flushes": 0,
        "pending_klines": 0,
        "parquet_flushes": 0,
        "flush_size": 0,
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
        elif event.kind == "hot_store":
            state["sql_buffer_klines"] = event.sql_buffer_klines
            state["sqlite_flushes"] = event.sqlite_flushes
            state["pending_klines"] = event.pending_klines
            state["parquet_flushes"] = event.parquet_flushes
            state["flush_size"] = event.flush_size
        state["current"] = event.symbol
        progress.update(
            task_id,
            symbols=state["symbols"],
            connections=state["connections"],
            ws_klines=state["ws_klines"],
            rest_klines=state["rest_klines"],
            rest_requests=state["rest_requests"],
            sql_buffer_klines=state["sql_buffer_klines"],
            sqlite_flushes=state["sqlite_flushes"],
            pending_klines=state["pending_klines"],
            parquet_flushes=state["parquet_flushes"],
            flush_size=state["flush_size"],
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
