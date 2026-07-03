from quant_binance_sync.checkpoints import Checkpoint, CheckpointStore, mark_inactive_checkpoints
from quant_binance_sync.cli import load_interval_checkpoint_store


def test_checkpoint_store_loads_missing_file_as_empty_and_persists_updates(tmp_path) -> None:
    store = CheckpointStore(tmp_path / "checkpoints.json")

    assert store.load() == {}

    checkpoints = {"BTCUSDT|1m": Checkpoint(last_open_time_ms=1719792000000, status="active")}
    store.save(checkpoints)

    loaded = store.load()
    assert loaded == checkpoints


def test_mark_inactive_checkpoints_keeps_history_but_stops_sync_for_removed_symbols() -> None:
    checkpoints = {
        "BTCUSDT|1m": Checkpoint(last_open_time_ms=1, status="active"),
        "OLDUSDT|1m": Checkpoint(last_open_time_ms=2, status="active"),
        "OLDUSDT|5m": Checkpoint(last_open_time_ms=3, status="active"),
    }

    mark_inactive_checkpoints(checkpoints, active_symbols=["BTCUSDT"], interval="1m")

    assert checkpoints["BTCUSDT|1m"].status == "active"
    assert checkpoints["OLDUSDT|1m"] == Checkpoint(last_open_time_ms=2, status="inactive")
    assert checkpoints["OLDUSDT|5m"].status == "active"


def test_load_interval_checkpoint_store_migrates_only_matching_interval(tmp_path) -> None:
    legacy_store = CheckpointStore(tmp_path / "usdm_kline_checkpoints.json")
    legacy_store.save(
        {
            "BTCUSDT|1m": Checkpoint(last_open_time_ms=1000, status="active"),
            "BTCUSDT|15m": Checkpoint(last_open_time_ms=15000, status="active"),
        }
    )

    store, checkpoints = load_interval_checkpoint_store(tmp_path, interval="15m")
    store.save(checkpoints)

    assert store.path == tmp_path / "usdm_kline_checkpoints_15m.json"
    assert checkpoints == {
        "BTCUSDT|15m": Checkpoint(last_open_time_ms=15000, status="active")
    }
    assert legacy_store.load()["BTCUSDT|1m"] == Checkpoint(
        last_open_time_ms=1000,
        status="active",
    )
