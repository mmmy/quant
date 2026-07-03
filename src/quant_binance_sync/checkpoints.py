from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class Checkpoint:
    last_open_time_ms: int | None
    status: str


class CheckpointStore:
    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)

    def load(self) -> dict[str, Checkpoint]:
        if not self.path.exists():
            return {}
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        return {key: Checkpoint(**value) for key, value in payload.items()}

    def save(self, checkpoints: dict[str, Checkpoint]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {key: asdict(value) for key, value in sorted(checkpoints.items())}
        self.path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def mark_inactive_checkpoints(
    checkpoints: dict[str, Checkpoint],
    *,
    active_symbols: list[str],
    interval: str,
) -> None:
    active_keys = {f"{symbol}|{interval}" for symbol in active_symbols}
    suffix = f"|{interval}"
    for key, checkpoint in list(checkpoints.items()):
        if key.endswith(suffix) and key not in active_keys:
            checkpoints[key] = Checkpoint(
                last_open_time_ms=checkpoint.last_open_time_ms,
                status="inactive",
            )
