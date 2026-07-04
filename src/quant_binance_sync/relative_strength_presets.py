from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib


@dataclass(frozen=True)
class RelativeStrengthPreset:
    tf: str
    tail_bars: int
    warmup_bars: int
    liquidity_top_n: int
    liquidity_lookback_bars: int
    max_abs_gap_atr: float
    max_ret: float


RELATIVE_STRENGTH_PRESETS_BY_TF = {
    "1": RelativeStrengthPreset("1", 240, 80, 100, 240, 1.2, 0.03),
    "2": RelativeStrengthPreset("2", 220, 80, 100, 200, 1.3, 0.04),
    "3": RelativeStrengthPreset("3", 200, 80, 100, 160, 1.4, 0.05),
    "4": RelativeStrengthPreset("4", 180, 80, 100, 140, 1.5, 0.055),
    "5": RelativeStrengthPreset("5", 160, 80, 100, 120, 1.5, 0.06),
    "8": RelativeStrengthPreset("8", 140, 80, 100, 110, 1.6, 0.07),
    "10": RelativeStrengthPreset("10", 130, 80, 100, 100, 1.7, 0.075),
    "15": RelativeStrengthPreset("15", 120, 80, 100, 96, 1.8, 0.08),
    "20": RelativeStrengthPreset("20", 110, 70, 100, 72, 1.9, 0.10),
    "30": RelativeStrengthPreset("30", 100, 60, 100, 48, 2.0, 0.12),
    "45": RelativeStrengthPreset("45", 100, 60, 100, 48, 2.1, 0.15),
    "60": RelativeStrengthPreset("60", 100, 60, 100, 48, 2.2, 0.18),
    "90": RelativeStrengthPreset("90", 100, 60, 100, 40, 2.4, 0.22),
    "120": RelativeStrengthPreset("120", 100, 60, 100, 36, 2.5, 0.25),
    "180": RelativeStrengthPreset("180", 100, 60, 100, 32, 2.8, 0.30),
    "240": RelativeStrengthPreset("240", 100, 60, 100, 30, 3.0, 0.35),
    "360": RelativeStrengthPreset("360", 100, 50, 100, 20, 3.0, 0.40),
    "480": RelativeStrengthPreset("480", 100, 50, 100, 18, 3.2, 0.50),
    "720": RelativeStrengthPreset("720", 100, 50, 100, 14, 3.5, 0.60),
    "D": RelativeStrengthPreset("D", 120, 80, 100, 30, 4.0, 1.00),
}

RELATIVE_STRENGTH_PRESET_GROUPS = {
    "scalp": ["1", "3", "5", "15"],
    "intraday": ["15", "30", "60", "120", "240", "360"],
    "swing": ["240", "360", "720", "D"],
    "all": [
        "1",
        "2",
        "3",
        "4",
        "5",
        "8",
        "10",
        "15",
        "20",
        "30",
        "45",
        "60",
        "90",
        "120",
        "180",
        "240",
        "360",
        "480",
        "720",
        "D",
    ],
}


REQUIRED_TIMEFRAME_FIELDS = {
    "tail_bars",
    "warmup_bars",
    "liquidity_top_n",
    "liquidity_lookback_bars",
    "max_abs_gap_atr",
    "max_ret",
}


def resolve_relative_strength_presets(
    preset: str,
    *,
    config_path: Path | str | None = None,
) -> list[RelativeStrengthPreset]:
    groups = dict(RELATIVE_STRENGTH_PRESET_GROUPS)
    presets_by_tf = dict(RELATIVE_STRENGTH_PRESETS_BY_TF)
    if config_path is not None:
        config_groups, config_presets = load_relative_strength_preset_config(config_path)
        groups.update(config_groups)
        presets_by_tf.update(config_presets)

    normalized = preset.lower()
    if normalized not in groups:
        available = ", ".join(groups)
        raise ValueError(f"unknown relative strength preset: {preset}; available: {available}")
    return [presets_by_tf[tf] for tf in groups[normalized]]


def resolve_relative_strength_preset_timeframe(
    tf: str,
    *,
    config_path: Path | str | None = None,
) -> RelativeStrengthPreset:
    presets_by_tf = dict(RELATIVE_STRENGTH_PRESETS_BY_TF)
    if config_path is not None:
        _, config_presets = load_relative_strength_preset_config(config_path)
        presets_by_tf.update(config_presets)

    normalized = normalize_tf(tf)
    if normalized not in presets_by_tf:
        available = ", ".join(presets_by_tf)
        raise ValueError(f"unknown relative strength timeframe: {tf}; available: {available}")
    return presets_by_tf[normalized]


def load_relative_strength_preset_config(
    config_path: Path | str,
) -> tuple[dict[str, list[str]], dict[str, RelativeStrengthPreset]]:
    path = Path(config_path)
    with path.open("rb") as file:
        config = tomllib.load(file)

    groups = {
        name.lower(): [normalize_tf(tf) for tf in values]
        for name, values in config.get("groups", {}).items()
    }
    presets = {
        normalize_tf(tf): parse_timeframe_config(normalize_tf(tf), values)
        for tf, values in config.get("timeframes", {}).items()
    }
    return groups, presets


def parse_timeframe_config(tf: str, values: dict) -> RelativeStrengthPreset:
    missing = sorted(REQUIRED_TIMEFRAME_FIELDS - set(values))
    if missing:
        fields = ", ".join(missing)
        raise ValueError(f'timeframes."{tf}" missing: {fields}')
    return RelativeStrengthPreset(
        tf=tf,
        tail_bars=int(values["tail_bars"]),
        warmup_bars=int(values["warmup_bars"]),
        liquidity_top_n=int(values["liquidity_top_n"]),
        liquidity_lookback_bars=int(values["liquidity_lookback_bars"]),
        max_abs_gap_atr=float(values["max_abs_gap_atr"]),
        max_ret=float(values["max_ret"]),
    )


def normalize_tf(tf: object) -> str:
    return str(tf).upper()
