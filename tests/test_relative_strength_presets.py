import pytest

from quant_binance_sync.relative_strength_presets import (
    RelativeStrengthPreset,
    resolve_relative_strength_preset_timeframe,
    resolve_relative_strength_presets,
)


@pytest.mark.parametrize(
    ("preset", "expected_tfs"),
    [
        ("scalp", ["1", "3", "5", "15"]),
        ("intraday", ["15", "30", "60", "120", "240", "360"]),
        ("swing", ["240", "360", "720", "D"]),
        (
            "all",
            [
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
        ),
    ],
)
def test_resolve_relative_strength_presets_expands_requested_group(
    preset: str,
    expected_tfs: list[str],
) -> None:
    resolved = resolve_relative_strength_presets(preset)

    assert [item.tf for item in resolved] == expected_tfs
    assert all(isinstance(item, RelativeStrengthPreset) for item in resolved)


def test_resolve_relative_strength_presets_uses_practical_intraday_filters() -> None:
    preset_30 = next(
        item for item in resolve_relative_strength_presets("intraday") if item.tf == "30"
    )

    assert preset_30.tail_bars == 100
    assert preset_30.warmup_bars == 60
    assert preset_30.liquidity_top_n == 100
    assert preset_30.liquidity_lookback_bars == 48
    assert preset_30.max_abs_gap_atr == 2.0
    assert preset_30.max_ret == 0.12


def test_resolve_relative_strength_presets_rejects_unknown_group() -> None:
    with pytest.raises(ValueError, match="unknown relative strength preset"):
        resolve_relative_strength_presets("weekly")


def test_resolve_relative_strength_presets_can_load_toml_config(tmp_path) -> None:
    config_path = tmp_path / "relative_strength_presets.toml"
    config_path.write_text(
        """
[groups]
custom = ["15", "60"]

[timeframes."15"]
tail_bars = 88
warmup_bars = 34
liquidity_top_n = 55
liquidity_lookback_bars = 21
max_abs_gap_atr = 1.6
max_ret = 0.07
""",
        encoding="utf-8",
    )

    resolved = resolve_relative_strength_presets("custom", config_path=config_path)

    assert [item.tf for item in resolved] == ["15", "60"]
    assert resolved[0] == RelativeStrengthPreset("15", 88, 34, 55, 21, 1.6, 0.07)
    assert resolved[1].tail_bars == 100


def test_resolve_relative_strength_presets_rejects_incomplete_config_timeframe(tmp_path) -> None:
    config_path = tmp_path / "relative_strength_presets.toml"
    config_path.write_text(
        """
[groups]
custom = ["2"]

[timeframes."2"]
tail_bars = 88
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match='timeframes."2" missing'):
        resolve_relative_strength_presets("custom", config_path=config_path)


def test_resolve_relative_strength_preset_timeframe_uses_config_params(tmp_path) -> None:
    config_path = tmp_path / "relative_strength_presets.toml"
    config_path.write_text(
        """
[timeframes."60"]
tail_bars = 88
warmup_bars = 34
liquidity_top_n = 55
liquidity_lookback_bars = 21
max_abs_gap_atr = 1.6
max_ret = 0.07
""",
        encoding="utf-8",
    )

    resolved = resolve_relative_strength_preset_timeframe("60", config_path=config_path)

    assert resolved == RelativeStrengthPreset("60", 88, 34, 55, 21, 1.6, 0.07)
