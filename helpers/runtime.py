from __future__ import annotations

from typing import Any

from helpers.print_style import PrintStyle

PLUGIN_NAME = "sfera_ai"

DEFAULT_CONFIG = {
    "sse_enabled": True,
    "keepalive_interval": 30,
    "pending_events_ttl": 86400,  # 24 hours in seconds
    "max_pending_events": 100,
    "broadcast_on_scheduler_task": True,
}


def normalize_config(config: dict[str, Any] | None) -> dict[str, Any]:
    normalized = dict(DEFAULT_CONFIG)
    if not isinstance(config, dict):
        return normalized
    for key, default_val in DEFAULT_CONFIG.items():
        if key in config and config[key] is not None:
            normalized[key] = type(default_val)(config[key])
    return normalized


def get_config() -> dict[str, Any]:
    from helpers import plugins
    config = plugins.get_plugin_config(PLUGIN_NAME) or {}
    return normalize_config(config)


def is_globally_enabled() -> bool:
    from helpers import plugins
    return plugins.determined_toggle_from_paths(
        True, reversed(plugins.get_plugin_roots(PLUGIN_NAME))
    )
