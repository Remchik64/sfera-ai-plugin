from __future__ import annotations
from plugins.sfera_ai.helpers import runtime


def get_plugin_config(default=None, **kwargs):
    return runtime.normalize_config(default or {})


def save_plugin_config(default=None, settings=None, **kwargs):
    return runtime.normalize_config(settings or default or {})
