"""Plugins: code that runs alongside the pipeline without living in it.

A plugin is any object with ``start(context)`` and ``shutdown()``. The
pipeline calls ``start`` when a run begins and ``shutdown`` when it ends,
and hands each plugin a ``PluginContext``: the event log (register
listeners, tail it from a thread), the state, the commands facade (act on
the run), the config and the output dir. A plugin may start threads; it
must never emit events from inside a listener (listeners run under the log
lock) and must never touch the run's files directly.

Plugins are named on the command line (``--plugin NAME``) and resolved
either through the ``specimux_suite.plugins`` entry-point group, where a
package registers ``name = "pkg.module:factory"``, or as a dotted path
``pkg.module:factory`` for anything unpublished. The factory is called
with the flat options dict from ``--plugin-opt KEY=VALUE`` and returns the
plugin. The suite ships one plugin of its own, the HTTP event forwarder
(``forward.py``).
"""

import importlib
import logging
from dataclasses import dataclass, field
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any, Callable, Protocol

from .commands import Commands
from .config import PipelineConfig
from .events import EventLog
from .state import PipelineState

logger = logging.getLogger(__name__)

ENTRY_POINT_GROUP = "specimux_suite.plugins"


@dataclass
class PluginContext:
    event_log: EventLog
    state: PipelineState
    commands: Commands
    config: PipelineConfig
    output_dir: Path
    options: dict = field(default_factory=dict)


class Plugin(Protocol):
    def start(self, context: PluginContext) -> None: ...

    def shutdown(self) -> None: ...


PluginFactory = Callable[[dict], Any]


def resolve_factory(name: str) -> PluginFactory:
    """An entry-point name or a ``module:attr`` path → the plugin factory."""
    if ":" in name:
        module_name, _, attr = name.partition(":")
        module = importlib.import_module(module_name)
        try:
            return getattr(module, attr)
        except AttributeError:
            raise ValueError(f"Plugin {name!r}: {module_name} has no {attr!r}") from None
    matches = [ep for ep in entry_points(group=ENTRY_POINT_GROUP) if ep.name == name]
    if not matches:
        known = sorted(ep.name for ep in entry_points(group=ENTRY_POINT_GROUP))
        raise ValueError(f"Unknown plugin {name!r} (installed: {known or 'none'}; "
                         f"or give a module:attr path)")
    return matches[0].load()


def load_plugin(name: str, options: dict | None = None) -> Any:
    """Instantiate a plugin by name with the given options."""
    plugin = resolve_factory(name)(dict(options or {}))
    for method in ("start", "shutdown"):
        if not callable(getattr(plugin, method, None)):
            raise ValueError(f"Plugin {name!r} has no {method}()")
    return plugin


def parse_options(pairs: list[str] | None) -> dict:
    """``["a=1", "b=x"]`` → ``{"a": "1", "b": "x"}``."""
    options = {}
    for pair in pairs or []:
        key, sep, value = pair.partition("=")
        if not sep or not key:
            raise ValueError(f"--plugin-opt expects KEY=VALUE, got {pair!r}")
        options[key.strip()] = value
    return options
