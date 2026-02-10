"""Module-scoped datetime freeze helper."""

from __future__ import annotations

import importlib
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
from unittest.mock import patch

DEFAULT_MODULES = (
    "main",
    "reports",
    "db",
    "bot.handlers",
    "moderation_middleware",
)


def _build_frozen_datetime(frozen_utc: datetime):
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return frozen_utc.replace(tzinfo=None)
            return frozen_utc.astimezone(tz)

        @classmethod
        def utcnow(cls):
            return frozen_utc.replace(tzinfo=None)

    return FrozenDateTime


@contextmanager
def freeze_utc(
    dt_utc: datetime,
    *,
    modules: tuple[str, ...] = DEFAULT_MODULES,
):
    if dt_utc.tzinfo is None:
        raise ValueError("freeze_utc expects timezone-aware datetime")
    frozen_utc = dt_utc.astimezone(timezone.utc)
    frozen_type = _build_frozen_datetime(frozen_utc)
    with ExitStack() as stack:
        for module_name in modules:
            module = importlib.import_module(module_name)
            if hasattr(module, "datetime"):
                stack.enter_context(
                    patch.object(module, "datetime", frozen_type)
                )
        yield

