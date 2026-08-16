from __future__ import annotations

import json
from typing import Any


def json_dumps_compact(value: Any) -> str:
    """Compact JSON encoding for values stashed in a single CSV cell
    (e.g. a fee histogram or a list of coinbase output addresses)."""
    return json.dumps(value, separators=(",", ":"))
