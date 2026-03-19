from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


@dataclass
class DebeziumEvent:
    """Represents a single CDC event from a Debezium-produced Redis Stream message."""

    stream: str
    message_id: str
    table: str
    operation: str  # c, u, d, r (create, update, delete, read/snapshot)
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    source: dict[str, Any] = field(default_factory=dict)
    ts_ms: int = 0

    # ------------------------------------------------------------------ #
    # Factory                                                              #
    # ------------------------------------------------------------------ #

    @classmethod
    def from_stream_message(
        cls,
        stream: str,
        message_id: str,
        table: str,
        data: dict[str, str],
    ) -> "DebeziumEvent":
        """Parse a Redis Stream message into a DebeziumEvent.

        Supports two formats:
        1. Full Debezium envelope  – ``data["payload"]`` is a JSON string
           containing ``before``, ``after``, ``op``, ``source``, ``ts_ms``.
        2. Flattened SMT format    – individual keys ``op``, ``after_*`` /
           ``before_*`` (or just flat key/value pairs representing the row).
        """
        # --- Full envelope ---
        if "payload" in data:
            try:
                envelope = json.loads(data["payload"])
            except (json.JSONDecodeError, TypeError):
                envelope = data["payload"] if isinstance(data["payload"], dict) else {}

            op = envelope.get("op", "r")
            before = envelope.get("before") or None
            after = envelope.get("after") or None
            source = envelope.get("source") or {}
            ts_ms = int(envelope.get("ts_ms", 0))

            return cls(
                stream=stream,
                message_id=message_id,
                table=table,
                operation=op,
                before=before,
                after=after,
                source=source,
                ts_ms=ts_ms,
            )

        # --- Flattened SMT format ---
        op = data.get("op", "r")
        ts_ms = int(data.get("ts_ms", 0))

        # Collect before_ / after_ prefixed keys
        before: dict[str, Any] | None = None
        after: dict[str, Any] | None = None

        before_keys = {k[7:]: v for k, v in data.items() if k.startswith("before_")}
        after_keys = {k[6:]: v for k, v in data.items() if k.startswith("after_")}

        if before_keys:
            before = before_keys
        if after_keys:
            after = after_keys

        # Fallback: treat all non-meta keys as the "after" row (flat format)
        if after is None and before is None:
            meta_keys = {"op", "ts_ms", "source"}
            flat = {k: v for k, v in data.items() if k not in meta_keys}
            if flat:
                after = flat

        source_raw = data.get("source", "{}")
        try:
            source = json.loads(source_raw) if isinstance(source_raw, str) else source_raw
        except (json.JSONDecodeError, TypeError):
            source = {}

        return cls(
            stream=stream,
            message_id=message_id,
            table=table,
            operation=op,
            before=before,
            after=after,
            source=source,
            ts_ms=ts_ms,
        )

    # ------------------------------------------------------------------ #
    # Properties                                                           #
    # ------------------------------------------------------------------ #

    @property
    def is_delete(self) -> bool:
        return self.operation == "d"

    @property
    def payload(self) -> dict[str, Any]:
        """Return the effective row data: ``after`` for inserts/updates, ``before`` for deletes."""
        if self.is_delete:
            return self.before or {}
        return self.after or {}
