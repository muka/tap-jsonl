"""Jsonl tap class."""

from __future__ import annotations

import sys

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_jsonl.streams import JsonlFileStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class TapJsonl(Tap):
    """Singer tap for JSONL files."""

    name = "tap-jsonl"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "path",
            th.StringType(nullable=False),
            required=True,
            title="Path Glob",
            description="Glob pattern for JSONL files, e.g. /data/**/*.jsonl",
        ),
        th.Property(
            "stream_name",
            th.StringType(nullable=True),
            default="jsonl",
            title="Stream Name",
            description="Override the Singer stream name (default: jsonl).",
        ),
        th.Property(
            "primary_keys",
            th.ArrayType(th.StringType(nullable=False), nullable=True),
            default=[],
            title="Primary Keys",
            description=(
                "Optional list of primary key fields for the stream. "
                "Example: ['id'] or ['project_id','org_id']."
            ),
        ),
        th.Property(
            "encoding",
            th.StringType(nullable=True),
            default="utf-8",
            title="File Encoding",
            description="File encoding used when reading JSONL files (default: utf-8).",
        ),
        th.Property(
            "state_strategy",
            th.StringType(nullable=True),
            default="line",
            title="State Strategy",
            description="Currently only 'line' is supported (track last processed line per file).",
        ),
        th.Property(
            "emit_state_every",
            th.IntegerType(nullable=True),
            default=500,
            title="Emit State Every N Records",
            description="How often to persist state while reading a file (default: 500).",
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[JsonlFileStream]:
        return [JsonlFileStream(self)]


if __name__ == "__main__":
    TapJsonl.cli()
