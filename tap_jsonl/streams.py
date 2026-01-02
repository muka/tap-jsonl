"""Stream type classes for tap-jsonl."""

from __future__ import annotations

from datetime import datetime, timezone
import os
import re
from pathlib import Path

from singer_sdk import Tap, typing as th
from singer_sdk.streams import Stream
from singer_sdk.helpers.types import Context

import typing as t

from tap_jsonl.client import iter_jsonl_file

SDC_INCREMENTAL_KEY = "_sdc_last_modified"
SDC_FILENAME = "_sdc_filename"
SDC_STREAM = "_sdc_stream"

ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
ISO_DT_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?$"
)


def _looks_like_date(s: str) -> bool:
    return bool(ISO_DATE_RE.match(s))


def _looks_like_datetime(s: str) -> bool:
    return bool(ISO_DT_RE.match(s))


def to_iso8601(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def parse_bookmark(val: str | None) -> datetime | None:
    if not val:
        return None
    clean = val.replace("Z", "+00:00")
    return datetime.fromisoformat(clean).astimezone(timezone.utc)


class JsonlFileStream(Stream):
    """A single logical stream backed by one-or-many JSONL files."""

    def __init__(self, tap: Tap) -> None:

        self.stream_name = tap.config.get("stream_name") or "jsonl"

        super().__init__(tap, name=self.stream_name)

        self.primary_keys = self.config.get("primary_keys") or []

        self.state_partitioning_keys = [SDC_FILENAME, SDC_STREAM]
        self.replication_key = SDC_INCREMENTAL_KEY
        self.forced_replication_method = "INCREMENTAL"

        self.file_path = self.config.get("path")
        self._encoding = self.config.get("encoding") or "utf-8"
        self._emit_state_every = int(self.config.get("emit_state_every") or 500)

    @property
    def is_sorted(self) -> bool:
        """The stream returns records in order."""
        return False

    def _infer_schema(self, v: t.Any) -> th.JSONTypeHelper:
        if isinstance(v, bool):
            return th.BooleanType(nullable=True)
        if isinstance(v, int) and not isinstance(v, bool):
            return th.IntegerType(nullable=True)
        if isinstance(v, float):
            return th.NumberType(nullable=True)
        if isinstance(v, str) and _looks_like_datetime(v):
            return th.DateTimeType(nullable=True)
        if isinstance(v, str) and _looks_like_date(v):
            return th.DateType(nullable=True)
        if isinstance(v, str):
            return th.StringType(nullable=True)

        if isinstance(v, dict):
            return th.ObjectType(
                *[th.Property(k, self._infer_schema(vv)) for k, vv in v.items()],
                additional_properties=True,
                nullable=True,
            )

        if isinstance(v, list):
            # infer array item type from first non-null element
            first = next((x for x in v if x is not None), None)
            item = (
                self._infer_schema(first)
                if first is not None
                else th.StringType(nullable=True)
            )
            return th.ArrayType(item, nullable=True)

        return th.StringType(nullable=True)

    @property
    def schema(self) -> dict:
        if self._schema:
            return self._schema

        props = []

        files = self.get_files()
        sample: dict[str, t.Any] | None = None
        for p in files:
            for rec, _ in iter_jsonl_file(
                file_path=str(p), encoding=self._encoding, logger=self.logger
            ):
                sample = rec
                break
            if sample:
                break

        if sample:
            for k, v in sample.items():
                props.append(th.Property(k, self._infer_schema(v)))

        props.append(
            th.Property(
                SDC_INCREMENTAL_KEY,
                th.DateTimeType(nullable=True),
                description="Replication checkpoint (file mtime or row date)",
            )
        )
        props.append(
            th.Property(
                SDC_FILENAME,
                th.StringType(nullable=True),
                description="Filename reference",
            ),
        )
        props.append(
            th.Property(
                SDC_STREAM,
                th.StringType(nullable=True),
                description="Stream (table_name) reference",
            )
        )

        self._schema = th.PropertiesList(*props).to_dict()

        return self._schema

    def get_partition_name(self, filepath: str) -> str:
        return str(Path(filepath).absolute())

    def get_partition_context(self, filepath: str) -> dict[str, t.Any]:
        """Return the one true partition context for this file."""
        return {
            SDC_FILENAME: self.get_partition_name(filepath),
            SDC_STREAM: self.stream_name,
        }

    def get_files(self) -> t.List[Path]:

        pattern = self.config.get("path")
        if not pattern:
            return []

        expanded = os.path.expanduser(pattern)
        # glob with ** support
        paths = sorted(
            Path().glob(expanded)
            if not expanded.startswith("/")
            else Path("/").glob(expanded[1:])
        )

        # Fallback for patterns that Path.glob might not like (Windows-ish or edge cases):
        if not paths:
            import glob as _glob

            paths = [Path(p) for p in sorted(_glob.glob(expanded, recursive=True))]

        return paths

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Yield records for all files matching this stream's glob."""
        files = self.get_files()
        if not files:
            self.logger.warning(f"No files found for {self.file_path}")
            yield from []

        for filepath in sorted(files):
            file_ctx = self.get_partition_context(str(filepath))
            yield from self.process_file(str(filepath), file_ctx)

    def process_file(
        self,
        filepath: str,
        context: Context,
    ) -> list[dict]:
        """Process one file with state awareness and return its records."""

        # load bookmark
        pstate = self.get_context_state(context) or {}
        bookmark_dt = parse_bookmark(pstate.get(SDC_INCREMENTAL_KEY))

        mtime: datetime = datetime.fromtimestamp(
            Path(filepath).stat().st_mtime, tz=timezone.utc
        ).replace(microsecond=0)

        self.logger.info(
            "Partition context: %s, last_bookmark=%s, mtime=%s",
            context,
            bookmark_dt,
            mtime,
        )

        # skip if already processed
        if bookmark_dt and mtime <= bookmark_dt:
            self.logger.info(
                "Skipping %s (mtime=%s <= bookmark=%s)", filepath, mtime, bookmark_dt
            )
            return []

        records: list[dict] = []

        for record, current_line in iter_jsonl_file(
            file_path=filepath,
            encoding=self._encoding,
            logger=self.logger,
        ):
            record[SDC_INCREMENTAL_KEY] = to_iso8601(mtime)
            record[SDC_FILENAME] = filepath
            record[SDC_STREAM] = self.stream_name

            records.append(record)

        if records:
            self.logger.info("Processed %d rows from %s", len(records), filepath)
            self._increment_stream_state(
                {SDC_INCREMENTAL_KEY: to_iso8601(mtime)},
                context=context,
            )

        return records
