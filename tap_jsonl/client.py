"""Helpers for streaming JSONL files."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, Optional, Tuple


@dataclass(frozen=True)
class JsonlReadError:
    file_path: str
    line_number: int
    line: str
    error: str


def iter_jsonl_file(
    *,
    file_path: str,
    encoding: str = "utf-8",
    start_line: int = 1,
    logger=None,
) -> Iterator[Tuple[Dict[str, Any], int]]:
    """Yield (record, line_number) from a JSONL file, streaming line-by-line.

    Args:
        file_path: Path to the JSONL file.
        encoding: File encoding.
        start_line: 1-based line number to start from (used for resuming state).
        logger: Optional logger (from Singer SDK) for warnings.

    Yields:
        Tuple(record_dict, current_line_number)
    """
    # Use newline='' for predictable line handling across platforms.
    with open(file_path, "r", encoding=encoding, newline="") as f:
        for i, raw in enumerate(f, start=1):
            if i < start_line:
                continue

            line = raw.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
            except Exception as e:  # noqa: BLE001
                if logger:
                    logger.warning(
                        "Skipping invalid JSON at %s:%s (%s)",
                        file_path,
                        i,
                        str(e),
                    )
                continue

            # Ensure a dict-like record (Singer expects object records)
            if not isinstance(obj, dict):
                if logger:
                    logger.warning(
                        "Skipping non-object JSON at %s:%s (type=%s)",
                        file_path,
                        i,
                        type(obj).__name__,
                    )
                continue

            yield obj, i
