from pathlib import Path
import json
import pytest
from singer_sdk.testing import get_tap_test_class

from tap_jsonl.tap import TapJsonl


@pytest.fixture
def jsonl_dir(tmp_path: Path) -> Path:
    return tmp_path / "data"


@pytest.fixture
def make_jsonl(jsonl_dir: Path):
    jsonl_dir.mkdir(parents=True, exist_ok=True)

    def _make(name: str, rows: list[dict]) -> Path:
        p = jsonl_dir / name
        with p.open("w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
        return p

    return _make


@pytest.fixture
def tap_factory(jsonl_dir: Path):
    def _make():
        return TapJsonl(
            config={
                "path": str(jsonl_dir / "*.jsonl"),
                "stream_name": "test_stream",
                "primary_keys": ["id"],
            }
        )

    return _make
