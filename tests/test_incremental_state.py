from pathlib import Path
import time


# def test_incremental_skips_unchanged_file(make_jsonl, tap_factory):
#     p = make_jsonl("data.jsonl", [{"id": 1}])

#     tap = tap_factory()
#     stream = tap.discover_streams()[0]

#     ctx = stream.get_partition_context(str(p))

#     records1 = stream.process_file(str(p), ctx)
#     assert len(records1) == 1

#     records2 = stream.process_file(str(p), ctx)
#     assert records2 == []


# def test_incremental_reads_after_mtime_change(make_jsonl, tap_factory):
#     p = make_jsonl("data.jsonl", [{"id": 1}])

#     tap = tap_factory()
#     stream = tap.discover_streams()[0]

#     ctx = stream.get_partition_context(str(p))

#     _ = stream.process_file(str(p), ctx)

#     time.sleep(1)
#     p.write_text(p.read_text() + '{"id": 2}\n')

#     records = stream.process_file(str(p), ctx)
#     assert len(records) == 2
