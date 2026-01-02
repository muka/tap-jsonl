def test_get_files(make_jsonl, tap_factory):
    make_jsonl("a.jsonl", [{"id": 1}])
    make_jsonl("b.jsonl", [{"id": 2}])

    tap = tap_factory()
    stream = tap.discover_streams()[0]

    files = stream.get_files()
    names = sorted(p.name for p in files)

    assert names == ["a.jsonl", "b.jsonl"]
