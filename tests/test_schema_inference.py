def test_schema_infers_types(make_jsonl, tap_factory):
    make_jsonl(
        "sample.jsonl",
        [
            {
                "id": 1,
                "flag": True,
                "amount": 12.5,
                "day": "2024-01-01",
                "ts": "2024-01-01T10:11:12Z",
                "meta": {"a": 1, "b": "x"},
            }
        ],
    )

    tap = tap_factory()
    stream = tap.discover_streams()[0]
    schema = stream.schema["properties"]

    assert schema["id"]["type"] == ["integer", "null"]
    assert schema["flag"]["type"] == ["boolean", "null"]
    assert schema["amount"]["type"] == ["number", "null"]
    assert schema["day"]["format"] == "date"
    assert schema["ts"]["format"] == "date-time"
    assert schema["meta"]["type"] == ["object", "null"]
    assert "properties" in schema["meta"]
