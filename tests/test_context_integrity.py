def test_partition_context_not_empty(make_jsonl, tap_factory):
    make_jsonl("x.jsonl", [{"id": 1}])

    tap = tap_factory()
    stream = tap.discover_streams()[0]

    contexts = []

    original_process_file = stream.process_file

    def spy(filepath, context):
        contexts.append(context)
        return original_process_file(filepath, context)

    stream.process_file = spy  # monkey-patch

    list(stream.get_records(context=None))

    assert contexts
    for ctx in contexts:
        assert "_sdc_filename" in ctx
        assert "_sdc_stream" in ctx
