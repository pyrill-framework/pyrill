======================
Parsing and converting
======================

Data rarely arrives in the shape you need. Bytes come in arbitrary chunks, text
needs decoding, and one format has to become another. This guide shows how to deal
with each of those, one element at a time.

Rebuild records from byte chunks
--------------------------------

A network connection delivers bytes in chunks that have nothing to do with the
records inside them. A chunk can stop in the middle of a line. The re-chunking
stages put the records back together.

This example reads a request from an :mod:`asyncio` stream in chunks of 7 bytes
and rebuilds the lines, which end with ``\r\n``:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import ListAcc
    from pyrill.bytes import BytesChunksSeparator, Decode
    from pyrill.io import ChunkedStreamReaderSource
    from pyrill.sinks import Last
    from pyrill.strlike import Strip


    async def main() -> None:
        reader = asyncio.StreamReader()
        reader.feed_data(b"GET /index HTTP/1.1\r\nHost: example.org\r\n\r\n")
        reader.feed_eof()

        sink = (
            ChunkedStreamReaderSource(stream=reader, chunk_size=7)
            >> BytesChunksSeparator(separator=b"\r\n")
            >> Decode()
            >> Strip()
            >> ListAcc()
            >> Last()
        )
        print(await sink.get_frame())


    asyncio.run(main())

.. code-block:: text

    ['GET /index HTTP/1.1', 'Host: example.org', '']

In a real program the ``StreamReader`` comes from :func:`asyncio.open_connection`
or from a server callback, and the pipeline reads from it as the data arrives.

Other re-chunking stages cover other formats:

* :class:`~pyrill.bytes.BytesSizedChunks` regroups bytes into chunks of a bounded
  size.
* :class:`~pyrill.bytes.BytesChunksFirstSeparator` splits only at the first
  separator, which is how you separate a header from a body.
* :class:`~pyrill.chunks.SkipUntil` drops everything before a marker, even when the
  marker is split across chunks.

The ``String`` versions in :mod:`pyrill.strings` do the same for text.

Decode text safely
------------------

UTF-8 characters can use several bytes, and a chunk boundary can fall in the middle
of one. Decoding such a chunk fails. :class:`~pyrill.bytes.UnicodeChunks` holds the
incomplete bytes back until the rest arrives:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import ListAcc
    from pyrill.bytes import Decode, UnicodeChunks
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource

    chunks = [b"caf\xc3", b"\xa9 au lait"]  # the "é" is split across the chunks


    async def main() -> None:
        safe = IterableSource(source=chunks) >> UnicodeChunks() >> Decode()
        print(await (safe >> ListAcc() >> Last()).get_frame())

        unsafe = IterableSource(source=chunks) >> Decode()
        try:
            await (unsafe >> ListAcc() >> Last()).get_frame()
        except UnicodeDecodeError as error:
            print(type(error).__name__)


    asyncio.run(main())

.. code-block:: text

    ['caf', 'é au lait']
    UnicodeDecodeError

Put ``UnicodeChunks`` before ``Decode`` whenever the bytes come from a stream you do
not control.

Encode values as JSON
---------------------

Usually, :class:`~pyrill.json.ToJson` is the only JSON encoding stage you need.
It handles strings, numbers, booleans, nulls, dictionaries and collections,
including nested values. It automatically selects the array and object
serializers, so you do not need to assemble them yourself.

Each input frame holds a value to encode. Output is a stream of text fragments;
join them with :class:`~pyrill.accumulators.SumAcc` when you need a complete
string, or send them directly to a text sink. Here one frame contains the whole
list of people:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import SumAcc
    from pyrill.json import ToJson
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource

    people = [{"name": "Ann", "age": 30}, {"name": "Cleo", "age": 41}]


    async def main() -> None:
        sink = IterableSource(source=[people]) >> ToJson() >> SumAcc() >> Last()
        print(repr(await sink.get_frame()))


    asyncio.run(main())

.. code-block:: text

    '[{"name": "Ann", "age": 30}, {"name": "Cleo", "age": 41}]'

``ToJson`` concatenates the documents from successive input frames without
separators. Use :class:`~pyrill.json.ToJsonPerLine` for JSON Lines, or the
container stages described below to collect separate frames into one document.

Convert CSV to JSON Lines
-------------------------

:class:`~pyrill.csv.DictFromCsv` parses CSV text into dictionaries, one per row.
The first row it receives is the header. ``maps`` converts columns to other types.
:class:`~pyrill.json.ToJsonPerLine` writes each dictionary as one line of JSON.

This pipeline keeps the people who are at least 30 years old:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import SumAcc
    from pyrill.csv import DictFromCsv
    from pyrill.json import ToJsonPerLine
    from pyrill.primitives import SkipIf
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource

    rows = ["name,age\n", "Ann,30\n", "Bob,25\n", "Cleo,41\n"]


    async def main() -> None:
        sink = (
            IterableSource(source=rows)
            >> DictFromCsv(maps={"age": int})
            >> SkipIf(check_func=lambda row: row["age"] >= 30)
            >> ToJsonPerLine()
            >> SumAcc()
            >> Last()
        )
        print(await sink.get_frame(), end="")


    asyncio.run(main())

.. code-block:: text

    {"name":"Ann","age":30}
    {"name":"Cleo","age":41}

A frame can hold several CSV rows, so you can read the text in any chunking that is
convenient. Combine it with :class:`~pyrill.bytes.BytesChunksSeparator` to read a
CSV file line by line.

Build one JSON document
-----------------------

When a list or dictionary is already available as one value, pass it to
:class:`~pyrill.json.ToJson` as shown above. The container stages are useful
when the entries arrive as separate input frames: :class:`~pyrill.json.ToJsonList`
wraps a stream of values in one array, and :class:`~pyrill.json.ToJsonObject`
wraps a stream of string-keyed pairs in one object.

In this example, ``source=people`` emits each person separately, and
``ToJsonList`` combines them into one array. All these serializers emit
fragments, so the document can be written as entries arrive. Here
:class:`~pyrill.accumulators.SumAcc` collects the complete text:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import SumAcc
    from pyrill.json import ToJsonList
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource

    people = [{"name": "Ann", "age": 30}, {"name": "Cleo", "age": 41}]


    async def main() -> None:
        sink = IterableSource(source=people) >> ToJsonList() >> SumAcc() >> Last()
        print(await sink.get_frame())


    asyncio.run(main())

.. code-block:: text

    [{"name": "Ann", "age": 30}, {"name": "Cleo", "age": 41}]

Items can be nested lists and dictionaries, and they can even be other
``ToJson``, ``ToJsonList`` or ``ToJsonObject`` pipelines: their output is placed
inside the document.

Build an object from queued JSON producers
------------------------------------------

Use :class:`~pyrill.queues.QueueSource` when application code supplies the object
entries. Each pushed frame is a ``(key, producer)`` tuple: the key becomes an
object key, and the :class:`~pyrill.json.BaseToJsonProducer` supplies the JSON
fragments for its value. :class:`~pyrill.json.ToJsonObject` adds the braces,
escapes the keys and inserts the separators.

This example queues three entries whose values come from different JSON
pipelines. Mount the outer stage before pushing, then push
``StopAsyncIteration()`` after the last entry so the object can finish:

.. code-block:: python

    import asyncio

    from pyrill.json import (
        BaseToJsonProducer,
        JSONValue,
        ToJson,
        ToJsonList,
        ToJsonObject,
    )
    from pyrill.queues import QueueSource
    from pyrill.sources import IterableSource


    async def main() -> None:
        source = QueueSource[tuple[str, BaseToJsonProducer]]()
        stage = source >> ToJsonObject()
        entries: list[tuple[str, BaseToJsonProducer]] = [
            (
                "profile",
                IterableSource[JSONValue](source=[{"name": "Ann"}]) >> ToJson(),
            ),
            (
                "scores",
                IterableSource[JSONValue](source=[10, 20, 30]) >> ToJsonList(),
            ),
            (
                "metadata",
                IterableSource[tuple[str, JSONValue]](source=[("active", True)])
                >> ToJsonObject(),
            ),
        ]

        await stage.mount()
        for key, producer in entries:
            await source.push_frame((key, producer))
        await source.end_of_stream()

        text = "".join([fragment async for fragment in stage])
        print(text)


    asyncio.run(main())

.. code-block:: text

    {"profile": {"name": "Ann"}, "scores": [10, 20, 30], "metadata": {"active": true}}

The queue is unlimited by default, so this example can enqueue all entries
before reading. With a bounded ``queue_size``, push entries in a separate
asynchronous task while consuming the stage. Each producer should emit one
complete JSON value; its fragments are included directly with its own encoder
settings.

Write CSV
---------

:class:`~pyrill.csv.DictToCsv` and :class:`~pyrill.csv.ListToCsv` do the opposite.
``columns`` chooses and orders the columns, and the first frame they emit is the
header. Pass ``csv_kwargs`` to change the dialect, here the line ending:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import SumAcc
    from pyrill.csv import DictToCsv
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource

    people = [{"name": "Ann", "age": 30}, {"name": "Bob", "age": 25}]


    async def main() -> None:
        sink = (
            IterableSource(source=people)
            >> DictToCsv(columns=["name", "age"], csv_kwargs={"lineterminator": "\n"})
            >> SumAcc()
            >> Last()
        )
        print(await sink.get_frame(), end="")


    asyncio.run(main())

.. code-block:: text

    name,age
    Ann,30
    Bob,25

Columns can also be renamed and transformed. See :class:`~pyrill.csv.DictToCsv` and
:class:`~pyrill.csv.ListToCsv` for the accepted forms.
