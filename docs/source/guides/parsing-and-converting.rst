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

Convert CSV to JSON
-------------------

:class:`~pyrill.csv.DictFromCsv` parses CSV text into dictionaries, one per row.
The first row it receives is the header. ``maps`` converts columns to other types.
:class:`~pyrill.json.ToJsonPerLine` writes each dictionary as one line of JSON.

This pipeline keeps the people who are at least 30 years old:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import ListAcc
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
            >> ListAcc()
            >> Last()
        )
        for line in await sink.get_frame():
            print(line, end="")


    asyncio.run(main())

.. code-block:: text

    {"name":"Ann","age":30}
    {"name":"Cleo","age":41}

A frame can hold several CSV rows, so you can read the text in any chunking that is
convenient. Combine it with :class:`~pyrill.bytes.BytesChunksSeparator` to read a
CSV file line by line.

Build one JSON document
-----------------------

:class:`~pyrill.json.ToJsonPerLine` gives you one JSON document per frame, which
suits log files and message streams. To produce a single JSON array or object from
many frames, use :class:`~pyrill.json.ToJsonList` or
:class:`~pyrill.json.ToJsonObject`. They emit the text in fragments as the items
arrive, so a large document does not have to be built as one string. Join the
fragments with :class:`~pyrill.accumulators.SumAcc` when you do want the whole
text:

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
