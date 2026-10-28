==============
Files and text
==============

A pipeline reads a file a chunk at a time and passes each chunk through the stages,
so memory use stays the same whether the file has a hundred lines or a hundred
million.

Filter a log file
-----------------

This pipeline reads a log file, keeps the lines that report an error and writes
them to another file:

.. code-block:: python

    import asyncio
    import tempfile
    from pathlib import Path

    from pyrill.bytes import BytesChunksSeparator, Decode
    from pyrill.io import ChunkedFileSource, FileSink
    from pyrill.primitives import SkipIf


    async def main() -> None:
        with tempfile.TemporaryDirectory() as folder:
            log_path = Path(folder) / "app.log"
            errors_path = Path(folder) / "errors.log"
            log_path.write_text(
                "INFO start\nERROR disk full\nINFO retry\nERROR timeout\n"
            )

            sink = (
                ChunkedFileSource(path=log_path, mode="rb", chunk_size=8)
                >> BytesChunksSeparator()
                >> Decode()
                >> SkipIf(check_func=lambda line: line.startswith("ERROR"))
                >> FileSink(path=errors_path)
            )
            await sink.consume_all()

            print(errors_path.read_text(), end="")


    asyncio.run(main())

.. code-block:: text

    ERROR disk full
    ERROR timeout

Each element has one job:

* :class:`~pyrill.io.ChunkedFileSource` reads the file in chunks of ``chunk_size``.
  The chunks are small here (8 bytes) to make a point: they cut lines in the
  middle.
* :class:`~pyrill.bytes.BytesChunksSeparator` puts the lines back together. It
  emits one frame per line, whatever the chunk boundaries were. Use a different
  ``separator`` for other formats, such as ``b"\r\n"``.
* :class:`~pyrill.bytes.Decode` turns bytes into text.
* :class:`~pyrill.primitives.SkipIf` keeps the frames for which the function returns
  true, and drops the others. Despite its name, it *keeps* the frames that pass the
  test.
* :class:`~pyrill.io.FileSink` writes each frame to the output file. It is a sink
  that ends the chain, so you run the pipeline with ``consume_all()``.

In real code use a larger ``chunk_size`` (the default is 1024). The pipeline works
the same way.

Process a text file
-------------------

Open a file in text mode (the default) to read text directly, and use the ``String``
counterparts of the byte stages. This pipeline removes blank lines and the spaces
around each line:

.. code-block:: python

    import asyncio
    import tempfile
    from pathlib import Path

    from pyrill.accumulators import ListAcc
    from pyrill.io import ChunkedFileSource
    from pyrill.primitives import SkipIf
    from pyrill.sinks import Last
    from pyrill.strings import StringChunksSeparator
    from pyrill.strlike import Strip


    async def main() -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "notes.txt"
            path.write_text("  alpha  \n\nbeta\n   \ngamma")

            sink = (
                ChunkedFileSource(path=path, chunk_size=16)
                >> StringChunksSeparator()
                >> Strip()
                >> SkipIf(check_func=bool)
                >> ListAcc()
                >> Last()
            )
            print(await sink.get_frame())


    asyncio.run(main())

.. code-block:: text

    ['alpha', 'beta', 'gamma']

The last line has no newline at the end of the file, and it still arrives as a
frame: when the stream ends, the separator stage flushes what it holds.

Writing files
-------------

:class:`~pyrill.io.FileSink` overwrites an existing file by default. Pass
``mode="a"`` to append, ``mode="wb"`` to write bytes, and ``open_kwargs`` for
anything else that :meth:`pathlib.Path.open` accepts:

.. code-block:: python3

    FileSink(path=output_path, mode="a", open_kwargs={"encoding": "utf-8"})

The file is created when the first frame arrives, so a pipeline with no frames
leaves no empty file behind.

Other sources and sinks
-----------------------

The same ideas apply to other kinds of input and output:

* :class:`~pyrill.io.LineIOSource` and :class:`~pyrill.io.ChunkedIOSource` read from
  a file object you already have open.
* :class:`~pyrill.io.ChunkedStreamReaderSource` and
  :class:`~pyrill.io.StreamReaderSource` read from an :mod:`asyncio` stream, such as
  a socket. :doc:`parsing-and-converting` shows one.
* :class:`~pyrill.io.StreamReaderSink` feeds the frames of a pipeline into an
  :class:`asyncio.StreamReader`, and :class:`~pyrill.io.WriteTransportSource` turns
  what an :class:`asyncio.StreamWriter` writes into frames.
