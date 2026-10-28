===============
What is PyRill?
===============

PyRill is a small library for building *pipelines* that process streams of data
asynchronously. A pipeline is a chain of reusable elements: a source produces
data, stages transform it, and a sink consumes the result. You describe the chain
once, and PyRill takes care of reading, buffering, ending the stream and cleaning
up.

The problem
-----------

Programs that handle streams (files, sockets, API responses, queues) tend to grow
hand-written loops that mix several concerns in one place: reading, splitting the
data into records, decoding, parsing, filtering, timing, error handling and
writing. Such loops are hard to test, hard to reuse and hard to change, because
nothing in them is a separate piece.

PyRill separates those concerns. Each one becomes a small element with one job
and a typed input and output. You build the program by connecting the elements:

.. code-block:: python3

    pipeline = (
        ChunkedFileSource(path=log_path, mode="rb")   # read bytes
        >> BytesChunksSeparator()                     # rebuild lines
        >> Decode()                                   # bytes to text
        >> SkipIf(check_func=is_error)                # keep errors only
        >> FileSink(path=errors_path)                 # write them
    )

Every element can be tested alone, reused in another pipeline, or replaced without
touching the rest.

How it works
------------

* **Pull-based.** A sink asks its source for a frame, which asks its own source,
  and so on up the chain. Work happens only when a frame is needed, so a slow
  consumer naturally slows down the whole pipeline, and frames do not pile up in
  memory unless you add a buffer on purpose.
* **One frame at a time.** A pipeline holds only the frame being processed, so
  memory use depends on the size of the frames, not on the size of the stream. The
  exception is when you accumulate frames on purpose, for example with
  :class:`~pyrill.accumulators.ListAcc`.
* **Built on asyncio.** Elements are async, so a pipeline can wait on files,
  sockets and timers without blocking other tasks.
* **Typed.** Elements are generic classes (``IterableSource[int]``,
  ``Map[int, str]``), so type checkers such as mypy and pyright can follow the
  frames through a pipeline.
* **No dependencies.** PyRill needs only Python 3.14 or later.

What you can use it for
-----------------------

Streaming data processing
    Read a large file or response in chunks, transform it frame by frame and write
    the result, without loading everything into memory. See
    :doc:`guides/files-and-text`.

Parsing network and byte streams
    Rebuild lines or records from data that arrives in arbitrary chunks, and decode
    text safely even when a multi-byte character is split across chunks. See
    :doc:`guides/parsing-and-converting`.

Format conversion
    Turn CSV text into dictionaries, dictionaries into JSON, and back, one row at a
    time. See :doc:`guides/parsing-and-converting`.

Branching and merging
    Send one stream to several consumers, pair frames from several streams, or
    merge live feeds into one. See :doc:`guides/branching-and-merging`.

Timing and flow control
    Throttle a stream, give each frame a deadline, start or stop at a given moment,
    or pause and resume a stream from another task. See
    :doc:`guides/timing-and-flow`.

Bridging callbacks to streams
    Push frames into a pipeline from any code with a queue-backed source. See
    :doc:`guides/timing-and-flow`.

Reusable building blocks
    Write your own sources, stages and sinks, and package a chain of stages as a
    single reusable stage. See :doc:`guides/custom-elements`.

Resilient pipelines
    Drop bad frames, retry failed producers and handle errors in one place. See
    :doc:`guides/errors-and-retries`.

When PyRill is a good fit
-------------------------

PyRill works well when you have a flow of data that goes through several steps,
when the data is larger than you want to hold in memory, or when the steps
depend on I/O or time. It also helps when you want the steps to be reusable and
easy to test.

When it is not
--------------

* **It runs in one process, in one event loop.** It is not a distributed system
  and it is not a message broker. Use it to process a stream inside a program, not
  to move data between machines.
* **It does not parallelise CPU-bound work.** A stage that computes for a long time
  blocks the event loop like any other async code. Move heavy work to a process
  pool, or to another tool.
* **It needs Python 3.14 or later.**
* **For a single loop over a list, plain Python is simpler.** PyRill pays off when
  a pipeline has several steps, I/O, timing or reuse.

Where to go next
----------------

* :doc:`quickstart` installs PyRill and builds a first pipeline.
* :doc:`concepts` explains elements, frames, connections and the lifecycle.
* :doc:`guides/index` has task-oriented guides with complete examples.
* :doc:`catalog/index` documents every element in detail.
