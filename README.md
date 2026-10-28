# PyRill

Asynchronous streams for Python.

PyRill builds asynchronous data pipelines in plain Python. You connect small
elements (sources, stages and sinks) with `>>`, and the data flows through them one
frame at a time, on top of `asyncio` and with no dependencies.

```python
import asyncio

from pyrill.accumulators import ListAcc
from pyrill.mappers import Map
from pyrill.sinks import Last
from pyrill.sources import IterableSource


async def main() -> None:
    pipeline = (
        IterableSource(source=["alpha", "beta", "gamma"])
        >> Map(map_func=str.upper)
        >> ListAcc()
        >> Last()
    )
    print(await pipeline.get_frame())


asyncio.run(main())
```

```text
['ALPHA', 'BETA', 'GAMMA']
```

## Why PyRill

Programs that handle streams (files, sockets, API responses, queues) tend to grow
hand-written loops that mix reading, parsing, filtering, timing and error handling.
PyRill turns each of those concerns into a small, typed, reusable element that you
can test alone and connect with the others.

- **Pull-based.** Work happens only when a frame is needed, so a slow consumer slows
  the whole pipeline, and frames do not pile up in memory unless you add a buffer.
- **Streaming.** A pipeline works on one frame at a time, so memory use depends on
  the size of the frames, not on the length of the stream.
- **Batteries included.** Elements for files and asyncio streams, byte and text
  re-chunking, safe UTF-8 decoding, CSV, JSON, branching (`Tee`), pairing (`Zip`),
  merging (`Union`), timeouts and deadlines, pausing (`Gate`), buffering (`Queue`)
  and more.
- **Extensible.** Write your own sources, stages and sinks, or package a chain of
  stages as a single reusable stage.
- **Typed.** Elements are generic classes, so type checkers follow the frames through
  a pipeline.

## Installation

PyRill needs Python 3.14 or later and has no other dependencies.

```console
$ pip install pyrill
```

To work on PyRill itself, clone the repository, run `uv sync`, and run the tests with
`uv run pytest`.

## Documentation

Full documentation, including a quickstart, the core concepts, task-oriented guides
and a catalog of every element, is at
<https://pyrill-framework.readthedocs.io/stable/>. The same pages are in the
[`docs/`](docs/source) folder.

The [messages and monitoring guide](docs/source/guides/messages-and-monitoring.rst)
explains how to observe lifecycle changes, rejected frames and file activity, and
publish custom progress events through the pipeline's message bus.

## Project

- Repository and issue tracker: <https://github.com/pyrill-framework/pyrill>
- License: LGPL-2.1-or-later
