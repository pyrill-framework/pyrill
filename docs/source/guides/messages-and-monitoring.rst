=======================
Messages and monitoring
=======================

A :class:`~pyrill.base.Message` is an event about a pipeline: an element became
ready, a frame was rejected, or a file was opened. Frames carry the data being
processed through ``>>`` connections; messages go to registered callbacks on a
separate bus. Listening to messages does not consume or transform frames.

Use messages to collect metrics, report failures, trace resource activity, or
publish application-specific progress from your own elements.

What a message contains
-----------------------

``Message`` is a dataclass imported from ``pyrill.base`` with four fields:

.. list-table::
    :header-rows: 1

    * - Field
      - Meaning
    * - ``sender: str``
      - Name of the originating element. Give elements explicit ``name`` values
        when you need stable monitoring labels; defaults include a UUID.
    * - ``pipeline: str``
      - Name of the originating pipeline.
    * - ``type: str``
      - Event name, such as ``"frame-skipped"`` or your own application event.
    * - ``params: dict[str, Any]``
      - Event-specific data; defaults to an empty dictionary.

Parameters contain Python objects, including exceptions and ``Path`` instances.
Convert them explicitly before exporting events as JSON. Messages have no built-in
timestamp or run identifier; add those in your monitoring code if needed.

Subscribe to an element or a pipeline
-------------------------------------

Call ``element.add_message_callback(callback, message_type=None)`` to observe one
element, or ``pipeline.add_message_callback(...)`` to observe all elements in that
pipeline whose messages propagate to it. Register callbacks before running the
pipeline so that you see its startup events.

Callbacks are regular synchronous functions called as ``callback(broker, message)``.
The first argument is the element for an element subscription and the ``Pipeline``
for a pipeline subscription. Use ``message.sender`` to identify the originating
element in a pipeline callback. The optional ``message_type`` is an exact string
filter; omit it to receive all types. It does not accept a list or wildcard.

Delivery happens inline, in this order:

1. Matching callbacks on the emitting element, in registration order.
2. The pipeline's debug log (``Sending message: ...``).
3. Matching callbacks on the pipeline, in registration order.

Messages do not travel through neighboring stages. There is no history or replay
for subscribers added later. Callbacks stay registered across mounts unless they
remove themselves through their return value:

.. list-table::
    :header-rows: 1

    * - Return value
      - Subscription and delivery
    * - ``None`` or ``False``
      - Keep this callback and continue delivery.
    * - ``True``
      - Remove this callback after this message and continue delivery.
    * - ``(False, False)``
      - Keep this callback and stop delivery of this message.
    * - ``(True, False)``
      - Remove this callback and stop delivery of this message.
    * - ``(remove, True)``
      - Remove this callback if ``remove`` is true and continue delivery.

Stopping delivery on an element also prevents the pipeline log and pipeline
callbacks from seeing that message. Stopping it on the pipeline skips the remaining
pipeline callbacks. For monitoring, return ``None`` so other observers still run.

Keep callbacks short and avoid raising exceptions: they execute inside the operation
that emitted the event. A callback failure can affect that operation, although some
error and skipped-frame notification paths suppress callback exceptions. Coroutine
callbacks are not awaited. For network exports, enqueue events with a synchronous
callback and let a separate async worker send them; choose a bounded queue and an
explicit overflow policy so monitoring cannot grow without limit.

Built-in events
---------------

The lifecycle, stream and skipped-frame constants are in ``pyrill.base``:

.. list-table::
    :header-rows: 1

    * - Constant / type
      - When emitted
      - Parameters
    * - ``MSG_ELEMENT_READY`` / ``element-ready``
      - After an element mounts successfully and enters ``READY``.
      - None.
    * - ``MSG_ELEMENT_NULL`` / ``element-null``
      - After an element unmounts successfully and enters ``NULL``.
      - None.
    * - ``MSG_ELEMENT_ERROR`` / ``element-error``
      - When an element enters ``ERROR`` after a failure.
      - ``ex``: the exception object.
    * - ``MSG_STREAM_START`` / ``stream-start``
      - A ``BaseSource`` begins its first read after mounting, before fetching
        the first frame. Mounting alone does not emit it; empty sources emit it too.
      - None.
    * - ``MSG_STREAM_STOP`` / ``stream-stop``
      - A ``BaseSource`` encounters ``StopAsyncIteration`` or a consumer encounters
        upstream exhaustion.
      - None.
    * - ``MSG_FRAME_SKIPPED`` / ``frame-skipped``
      - When a frame is skipped, including rejected ``SkipIf`` frames and data
        discarded by ``SkipUntil``.
      - ``frame``: discarded data or the value supplied to ``FrameSkipped``.

These are element events, not whole-pipeline status updates. One failure can put
several downstream elements into ``ERROR`` and generate several notifications.
``element-null`` means resources were released; it can also follow an early stop,
so it alone does not prove that every input was processed. No message is emitted
for the intermediate ``MOUNTING`` and ``UNMOUNTING`` states.

The file event constants are in ``pyrill.io``:

.. list-table::
    :header-rows: 1

    * - Constant / type
      - When emitted
      - Parameters
    * - ``MSG_FILE_READ_START`` / ``file-read-start``
      - A ``BaseFileSource`` opens its file, normally on the first read.
      - ``path``: a ``Path``; ``size``: file size in bytes from ``stat()``.
    * - ``MSG_FILE_READ_END`` / ``file-read-end``
      - That source closes its file.
      - ``path``.
    * - ``MSG_FILE_WRITE_START`` / ``file-write-start``
      - ``FileSink`` opens its file on the first frame.
      - ``path``.
    * - ``MSG_FILE_WRITE_END`` / ``file-write-end``
      - That sink closes its file.
      - ``path``; ``size``: the stream's final ``tell()`` value.

Use these events for file activity logs and timing how long a file stays open.
They do not report progress after each chunk, and a close event is not a guarantee
of successful processing. In text mode, the write-end position is not a portable
count of bytes written. An empty ``FileSink`` creates no file and emits no file
open/close events.

Stream events describe individual elements, not a single job-wide start or stop.
A source emits one start per mount/read cycle and resets that tracking when
unmounted. Stages and sinks do not automatically emit start events. A source's
stop precedes its ``element-null`` event on normal exhaustion. Consumers also
emit stop when they observe exhaustion. A full sink emits one stop on normal
exhaustion, before unmounting. A pipeline subscription can receive stop events
from several elements in the same run; use the sender to distinguish them rather
than counting each message as a completed job.

An explicit unmount, an early consumer such as ``First``, or a failure does not
necessarily produce a stream-stop event. Use error events for failures and the
return or exception from the awaited sink operation to determine job completion.
The old ``BUS_MSG_*`` names are no longer available; use ``MSG_STREAM_START`` and
``MSG_STREAM_STOP`` for stream events. Reset and log event constants were removed.

Monitor lifecycle, rejected frames and errors
---------------------------------------------

This example uses an explicit pipeline to isolate subscriptions, names its
elements, and counts events by sender and type. ``SkipIf`` keeps frames for which
the predicate returns true, so the negative number generates one skipped event.

.. code-block:: python

    import asyncio
    from collections import Counter

    from pyrill.base import (
        MSG_ELEMENT_ERROR,
        MSG_FRAME_SKIPPED,
        Message,
        MessageBrokerMixin,
        Pipeline,
    )
    from pyrill.primitives import SkipIf
    from pyrill.sinks import BlackHole
    from pyrill.sources import IterableSource


    async def main() -> None:
        pipeline = Pipeline("validation")
        events: Counter[tuple[str, str]] = Counter()

        def monitor(broker: MessageBrokerMixin, message: Message) -> None:
            events[message.sender, message.type] += 1
            if message.type == MSG_ELEMENT_ERROR:
                print("failed:", message.sender, repr(message.params["ex"]))

        pipeline.add_message_callback(monitor)
        source = IterableSource[int](
            source=[1, -2, 3], name="input", pipeline=pipeline
        )
        valid = source >> SkipIf[int](
            check_func=lambda value: value >= 0,
            name="validation",
            pipeline=pipeline,
        )
        sink = valid >> BlackHole[int](name="output", pipeline=pipeline)
        await sink.consume_all()
        print("rejected:", events["validation", MSG_FRAME_SKIPPED])


    asyncio.run(main())

.. code-block:: text

    rejected: 1

For an error dashboard, subscribe only to ``MSG_ELEMENT_ERROR`` and record
``message.sender`` and ``message.params["ex"]``. Still handle exceptions where
you await the sink: observing an error message does not handle or recover from it.
Avoid counting each downstream error as a separate failed job.

There is no built-in message for every successfully processed frame. For throughput
or progress, add instrumentation in a stage and emit your own event, as below.

Publish application progress
----------------------------

Custom elements can use ``self._build_message(type, **params)`` to fill in sender
and pipeline names, then ``self._send_message(message)`` to use the normal element
and pipeline delivery path. These protected hooks are intended for subclass code.
Choose an application-specific event name to avoid collisions with built-in types.

.. code-block:: python

    import asyncio

    from pyrill.base import BaseStage, Message, MessageBrokerMixin, Pipeline
    from pyrill.sinks import BlackHole
    from pyrill.sources import IterableSource

    PROGRESS = "import-progress"


    class ReportProgress(BaseStage[int, int]):
        async def _mount(self) -> None:
            await super()._mount()
            self.processed: int = 0

        async def _process_frame(self, frame: int) -> int:
            self.processed += 1
            self._send_message(
                self._build_message(PROGRESS, processed=self.processed)
            )
            return frame


    def show_progress(broker: MessageBrokerMixin, message: Message) -> None:
        print(f"{message.pipeline}/{message.sender}: {message.params['processed']}")


    async def main() -> None:
        pipeline = Pipeline("import")
        pipeline.add_message_callback(show_progress, PROGRESS)
        source = IterableSource[int](source=[10, 20], pipeline=pipeline)
        progress = source >> ReportProgress(name="records", pipeline=pipeline)
        sink = progress >> BlackHole[int](pipeline=pipeline)
        await sink.consume_all()


    asyncio.run(main())

.. code-block:: text

    import/records: 1
    import/records: 2

This counts frames at the reporting stage; it does not confirm that a later sink
has written them successfully. Place instrumentation at the point whose work you
want to measure. For high-volume streams, report periodically instead of emitting
one message per frame.

Application code outside an element can construct a ``Message`` and call
``pipeline.send_message(message)`` directly. Supply ``sender``, ``pipeline`` and
``type`` explicitly; this method logs and dispatches the message to pipeline
callbacks without visiting element callbacks or validating the origin fields.
