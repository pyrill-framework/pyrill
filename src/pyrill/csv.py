from _csv import Writer as CsvWriter
from abc import abstractmethod
from asyncio.locks import Lock
from collections import deque
from collections.abc import Callable, Iterable, Mapping, Sequence
from csv import DictReader, reader, writer
from io import StringIO
from typing import Any, cast

from .base import BaseStage, FrameSkipped

__all__ = [
    "BaseFromCsv",
    "BaseToCsv",
    "ColumnsDict",
    "DictFromCsv",
    "DictToCsv",
    "FromCSVMapper",
    "ListFromCsv",
    "ListToCsv",
    "ToCSVMapper",
]

type ToCSVMapper = Callable[[Any], str | int | float]
type FromCSVMapper = Callable[[str | int | float], Any]

# _columns maps CSV column label (str) → (source field key, optional mapper).
# Field key is str for dict-backed rows, int for sequence-backed rows.
type ColumnsDict = dict[str, tuple[str | int, ToCSVMapper | None]]


class BaseToCsv[Sink_co: (Mapping[str, Any], Mapping[int, Any], Sequence[Any])](
    BaseStage[Sink_co, str]
):
    """
    Base stage that encodes each incoming row as one CSV text frame.

    The base class provides the CSV writer, the optional header row and the value
    mapping. A subclass only has to fill ``self._columns`` in ``__init__``: a dict
    that maps each CSV column label to a ``(field, mapper)`` pair, where ``field`` is
    the key or index read from the incoming row and ``mapper`` is a callable or
    ``None``. :class:`DictToCsv` and :class:`ListToCsv` do this from their ``columns``
    argument.

    When ``header`` is true, the first emitted frame is the header row, built from the
    column labels, and one frame is emitted per incoming row after it. A missing key
    or index produces an empty field, and ``None`` values are written as empty fields.
    Values that are not numbers are converted with ``str`` after the mapper runs. The
    writer is created on mount, so the element can be mounted again.

    :param csv_kwargs: Extra keyword arguments for :func:`csv.writer`, for example
        ``{"delimiter": ";"}``. Defaults to none.
    :param header: Emit a header row before the data. Defaults to ``True``.

    Input and output::

        input:  ["Ann", "x"], ["Bob", "y"]
        output: "left,right\\r\\n", "Ann,X\\r\\n", "Bob,Y\\r\\n"

    Usage:

    .. code-block:: python

        from collections.abc import Sequence
        from typing import Any

        from pyrill.csv import BaseToCsv
        from pyrill.sources import IterableSource


        class PairToCsv(BaseToCsv[Sequence[Any]]):
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                super().__init__(*args, **kwargs)
                self._columns = {"left": (0, None), "right": (1, str.upper)}


        source = IterableSource(source=[["Ann", "x"], ["Bob", "y"]])
        stage = source >> PairToCsv()

        await stage.mount()
        frames = [frame async for frame in stage]
        # ["left,right\\r\\n", "Ann,X\\r\\n", "Bob,Y\\r\\n"]
    """

    def __init__(
        self,
        *args: Any,
        csv_kwargs: dict[str, Any] | None = None,
        header: bool = True,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        self.io: StringIO | None = None
        self.csv_kwargs = csv_kwargs or {}
        self.header = header

        self._writer: CsvWriter | None = None
        self._first = True
        self._columns: ColumnsDict = {}

        self._lock = Lock()

    @property
    def columns(self) -> ColumnsDict:
        """
        Return a copy of the normalized columns: label to ``(field, mapper)``.
        """

        return self._columns.copy()

    async def build_headers(self) -> str:
        """
        Return the header row, built from the column labels, as one CSV text frame.
        """

        return await self.get_csv_string(list(self.columns.keys()))

    async def get_csv_string(self, lst: list[str | int | float | None]) -> str:
        """
        Encode one list of values as a CSV row and return it as text.

        Raise :class:`RuntimeError` when the element is not mounted.
        """

        if self.io is None:
            raise RuntimeError("Stream not initialized")

        assert self._writer is not None

        async with self._lock:
            self._writer.writerow(lst)
            self.io.seek(0)
            result = self.io.getvalue()
            self.io.seek(0)
            self.io.truncate()
            return result

    async def _map_value(self, key: str, value: Any) -> Any:
        if value is None:
            return value

        mapper = self._columns[key][1]
        if mapper is not None:
            value = mapper(value)

        if isinstance(value, (int, float)):
            return value

        return str(value)

    async def _process_frame(self, frame: Sink_co) -> str:
        data: list[str | int | float | None] = []

        for label, (field, _) in self.columns.items():
            try:
                data.append(
                    await self._map_value(
                        label, cast(Mapping[str | int, Any], frame)[field]
                    )
                )
            except KeyError, IndexError:
                data.append(None)
        return await self.get_csv_string(data)

    async def _next_frame(self) -> str:
        if self.header and self._first:
            self._first = False
            return await self.build_headers()

        return await super()._next_frame()

    async def _mount(self) -> None:
        self.io = StringIO()
        self._writer = writer(self.io, **self.csv_kwargs)
        self._first = True

        await super()._mount()

    async def _unmount(self) -> None:
        self.io = None
        self._writer = None

        await super()._unmount()


def _normalize_column(
    label: Any,
    field: Any = None,
    mapper: ToCSVMapper | None = None,
) -> tuple[str, tuple[str | int, ToCSVMapper | None]]:
    if isinstance(label, tuple):
        return _normalize_column(*label)  # pyright: ignore[reportUnknownArgumentType]

    if field is None:
        field = label
    elif isinstance(field, tuple):
        match len(field):  # pyright: ignore[reportUnknownArgumentType]
            case 0:
                return _normalize_column(label)
            case 1:
                return _normalize_column(label, field[0])
            case _:
                return cast(str, label), field[:2]  # pyright: ignore[reportUnknownVariableType]
    elif callable(field):
        return cast(str, label), (cast(str, label), cast(ToCSVMapper, field))

    return cast(str, label), (field, mapper)


class DictToCsv(BaseToCsv[Mapping[str, Any]]):
    """
    Stage that encodes each incoming dict as one CSV row.

    The ``columns`` argument selects and orders the CSV columns. It accepts a list or
    a dict. In both forms a missing key gives an empty field, and a mapper is a
    callable applied to the value before it is written.

    List forms (the label is also the key when no key is given):

    * ``"name"``: label and key are ``"name"``.
    * ``("Name", "name")``: label ``"Name"``, key ``"name"``.
    * ``("Name", "name", mapper)``: as above, with a mapper.
    * ``("name", mapper)``: key ``"name"`` (equal to the label), with a mapper.

    Dict forms (the dict key is the label):

    * ``{"Name": "name"}``: label ``"Name"``, key ``"name"``.
    * ``{"name": mapper}``: key ``"name"`` (equal to the label), with a mapper.
    * ``{"Name": ("name",)}``: label ``"Name"``, key ``"name"``.
    * ``{"Name": ("name", mapper)}``: label ``"Name"``, key ``"name"``, with a mapper.

    See :class:`BaseToCsv` for the header row and the other behaviour.

    :param columns: Column specification, in one of the forms above. Required.
    :param header: Emit a header row first. Defaults to ``True``.
    :param csv_kwargs: Extra keyword arguments for :func:`csv.writer`.

    Input and output::

        input:  {"name": "Ann", "age": 30, "score": 1.234}
        output: "Name,Age,Score\\r\\n", "ANN,30,1.2\\r\\n"

    Usage:

    .. code-block:: python

        from pyrill.csv import DictToCsv
        from pyrill.sources import IterableSource

        source = IterableSource(source=[{"name": "Ann", "age": 30, "score": 1.234}])
        stage = source >> DictToCsv(
            columns=[
                ("Name", "name", str.upper),
                ("Age", "age"),
                ("Score", "score", lambda value: round(value, 1)),
            ]
        )

        await stage.mount()
        frames = [frame async for frame in stage]
        # ["Name,Age,Score\\r\\n", "ANN,30,1.2\\r\\n"]
    """

    def __init__(
        self,
        *args: Any,
        columns: dict[
            str,
            str
            | ToCSVMapper
            | tuple[str]
            | tuple[ToCSVMapper]
            | tuple[str, ToCSVMapper],
        ]
        | list[
            str
            | tuple[str, str]
            | tuple[str, ToCSVMapper]
            | tuple[str, str, ToCSVMapper]
        ],
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        self._columns = dict(
            [
                _normalize_column(c)
                for c in (columns.items() if isinstance(columns, Mapping) else columns)
            ]
        )


def _normalize_list_columns(
    columns: Iterable[
        str
        | tuple[str]
        | tuple[str, int]
        | tuple[str, ToCSVMapper]
        | tuple[str, int, ToCSVMapper]
    ],
) -> Iterable[tuple[str, tuple[str | int, ToCSVMapper | None]]]:
    for i, c in enumerate(columns):
        if isinstance(c, str):
            yield _normalize_column(c, i)
        else:
            if len(c) == 1:
                yield _normalize_column(c[0], i)
            elif len(c) == 2:
                c2 = c[1]
                if callable(c2):
                    yield _normalize_column(c[0], i, c2)
                elif isinstance(c2, tuple):
                    if len(c2) > 0:
                        if callable(c2[0]):  # pyright: ignore[reportUnknownArgumentType]
                            yield _normalize_column(c[0], i, c2[0])  # pyright: ignore[reportUnknownArgumentType]
                        else:
                            yield _normalize_column(c[0], *c2)  # pyright: ignore[reportUnknownArgumentType]
                    else:
                        yield _normalize_column(c[0], i)
                else:
                    yield _normalize_column(c[0], c2)
            else:
                yield _normalize_column(*c)


class ListToCsv(BaseToCsv[Sequence[Any]]):
    """
    Stage that encodes each incoming sequence as one CSV row.

    The ``columns`` argument selects and orders the CSV columns by position in the
    incoming sequence. It accepts a list or a dict. A missing index gives an empty
    field, and a mapper is a callable applied to the value before it is written.

    List forms (without an index, the column position in ``columns`` is used):

    * ``"name"``: label ``"name"``, index equal to the position of the column.
    * ``("Name",)``: the same, with label ``"Name"``.
    * ``("Name", 2)``: label ``"Name"``, index ``2``.
    * ``("Name", mapper)``: index equal to the position of the column, with a mapper.
    * ``("Name", 2, mapper)``: label ``"Name"``, index ``2``, with a mapper.

    Dict forms (the dict key is the label):

    * ``{"Name": 2}``: label ``"Name"``, index ``2``.
    * ``{"Name": mapper}``: index equal to the position of the column, with a mapper.
    * ``{"Name": (2,)}``: label ``"Name"``, index ``2``.
    * ``{"Name": (2, mapper)}``: label ``"Name"``, index ``2``, with a mapper.

    See :class:`BaseToCsv` for the header row and the other behaviour.

    :param columns: Column specification, in one of the forms above. Required.
    :param header: Emit a header row first. Defaults to ``True``.
    :param csv_kwargs: Extra keyword arguments for :func:`csv.writer`.

    Input and output::

        input:  ["Ann", 30, "eng"], ["Bob", 25, "ops"]
        output: "Name,Role\\r\\n", "Ann,ENG\\r\\n", "Bob,OPS\\r\\n"

    Usage:

    .. code-block:: python

        from pyrill.csv import ListToCsv
        from pyrill.sources import IterableSource

        source = IterableSource(source=[["Ann", 30, "eng"], ["Bob", 25, "ops"]])
        stage = source >> ListToCsv(columns={"Name": 0, "Role": (2, str.upper)})

        await stage.mount()
        frames = [frame async for frame in stage]
        # ["Name,Role\\r\\n", "Ann,ENG\\r\\n", "Bob,OPS\\r\\n"]
    """

    def __init__(
        self,
        *args: Any,
        columns: dict[
            str,
            int
            | ToCSVMapper
            | tuple[int]
            | tuple[ToCSVMapper]
            | tuple[int, ToCSVMapper],
        ]
        | list[
            str
            | tuple[str]
            | tuple[str, int]
            | tuple[str, ToCSVMapper]
            | tuple[str, int, ToCSVMapper],
        ],
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        if isinstance(columns, (dict, Mapping)):
            self._columns = dict(
                _normalize_list_columns(
                    cast(Iterable[tuple[str, Any]], columns.items())
                )
            )
        else:
            self._columns = dict(_normalize_list_columns(columns))


class BaseFromCsv[ColumnType](BaseStage[str, dict[str, Any] | list[str]]):
    """
    Base stage that parses CSV text frames into rows.

    Each incoming frame is a string with one or more CSV rows. Every row is emitted
    as its own frame, in order, so a multi-row frame produces several output frames.
    A frame that holds no row (for example an empty string) is skipped. Rows are
    parsed by ``self.reader``, which is created on mount and reads the text of the
    current frame. Quoted fields are supported, and the parser state (for example the
    header of a dict reader) is kept across frames.

    A subclass has to implement the abstract ``_process_row(row)``, which converts a
    parsed row into the output frame, and has to set ``self.reader`` after
    ``await super()._mount()`` in its ``_mount``, using ``self.io`` as the file.
    :class:`ListFromCsv` and :class:`DictFromCsv` do this. :meth:`map_value` applies
    the optional per-column ``maps``.

    :param csv_kwargs: Extra keyword arguments for the reader, for example
        ``{"delimiter": ";"}``. Defaults to none.
    :param maps: Callables applied to the value of a column, keyed by column
        (position or name, depending on the subclass). Defaults to none.

    Input and output::

        input:  "Ann,30\\nBob,25\\n"
        output: {"n": 2, "first": "Ann"}, {"n": 2, "first": "Bob"}

    Usage:

    .. code-block:: python

        import csv
        from typing import Any

        from pyrill.csv import BaseFromCsv
        from pyrill.sources import IterableSource


        class SummaryFromCsv(BaseFromCsv[int]):
            async def _process_row(self, row: Any) -> Any:
                return {"n": len(row), "first": row[0]}

            async def _mount(self) -> None:
                await super()._mount()
                self.reader = csv.reader(self.io, **self.csv_kwargs)


        source = IterableSource(source=["Ann,30\\nBob,25\\n"])
        stage = source >> SummaryFromCsv()

        await stage.mount()
        frames = [frame async for frame in stage]
        # [{"n": 2, "first": "Ann"}, {"n": 2, "first": "Bob"}]
    """

    def __init__(
        self,
        *args: Any,
        csv_kwargs: dict[str, Any] | None = None,
        maps: dict[ColumnType, FromCSVMapper] | None = None,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        self.io: StringIO | None = None
        self.csv_kwargs = csv_kwargs or {}
        self.reader: Iterable[list[str]] | Iterable[dict[str, Any]] | None = None

        self.maps: dict[ColumnType, FromCSVMapper] = maps or {}

        # Rows parsed from a multi-row frame and not yet emitted.
        self._rows: deque[dict[str, Any] | list[str]] = deque()

        self._lock = Lock()

    async def get_data_from_csv(self, frame: str) -> dict[str, Any] | list[Any]:
        """
        Parse one CSV text frame and return its first row, queueing the other rows.

        Raise :class:`RuntimeError` when the element is not mounted. A frame without rows
        is skipped.
        """

        if self.io is None:
            raise RuntimeError("Stream not initialized")
        async with self._lock:
            self.io.seek(0)
            self.io.truncate()
            self.io.write(frame)
            self.io.seek(0)

            assert self.reader is not None
            self._rows.extend(self.reader)
            if not self._rows:
                raise FrameSkipped(frame)
            return self._rows.popleft()

    async def map_value(self, key: ColumnType, value: str | float) -> Any:
        """
        Return the value after applying the mapper registered for ``key`` in ``maps``.

        When there is no mapper for the key, the value is returned unchanged.
        """

        mapper = self.maps.get(key)
        if mapper is not None:
            value = mapper(value)

        return value

    async def _next_frame(self) -> dict[str, Any] | list[Any]:
        if self._rows:
            return await self._process_row(self._rows.popleft())
        return await super()._next_frame()

    async def _process_frame(self, frame: str) -> dict[str, Any] | list[Any]:
        return await self._process_row(await self.get_data_from_csv(frame))

    @abstractmethod
    async def _process_row(
        self, row: dict[str, Any] | list[Any]
    ) -> dict[str, Any] | list[Any]:
        raise NotImplementedError()

    async def _mount(self) -> None:
        self.io = StringIO()
        self._rows.clear()
        await super()._mount()

    async def _unmount(self) -> None:
        self.io = None
        self.reader = None
        self._rows.clear()
        await super()._unmount()


class ListFromCsv(BaseFromCsv[int]):
    """
    Stage that parses CSV text frames into lists of values.

    Every row of a frame becomes one list, so a frame with several rows emits several
    lists. Values are strings unless a mapper is configured in ``maps``, keyed by the
    column position. There is no header handling: a header row is emitted as an
    ordinary row. See :class:`BaseFromCsv` for the other behaviour.

    :param maps: Callables applied to a column, keyed by its position, for example
        ``{1: int}``. Defaults to none.
    :param csv_kwargs: Extra keyword arguments for :func:`csv.reader`.

    Input and output with ``maps={1: int}``::

        input:  "Ann,30\\nBob,25\\n", "Eve,41\\n"
        output: ["Ann", 30], ["Bob", 25], ["Eve", 41]

    Usage:

    .. code-block:: python

        from pyrill.csv import ListFromCsv
        from pyrill.sources import IterableSource

        source = IterableSource(source=["Ann,30\\nBob,25\\n", "Eve,41\\n"])
        stage = source >> ListFromCsv(maps={1: int})

        await stage.mount()
        frames = [frame async for frame in stage]
        # [["Ann", 30], ["Bob", 25], ["Eve", 41]]
    """

    async def _process_row(self, row: dict[str, Any] | list[Any]) -> list[Any]:
        return [await self.map_value(k, v) for k, v in enumerate(row)]

    async def _mount(self) -> None:
        await super()._mount()
        assert self.io is not None
        self.reader = reader(self.io, **self.csv_kwargs)


class DictFromCsv(BaseFromCsv[str]):
    """
    Stage that parses CSV text frames into dicts.

    Every row of a frame becomes one dict, so a frame with several rows emits several
    dicts. The keys come from the first row received, which is used as the header, or
    from ``fieldnames`` in ``csv_kwargs`` (then no header row is expected). Values are
    strings unless a mapper is configured in ``maps``, keyed by the column name. See
    :class:`BaseFromCsv` for the other behaviour.

    :param maps: Callables applied to a column, keyed by its name, for example
        ``{"age": int}``. Defaults to none.
    :param csv_kwargs: Extra keyword arguments for :class:`csv.DictReader`, for
        example ``{"fieldnames": ["name", "age"]}``.

    Input and output with ``maps={"age": int}``::

        input:  "name,age\\nAnn,30\\n", "Bob,25\\n"
        output: {"name": "Ann", "age": 30}, {"name": "Bob", "age": 25}

    Usage:

    .. code-block:: python

        from pyrill.csv import DictFromCsv
        from pyrill.sources import IterableSource

        source = IterableSource(source=["name,age\\nAnn,30\\n", "Bob,25\\n"])
        stage = source >> DictFromCsv(maps={"age": int})

        await stage.mount()
        frames = [frame async for frame in stage]
        # [{"name": "Ann", "age": 30}, {"name": "Bob", "age": 25}]
    """

    async def _process_row(self, row: dict[str, Any] | list[Any]) -> dict[str, Any]:
        assert isinstance(row, dict)
        return {k: await self.map_value(k, v) for k, v in row.items()}

    async def _mount(self) -> None:
        await super()._mount()
        assert self.io is not None
        self.reader = DictReader(self.io, **self.csv_kwargs)
