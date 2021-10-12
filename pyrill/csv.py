from asyncio.locks import Lock
from csv import DictReader, reader, writer
from io import StringIO
from typing import (Any, Callable, Dict, Generic, Iterable, List, Mapping,
                    Optional, Tuple, TypeVar, Union)

from .base import BaseStage, FrameSkippedError, Sink_co, Source_co

__all__ = ['BaseToCsv', 'BaseFromCsv', 'DictToCsv', 'DictFromCsv', 'ListToCsv', 'ListFromCsv']

ToCSVMapper = Callable[[Any], Union[str, int, float]]
FromCSVMapper = Callable[[Union[str, int, float]], Any]


class BaseToCsv(BaseStage[Sink_co, str]):

    def __init__(self,
                 *args,
                 csv_kwargs: Dict = None,
                 header: bool = True,
                 **kwargs):
        super(BaseToCsv, self).__init__(*args, **kwargs)

        self.io: Optional[StringIO] = None
        self.csv_kwargs = csv_kwargs or {}
        self.header = header

        self._writer = None
        self._first = True
        self._columns: Dict[str, Tuple[str, ToCSVMapper]] = {}

        self._lock = Lock()

    @property
    def columns(self) -> Dict[str, Tuple[str, ToCSVMapper]]:
        return self._columns.copy()

    async def build_headers(self) -> str:
        return await self.get_csv_string(list(self.columns.keys()))

    async def get_csv_string(self, lst: List[Union[str, int, float, None]]) -> str:
        if self.io is None:
            raise RuntimeError('Stream not initialized')

        async with self._lock:
            self._writer.writerow(lst)
            self.io.seek(0)
            result = self.io.getvalue()
            self.io.seek(0)
            self.io.truncate()
            return result

    async def map_value(self, key: str, value: Optional[Any]):
        if value is None:
            return value

        try:
            value = self.columns[key][1](value)
        except TypeError:
            pass

        if isinstance(value, (int, float)):
            return value

        return str(value)

    async def process_frame(self, frame: Sink_co) -> str:
        data = []

        for label, (field, mapper) in self.columns.items():
            try:
                data.append(await self.map_value(label, frame[field]))
            except (KeyError, IndexError):
                data.append(None)
        return await self.get_csv_string(data)

    async def _next_frame(self) -> str:
        if self.header and self._first:
            self._first = False
            return await self.build_headers()

        return await super(BaseToCsv, self)._next_frame()

    async def _mount(self):
        self.io = StringIO()
        self._writer = writer(self.io, **self.csv_kwargs)
        self._first = True

        await super(BaseToCsv, self)._mount()

    async def _unmount(self):
        self.io = None
        self._writer = None

        await super(BaseToCsv, self)._unmount()


ColumnType = TypeVar('ColumnType', int, str)


def _normalize_column(label: Union[str,
                                   Tuple[str, ToCSVMapper],
                                   Tuple[str, ColumnType],
                                   Tuple[str, ColumnType, ToCSVMapper]],
                      field: Union[ColumnType,
                                   str,
                                   ToCSVMapper,
                                   Tuple[ColumnType, ToCSVMapper]] = None,
                      mapper: ToCSVMapper = None) -> Tuple[str,
                                                           Tuple[ColumnType,
                                                                 ToCSVMapper]]:
    if isinstance(label, tuple):
        return _normalize_column(*label)

    if field is None:
        field = label
    elif isinstance(field, tuple):
        if len(field) == 0:
            return _normalize_column(label)
        if len(field) == 1:
            return _normalize_column(label, field[0])
        return label, field[:2]
    elif callable(field):
        return label, (label, field)

    return label, (field, mapper)


class DictToCsv(BaseToCsv[Dict]):

    def __init__(self,
                 *args,
                 columns: Union[Dict[str, Union[str,
                                                ToCSVMapper,
                                                Tuple[str],
                                                Tuple[ToCSVMapper],
                                                Tuple[str, ToCSVMapper]]],
                                List[Union[str,
                                           Tuple[str, str],
                                           Tuple[str, ToCSVMapper],
                                           Tuple[str, str, ToCSVMapper]]]],
                 **kwargs):
        super(DictToCsv, self).__init__(*args, **kwargs)

        self._columns = dict([_normalize_column(c)
                              for c in (columns.items() if isinstance(columns, (dict, Mapping))
                                        else columns)])


def _normalize_list_columns(
        columns: List[Union[str,
                            Tuple[str, int],
                            Tuple[str, ToCSVMapper],
                            Tuple[str, int, ToCSVMapper]]]
) -> Iterable[Tuple[str, Tuple[int, ToCSVMapper]]]:
    for i, c in enumerate(columns):
        if isinstance(c, str):
            yield _normalize_column(c, i)
        elif isinstance(c, tuple):
            if len(c) == 1:
                yield _normalize_column(c[0], i)
            elif len(c) == 2:
                if callable(c[1]):
                    yield _normalize_column(c[0], i, c[1])
                elif isinstance(c[1], tuple):
                    if len(c[1]) > 0:
                        if callable(c[1][0]):
                            yield _normalize_column(c[0], i, c[1][0])
                        else:
                            yield _normalize_column(c[0], *c[1])
                    else:
                        yield _normalize_column(c[0], i)
                else:
                    yield _normalize_column(c[0], c[1])
            else:
                yield _normalize_column(*c)
        else:
            raise ValueError(f'Invalid column definition: {c}')


class ListToCsv(BaseToCsv[Iterable[Any]]):

    def __init__(self,
                 *args,
                 columns: Union[Dict[str, Union[int,
                                                ToCSVMapper,
                                                Tuple[int],
                                                Tuple[ToCSVMapper],
                                                Tuple[int, ToCSVMapper]]],
                                List[Union[str,
                                           Tuple[str],
                                           Tuple[str, int],
                                           Tuple[str, ToCSVMapper],
                                           Tuple[str, int, ToCSVMapper]]]],
                 **kwargs):
        super(ListToCsv, self).__init__(*args, **kwargs)

        if isinstance(columns, (dict, Mapping)):
            self._columns = dict(_normalize_list_columns([c for c in columns.items()]))
        else:
            self._columns = dict(_normalize_list_columns(columns))


class BaseFromCsv(BaseStage[str, Union[Dict[str, Any], List[str]]], Generic[Source_co, ColumnType]):

    def __init__(self,
                 *args,
                 csv_kwargs: Dict = None,
                 maps: Dict[ColumnType, FromCSVMapper] = None,
                 **kwargs):
        super(BaseFromCsv, self).__init__(*args, **kwargs)

        self.io: Optional[StringIO] = None
        self.csv_kwargs = csv_kwargs or {}
        self.reader: Optional[Iterable[List[str]]] = None

        self.maps: Dict[ColumnType, FromCSVMapper] = maps or {}

        self._lock = Lock()

    async def get_data_from_csv(self, frame: str) -> Union[Dict[str, Any], List[Any]]:
        if self.io is None:
            raise RuntimeError('Stream not initialized')
        async with self._lock:
            self.io.seek(0)
            self.io.truncate()
            self.io.write(frame)
            self.io.seek(0)

            for row in self.reader:
                return row
            else:
                raise FrameSkippedError(frame)

    async def map_value(self, key: ColumnType, value: Union[str, int, float]) -> Any:
        try:
            value = self.maps[key](value)
        except KeyError:
            pass

        return value

    async def _mount(self):
        self.io = StringIO()
        await super(BaseFromCsv, self)._mount()

    async def _unmount(self):
        self.io = None
        self.reader = None
        await super(BaseFromCsv, self)._unmount()


class ListFromCsv(BaseFromCsv[List[Any], int]):

    async def process_frame(self, frame) -> List[Any]:
        return [await self.map_value(k, v) for k, v in enumerate(await self.get_data_from_csv(frame))]

    async def _mount(self):
        await super(ListFromCsv, self)._mount()
        self.reader = reader(self.io, **self.csv_kwargs)


class DictFromCsv(BaseFromCsv[Dict[str, Any], str]):

    async def process_frame(self, frame) -> Dict[str, Any]:
        return {k: await self.map_value(k, v) for k, v in (await self.get_data_from_csv(frame)).items()}

    async def _mount(self):
        await super(DictFromCsv, self)._mount()
        self.reader = DictReader(self.io, **self.csv_kwargs)
