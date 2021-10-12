from asyncio import Future
from json import loads
from typing import Any, Tuple
from unittest import IsolatedAsyncioTestCase

from pyrill.accumulators import ListAcc, SumAcc
from pyrill.json import (FromJson, ToJson, ToJsonList, ToJsonObject,
                         ToJsonPerLine)
from pyrill.sinks import Last
from pyrill.sources import SyncSource
from pyrill.strlike import Join


class FromJsonTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        sink: Last = SyncSource[Any](source=['{"field_1": "id1", "field_2": 1, "field_3": "pong_1"}',
                                             '["id2", 2, "pong_2"]']) \
            >> FromJson() \
            >> ListAcc() \
            >> Last()

        self.assertEqual(
            await sink.get_frame(),
            [{'field_1': 'id1', 'field_2': 1, 'field_3': 'pong_1'},
             ['id2', 2, 'pong_2']]
        )


class ToJsonTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        sink: Last = SyncSource[Any](source=[{'field_1': 'id1', 'field_2': 1, 'field_3': 'pong_1'},
                                             ['id2', 2, 'pong_2']]) \
            >> ToJson() \
            >> ListAcc() \
            >> Last()

        self.assertEqual(
            await sink.get_frame(),
            ['{"field_1": "id1", "field_2": 1, "field_3": "pong_1"}',
             '["id2", 2, "pong_2"]']
        )


class ToJsonPerLineTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        sink: Last = SyncSource[Any](source=[{'field_1': 'id1', 'field_2': 1, 'field_3': 'pong_1'},
                                             ['id2', 2, 'pong_2']]) \
            >> ToJsonPerLine() \
            >> ListAcc() \
            >> Join(join_str='') \
            >> Last()

        self.assertEqual(
            await sink.get_frame(),
            """{"field_1":"id1","field_2":1,"field_3":"pong_1"}\n["id2",2,"pong_2"]\n"""
        )


class ToJsonListTestCase(IsolatedAsyncioTestCase):

    async def test_success_basic(self):
        source = SyncSource[Any](source=[
            'id2', 2, 'pong_2'
        ])
        sink: Last = ToJsonList(source=source) \
            >> SumAcc[str]() \
            >> Last()

        self.assertEqual(await sink.get_frame(), '["id2", 2, "pong_2"]')

    async def test_success(self):
        source = SyncSource[Any](source=[
            {'field_1': 'id1', 'field_2': 1, 'field_3': 'pong_1'},
            ['id2', 2, 'pong_2']
        ])
        sink: Last = ToJsonList(source=source) \
            >> SumAcc[str]() \
            >> Last()

        self.assertEqual(
            await sink.get_frame(),
            '[{"field_1": "id1", "field_2": 1, "field_3": "pong_1"}, ["id2", 2, "pong_2"]]'
        )

    async def test_success_whole_json(self):
        source = SyncSource[Any](source=[
            {'field_1': 'id1', 'field_2': 1, 'field_3': 'pong_1'},
            ['id2', 2, 'pong_2']
        ])

        sink: Last = ToJsonList(source=source) \
            >> SumAcc[str]() \
            >> Last()

        js = await sink.get_frame()
        self.assertEqual(
            loads(js),
            [
                {'field_1': 'id1', 'field_2': 1, 'field_3': 'pong_1'},
                ['id2', 2, 'pong_2']
            ],
            js
        )

    async def test_success_composed_stream(self):
        source = SyncSource[Any](source=[
            {'field_1': 'id1', 'field_2': 1, 'field_3': 'pong_1'},
            ToJsonList(source=SyncSource[Any](source=['asas', 'fdfd', 'dfdf'])),
            ['id2', 2, 'pong_2']
        ])

        sink: Last = ToJsonList(source=source) \
            >> SumAcc[str]() \
            >> Last()

        js = await sink.get_frame()
        self.assertEqual(
            loads(js),
            [
                {'field_1': 'id1', 'field_2': 1, 'field_3': 'pong_1'},
                ['asas', 'fdfd', 'dfdf'],
                ['id2', 2, 'pong_2']
            ],
            js
        )

    async def test_success_composed_stream_2(self):
        source = SyncSource[Any](source=[
            ToJsonList(source=SyncSource[Any](source=['asas', 'fdfd', 'dfdf'])),
            {'field_1': 'id1', 'field_2': 1, 'field_3': 'pong_1'},
            ToJsonList(source=SyncSource[Any](source=['id2', 2, 'pong_2']))
        ])

        sink: Last = ToJsonList(source=source) \
            >> SumAcc[str]() \
            >> Last()

        js = await sink.get_frame()

        self.assertEqual(
            loads(js),
            [
                ['asas', 'fdfd', 'dfdf'],
                {'field_1': 'id1', 'field_2': 1, 'field_3': 'pong_1'},
                ['id2', 2, 'pong_2']
            ],
            js
        )


class ToJsonObjectTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource[Any](source=[
            {'field_1': 'id1', 'field_2': 1, 'field_3': 'pong_1'},
            ['id2', 2, 'pong_2']
        ])

        sink: Last = SyncSource[Any](source=[('array',
                                              ToJsonList(source=source))]) \
            >> ToJsonObject() \
            >> SumAcc[str]() \
            >> Last()
        result = await sink.get_frame()
        self.assertEqual(
            result,
            '{"array": [{"field_1": "id1", "field_2": 1, "field_3": "pong_1"}, ["id2", 2, "pong_2"]]}',
            result
        )

    async def test_success_full_string(self):
        source = SyncSource[Any](source=[
            {'field_1': 'id1', 'field_2': 1, 'field_3': 'pong_1'},
            ['id2', 2, 'pong_2']
        ])

        sink: Last = SyncSource[Any](source=[('array',
                                              ToJsonList(source=source))]) \
            >> ToJsonObject() \
            >> SumAcc[str]() \
            >> Last()
        js = await sink.get_frame()

        self.assertEqual(
            loads(js),
            {
                "array": [
                    {'field_1': 'id1', 'field_2': 1, 'field_3': 'pong_1'},
                    ['id2', 2, 'pong_2']
                ]
            },
            js
        )

    async def test_success_multi_stream(self):
        source = SyncSource[Any](source=[
            ('array_1',
             ToJsonList(
                 source=SyncSource[Tuple[str, Any]](source=[{'field_1': 'id1', 'field_2': 1, 'field_3': 'pong_1'}])
             )),
            ('array_2',
             ToJsonList(
                 source=SyncSource[Tuple[str, Any]](source=[['id2', 2, 'pong_2']])
             ))
        ])

        sink: Last = source \
            >> ToJsonObject() \
            >> SumAcc[str]() \
            >> Last()
        result = await sink.get_frame()
        self.assertEqual(
            result,
            '{"array_1": [{"field_1": "id1", "field_2": 1, "field_3": "pong_1"}],'
            ' "array_2": [["id2", 2, "pong_2"]]}',
            result
        )

    async def test_success_multi_stream_full_string(self):
        source = SyncSource[Any](source=[
            ('array_1',
             ToJsonList(
                 source=SyncSource[Tuple[str, Any]](source=[{'field_1': 'id1', 'field_2': 1, 'field_3': 'pong_1'}])
             )),
            ('array_2',
             ToJsonList(
                 source=SyncSource[Tuple[str, Any]](source=[['id2', 2, 'pong_2']])
             ))
        ])

        sink: Last = source \
            >> ToJsonObject() \
            >> SumAcc[str]() \
            >> Last()
        js = await sink.get_frame()
        self.assertEqual(
            loads(js),
            {
                "array_1": [
                    {'field_1': 'id1', 'field_2': 1, 'field_3': 'pong_1'},
                ],
                "array_2": [
                    ['id2', 2, 'pong_2']
                ]
            },
            js
        )

    async def test_success_static_values(self):
        source = SyncSource[Any](source=[
            ('int', 3),
            ('string', 'rettt'),
            ('float', 3.3),
            ('dict', {'t1': 3}),
            ('list', ['t1', 3])
        ])

        sink: Last = source \
            >> ToJsonObject() \
            >> SumAcc[str]() \
            >> Last()

        js = await sink.get_frame()

        self.assertEqual(
            loads(js),
            {
                "int": 3,
                "string": "rettt",
                "float": 3.3,
                "dict": {'t1': 3},
                "list": ['t1', 3]
            },
            js
        )

    async def test_success_all_types(self):
        fut = Future()
        fut.set_result('future')

        stage_2 = SyncSource[Any](source=[('float', 3.3),
                                          ('dict', {'t1': 3}),
                                          ('list', ['t1', 3]),
                                          ('array',
                                           ToJsonList(source=SyncSource[Any](source=['id2', 2, 'pong_2']))),
                                          ('fut', fut)]) \
            >> ToJsonObject()

        source = SyncSource[Any](source=[
            ('int', 3),
            ('string', 'rettt'),
            ('array',
             ToJsonList(source=SyncSource[Any](source=[
                 {'field_1': 'id1', 'field_2': 1, 'field_3': 'pong_1'},
             ]))),
            ('object', stage_2),
        ])

        sink: Last = source \
            >> ToJsonObject() \
            >> SumAcc[str]() \
            >> Last()

        js = await sink.get_frame()

        self.assertEqual(
            loads(js),
            {
                "int": 3,
                "string": "rettt",
                "array": [{'field_1': 'id1', 'field_2': 1, 'field_3': 'pong_1'}],
                "object": {
                    "float": 3.3,
                    "dict": {'t1': 3},
                    "list": ['t1', 3],
                    "array": ['id2', 2, 'pong_2'],
                    "fut": "future"
                }
            },
            js
        )
