from unittest import IsolatedAsyncioTestCase

from pyrill.accumulators import ListAcc
from pyrill.base import BaseStage
from pyrill.csv import DictFromCsv, DictToCsv, ListFromCsv, ListToCsv
from pyrill.sinks import Last
from pyrill.sources import SyncSource
from pyrill.strlike import Join


class DictToCsvTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource[int](source=[
            {'field_1': 'id1', 'field_2': 1, 'field_3': 'pong_1'},
            {'field_1': 'id2', 'field_2': 2, 'field_3': 'pong_2'},
            {'field_1': 'id3', 'field_2': 3, 'field_3': 'pong_3'}
        ])

        stage: BaseStage = DictToCsv(source=source, columns=['field_1', 'field_2', 'field_3'])
        stage = ListAcc(source=stage)
        stage = Join(source=stage, join_str='')
        sink = Last(source=stage)

        self.assertEqual(
            await sink.get_frame(),
            """field_1,field_2,field_3\r\nid1,1,pong_1\r\nid2,2,pong_2\r\nid3,3,pong_3\r\n"""
        )

    async def test_success_dict_columns(self):
        source = SyncSource[int](source=[
            {'field_1': 'id1', 'field_2': 1, 'field_3': 'pong_1'},
            {'field_1': 'id2', 'field_2': 2, 'field_3': 'pong_2'},
            {'field_1': 'id3', 'field_2': 3, 'field_3': 'pong_3'}
        ])

        stage: BaseStage = DictToCsv(source=source, columns={'id': 'field_1',
                                                             'field_2': lambda x: x * 2,
                                                             'msg': ('field_3', lambda x: x * 2)})
        stage = ListAcc(source=stage)
        stage = Join(source=stage, join_str='')
        sink = Last(source=stage)

        self.assertEqual(
            await sink.get_frame(),
            """id,field_2,msg\r\nid1,2,pong_1pong_1\r\nid2,4,pong_2pong_2\r\nid3,6,pong_3pong_3\r\n"""
        )

    async def test_success_list_columns(self):
        source = SyncSource[int](source=[
            {'field_1': 'id1', 'field_2': 1, 'field_3': 'pong_1'},
            {'field_1': 'id2', 'field_2': 2, 'field_3': 'pong_2'},
            {'field_1': 'id3', 'field_2': 3, 'field_3': None},
        ])

        stage: BaseStage = DictToCsv(source=source, columns=[('id', 'field_1'),
                                                             ('field_2', lambda x: x * 2),
                                                             ('msg', 'field_3', lambda x: x * 2)])
        stage = ListAcc(source=stage)
        stage = Join(source=stage, join_str='')
        sink = Last(source=stage)

        self.assertEqual(
            await sink.get_frame(),
            """id,field_2,msg\r\nid1,2,pong_1pong_1\r\nid2,4,pong_2pong_2\r\nid3,6,\r\n"""
        )

    async def test_success_list_columns_2(self):
        source = SyncSource[int](source=[
            {'field_1': 'id1', 'field_2': 1, 'field_3': 'pong_1'},
            {'field_1': 'id2', 'field_2': 2, 'field_3': 'pong_2'},
            {'field_1': 'id3', 'field_2': 3},
        ])

        stage: BaseStage = DictToCsv(source=source, columns=[('id', 'field_1'),
                                                             ('field_2', tuple()),
                                                             ('msg', ('field_3',))])
        stage = ListAcc(source=stage)
        stage = Join(source=stage, join_str='')
        sink = Last(source=stage)

        self.assertEqual(
            await sink.get_frame(),
            """id,field_2,msg\r\nid1,1,pong_1\r\nid2,2,pong_2\r\nid3,3,\r\n"""
        )


class ListToCsvTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource[int](source=[
            ['id1', 1, 'pong_1'],
            ['id2', 2, 'pong_2'],
            ['id3', 3, 'pong_3']
        ])

        stage: BaseStage = ListToCsv(source=source, columns=['field_1', 'field_2', 'field_3'])
        stage = ListAcc(source=stage)
        stage = Join(source=stage, join_str='')
        sink = Last(source=stage)

        self.assertEqual(
            await sink.get_frame(),
            """field_1,field_2,field_3\r\nid1,1,pong_1\r\nid2,2,pong_2\r\nid3,3,pong_3\r\n"""
        )

    async def test_success_dict_columns(self):
        source = SyncSource[int](source=[
            ['id1', 1, 'pong_1'],
            ['id2', 2, 'pong_2'],
            ['id3', 3, 'pong_3']
        ])

        stage: BaseStage = ListToCsv(source=source, columns={'id': 0,
                                                             'field_2': lambda x: x * 2,
                                                             'msg': (2, lambda x: x * 2)})
        stage = ListAcc(source=stage)
        stage = Join(source=stage, join_str='')
        sink = Last(source=stage)

        self.assertEqual(
            await sink.get_frame(),
            """id,field_2,msg\r\nid1,2,pong_1pong_1\r\nid2,4,pong_2pong_2\r\nid3,6,pong_3pong_3\r\n"""
        )

    async def test_success_list_columns(self):
        source = SyncSource[int](source=[
            ['id1', 1, 'pong_1'],
            ['id2', 2, 'pong_2'],
            ['id3', 3, None]
        ])

        stage: BaseStage = ListToCsv(source=source, columns=[('id', 0),
                                                             ('field_2', (lambda x: x * 2,)),
                                                             ('msg', 2, lambda x: x * 2)])
        stage = ListAcc(source=stage)
        stage = Join(source=stage, join_str='')
        sink = Last(source=stage)

        self.assertEqual(
            await sink.get_frame(),
            """id,field_2,msg\r\nid1,2,pong_1pong_1\r\nid2,4,pong_2pong_2\r\nid3,6,\r\n"""
        )

    async def test_success_list_columns_2(self):
        source = SyncSource[int](source=[
            ['id1', 1, 'pong_1'],
            ['id2', 2, 'pong_2'],
            ['id3', 3]
        ])

        stage: BaseStage = ListToCsv(source=source, columns=[('id',),
                                                             ('field_2', tuple()),
                                                             ('msg', (2,))])
        stage = ListAcc(source=stage)
        stage = Join(source=stage, join_str='')
        sink = Last(source=stage)

        self.assertEqual(
            await sink.get_frame(),
            """id,field_2,msg\r\nid1,1,pong_1\r\nid2,2,pong_2\r\nid3,3,\r\n"""
        )

    async def test_fail_invalid_columns(self):
        with self.assertRaises(ValueError):
            ListToCsv(columns=[1, 2, 4])


class DictFromCsvTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource[int](source=[
            'field_1,field_2,field_3\r\n',
            'id1,1,pong_1\r\n',
            'id2,2,pong_2\r\n',
            'id3,3,pong_3\r\n'

        ])

        stage: BaseStage = DictFromCsv(source=source)
        stage = ListAcc(source=stage)
        sink = Last(source=stage)

        self.assertEqual(
            await sink.get_frame(),
            [{'field_1': 'id1', 'field_2': '1', 'field_3': 'pong_1'},
             {'field_1': 'id2', 'field_2': '2', 'field_3': 'pong_2'},
             {'field_1': 'id3', 'field_2': '3', 'field_3': 'pong_3'}]
        )

    async def test_success_with_maps(self):
        source = SyncSource[int](source=[
            'field_1,field_2,field_3\r\n',
            'id1,1,pong_1\r\n',
            'id2,2,pong_2\r\n',
            'id3,3,pong_3\r\n'

        ])

        stage: BaseStage = DictFromCsv(source=source, maps={'field_2': lambda x: int(x)})
        stage = ListAcc(source=stage)
        sink = Last(source=stage)

        self.assertEqual(
            await sink.get_frame(),
            [{'field_1': 'id1', 'field_2': 1, 'field_3': 'pong_1'},
             {'field_1': 'id2', 'field_2': 2, 'field_3': 'pong_2'},
             {'field_1': 'id3', 'field_2': 3, 'field_3': 'pong_3'}]
        )


class ListFromCsvTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource[int](source=[
            'field_1,field_2,field_3\r\n',
            'id1,1,pong_1\r\n',
            'id2,2,pong_2\r\n',
            'id3,3,pong_3\r\n'

        ])

        stage: BaseStage = ListFromCsv(source=source)
        stage = ListAcc(source=stage)
        sink = Last(source=stage)

        self.assertEqual(
            await sink.get_frame(),
            [['field_1', 'field_2', 'field_3'],
             ['id1', '1', 'pong_1'],
             ['id2', '2', 'pong_2'],
             ['id3', '3', 'pong_3']]
        )

    async def test_success_with_maps(self):
        source = SyncSource[int](source=[
            'id1,1,pong_1\r\n',
            'id2,2,pong_2\r\n',
            'id3,3,pong_3\r\n'

        ])

        stage: BaseStage = ListFromCsv(source=source, maps={1: lambda x: int(x)})
        stage = ListAcc(source=stage)
        sink = Last(source=stage)

        self.assertEqual(
            await sink.get_frame(),
            [['id1', 1, 'pong_1'],
             ['id2', 2, 'pong_2'],
             ['id3', 3, 'pong_3']]
        )
