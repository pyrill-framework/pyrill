from unittest import IsolatedAsyncioTestCase

from pyrill.accumulators import ListAcc
from pyrill.base import ElementState
from pyrill.sinks import Last
from pyrill.sources import SyncSource
from pyrill.strings import Encode, Lower, StringChunksSeparator, Upper


class LowerTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource(source=['text to lowercase',
                                    'TeXt tO LoWeRCasE',
                                    'TEXT TO LOWERCASE'])

        stage = Lower(source=source)

        i = 0
        async for t in stage:
            self.assertEqual(t, 'text to lowercase')
            i += 1

        self.assertEqual(i, 3)

    async def test_fail(self):
        source = SyncSource(source=['text to lowercase',
                                    1,
                                    'TeXt tO LoWeRCasE',
                                    2,
                                    'TEXT TO LOWERCASE'])

        stage = Lower(source=source)

        with self.assertRaises(AttributeError):
            [t async for t in stage]


class UpperTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource(source=['text to uppercase',
                                    'TeXt tO UpPeRCasE',
                                    'TEXT TO UPPERCASE'])

        stage = Upper(source=source)

        i = 0
        async for t in stage:
            self.assertEqual(t, 'TEXT TO UPPERCASE')
            i += 1

        self.assertEqual(stage.state, ElementState.NULL)

        self.assertEqual(i, 3)

    async def test_fail(self):
        source = SyncSource(source=['text to uppercase',
                                    1,
                                    'TeXt tO UpPeRCasE',
                                    2,
                                    'TEXT TO UPPERCASE'])

        stage = Upper(source=source)

        with self.assertRaises(AttributeError):
            [t async for t in stage]


class EncodeTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource(source=['text',
                                    'to',
                                    'encode'])

        stage = Encode(source=source)

        result = [t async for t in stage]

        self.assertEqual(result, [b'text', b'to', b'encode'])

    async def test_fail(self):
        source = SyncSource(source=['text',
                                    'to',
                                    'no ASCII text ????????',
                                    'encode'])

        stage = Encode(source=source, encoding='ASCII')

        try:
            with self.assertRaises(UnicodeEncodeError):
                [t async for t in stage]
        finally:
            await stage.unmount()

    async def test_fail_ignore(self):
        source = SyncSource(source=['text',
                                    'to',
                                    'no ASCII text ????????',
                                    'encode'])

        stage = Encode(source=source, encoding='ASCII', errors='ignore')

        result = [t async for t in stage]
        await stage.unmount()

        self.assertEqual(result, [b'text', b'to', b'no ASCII text ', b'encode'])


class StringChunksSeparatorTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource(source=['text\n',
                                    't\no',
                                    'en\nco\nde'])

        stage = StringChunksSeparator(source=source) >> ListAcc() >> Last()

        result = await stage.get_frame()

        self.assertEqual(result, ['text\n',
                                  't\n',
                                  'oen\n',
                                  'co\n',
                                  'de'])
