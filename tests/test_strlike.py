from unittest import IsolatedAsyncioTestCase

from pyrill.sources import SyncSource
from pyrill.strlike import Join, LStrip, Replace, RStrip, Split, Strip


class SplitTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource(source=['text to split'])

        stage = Split[str](source=source, sep=' ')

        result = [t async for t in stage]

        self.assertEqual(result, [['text', 'to', 'split']])

    async def test_success_with_maxsplit(self):
        source = SyncSource(source=['text to split'])

        stage = Split[str](source=source, sep=' ', maxsplit=1)

        result = [t async for t in stage]

        self.assertEqual(result, [['text', 'to split']])

    async def test_fail(self):
        source = SyncSource(source=[1, 'text to split', 2])

        stage = Split[str](source=source, sep=' ', maxsplit=1)

        with self.assertRaises(AttributeError):
            [t async for t in stage]


class JoinTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource(source=[['text', 'to', 'join']])

        stage = Join[str](source=source, join_str=' ')

        i = 0
        async for t in stage:
            self.assertEqual(t, 'text to join')
            i += 1

        self.assertEqual(i, 1)

    async def test_fail(self):
        source = SyncSource(source=['texttojoin',
                                    1,
                                    ['text', 'to', 'join']])

        stage = Join[str](source=source, join_str='')

        with self.assertRaises(TypeError):
            [t async for t in stage]


class ReplaceTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource(source=['text to replace to text',
                                    'text 2 replace to text',
                                    'text 2 replace 2 text'])

        stage = Replace[str](source=source, old='to', new='2')

        i = 0
        async for t in stage:
            self.assertEqual(t, 'text 2 replace 2 text')
            i += 1

        self.assertEqual(i, 3)

    async def test_success_with_count(self):
        source = SyncSource(source=['text to replace to text'])

        stage = Replace[str](source=source, old='to', new='2', count=1)

        i = 0
        async for t in stage:
            self.assertEqual(t, 'text 2 replace to text')
            i += 1

        self.assertEqual(i, 1)

    async def test_fail(self):
        source = SyncSource(source=['text to replace to text',
                                    1,
                                    'text 2 replace to text',
                                    2,
                                    'text 2 replace 2 text'])

        stage = Replace[str](source=source, old='to', new='2')

        with self.assertRaises(AttributeError):
            [t async for t in stage]


class StripTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource(source=['     text to strip      ',
                                    '   \n text to strip   \n    ',
                                    'text to strip'])

        stage = Strip[str](source=source)

        i = 0
        async for t in stage:
            self.assertEqual(t, 'text to strip')
            i += 1

        self.assertEqual(i, 3)

    async def test_success_with_chars(self):
        source = SyncSource(source=['aaabbbcccctext to stripcccbbbaaa',
                                    'text to strip'])

        stage = Strip[str](source=source, chars='abc')

        i = 0
        async for t in stage:
            self.assertEqual(t, 'text to strip')
            i += 1

        self.assertEqual(i, 2)

    async def test_fail(self):
        source = SyncSource(source=['     text to strip      ',
                                    1,
                                    '   \n text to strip   \n    ',
                                    2,
                                    'text to strip'])

        stage = Strip[str](source=source)

        with self.assertRaises(AttributeError):
            [t async for t in stage]


class RStripTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource(source=['     text to strip      ',
                                    '     text to strip \n          ',
                                    '     text to strip'])

        stage = RStrip[str](source=source)

        i = 0
        async for t in stage:
            self.assertEqual(t, '     text to strip')
            i += 1

        self.assertEqual(i, 3)

    async def test_success_with_chars(self):
        source = SyncSource(source=['aaabbbcccctext to stripcccbbbaaa',
                                    'aaabbbcccctext to strip'])

        stage = RStrip[str](source=source, chars='abc')

        i = 0
        async for t in stage:
            self.assertEqual(t, 'aaabbbcccctext to strip')
            i += 1

        self.assertEqual(i, 2)

    async def test_fail(self):

        source = SyncSource(source=['     text to strip      ',
                                    1,
                                    '     text to strip \n          ',
                                    2,
                                    '     text to strip'])

        stage = RStrip(source=source)

        with self.assertRaises(AttributeError):
            [t async for t in stage]


class LStripTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource(source=['     text to strip      ',
                                    '   \n text to strip      ',
                                    'text to strip      '])

        stage = LStrip[str](source=source)

        i = 0
        async for t in stage:
            self.assertEqual(t, 'text to strip      ')
            i += 1

        self.assertEqual(i, 3)

    async def test_success_with_chars(self):
        source = SyncSource(source=['aaabbbcccctext to stripcccbbbaaa',
                                    'text to stripcccbbbaaa'])

        stage = LStrip[str](source=source, chars='abc')

        i = 0
        async for t in stage:
            self.assertEqual(t, 'text to stripcccbbbaaa')
            i += 1

        self.assertEqual(i, 2)

    async def test_fail(self):

        source = SyncSource(source=['     text to strip      ',
                                    1,
                                    '   \n text to strip      ',
                                    2,
                                    'text to strip      '])

        stage = LStrip[str](source=source)

        with self.assertRaises(AttributeError):
            [t async for t in stage]
