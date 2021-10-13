from unittest import IsolatedAsyncioTestCase

from pyrill import SyncSource, UnicodeChunks
from pyrill.bytes import BytesChunksSeparator, BytesSizedChunksSource


class SizedChunksSourceTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = BytesSizedChunksSource(min_size=1, max_size=10)
        await source.mount()
        await source.push_frame(b'12334t4rgfvd435t4rgdfd435t4r')
        await source.push_frame(StopAsyncIteration())

        result = [d async for d in source]
        self.assertEqual(result, [b'12334t4rgf', b'vd435t4rgd', b'fd435t4r'])

    async def test_success_chunked(self):
        source = BytesSizedChunksSource(min_size=3, max_size=4)
        await source.mount()

        data = [b'12334', b'23', b'3453', b'3242534', b'323', StopAsyncIteration()]

        await source.push_frame(data.pop(0))

        result = []
        async for d in source:
            result.append(d)
            try:
                await source.push_frame(data.pop(0))
            except IndexError:
                pass
        self.assertEqual(result, [b'1233', b'423', b'3453', b'3242', b'5343', b'23'])


class UnicodeChunksTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        data = 'asas💩️😍️👉🏾️😃️🥶️🥵️👨🏾‍❤️‍💋‍👨🏾️'.encode()
        source = SyncSource(source=[data[i:i + 2] for i in range(0, len(data), 2)]) \
            >> UnicodeChunks()

        result = [d async for d in source]
        self.assertEqual(result,
                         [b'as', b'as', b'\xf0\x9f\x92\xa9', b'\xef\xb8\x8f', b'\xf0\x9f\x98\x8d', b'\xef\xb8\x8f',
                          b'\xf0\x9f\x91\x89', b'\xf0\x9f\x8f\xbe', b'\xef\xb8\x8f', b'\xf0\x9f\x98\x83',
                          b'\xef\xb8\x8f', b'\xf0\x9f\xa5\xb6', b'\xef\xb8\x8f', b'\xf0\x9f\xa5\xb5', b'\xef\xb8\x8f',
                          b'\xf0\x9f\x91\xa8', b'\xf0\x9f\x8f\xbe', b'\xe2\x80\x8d', b'\xe2\x9d\xa4', b'\xef\xb8\x8f',
                          b'\xe2\x80\x8d', b'\xf0\x9f\x92\x8b', b'\xe2\x80\x8d', b'\xf0\x9f\x91\xa8',
                          b'\xf0\x9f\x8f\xbe', b'\xef\xb8\x8f'])

        self.assertEqual([r.decode() for r in result],
                         ['as', 'as', '💩', '️', '😍', '️', '👉',  # be careful empty strings are not empty
                          '🏾', '️', '😃', '️', '🥶', '️', '🥵', '️', '👨',  # be careful empty strings are not empty
                          '🏾', '\u200d', '❤', '️', '\u200d', '💋', '\u200d',  # be careful empty strings are not empty
                          '👨', '🏾', '️'])  # be careful empty strings are not empty

    async def test_success_not_all(self):
        source = SyncSource(source=[b'as', b'as', b'\xf0\x9f', b'\x92\xa9',
                                    b'\xef\xb8', b'\x8f', b'\xf0\x9f\x98\x8d',
                                    b'\xef\xb8', b'\x8f',
                                    b'\xf0\x9f', b'\x91\x89', b'\xf0', b'\x9f\x8f', b'\xbe',
                                    b'\xef\xb8\x8f', b'\xf0\x9f\x98\x83',
                                    b'\xef', b'\xb8\x8f', b'\xf0\x9f', b'\xa5\xb6', b'\xef\xb8\x8f',
                                    b'\xf0\x9f\xa5\xb5',
                                    b'\xef\xb8\x8f',
                                    b'\xf0\x9f\x91\xa8', b'\xf0\x9f\x8f\xbe',
                                    b'\xe2\x80\x8d', b'\xe2\x9d\xa4',
                                    b'\xef', b'\xb8\x8f',
                                    b'\xe2\x80\x8d', b'\xf0\x9f\x92\x8b',
                                    b'\xe2\x80\x8d', b'\xf0\x9f\x91\xa8',
                                    b'\xf0\x9f\x8f\xbe', b'\xef']) \
            >> UnicodeChunks()

        result = [d async for d in source]
        self.assertEqual(result,
                         [b'as', b'as', b'\xf0\x9f\x92\xa9', b'\xef\xb8\x8f', b'\xf0\x9f\x98\x8d', b'\xef\xb8\x8f',
                          b'\xf0\x9f\x91\x89', b'\xf0\x9f\x8f\xbe', b'\xef\xb8\x8f', b'\xf0\x9f\x98\x83',
                          b'\xef\xb8\x8f', b'\xf0\x9f\xa5\xb6', b'\xef\xb8\x8f', b'\xf0\x9f\xa5\xb5', b'\xef\xb8\x8f',
                          b'\xf0\x9f\x91\xa8', b'\xf0\x9f\x8f\xbe', b'\xe2\x80\x8d', b'\xe2\x9d\xa4', b'\xef\xb8\x8f',
                          b'\xe2\x80\x8d', b'\xf0\x9f\x92\x8b', b'\xe2\x80\x8d', b'\xf0\x9f\x91\xa8',
                          b'\xf0\x9f\x8f\xbe'])

        self.assertEqual([r.decode() for r in result],
                         ['as', 'as', '💩', '️', '😍', '️', '👉',  # be careful empty strings are not empty
                          '🏾', '️', '😃', '️', '🥶', '️', '🥵', '️', '👨',  # be careful empty strings are not empty
                          '🏾', '\u200d', '❤', '️', '\u200d', '💋', '\u200d',  # be careful empty strings are not empty
                          '👨', '🏾'])  # be careful empty strings are not empty


class ChunksSeparatorTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource(source=[b'12334\nt4rgf', b'vd4\n35t4rgd', b'fd\n435\nt4r']) \
            >> BytesChunksSeparator(separator=b'\n')

        result = [d async for d in source]
        self.assertEqual(result, [b'12334\n', b't4rgfvd4\n', b'35t4rgdfd\n', b'435\n', b't4r'])

    async def test_success_chunked(self):
        source = SyncSource(source=[b'12334t4rgf', b'vd435t4rgd', b'fd435t4r']) \
            >> BytesChunksSeparator(separator=b'\n')

        result = [d async for d in source]
        self.assertEqual(result, [b'12334t4rgfvd435t4rgdfd435t4r'])
