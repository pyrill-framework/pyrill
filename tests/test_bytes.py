from unittest import IsolatedAsyncioTestCase

from pyrill.bytes import SizedChunksSource


class SizedChunksSourceTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SizedChunksSource(min_size=1, max_size=10)
        await source.mount()
        await source.push_frame(b'12334t4rgfvd435t4rgdfd435t4r')
        await source.push_frame(StopAsyncIteration())

        result = [d async for d in source]
        self.assertEqual(result, [b'12334t4rgf', b'vd435t4rgd', b'fd435t4r'])

    async def test_success_chunked(self):
        source = SizedChunksSource(min_size=3, max_size=4)
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
