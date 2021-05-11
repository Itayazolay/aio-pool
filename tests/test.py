import logging
import os
from aio_pool import AioPool

import unittest


async def initliazer_async(param):
    get_initializer_param.param = param


def initializer_sync(param):
    get_initializer_param.param = param


async def get_initializer_param():
    return get_initializer_param.param


def pow2_sync(n):
    return n ** 2


async def pow2_async(n):
    return n ** 2


def mul_sync(a, b):
    return a * b


async def mul_async(a, b):
    return a * b


class TestAIOPool(unittest.TestCase):
    def setUp(self) -> None:
        logging.basicConfig(level=logging.DEBUG)
        os.environ["PYTHONASYNCIODEBUG"] = "1"

    def test_initializer(self):
        param_val = True
        with AioPool(
            processes=1, initializer=initliazer_async, initargs=(param_val,)
        ) as pool:
            res = pool.apply(get_initializer_param)
            self.assertEqual(res, param_val)
        with AioPool(
            processes=1, initializer=initializer_sync, initargs=(param_val,)
        ) as pool:
            res = pool.apply(get_initializer_param)
            self.assertEqual(res, param_val)

    def test_map_sync(self):
        with AioPool(processes=2, concurrency_limit=2) as pool:
            inputs = [i for i in range(40)]
            expected = [i ** 2 for i in inputs]
            for chunksize in [1, 2, 4]:
                result = pool.map(pow2_sync, inputs, chunksize=chunksize)
                self.assertListEqual(expected, result)

    def test_map_async(self):
        with AioPool(processes=2, concurrency_limit=2) as pool:
            inputs = [i for i in range(40)]
            expected = [i ** 2 for i in inputs]
            for chunksize in [1, 2, 4]:
                result = pool.map(pow2_async, inputs, chunksize=chunksize)
                self.assertListEqual(expected, result)

    def test_starmap_sync(self):
        with AioPool(processes=2, concurrency_limit=2) as pool:
            inputs = [(i, i // 2) for i in range(40)]
            expected = [mul_sync(a, b) for a, b in inputs]
            for chunksize in [1, 2, 4]:
                result = pool.starmap(mul_sync, inputs, chunksize=chunksize)
                self.assertListEqual(expected, result)

    def test_starmap_async(self):
        with AioPool(processes=2, concurrency_limit=2) as pool:
            inputs = [(i, i // 2) for i in range(40)]
            expected = [mul_sync(a, b) for a, b in inputs]
            for chunksize in [1, 2, 4]:
                result = pool.starmap(mul_async, inputs, chunksize=chunksize)
                self.assertListEqual(expected, result)


if __name__ == "__main__":
    unittest.main()
