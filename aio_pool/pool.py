import asyncio
import logging
import sys
from asyncio.base_events import BaseEventLoop
from asyncio.coroutines import iscoroutinefunction
from asyncio.locks import Semaphore
from asyncio.tasks import Task
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from functools import singledispatch
from multiprocessing.pool import (  # type: ignore # noqa
    ExceptionWithTraceback, MaybeEncodingError, Pool,
    _helper_reraises_exception, mapstar, starmapstar)
from typing import Any, Awaitable, Callable, Deque, Optional, Set, Tuple, Union

__all__ = ["AioPool"]


logger = logging.getLogger("aiopool")
logger.addHandler(logging.NullHandler())



async def _create_bounded_task(func, args, kwds: dict, sem: Semaphore, loop: "BaseEventLoop", iscoroutinefunction=iscoroutinefunction):
    if iscoroutinefunction(func):
        task = await _create_bounded_task_coro(func, args, kwds, sem, loop)
    elif func is mapstar:
        task = await _create_bounded_task_mapstar(func, args, kwds, sem, loop)
    elif func is starmapstar:
        task = await _create_bounded_task_starmapstar(func, args, kwds, sem, loop)
    else:
        task = await _create_bounded_task_thread(func, args, kwds, sem, loop)
    return task

async def _create_bounded_task_thread(func, args, kwds: dict, sem, loop: "BaseEventLoop"):
    await sem.acquire()
    task = loop.run_in_executor(None, lambda: func(*args, **kwds))
    task.add_done_callback(lambda t: sem.release())
    return task

async def _create_bounded_task_coro(func, args, kwds: dict, sem: Semaphore, loop: "BaseEventLoop"):
    await sem.acquire()
    task = loop.create_task(func(*args, **kwds))
    task.add_done_callback(lambda t: sem.release())
    return task
    
async def _create_bounded_task_mapstar(func: mapstar, args, kwds: dict, sem: Semaphore, loop: "BaseEventLoop"):
    underlying_func = args[0][0]
    underlying_params = args[0][1]
    results: Deque[Awaitable[Any]] = deque([])
    append = results.append
    for params in underlying_params:
        append(await _create_bounded_task(
            underlying_func, (params, ), {}, sem, loop))
    return asyncio.gather(*results, return_exceptions=True)

async def _create_bounded_task_starmapstar(func: starmapstar, args, kwds: dict, sem: Semaphore, loop: "BaseEventLoop"):
    underlying_func = args[0][0]
    underlying_params = args[0][1]
    results = deque([])
    append = results.append
    for params in underlying_params:
        append(await _create_bounded_task(
            underlying_func, params, {}, sem, loop))
    return asyncio.gather(*results)


async def task_wrapper(
    job, i, func,
    task: Awaitable[Any],
    put: Callable[[Any], Awaitable[None]],
    wrap_exception: bool = False,
) -> None:
    try:
        result = (True, await task)
    except Exception as e:
        if wrap_exception and func is not _helper_reraises_exception:
            e = ExceptionWithTraceback(e, e.__traceback__)
        result = (False, e)
    try:
        await put((job, i, result))
    except Exception as e:
        wrapped = MaybeEncodingError(e, result[1])
        logger.debug("Possible encoding error while sending result: %s" % (wrapped))
        await put((job, i, (False, wrapped)))


async def _run_worker(
    get: Callable[[], Awaitable[Any]],
    put: Callable[[Any], Awaitable[None]],
    loop: asyncio.BaseEventLoop,
    initializer=None,
    initargs=(),
    maxtasks=None,
    wrap_exception=False,
    concurrency_limit=128,
    iscoroutinefunction=asyncio.iscoroutinefunction,
) -> None:
    if initializer is not None:
        if iscoroutinefunction(initializer):
            await initializer(*initargs)
        else:
            initializer(*initargs)
    completed = 0
    sem_concurrency_limit = asyncio.BoundedSemaphore(concurrency_limit)

    tasks: Set[Task[Any]] = set()

    def remove_task(t: Task, *, tasks: Set[Task] = tasks) -> None:
        tasks.remove(t)

    while maxtasks is None or (maxtasks and completed < maxtasks):
        async with sem_concurrency_limit:
            try:
                task = await get()
            except (EOFError, OSError):
                logger.debug("worker got EOFError or OSError -- exiting")
                for task in tasks:
                    task.cancel()
                tasks.clear()  # Don't wait for anything.
                break

            if task is None:
                logger.debug("worker got sentinel -- exiting")
                break
        
        job, i, func, args, kwds = task
        task = await _create_bounded_task(func, args, kwds, sem=sem_concurrency_limit, loop=loop)

        new_task = loop.create_task(
            task_wrapper(job, i, func,
                task,
                put=put,
                wrap_exception=wrap_exception,
            )
        )
        tasks.add(new_task)
        new_task.add_done_callback(remove_task)

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    logger.debug("worker exiting after %d tasks" % completed)


def worker(
    inqueue,
    outqueue,
    initializer=None,
    initargs=(),
    loop_initializer=asyncio.new_event_loop,
    threads=1,
    maxtasks: Optional[int] = None,
    wrap_exception: bool = False,
    concurrency_limit=128,
) -> None:
    loop: asyncio.BaseEventLoop = loop_initializer()
    asyncio.set_event_loop(loop)
    worker_tp = ThreadPoolExecutor(threads, thread_name_prefix="Worker_TP_")
    loop.set_default_executor(worker_tp)
    get_tp = ThreadPoolExecutor(1, thread_name_prefix="GetTask_TP_")
    put_tp = ThreadPoolExecutor(1, thread_name_prefix="PutTask_TP_")

    async def get_task(*, loop=loop, tp=get_tp, queue=inqueue) -> tuple:
        return await loop.run_in_executor(tp, queue.get)

    async def put_result(result, *, loop=loop, tp=put_tp, queue=outqueue) -> None:
        return await loop.run_in_executor(tp, queue.put, result)

    try:
        loop.run_until_complete(
            _run_worker(
                get_task,
                put_result,
                loop=loop,
                initializer=initializer,
                initargs=initargs,
                maxtasks=maxtasks,
                wrap_exception=wrap_exception,
                concurrency_limit=concurrency_limit,
            )
        )
    except Exception as err:
        logger.exception("worker got exception %s", err)
    finally:
        logger.debug("shutdown workers")
        get_tp.shutdown()
        put_tp.shutdown()
        worker_tp.shutdown()
        logger.debug("shutdown asyncgens")
        loop.run_until_complete(loop.shutdown_asyncgens())
        if loop.is_running():
            loop.close()
        logger.debug("Worker done")


class AioPool(Pool):
    def __init__(
        self,
        processes: Optional[int] = None,
        initializer: Optional[Callable[..., Union[Awaitable[Any], Any]]] = None,
        initargs: Tuple[Any, ...] = (),
        maxtasksperchild: int = None,
        context=None,
        loop_initializer: Callable[[], BaseEventLoop] = None,
        pool_size: int = 1,
        concurrency_limit: int = 128,
    ) -> None:
        """Process pool implementation that support async functions.
        Support the same funcitonalilty as the original process pool.

        Args:
            processes: number of processes to run, same behaviour as Pool.
                Defaults to None.
            initializer: Initializer function that being executed first by each process.
                Can be async. Optional. Defaults to None.
            initargs: Arguments to pass to initializer. Defaults to ().
            maxtasksperchild: max tasks per process. same behaviour as Pool. Defaults to None.
            context: determine how to start the child processes. same behaviour as Pool. Defaults to None.
            loop_initializer: Function that create the new event loop. Defaults to None.
            pool_size: size for the default pool for the event loop in the new process. Defaults to 1.
            concurrency_limit: Maximume concurrent tasks to run in each process. Defaults to 128.
        """
        self._loop_initializer = loop_initializer or asyncio.new_event_loop
        if pool_size <= 0:
            raise ValueError("Thread pool size must be at least 1")
        self._pool_size = pool_size
        if concurrency_limit < 1:
            raise ValueError("Conccurency limit must be at least 1.")
        self._concurrency_limit = concurrency_limit
        super().__init__(processes, initializer, initargs, maxtasksperchild, context)

    if sys.version_info.minor < 8:

        def _repopulate_pool(self) -> None:
            """Bring the number of pool processes up to the specified number,
            for use after reaping workers which have exited.
            """
            for _ in range(self._processes - len(self._pool)):
                w = self.Process(
                    target=worker,
                    args=(
                        self._inqueue,
                        self._outqueue,
                        self._initializer,
                        self._initargs,
                        self._loop_initializer,
                        self._pool_size,
                        self._maxtasksperchild,
                        self._wrap_exception,
                        self._concurrency_limit,
                    ),
                )
                self._pool.append(w)
                w.name = w.name.replace("Process", "PoolWorker")
                w.daemon = True
                w.start()
                logger.debug("added worker")

    elif sys.version_info.minor >= 8:

        def _repopulate_pool(self) -> None:
            return self._repopulate_pool_static(
                self._ctx,
                self.Process,
                self._processes,
                self._pool,
                self._inqueue,
                self._outqueue,
                self._initializer,
                self._initargs,
                self._loop_initializer,
                self._maxtasksperchild,
                self._wrap_exception,
                self._pool_size,
                self._concurrency_limit,
            )

        @staticmethod
        def _repopulate_pool_static(
            ctx,
            Process,
            processes,
            pool,
            inqueue,
            outqueue,
            initializer,
            initargs,
            loop_initializer,
            maxtasksperchild,
            wrap_exception,
            pool_size,
            concurrency_limit,
        ) -> None:
            """Bring the number of pool processes up to the specified number,
            for use after reaping workers which have exited.
            """
            for i in range(processes - len(pool)):
                w = Process(
                    ctx,
                    target=worker,
                    args=(
                        inqueue,
                        outqueue,
                        initializer,
                        initargs,
                        loop_initializer,
                        pool_size,
                        maxtasksperchild,
                        wrap_exception,
                        concurrency_limit,
                    ),
                )
                w.name = w.name.replace("Process", "PoolWorker")
                w.daemon = True
                w.start()
                pool.append(w)
                logger.debug("added worker")
