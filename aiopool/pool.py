from asyncio.base_events import BaseEventLoop
import logging
import asyncio
from typing import Any, Callable, Deque, Dict, Iterable, List, Set
from asyncio.tasks import Task
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from multiprocessing.pool import (  # noqa
    ExceptionWithTraceback,
    MaybeEncodingError,
    _helper_reraises_exception,
    Pool,
    mapstar,
    starmapstar,
)

__all__ = ["AioPool"]


logger = logging.getLogger("aiopool")


async def _fix_mapstar(
    sem: asyncio.Semaphore,
    loop: BaseEventLoop,
    func: Callable[..., Any],
    args: Iterable[Any],
    kwds: Dict[str, Any],
    iscoroutinefunction: Callable[[Callable[..., Any]], bool],
) -> List[Any]:
    results = deque([])
    underlying_func = args[0][0]
    underlying_params = args[0][1]

    def release_sem(t: Task, sem: asyncio.Semaphore = sem) -> None:
        sem.release()

    if iscoroutinefunction(underlying_func):
        for coroutine in func(*args, **kwds):
            await sem.acquire()
            task = loop.create_task(coroutine)
            task.add_done_callback(release_sem)
            results.append(task)
    else:
        for params in underlying_params:
            await sem.acquire()
            if func is mapstar:
                task = loop.run_in_executor(None, underlying_func, params)
            else:
                task = loop.run_in_executor(None, underlying_func, *params)
            task.add_done_callback(release_sem)
            results.append(task)
    return await asyncio.gather(*results)


async def task_wrapper(
    task,
    put_tp: ThreadPoolExecutor,
    put: Callable[..., None],
    loop: asyncio.BaseEventLoop,
    sem_concurrency_limit,
    wrap_exception: bool = False,
    iscoroutinefunction=asyncio.iscoroutinefunction,
) -> None:
    job, i, func, args, kwds = task
    try:

        if iscoroutinefunction(func):
            result = (True, await func(*args, **kwds))
        elif func in (mapstar, starmapstar):
            sem_concurrency_limit.release()
            try:
                result = (
                    True,
                    await _fix_mapstar(
                        sem_concurrency_limit,
                        loop,
                        func,
                        args,
                        kwds,
                        iscoroutinefunction,
                    ),
                )
            finally:
                await sem_concurrency_limit.acquire()
        else:
            result = (
                True,
                await loop.run_in_executor(None, lambda: func(*args, **kwds)),
            )

    except Exception as e:
        if wrap_exception and func is not _helper_reraises_exception:
            e = ExceptionWithTraceback(e, e.__traceback__)
        result = (False, e)
    try:
        await loop.run_in_executor(put_tp, lambda: put((job, i, result)))
    except Exception as e:
        wrapped = MaybeEncodingError(e, result[1])
        logger.debug("Possible encoding error while sending result: %s" % (wrapped))
        await loop.run_in_executor(put_tp, lambda: put((job, i, (False, wrapped))))


def worker(
    inqueue,
    outqueue,
    initializer=None,
    initargs=(),
    loop_initializer=asyncio.new_event_loop,
    threads=1,
    maxtasks=None,
    wrap_exception=False,
    concurrency_limit=128,
) -> None:
    loop: asyncio.BaseEventLoop = loop_initializer()
    asyncio.set_event_loop(loop)
    worker_tp = ThreadPoolExecutor(threads, thread_name_prefix="Worker_TP_")
    loop.set_default_executor(worker_tp)
    get_tp = ThreadPoolExecutor(1, thread_name_prefix="GetTask_TP_")
    put_tp = ThreadPoolExecutor(1, thread_name_prefix="PutTask_TP_")

    async def run(
        get: Callable[..., None] = inqueue.get,
        put: Callable[..., None] = outqueue.put,
        loop: asyncio.BaseEventLoop = loop,
    ) -> None:
        if initializer is not None:
            if asyncio.iscoroutinefunction(initializer):
                await initializer(*initargs)
            else:
                initializer(*initargs)
        completed = 0
        sem_concurrency_limit = asyncio.Semaphore(concurrency_limit)

        tasks: Set[Task] = set()

        def release_sem(t: Task, sem=sem_concurrency_limit) -> None:
            sem.release()

        def remove_task(t: Task, tasks: Set[Task] = tasks) -> None:
            tasks.remove(t)

        while maxtasks is None or (maxtasks and completed < maxtasks):
            async with sem_concurrency_limit:
                try:
                    task = await loop.run_in_executor(get_tp, get)
                except (EOFError, OSError):
                    logger.debug("worker got EOFError or OSError -- exiting")
                    for task in tasks:
                        task.cancel()
                    break

                if task is None:
                    logger.debug("worker got sentinel -- exiting")
                    break
            await sem_concurrency_limit.acquire()
            new_task = asyncio.create_task(
                task_wrapper(
                    task,
                    put_tp=put_tp,
                    put=put,
                    loop=loop,
                    wrap_exception=wrap_exception,
                    sem_concurrency_limit=sem_concurrency_limit,
                )
            )
            tasks.add(new_task)
            new_task.add_done_callback(release_sem)
            new_task.add_done_callback(remove_task)

        await asyncio.wait(tasks)
        logger.debug("worker exiting after %d tasks" % completed)

    loop.run_until_complete(run())


class AioPool(Pool):
    def __init__(
        self,
        processes=None,
        initializer=None,
        initargs=(),
        maxtasksperchild=None,
        context=None,
        loop_initializer=None,
        threadpool_size=1,
        concurrency_limit=128,
    ) -> None:
        self._loop_initializer = loop_initializer or asyncio.new_event_loop
        self._threadpool_size = threadpool_size
        self._concurrency_limit = concurrency_limit
        super().__init__(processes, initializer, initargs, maxtasksperchild, context)

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
            self._threadpool_size,
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
        threadpool_size,
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
                    threadpool_size,
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
