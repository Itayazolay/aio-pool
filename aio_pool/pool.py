from asyncio.base_events import BaseEventLoop
from asyncio.futures import Future
import logging
import asyncio
import sys
from typing import (
    Any,
    Callable,
    Awaitable,
    Deque,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)
from asyncio.tasks import Task
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from multiprocessing.pool import (   # type: ignore # noqa
    ExceptionWithTraceback,
    MaybeEncodingError,
    _helper_reraises_exception,
    Pool,
    mapstar,
    starmapstar,
) 

__all__ = ["AioPool"]


logger = logging.getLogger("aiopool")
logger.addHandler(logging.NullHandler())


async def _fix_mapstar(
    sem: asyncio.Semaphore,
    loop: BaseEventLoop,
    func: Callable[..., Any],
    args: Tuple[Tuple[Callable[..., Any], Iterable[Any]]],
    kwds: Dict[str, Any],
    iscoroutinefunction: Callable[[Callable[..., Any]], bool],
) -> List[Any]:
    results: Deque[Any] = deque([])
    underlying_func = args[0][0]
    underlying_params = args[0][1]
    create_task: Callable[[], Union[Task[Any], Future[Any]]]
    if iscoroutinefunction(underlying_func):
        for coroutine in func(*args, **kwds):
            create_task = lambda: loop.create_task(coroutine)
            task = await _create_bounded_task(create_task, sem)
            results.append(task)
    else:
        for params in underlying_params:

            if func is mapstar:
                create_task = lambda: loop.run_in_executor(
                    None, underlying_func, params
                )
            else:
                create_task = lambda: loop.run_in_executor(
                    None, underlying_func, *params
                )

            task = await _create_bounded_task(create_task, sem)
            results.append(task)
    return await asyncio.gather(*results)


async def task_wrapper(
    task,
    put: Callable[[Any], Awaitable[None]],
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
            # with mapstar/starmapstar we get a bunch of tasks to execute.
            # In correctly handle the semaphore, we release the current 'task',
            # and acquire it once per each task we got in the mapstar/starmapstar.
            # Then, we acquire the lock again, send the result back and the
            # semaphore is released above correctly.
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
        await put((job, i, result))
    except Exception as e:
        wrapped = MaybeEncodingError(e, result[1])
        logger.debug("Possible encoding error while sending result: %s" % (wrapped))
        await put((job, i, (False, wrapped)))


async def _create_bounded_task(
    create_task: Callable[[], Union[Task, Future]], sem: asyncio.Semaphore
):
    await sem.acquire()
    task = create_task()
    task.add_done_callback(lambda t: sem.release())
    return task


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
    sem_concurrency_limit = asyncio.Semaphore(concurrency_limit)

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

        create_task = lambda: loop.create_task(
            task_wrapper(
                task,
                put=put,
                loop=loop,
                wrap_exception=wrap_exception,
                sem_concurrency_limit=sem_concurrency_limit,
            )
        )
        new_task = await _create_bounded_task(create_task, sem_concurrency_limit)
        tasks.add(new_task)
        new_task.add_done_callback(remove_task)
    if tasks:
        await asyncio.wait(tasks)
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
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


class AioPool(Pool):
    def __init__(
        self,
        processes: Optional[int] = None,
        initializer: Optional[Callable[..., Union[Awaitable[Any], Any]]] = None,
        initargs: Tuple[Any, ...] = (),
        maxtasksperchild: int = None,
        context=None,
        loop_initializer: Callable[[], BaseEventLoop] = None,
        threadpool_size: int = 1,
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
            threadpool_size: size for the default threadpool for the event loop in the new process. Defaults to 1.
            concurrency_limit: Maximume concurrent tasks to run in each process. Defaults to 128.
        """
        self._loop_initializer = loop_initializer or asyncio.new_event_loop
        if threadpool_size <= 0:
            raise ValueError("Thread pool size must be at least 1")
        self._threadpool_size = threadpool_size
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
                        self._threadpool_size,
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
