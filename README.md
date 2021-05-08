# aiopool

All methods of `multiprocessing.Pool` are supported.    
All paramters for multiprocessing.Pool are supported.  

## examples:
Setting concurrency limit. This means each process can run with up to 8 concurrent tasks at a time. 
```python
import asyncio
from aiopool import AioPool


async def powlong(a):
  await asyncio.sleep(1)
  return a**2

if __name__ == '__main__':
  with AioPool(processes=2, concurrency_limit=8) as pool:
    results = pool.map(powlong, [i for i in range(16)])  # Should take 2 seconds (2*8).
    print(results) 

```

Async initliazers are also suppported.

```python
import asyncio
from aiopool import AioPool

async def start(message):
  await asyncio.sleep(1)
  print(message)

async def powlong(a):
  await asyncio.sleep(1)
  return a**2

if __name__ == '__main__':
  with AioPool(processes=2, 
               concurrency_limit=8, 
               initializer=start,
               init_args=("Started with AioPool", )) as pool:
    results = pool.map(powlong, [i for i in range(16)])  # Should take 2 seconds (2*8).
    print(results) 
    
```

By default, AioPool also set up a default executor for any non-async tasks.  
The size can be determined by `threadpool_size` arguemnt, which defaults to 1.   
None default event loops(`uvloop` for example) are supported as well, using the `loop_initializer` argument.  
Also, non-async functions are supported by default, as the AioPool worker identify if the function is async or not.  
If the function is not async, it runs inside the threadpool, to allow the requested concurrency.   
This means that order of execution is not guaranteed, even if the function is not async.  
However, the order of results is guaranteed through the pool API (map, starmap, apply, etc...).  

```python
from aiopool import AioPool
import uvloop

with AioPool(loop_initializer=uvloop.new_event_loop, threadpool_size=4) pool:
  pool.map(print, [i for i in range(8)])
```
 


