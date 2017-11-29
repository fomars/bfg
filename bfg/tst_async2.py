import asyncio
import concurrent.futures
import random
import uuid

import aiohttp
import async_timeout
import time

import yaml


async def create_job(session):
    async with async_timeout.timeout(10):
        async with session.post('https://lunapark.test.yandex-team.ru/api/job/create.json',
                                json={"task": "LOAD-204",
                                      "person": "fomars",
                                      "tank": "localhost",
                                      "host": "localhost",
                                      "port": 5000}) as response:
            # return await response.json()
            # print(await response.text())
            return await response.json()


async def get_summary(session, job):
    async with async_timeout.timeout(10):
        async with session.get('https://lunapark.test.yandex-team.ru/api/job/{}/summary.json'.format(job)) as response:
            return await response.json()


async def task(session, i):
    start = time.time()
    # print('Task {} started'.format(i))
    job_info = await create_job(session)
    # print('Job {} for task {} created within {:.2f} seconds'.format(job_info, i, time.time() - start))
    summary = await get_summary(session, job_info[0]['job'])
    # print('Task {} took {:.2f} seconds'.format(i, time.time() - start))
    return summary


async def tasks(n):
    start = time.time()
    tsks = [asyncio.ensure_future(task(i)) for i in range(n)]
    await asyncio.wait(tsks)
    print('Took {:.2f} sec'.format(time.time() - start))


async def launch_tasks(n):
    start = time.time()
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
        futures = [task(session, i) for i in range(n)]
        for future in asyncio.as_completed(futures):
            result = await future
            # print(yaml.dump(result))
    return time.time() - start




def do_work(n_tasks):
    start = time.time()
    _id = uuid.uuid4()
    # print('Worker {} starting {} tasks'.format(_id, n_tasks))
    worker_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(worker_loop)
    t1 = worker_loop.run_until_complete(launch_tasks(n_tasks))
    print('Worker {} took {:.2f} seconds to perform {} tasks'.format(_id, t1, n_tasks))
    return time.time() - start


async def start(executor, tasks):
    start = time.time()
    loop = asyncio.get_event_loop()
    futures = [loop.run_in_executor(executor, do_work, task) for task in tasks]
    for future in asyncio.as_completed(futures):
        result = await future
        print('Total worker time: {:.2f}'.format(result))
    print('Total time {:.2f}'.format(time.time() - start))


if __name__ == '__main__':
    TASKS = [3, 6, 9, 4, 7, 3, 9, 6, 6, 3, 8, 3, 6, 3, 6, 8, 3, 5, 8, 4, 8, 5, 6, 8, 4, 7, 5, 6, 6, 9, 4, 8, 6, 9]
    executor = concurrent.futures.ProcessPoolExecutor(10)
    ioloop = asyncio.get_event_loop()
    try:
        ioloop.run_until_complete(
            start(executor, TASKS)
        )
    finally:
        ioloop.close()
