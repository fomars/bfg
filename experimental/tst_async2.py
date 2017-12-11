import asyncio
import concurrent.futures
import random
import uuid
from queue import Empty
import multiprocessing as mp
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


async def _start(executor, tasks):
    start = time.time()
    loop = asyncio.get_event_loop()
    futures = [loop.run_in_executor(executor, do_work, task) for task in tasks]
    for future in asyncio.as_completed(futures):
        result = await future
        print('Total worker time: {:.2f}'.format(result))
    print('Total time {:.2f}'.format(time.time() - start))


async def results_generator(index, n, results):
    gen_start = time.time()
    for i in range(n):
        start = time.time()
        await asyncio.sleep(random.random()*0.5)
        _time = time.time()
        results.append({'test {}'.format(index): (_time, _time - start)})
    return index, time.time() - gen_start


async def launch_tests(tests):
    results = []
    futures = [results_generator(i, n, results) for i, n in enumerate(tests)]
    for future in asyncio.as_completed(futures):
        index, _time = await future
        print('Test {} lasted {:.2f}'.format(index, _time))
    return results

#---------------- V3 -----------------------

async def test_script(param, results_queue):
    start = time.time()
    # print('test {} started'.format(param))
    sleep_time = random.random()
    await asyncio.sleep(sleep_time)
    results_queue.put(
        {'test': param,
         'start': start,
         'duration': time.time()-start,
         'sleep_time': sleep_time})
    # print('test {} finished'.format(param))


async def run_instance(params_queue, results_queue, params_ready_event):
    queue_block_timeout = 1
    while not params_ready_event.is_set():
        try:
            param = params_queue.get(queue_block_timeout)
            await test_script(param, results_queue)
        except Empty:
            continue
    else:
        while True:
            try:
                param = params_queue.get_nowait()
                await test_script(param, results_queue)
            except Empty:
                break


def run_worker(n_of_instances, params_queue, results_queue, params_ready_event, finish_event):
    # manager = mp.Manager()
    # params_queue = manager.Queue()
    # params = 'abcdefghijklmnoprstuvwxyz'
    # [params_queue.put(param) for param in params]
    # results_queue = manager.Queue()
    futures = [run_instance(params_queue, results_queue, params_ready_event) for i in range(n_of_instances)]
    gathered = asyncio.gather(*futures)
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(gathered)
    finally:
        finish_event.set()
        loop.close()


def feed_params(params_queue, params_ready_event):
    params = 'ABCDEGHIJKLMNOPRSTUVWXYZabcdefghijklmnoprstuvwxyz' * 4
    for param in params:
        time.sleep(random.random()*0.05)
        params_queue.put(param)
    params_ready_event.set()
    print('All params feeded')

if __name__ == '__main__':
    n_of_instances = 25

    manager = mp.Manager()

    params_queue = manager.Queue()
    results_queue = manager.Queue()

    params_ready_event = manager.Event()
    finish_event = manager.Event()

    p_worker = mp.Process(target=run_worker, args=(n_of_instances,
                                            params_queue, results_queue,
                                            params_ready_event, finish_event))
    p_feeder = mp.Process(target=feed_params, args=(params_queue, params_ready_event))
    p_worker.start()
    p_feeder.start()
    while not finish_event.is_set():
        try:
            print(results_queue.get(False))
        except Empty:
            continue
    p_feeder.join()
    p_worker.join()

