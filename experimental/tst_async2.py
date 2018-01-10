import asyncio
import json
import random
from queue import Empty
import multiprocessing as mp
import aiohttp
import async_timeout
import time


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



#---------------- V3 -----------------------

async def test_script(param, results_queue, instance_id):
    start = time.time()
    # print('test {} started'.format(param))
    sleep_time = random.random()
    await asyncio.sleep(sleep_time)
    results_queue.put(
        {'test': param,
         'start': start,
         'duration': time.time()-start,
         'sleep_time': sleep_time,
         'instance': instance_id})
    # print('test {} finished'.format(param))


async def run_instance(_id, worker_id, params_queue, results_queue, params_ready_event):
    # print('Instance {}.{} started'.format(worker_id, _id))
    queue_block_timeout = 1
    while not params_ready_event.is_set():
        try:
            param = params_queue.get(True, queue_block_timeout)
            await test_script(param, results_queue, '{}.{}'.format(worker_id, _id))
        except Empty:
            continue
    else:
        while True:
            try:
                param = params_queue.get_nowait()
                await test_script(param, results_queue, '{}.{}'.format(worker_id, _id))
            except Empty:
                break
    # print('Instance {}.{} finished'.format(worker_id, _id))


def run_worker(_id, n_of_instances, params_queue, results_queue, params_ready_event, finish_event):
    print('Worker {} started'.format(_id))
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    futures = [run_instance(i, _id, params_queue, results_queue, params_ready_event) for i in range(n_of_instances)]
    gathered = asyncio.gather(*futures)
    try:
        loop.run_until_complete(gathered)
    finally:
        finish_event.set()
        loop.close()
        print('Worker {} finished'.format(_id))


def feed_params(params_queue, params_ready_event):
    params = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz' * 1000 # 52000 chars
    for param in params:
        # time.sleep(random.random()*0.05)
        params_queue.put(param)
    params_ready_event.set()
    print('All params feeded')


def main(n_of_workers, instances_per_worker):
    manager = mp.Manager()

    params_queue = manager.Queue()
    results_queue = manager.Queue()

    params_ready_event = manager.Event()
    finish_event = manager.Event()

    p_workers = [mp.Process(target=run_worker, args=(i, instances_per_worker,
                                                     params_queue, results_queue,
                                                     params_ready_event, finish_event)) for i in range(n_of_workers)]
    p_feeder = mp.Process(target=feed_params, args=(params_queue, params_ready_event))

    p_feeder.start()
    time.sleep(5)

    gl_start = time.time()

    [p_worker.start() for p_worker in p_workers]
    with open('results.jsonl', 'w') as output:
        while True:
            try:
                json.dump(results_queue.get(True, 1), output)
                output.write('\n')
            except Empty:
                if finish_event.is_set():
                    break
                else:
                    continue
    p_feeder.join()
    [p_worker.join() for p_worker in p_workers]

    return time.time() - gl_start


if __name__ == '__main__':
    results = []
    for index, params in enumerate([(4, 1280), (4, 1536), (4, 1792), (4, 2048)]):  # (4, 1024), (4, 1280), (4, 1536), (4, 1792), (4, 2048), (4, 3072)
        workers, instances = params
        print('Test {}: {} workers * {} instances'.format(index, workers, instances))
        res = main(workers, instances)
        results.append({'workers': workers,
                        'instances': instances,
                        'time': res})
    print('workers\tinstances\ttime')
    for res in results:
        print('{}\t{}\t{}'.format(res['workers'], res['instances'], res['time']))
