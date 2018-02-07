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


# ---------------- V3 -----------------------

class Mean(object):
    def __init__(self, values=None):
        if values is None:
            self.__weight = 0
            self.__sum = 0
        else:
            self.__weight = len(values)
            self.__sum = sum(values)

    def update(self, value, weight=1):
        self.__sum += value
        self.__weight += weight

    def get(self):
        return self.__sum / self.__weight

    def __str__(self):
        return str(self.get())


def signal_handler(loop, interrupted_event, _signal):
    print("got signal %s: exit" % _signal)
    interrupted_event.set()
    loop.stop()


async def test_script(param, results_queue, instance_id, sleep_time=1):
    start = asyncio.get_event_loop().time()
    # print('test {} started'.format(param))
    await asyncio.sleep(sleep_time)
    results_queue.put(
        {'test': param,
         'start': start,
         'duration': asyncio.get_event_loop().time()-start,
         'sleep_time': sleep_time,
         'instance': instance_id})
    # print('test {} finished'.format(param))


async def run_instance(_id, worker_id, params_queue, results_queue, params_ready_event, sleep_time):
    # print('Instance {}.{} started'.format(worker_id, _id))
    queue_block_timeout = 1
    while not params_ready_event.is_set():
        try:
            param = params_queue.get(True, queue_block_timeout)
            await test_script(param, results_queue, '{}.{}'.format(worker_id, _id), sleep_time)
        except Empty:
            continue
    else:
        while True:
            try:
                param = params_queue.get_nowait()
                await test_script(param, results_queue, '{}.{}'.format(worker_id, _id), sleep_time)
            except Empty:
                break
    # print('Instance {}.{} finished'.format(worker_id, _id))


def run_worker(_id, n_of_instances, params_queue, results_queue, params_ready_event, finish_event, sleep_time):
    print('Worker {} started'.format(_id))
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    futures = [run_instance(i, _id, params_queue, results_queue, params_ready_event, sleep_time) for i in range(n_of_instances)]
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


def main(n_of_workers, instances_per_worker, sleep_time):
    manager = mp.Manager()

    params_queue = manager.Queue()
    results_queue = manager.Queue()

    params_ready_event = manager.Event()
    finish_event = manager.Event()

    p_workers = [mp.Process(target=run_worker, args=(i, instances_per_worker,
                                                     params_queue, results_queue,
                                                     params_ready_event, finish_event,
                                                     sleep_time)) for i in range(n_of_workers)]
    p_feeder = mp.Process(target=feed_params, args=(params_queue, params_ready_event))

    p_feeder.start()
    time.sleep(5)

    gl_start = time.time()

    [p_worker.start() for p_worker in p_workers]
    mean_duration = Mean()
    with open('results{}-{}.jsonl'.format(n_of_workers, instances_per_worker), 'w') as output:
        while True:
            try:
                result = results_queue.get(True, 1)
                mean_duration.update(result['duration'])
                json.dump(result, output)
                output.write('\n')
            except Empty:
                if finish_event.is_set():
                    break
                else:
                    continue
    p_feeder.join()
    [p_worker.join() for p_worker in p_workers]
    # print('Mean duration: %s' % mean_duration.get())
    return time.time() - gl_start, mean_duration.get()


if __name__ == '__main__':
    results = []
    for index, params in enumerate([(4, 1280, 0.1), (4, 1280, 0.2), (4, 1280, 1), (4, 1280, 2)]):  # (4, 1024), (4, 1280), (4, 1536), (4, 1792), (4, 2048), (4, 3072)
        workers, instances, sleep_time = params
        print('Test {}: {} workers * {} instances, sleep {}s'.format(index, workers, instances, sleep_time))
        total_time, mean_time = main(workers, instances, sleep_time)
        results.append({'workers': workers,
                        'instances': instances,
                        'sleep': sleep_time,
                        'total_time': total_time,
                        'mean_test_time': mean_time,})
    print('workers\tinstances\tsleep\ttotal time\t\t\tmean duration\t\t\trps')
    for res in results:
        print('{}\t\t{}\t\t{}\t\t{}\t{}\t{}'.format(res['workers'],res['instances'], res['sleep'],
                                                  res['total_time'], res['mean_test_time'], 52000/res['total_time']))
