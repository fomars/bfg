# test ThreadPoolExecutor
import asyncio
import json
import random
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from queue import Empty
import multiprocessing as mp

def test_script(param, results_queue):
    start = time.time()
    # print('test {} started'.format(param))
    sleep_time = random.random()*2
    asyncio.sleep(sleep_time)
    results_queue.put(
        {'test': param,
         'start': start,
         'duration': time.time()-start,
         'sleep_time': sleep_time})
    # print('test {} finished'.format(param))


async def run_instance(_id, worker_id, params_queue, results_queue, params_ready_event):
    # print('Instance {}.{} started'.format(worker_id, _id))
    queue_block_timeout = 1
    while not params_ready_event.is_set():
        try:
            param = params_queue.get(True, queue_block_timeout)
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
                # print('Instance {}.{} finished'.format(worker_id, _id))


def params_gen(params_queue, params_ready_event):
    queue_block_timeout = 1
    while not params_ready_event.is_set():
        try:
            yield params_queue.get(True, queue_block_timeout)
        except Empty:
            continue
    else:
        while True:
            try:
                yield params_queue.get_nowait()
            except Empty:
                break


def run_worker(_id, n_of_instances, params_queue, results_queue, params_ready_event, finish_event):
    print('Worker {} started'.format(_id))
    test = partial(test_script, results_queue=results_queue)
    params = params_gen(params_queue, params_ready_event)
    with ThreadPoolExecutor(n_of_instances) as executor:
        for result in executor.map(test, params):
            print(result)
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
    for index, params in enumerate([(4, 16), (4, 16), (4, 16),
                                    (4, 32), (4, 32), (4, 32),
                                    (4, 64), (4, 64), (4, 64)]):
        workers, instances = params
        print('Test {}: {} workers * {} instances'.format(index, workers, instances))
        res = main(workers, instances)
        results.append({'workers': workers,
                        'instances': instances,
                        'time': res})
    print('workers\tinstances\ttime')
    for res in results:
        print('{}\t{}\t{}'.format(res['workers'], res['instances'], res['time']))