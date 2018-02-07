import asyncio
import json
import multiprocessing as mp
import time
from queue import Empty

# ---------------- V4 -----------------------
from bfg.util import Mean


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


PARAMS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz' * 250 # 13000 chars


def feed_params(params_queue, params_ready_event):
    for param in PARAMS:
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
    with open('results{}-{}-{}.jsonl'.format(n_of_workers, instances_per_worker, sleep_time), 'w') as output:
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
    for index, params in enumerate([(1, 512, 0.1), (1, 512, 0.2), (1, 512, 1), (1, 512, 2)]):  # (4, 1024), (4, 1280), (4, 1536), (4, 1792), (4, 2048), (4, 3072)
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
                                                  res['total_time'], res['mean_test_time'], len(PARAMS)/res['total_time']))
