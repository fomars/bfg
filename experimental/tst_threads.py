import json
import threading
from queue import Empty
import multiprocessing as mp
import time
from bfg.util import Mean
import requests


def create_job(session):
    resp = requests.post('http://127.0.0.1:5000/api/job/create.json',
                         json={"task": "LOAD-204",
                               "person": "fomars",
                               "tank": "localhost",
                               "host": "localhost",
                               "port": 5000})
    return resp


def get_summary(job):
    resp = requests.get('http://127.0.0.1:5000/api/job/{}/summary.json'.format(job))
    return resp


def signal_handler(loop, interrupted_event, _signal):
    print("got signal %s: exit" % _signal)
    interrupted_event.set()
    loop.stop()


def test_script(param, results_queue, instance_id, sleep_time=1):
    start = time.time()
    # print('test {} started'.format(param))
    resp = get_summary(param)
    results_queue.put(
        {'test': param,
         'start': start,
         'duration': time.time()-start,
         # 'sleep_time': sleep_time,
         'instance': instance_id})
    # print('test {} finished'.format(param))


def run_instance(_id, worker_id, params_queue, results_queue, params_ready_event):
    # print('Instance {}.{} started'.format(worker_id, _id))
    queue_block_timeout = 1
    while not params_ready_event.is_set():
        try:
            param = params_queue.get()
            test_script(param, results_queue, '{}.{}'.format(worker_id, _id), sleep_time)
        except Empty:
            continue
    else:
        while True:
            try:
                param = params_queue.get_nowait()
                test_script(param, results_queue, '{}.{}'.format(worker_id, _id), sleep_time)
            except Empty:
                break
    # print('Instance {}.{} finished'.format(worker_id, _id))


def run_worker(_id, n_of_instances, params_queue, results_queue, params_ready_event, finish_event, sleep_time):
    print('Worker {} starting'.format(_id))
    try:
        threads = [threading.Thread(target=run_instance, args=(i, _id, params_queue, results_queue, params_ready_event))
                   for i in range(n_of_instances)]
        [thread.start() for thread in threads]
        [thread.join() for thread in threads]
    finally:
        finish_event.set()
    # loop = asyncio.new_event_loop()
    # asyncio.set_event_loop(loop)
    # futures = [run_instance(i, _id, params_queue, results_queue, params_ready_event, sleep_time) for i in range(n_of_instances)]
    # gathered = asyncio.gather(*futures)
    # try:
    #     loop.run_until_complete(gathered)
    # finally:
    #     finish_event.set()
    #     loop.close()
    #     print('Worker {} finished'.format(_id))


def feed_params(params_queue, params_ready_event):
    params = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz' * 100 # 5200 chars
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
    for index, params in enumerate([(1, 100, 1), (1, 200, 1), (1, 400, 1), (1, 800, 2)]):  # (4, 1024), (4, 1280), (4, 1536), (4, 1792), (4, 2048), (4, 3072)
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
