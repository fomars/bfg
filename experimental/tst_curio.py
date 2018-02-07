from queue import Empty

import curio
import multiprocessing as mp

import time
from bfg.util import Mean


async def test_script(param, results_queue, sleep_time=1):
    start = time.time()
    # print('test {} started'.format(param))
    await curio.sleep(sleep_time)
    results_queue.put({
        'test': param,
        'start': start,
        'duration': time.time()-start,
        'sleep_time': sleep_time,
        # 'instance': instance_id
    })
    # print('test {} finished'.format(param))

async def run_instance(params_queue, results_queue, params_ready_event, sleep_time):
    # print('Instance {}.{} started'.format(worker_id, _id))
    queue_block_timeout = 1
    while not params_ready_event.is_set():
        try:
            param = params_queue.get(True, queue_block_timeout)
            await test_script(param, results_queue, sleep_time)
        except Empty:
            continue
    else:
        while True:
            try:
                param = params_queue.get_nowait()
                await test_script(param, results_queue, sleep_time)
            except Empty:
                break

async def countdown(n):
    while n > 0:
        print('T-minus', n)
        await curio.sleep(1)
        n -= 1

async def kid():
    print('Building the Millenium Falcon in Minecraft')
    await curio.sleep(1000)

async def parent():
    PARAMS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz' * 500  # 26000 chars
    manager = mp.Manager()
    params_queue = manager.Queue()
    results_queue = manager.Queue()
    params_ready_event = manager.Event()
    for param in PARAMS:
        params_queue.put(param)
    params_ready_event.set()
    print('All params feeded')

    INSTANCES = 512
    SLEEP_TIME = 0.1

    tasks = []
    start = time.time()
    for i in range(INSTANCES):
        task = await curio.spawn(run_instance, params_queue, results_queue, params_ready_event, SLEEP_TIME)
        tasks.append(task)
    for task in tasks:
        await task.join()
    duration = time.time()-start

    mean_duration = Mean()
    while True:
        try:
            result = results_queue.get(True, 1)
            mean_duration.update(result['duration'])
            # json.dump(result, output)
            # output.write('\n')
        except Empty:
            break

    print('{} instances, {}s sleep time'.format(INSTANCES, SLEEP_TIME))
    print('{} seconds'.format(duration))
    print('{} rps avg.'.format(len(PARAMS)/duration))
    print('{} avg. duration'.format(mean_duration))


    # kid_task = await curio.spawn(kid)
    # await curio.sleep(5)
    #
    # print("Let's go")
    # count_task = await curio.spawn(countdown, 10)
    # await count_task.join()
    #
    # print("We're leaving!")
    # await kid_task.join()
    # print('Leaving')

if __name__ == '__main__':
    curio.run(parent, with_monitor=True)