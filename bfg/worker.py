import asyncio
import logging
import multiprocessing as mp
import signal
import threading
from multiprocessing.managers import SyncManager
import time
from collections import namedtuple
from queue import Empty, Full

import aiohttp

from .module_exceptions import ConfigurationError
from .util import FactoryBase

logger = logging.getLogger(__name__)

Task = namedtuple(
    'Task', 'ts,bfg,marker,data')


def shoot_with_delay(task, gun, start_time):
    task = task._replace(ts=start_time + (task.ts / 1000.0))
    delay = task.ts - time.time()
    if delay > 0:
        start = time.time()
        time.sleep(delay)
    if delay < -0.2:
        logger.warning('Delay: {}'.format(delay))
    gun.shoot(task)


async def async_shooter(_id, gun, task_queue, params_ready_event, interrupted_event, start_time, session):
    """

    :type task_queue: mp.Queue
    """
    # logger.info("Started shooter instance {} of worker {}".format(_id, mp.current_process().name))

    gun.setup()
    while not interrupted_event.is_set():
        # Read with retry while params are feeding
        if not params_ready_event.is_set():
            try:
                task = task_queue.get_nowait()
                if not task:
                    logger.info(
                        "Got poison pill. Exiting %s",
                        mp.current_process().name)
                    break
            except Empty:
                await asyncio.sleep(0.1)
                continue
        # Once all params are fed, we can do get_nowait() and terminate when Empty is raised
        else:
            try:
                task = task_queue.get_nowait()
                if not task:
                    logger.info(
                        "Got poison pill. Exiting %s",
                        mp.current_process().name)
                    break
            except Empty:
                break
        await shoot_with_delay(task, gun, start_time)
    else:
        # clear queue if test was interrupted
        while True:  # can't check with queue.empty() because of multiprocessing
            try:
                task_queue.get_nowait()
            except Empty:
                break
    gun.teardown()


def run_instance(gun, interrupted_event, params_ready_event, task_queue, start_time):
    gun.setup()
    try:
        while not interrupted_event.is_set():
            # Read with retry while params are feeding
            if not params_ready_event.is_set():
                try:
                    task = task_queue.get()
                    if not task:
                        logger.info(
                            "Got poison pill. Exiting %s",
                            mp.current_process().name)
                        break
                except Empty:
                    continue
            # Once all params are fed, we can do get_nowait() and terminate when Empty is raised
            else:
                try:
                    task = task_queue.get_nowait()
                    if not task:
                        logger.info(
                            "Got poison pill. Exiting %s",
                            mp.current_process().name)
                        break
                except Empty:
                    break
            shoot_with_delay(task, gun, start_time)
        else:
            # clear queue if test was interrupted
            while True:  # can't check with queue.empty() because of multiprocessing
                try:
                    task_queue.get_nowait()
                except Empty:
                    break
    finally:
        logger.debug('Tearing down scenario')
        gun.teardown()



def run_worker(n_of_instances, task_queue, gun, params_ready_event, interrupted_event):
    logger.info("Started shooter worker: %s", mp.current_process().name)
    # loop = asyncio.new_event_loop()
    # asyncio.set_event_loop(loop)
    start_time = time.time()

    # futures = [async_shooter(i, gun, task_queue, params_ready_event, interrupted_event, start_time) for i in range(n_of_instances)]
    # gathered = asyncio.gather(*futures)
    # try:
    #     loop.run_until_complete(run_with_aiohttp_session(loop, gun, task_queue, params_ready_event,
    #                                                      interrupted_event, start_time, n_of_instances))
    # finally:
    #     logger.info('Closing worker %s', mp.current_process().name)
    #     loop.close()
    interrupted = threading.Event()
    try:
        threads = [threading.Thread(target=run_instance, args=(gun, interrupted_event, params_ready_event,
                                                               task_queue, start_time))
                   for i in range(n_of_instances)]
        [thread.start() for thread in threads]
        [thread.join() for thread in threads]
    finally:
        pass


def feed_params(load_plan, params_queue, params_ready_event, interrupted_event, gun_name):
    for task in load_plan:
        while not interrupted_event.is_set():
            task = task._replace(bfg=gun_name)
            # Avoid blocking on put when queue reaches maxsize
            try:
                params_queue.put_nowait(task)
                break
            except Full:
                logger.debug("Params queue is full")
                time.sleep(0.1)
        else:
            logger.info('Feeding interrupted')
            break
    params_ready_event.set()
    logger.info('Feeding concluded')


# def mgr_init():
#     signal.signal(signal.SIGINT, signal.SIG_IGN)
#     logger.info('initialized manager')


class BFG(object):
    '''
    A BFG load generator that manages multiple workers as processes
    and feeds them with tasks
    '''

    def __init__(self, gun, load_plan, results, name, workers_num, instances_per_worker):
        self.name = name
        self.workers_n = workers_num
        self.gun = gun
        self.gun.results = results
        self.load_plan = load_plan
        self.instances_per_worker = instances_per_worker
        logger.info(
            '''
Name: {name}
Instances: {instances}
Gun: {gun.__class__.__name__}
'''.format(
                name=self.name,
                instances=self.workers_n,
                gun=gun,
            ))
        # self.manager = SyncManager()
        # self.manager.start(mgr_init)
        self.task_queue = mp.Queue(4096)
        self.params_ready_event = mp.Event()
        self.interrupted_event = mp.Event()
        self.p_feeder = mp.Process(target=feed_params, args=(self.load_plan,
                                                             self.task_queue,
                                                             self.params_ready_event,
                                                             self.interrupted_event,
                                                             self.name))
        self.workers_finished = False

    def start(self):
        self.p_feeder.start()
        self.start_time = time.time()
        # self.workers = [ProcessWorker(self.gun, self.task_queue, self.start_time,
        #                               self.instances_per_worker, name="%s-%s" % (self.name, i))
        #                 for i in range(0, self.workers_n)]
        self.workers = [
            mp.Process(target=run_worker,
                       args=(self.instances_per_worker, self.task_queue, self.gun,
                             self.params_ready_event, self.interrupted_event),
                       name="%s-%s" % (self.name, i))
            for i in range(0, self.workers_n)]

        for process in self.workers:
            process.start()

    def interrupt(self):
        self.interrupted_event.set()

    async def wait_for_finish(self, timeout=2):
        while self.p_feeder.is_alive():
            await asyncio.sleep(timeout)
        else:
            self.p_feeder.join()
            logger.info('Feeder joined.')

        while any(worker.is_alive() for worker in self.workers):
            logger.info('Some worker is still alive')
            await asyncio.sleep(timeout)
        else:
            [worker.join() for worker in self.workers]
            logger.info('All workers joined')

        # self.manager.shutdown()

    def running(self):
        """
        True while there are alive workers out there. Tank
        will quit when this would become False
        """
        # return not self.workers_finished
        return len(mp.active_children())


class BFGFactory(FactoryBase):
    FACTORY_NAME = 'bfg'

    def get(self, bfg_name):
        if bfg_name in self.factory_config:
            bfg_config = self.factory_config.get(bfg_name)
            ammo = self.component_factory.get_factory(
                'ammo', bfg_config.get('ammo'))
            schedule = self.component_factory.get_factory(
                'schedule', bfg_config.get('schedule'))
            lp = (
                Task(ts, bfg_name, marker, data)
                for ts, (marker, data) in zip(schedule, ammo))
            return BFG(
                name=bfg_name,
                gun=self.component_factory.get_factory(
                    'gun', bfg_config.get('gun')),
                load_plan=lp,
                workers_num=bfg_config.get('workers'),
                results=self.component_factory.get_factory(
                    'aggregator',
                    bfg_config.get('aggregator')).results_queue,
                instances_per_worker=bfg_config.get('instances')
            )
        else:
            raise ConfigurationError(
                "Configuration for '%s' BFG not found" % bfg_name)
