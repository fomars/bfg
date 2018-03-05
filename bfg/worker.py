import asyncio
import logging
import multiprocessing as mp
import signal
import threading
import traceback
from multiprocessing.managers import SyncManager
import time
from collections import namedtuple
from queue import Empty, Full

import aiohttp

from bfg.guns.ultimate import UltimateGun, GunSetupError
from .module_exceptions import ConfigurationError
from .util import FactoryBase

logger = logging.getLogger(__name__)

Task = namedtuple(
    'Task', 'ts,bfg,marker,data')


TOLERANCE = 0.0005


def shoot_with_delay(task, gun, start_time, interrupted_event):
    MAX_SLEEP = 5
    task = task._replace(ts=start_time + (task.ts / 1000.0))
    while task.ts - time.time() > TOLERANCE:
        if not interrupted_event.is_set():
            time.sleep(max(0, min(MAX_SLEEP, task.ts - time.time())))
        else:
            logger.debug('Interrupting scheduled test')
            return
    else:
        gun.shoot(task)


def run_instance(gun_config, interrupted_event, params_ready_event, task_queue, results_queue, start_time):
    gun = UltimateGun(gun_config, results_queue)
    try:
        gun.setup()
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
            shoot_with_delay(task, gun, start_time, interrupted_event)
        else:
            # clear queue if test was interrupted
            while True:  # can't check with queue.empty() because of multiprocessing
                try:
                    task_queue.get_nowait()
                except Empty:
                    break
    except GunSetupError:
        logger.error('Setup error')
    finally:
        gun.teardown()


def run_worker(n_of_instances, task_queue, gun_config, results_queue, params_ready_event, interrupted_event, start_time):
    logger.info("Started shooter worker: %s", mp.current_process().name)
    try:
        threads = [threading.Thread(target=run_instance, args=(gun_config, interrupted_event, params_ready_event,
                                                               task_queue, results_queue, start_time))
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

    def __init__(self, gun_config, load_plan, results, name, workers_num, instances_per_worker):
        self.name = name
        self.workers_n = workers_num
        self.gun_config = gun_config
        self.results_queue = results
        self.load_plan = load_plan
        self.instances_per_worker = instances_per_worker
        logger.info(
            '''
Name: {name}
Instances: {instances}
'''.format(
                name=self.name,
                instances=self.workers_n,
                # gun=gun_class,
            ))
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
        self.workers = [
            mp.Process(target=run_worker,
                       args=(self.instances_per_worker, self.task_queue, self.gun_config, self.results_queue,
                             self.params_ready_event, self.interrupted_event, self.start_time),
                       name="%s-%s" % (self.name, i))
            for i in range(0, self.workers_n)]

        for process in self.workers:
            process.start()

    def interrupt(self):
        self.interrupted_event.set()
        self.purge_queue()

    def purge_queue(self):
        while True:  # can't check with queue.empty() because of multiprocessing
            try:
                self.task_queue.get_nowait()
            except Empty:
                break

    async def wait_for_finish(self, timeout=2):
        # while self.p_feeder.is_alive():
        #     await asyncio.sleep(timeout)
        # else:
        #     self.p_feeder.join()
        #     logger.info('Feeder joined.')

        while any(worker.is_alive() for worker in self.workers):
            logger.info('Some worker is still alive')
            await asyncio.sleep(timeout)
        else:
            self.purge_queue()
            self.p_feeder.join()
            logger.info('Feeder joined.')
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
                gun_config=self.component_factory.config['gun'].get(bfg_config.get('gun')),
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
