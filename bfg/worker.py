import signal
import time
import multiprocessing as mp
import threading as th
from queue import Empty, Full

import functools

import sys

from .util import FactoryBase
from .module_exceptions import ConfigurationError
from collections import namedtuple
import asyncio
import logging

logger = logging.getLogger(__name__)


Task = namedtuple(
    'Task', 'ts,bfg,marker,data')


async def shoot_with_delay(task, gun, start_time):
    task = task._replace(ts=start_time + (task.ts / 1000.0))
    delay = task.ts - time.time()
    # logger.info('Task:\n{}\nDelay: {}'.format(task, delay))
    if delay > 0:
        await asyncio.sleep(delay)
    await gun.shoot(task)


async def async_shooter(_id, gun, task_queue, params_ready_event, interrupted_event, start_time):
    logger.info("Started shooter instance {} of worker {}".format(_id, mp.current_process().name))
    gun.setup()
    while not interrupted_event.is_set():
        if not params_ready_event.is_set():
            try:
                task = task_queue.get(True, timeout=1)
                if not task:
                    logger.info(
                        "Got poison pill. Exiting %s",
                        mp.current_process().name)
                    break
            except Empty:
                continue
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
    gun.teardown()


def run_worker(n_of_instances, task_queue, gun, params_ready_event, interrupted_event):
    logger.info("Started shooter worker: %s", mp.current_process().name)
    start_time = time.time()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    futures = [async_shooter(i, gun, task_queue, params_ready_event, interrupted_event, start_time) for i in range(n_of_instances)]
    gathered = asyncio.gather(*futures)
    try:
        loop.run_until_complete(gathered)
    finally:
        loop.close()


def feed_params(load_plan, params_queue, params_ready_event, interrupted_event, gun_name):
    for task in load_plan:
        if interrupted_event.is_set():
            break
        else:
            task = task._replace(bfg=gun_name)
            params_queue.put(task)
    params_ready_event.set()


class BFG(object):
    '''
    A BFG load generator that manages multiple workers as processes
    and feeds them with tasks
    '''
    def __init__(self, gun, load_plan, results, name, workers_num, instances_per_worker=4):
        self.name = name
        self.workers_n = workers_num
        self.gun = gun
        self.gun.results = results
        self.load_plan = load_plan
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
        manager = mp.Manager()
        self.quit = manager.Event()
        self.task_queue = manager.Queue(1024)
        self.params_ready_event = manager.Event()
        self.interrupted = manager.Event()
        n_of_instances = instances_per_worker
        self.workers = [
            mp.Process(target=run_worker,
                       args=(n_of_instances, self.task_queue, gun, self.params_ready_event, self.quit),
                       name="%s-%s" % (self.name, i))
            for i in range(0, self.workers_n)]
        self.p_feeder = mp.Process(target=feed_params, args=(self.load_plan,
                                                             self.task_queue,
                                                             self.params_ready_event,
                                                             self.interrupted,
                                                             self.name))
        self.workers_finished = False

    def start(self):
        self.start_time = time.time()
        self.p_feeder.start()
        for process in self.workers:
            process.start()

    def interrupt(self):
        self.interrupted.set()

    def wait_for_finish(self):
        self.p_feeder.join()
        [worker.join() for worker in self.workers]

    async def _wait(self):
        try:
            logger.info("%s is waiting for workers", self.name)
            while mp.active_children():
                logger.debug("Active children: %d", len(mp.active_children()))
                await asyncio.sleep(1)
            logger.info("All workers of %s have exited", self.name)
            self.workers_finished = True
        except (KeyboardInterrupt, SystemExit):
            self.task_queue.close()
            self.quit.set()
            while mp.active_children():
                logger.debug("Active children: %d", len(mp.active_children()))
                await asyncio.sleep(1)
            logger.info("All workers of %s have exited", self.name)
            self.workers_finished = True

    def running(self):
        '''
        True while there are alive workers out there. Tank
        will quit when this would become False
        '''
        #return not self.workers_finished
        return len(mp.active_children())

    def stop(self):
        '''
        Say the workers to finish their jobs and quit.
        '''
        self.quit.set()

    async def _feeder(self):
        '''
        A feeder coroutine
        '''
        for task in self.load_plan:
            task = task._replace(bfg=self.name)
            if self.quit.is_set():
                logger.info(
                    "%s observed quit flag and not going to feed anymore",
                    self.name)
                return
            # try putting a task to a queue unless there is a quit flag
            # or all workers have exited
            while True:
                try:
                    self.task_queue.put_nowait(task)
                    break
                except Full:
                    if self.quit.is_set() or self.workers_finished:
                        return
                    else:
                        await asyncio.sleep(1)
        workers_count = self.workers_n
        logger.info(
            "%s have feeded all data. Publishing %d poison pills",
            self.name, workers_count)
        while True:
            try:
                [self.task_queue.put_nowait(None) for _ in range(
                    0, workers_count)]
                break
            except Full:
                logger.warning(
                    "%s could not publish killer tasks."
                    "task queue is full. Retry in 1s", self.name)
                await asyncio.sleep(1)

    def _worker(self):
        '''
        A worker that runs in a distinct process
        '''

        # TODO: implement asyncio eventloop here
        loop = asyncio.SelectorEventLoop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.async_shooter())

    async def async_shooter(self):
        logger.info("Started shooter process: %s", mp.current_process().name)
        self.gun.setup()
        while not self.quit.is_set():
            try:
                task = self.task_queue.get(timeout=1)
                if not task:
                    logger.info(
                        "Got poison pill. Exiting %s",
                        mp.current_process().name)
                    break
                task = task._replace(ts=self.start_time + (task.ts / 1000.0))
                delay = task.ts - time.time()
                if delay > 0:
                    time.sleep(delay)
                self.gun.shoot(task)
            except (KeyboardInterrupt, SystemExit):
                break
            except Empty:
                if self.quit.is_set():
                    logger.debug(
                        "Empty queue and quit flag. Exiting %s",
                        mp.current_process().name)
                    break
        self.gun.teardown()


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
