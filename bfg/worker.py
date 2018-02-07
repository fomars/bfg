import asyncio
import logging
import multiprocessing as mp
import signal
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


async def shoot_with_delay(task, gun, start_time):
    task = task._replace(ts=start_time + (task.ts / 1000.0))
    delay = task.ts - asyncio.get_event_loop().time()
    if delay > 0:
        start = time.time()
        await asyncio.sleep(delay)
    if delay < -0.2:
        logger.warning('Delay: {}'.format(delay))
    await gun.shoot(task)


async def async_shooter(_id, gun, task_queue, params_ready_event, interrupted_event, start_time, session):
    """

    :type task_queue: mp.Queue
    """
    # logger.info("Started shooter instance {} of worker {}".format(_id, mp.current_process().name))

    gun.setup(session)
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

#  todo: experiment later with Loop.call_at()
# async def scheduler(gun, task_queue, params_ready_event, interrupted_event, start_time, event_loop):
#     """
#
#     :type task_queue: mp.Queue
#     """
#     logger.info("Started scheduler {}".format(mp.current_process().name))
#
#     tasks_pack_size = 2000
#     tasks = []
#
#     async with aiohttp.ClientSession(loop=event_loop) as session:
#         gun.setup(session)
#         while not interrupted_event.is_set():
#             # Read with retry while params are feeding
#             if not params_ready_event.is_set():
#                 try:
#                     task = task_queue.get(True, timeout=1)
#                     if not task:
#                         logger.info(
#                             "Got poison pill. Exiting %s",
#                             mp.current_process().name)
#                         break
#                 except Empty:
#                     continue
#             # Once all params are fed, we can do get_nowait() and terminate when Empty is raised
#             else:
#                 try:
#                     task = task_queue.get_nowait()
#                     if not task:
#                         logger.info(
#                             "Got poison pill. Exiting %s",
#                             mp.current_process().name)
#                         break
#                 except Empty:
#                     break
#             # actually schedule task
#             tasks = [task for task in tasks if not task.done()] +\
#                 [asyncio.ensure_future(shoot_with_delay(task, gun, start_time))]
#             # wait if there are too many
#             while len(tasks) > tasks_pack_size:
#                 await asyncio.sleep(0.1)
#                 tasks = [task for task in tasks if not task.done()]
#         else:
#             # clear queue if test was interrupted
#             while True:  # can't check with queue.empty() because of multiprocessing
#                 try:
#                     task_queue.get_nowait()
#                 except Empty:
#                     break
#         await asyncio.wait(tasks)
#     gun.teardown()


async def run_with_aiohttp_session(event_loop, gun, task_queue, params_ready_event, interrupted_event, start_time,
                                   n_of_instances):
    connector = aiohttp.TCPConnector()  # (limit=config.concurrency)
    async with aiohttp.ClientSession(loop=event_loop, connector=connector) as session:
        futures = [asyncio.ensure_future(async_shooter(i, gun, task_queue,
                                                       params_ready_event,
                                                       interrupted_event,
                                                       start_time, session)) for i in range(n_of_instances)]
        await asyncio.wait(futures)


def run_worker(n_of_instances, task_queue, gun, params_ready_event, interrupted_event):
    logger.info("Started shooter worker: %s", mp.current_process().name)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    start_time = loop.time()

    # futures = [async_shooter(i, gun, task_queue, params_ready_event, interrupted_event, start_time) for i in range(n_of_instances)]
    # gathered = asyncio.gather(*futures)
    try:
        loop.run_until_complete(run_with_aiohttp_session(loop, gun, task_queue, params_ready_event,
                                                         interrupted_event, start_time, n_of_instances))
    finally:
        logger.info('Closing worker %s', mp.current_process().name)
        loop.close()


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


def mgr_init():
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    logger.info('initialized manager')


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
        self.manager = SyncManager()
        self.manager.start(mgr_init)
        self.task_queue = self.manager.Queue(4096)
        self.params_ready_event = self.manager.Event()
        self.interrupted = self.manager.Event()
        n_of_instances = instances_per_worker
        self.workers = [
            mp.Process(target=run_worker,
                       args=(n_of_instances, self.task_queue, gun, self.params_ready_event, self.interrupted),
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

        self.manager.shutdown()

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
