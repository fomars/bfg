import asyncio
import logging
import multiprocessing as mp
import signal
from multiprocessing.managers import SyncManager
import time
from collections import namedtuple
from queue import Empty

from .module_exceptions import ConfigurationError
from .util import FactoryBase

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
        logger.info('Closing loop')
        loop.close()


def feed_params(load_plan, params_queue, params_ready_event, interrupted_event, gun_name):
    for task in load_plan:
        if interrupted_event.is_set():
            logger.info('Feeding interrupted')
            break
        else:
            task = task._replace(bfg=gun_name)
            params_queue.put(task)
    logger.info('Feeding concluded')
    params_ready_event.set()


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
        self.task_queue = self.manager.Queue(1024)
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

    async def wait_for_finish(self, timeout=1):
        while self.p_feeder.is_alive():
            await asyncio.sleep(timeout)
        else:
            self.p_feeder.join()
            logger.info('Feeder joined.')

        while any(worker.is_alive() for worker in self.workers):
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
        '''
        True while there are alive workers out there. Tank
        will quit when this would become False
        '''
        #return not self.workers_finished
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
