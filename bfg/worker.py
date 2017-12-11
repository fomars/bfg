import time
import multiprocessing as mp
import threading as th
from queue import Empty, Full
from .util import FactoryBase
from .module_exceptions import ConfigurationError
from collections import namedtuple
import asyncio
import logging

logger = logging.getLogger(__name__)


Task = namedtuple(
    'Task', 'ts,bfg,marker,data')


def signal_handler(signum, frame):
    pass


async def async_shooter(_id, gun, task_queue, quit_event, start_time):
    logger.info("Started shooter process: %s", mp.current_process().name)
    gun.setup()
    while not quit_event.is_set():
        try:
            task = task_queue.get(timeout=1)
            if not task:
                logger.info(
                    "Got poison pill. Exiting %s",
                    mp.current_process().name)
                break
            task = task._replace(ts=start_time + (task.ts / 1000.0))
            delay = task.ts - time.time()
            if delay > 0:
                time.sleep(delay)
            await gun.shoot(task)
        except (KeyboardInterrupt, SystemExit):
            break
        except Empty:
            if quit_event.is_set():
                logger.debug(
                    "Empty queue and quit_event flag. Exiting %s",
                    mp.current_process().name)
                break
    gun.teardown()


def run_worker(n_of_instances, task_queue, gun, quit_event):
    # manager = mp.Manager()
    # params_queue = manager.Queue()
    # params = 'abcdefghijklmnoprstuvwxyz'
    # [params_queue.put(param) for param in params]
    # results_queue = manager.Queue()
    start_time = time.time()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    futures = [async_shooter(i, gun, task_queue, quit_event, start_time) for i in range(n_of_instances)]
    gathered = asyncio.gather(*futures)
    try:
        loop.run_until_complete(gathered)
    finally:
        loop.close()


class BFG(object):
    '''
    A BFG load generator that manages multiple workers as processes
    and feeds them with tasks
    '''
    def __init__(
            self, gun, load_plan, results, name, instances, event_loop):
        self.name = name
        self.instances = instances
        self.gun = gun
        self.gun.results = results
        self.load_plan = load_plan
        self.event_loop = event_loop
        logger.info(
            '''
Name: {name}
Instances: {instances}
Gun: {gun.__class__.__name__}
'''.format(
            name=self.name,
            instances=self.instances,
            gun=gun,
        ))
        manager = mp.Manager()
        self.quit = manager.Event()
        self.task_queue = manager.Queue(1024)
        n_of_instances = 20
        self.pool = [
            mp.Process(target=run_worker,
                       args=(n_of_instances, self.task_queue, gun, self.quit),
                       name="%s-%s" % (self.name, i))
            for i in range(0, self.instances)]
        self.workers_finished = False

    def start(self):
        self.start_time = time.time()
        for process in self.pool:
            process.start()
        self.event_loop.create_task(self._feeder())

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
        workers_count = self.instances
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
                instances=bfg_config.get('instances'),
                results=self.component_factory.get_factory(
                    'aggregator',
                    bfg_config.get('aggregator')).results_queue,
                event_loop=self.event_loop,
            )
        else:
            raise ConfigurationError(
                "Configuration for '%s' BFG not found" % bfg_name)
