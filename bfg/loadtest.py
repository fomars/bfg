''' Load test: entry point '''
import concurrent.futures

from .worker import BFG
from .config import ComponentFactory
import time
import numpy as np
import asyncio
import logging


logger = logging.getLogger(__name__)


class LoadTest(object):

    ''' Load test lifecycle '''

    def __init__(self, config):
        self.config = config
        self.event_loop = asyncio.get_event_loop()

    def __del__(self):
        self.event_loop.close()

    def run_test(self):
        executor = concurrent.futures.ProcessPoolExecutor(10)
        try:
            self.event_loop.run_until_complete(
                self._test(executor)
            )
        finally:
            self.event_loop.close()

    async def _test(self, executor):
        ''' Main coroutine. Manage components' lifecycle '''

        # Configure factories using config files
        logger.info("Configuring component factory")
        cf = ComponentFactory(self.config, self.event_loop)

        # Create workers using 'bfg' section from config
        logger.info("Creating workers")
        workers = [
            cf.get_factory('bfg', bfg_name)
            for bfg_name in cf.get_config('bfg')]

        # Start workers and wait for them asyncronously
        logger.info("Starting workers")
        # [worker.start() for worker in workers]
        loop = asyncio.get_event_loop()
        blocking_tasks = []
        for worker in workers:
            blocking_tasks += [loop.run_in_executor(executor, worker.execute, task) for task in worker.load_plan]
        completed, pending = await asyncio.wait(blocking_tasks)
        results = [t.result() for t in completed]
        print('results: {}'.format(results))
        # logger.info("Waiting for workers")
        # while any(worker.running() for worker in workers):
        #     await asyncio.sleep(1)
        logger.info("All workers finished")

        # Stop aggregator
        rs = cf.get_factory('aggregator', 'lunapark')
        await rs.stop()
