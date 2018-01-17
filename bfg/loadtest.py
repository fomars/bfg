''' Load test: entry point '''
import signal

from .worker import BFG
from .config import ComponentFactory
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
        self._test()

    def _test(self):
        ''' Main coroutine. Manage components' lifecycle '''

        # Configure factories using config files
        logger.info("Configuring component factory")
        cf = ComponentFactory(self.config, self.event_loop)

        # Create workers using 'bfg' section from config
        logger.info("Creating workers")
        guns = [
            cf.get_factory('bfg', bfg_name)
            for bfg_name in cf.get_config('bfg')]

        # Set signal handling of SIGINT to ignore mode.
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        # Start workers
        logger.info("Starting workers")
        [gun.start() for gun in guns]

        # Restore signal handling for the parent process.
        def signal_handler(signal, frame):
            logger.info('Interrupting')
            [gun.interrupt() for gun in guns]

        signal.signal(signal.SIGINT, signal_handler)

        # Start aggregator
        aggregator = cf.get_factory('aggregator', 'lunapark')

        logger.info("Waiting for guns")

        # Block until guns terminate
        futures = [asyncio.ensure_future(gun.wait_for_finish()) for gun in guns]
        self.event_loop.run_until_complete(asyncio.gather(*futures))
        logger.info('All Guns finished')
        # Send aggregator stop signal
        aggregator.stop()
        # Block until remaining aggregator tasks finished
        pending = asyncio.Task.all_tasks()
        self.event_loop.run_until_complete(asyncio.gather(*pending))