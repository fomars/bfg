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

        # Save a reference to the original signal handler for SIGINT.
        default_handler = signal.getsignal(signal.SIGINT)
        # Set signal handling of SIGINT to ignore mode.
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        # Start workers
        logger.info("Starting workers")
        [gun.start() for gun in guns]

        # restore signal handling for the parent process.
        def signal_handler(signal, frame):
            logger.info('Interrupting')
            [gun.interrupt() for gun in guns]

        signal.signal(signal.SIGINT, signal_handler)

        logger.info("Waiting for guns")
        [gun.wait_for_finish() for gun in guns]
        # while any(worker.running() for worker in workers):
        #     time.sleep(1)
        logger.info("All guns finished")

        # Stop aggregator
        rs = cf.get_factory('aggregator', 'lunapark')
        self.event_loop.run_until_complete(rs.stop())
