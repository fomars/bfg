'''
Data aggregation facilities.
'''
# coding=utf-8
import io
import json
import os
import queue
import multiprocessing as mp
from .module_exceptions import ConfigurationError
from .util import FactoryBase
from .guns.base import Sample
from .util import q_to_dict
import asyncio
import time
from dateutil import tz
import pandas as pd
import arrow
import logging


logger = logging.getLogger(__name__)


class ResultsSink(object):
    ''' Just collects samples, does not aggregate '''

    def __init__(self, event_loop):
        self.event_loop = event_loop
        self.results = {}
        self.results_queue = mp.Queue()
        self._stop = False
        self.stopped = False
        self.event_loop.create_task(self._reader())

    async def stop(self):
        '''
        Signal the reading coroutine to stop and wait for it
        '''
        self._stop = True
        while not self.stopped:
            await asyncio.sleep(1)

    async def _reader(self):
        '''
        Read from results queue asyncronously and put samples into
        results dict
        '''
        logger.info("Results reader started")
        while not self._stop:
            try:
                sample = self.results_queue.get_nowait()
                self.results.setdefault(sample.ts, []).append(sample)
            except queue.Empty:
                await asyncio.sleep(1)
        logger.info("Results reader stopped")
        self.stopped = True


class CachingAggregator(object):
    '''
    Caching aggregator that can also notify its listeners
    and write raw samples to a file. Listeners should have
    a publish(timestamp, aggregated_data) method
    '''

    def __init__(
            self, event_loop,
            cache_depth=10, listeners=None,
            raw_filename='result.samples'):
        self.raw_file = open(raw_filename, 'w')
        self.first_write = True
        self.cache_depth = cache_depth
        self.event_loop = event_loop
        self.results = {}
        self.aggregated_results = {}
        self.results_queue = mp.Queue()
        self._stop = False
        self.reader_stopped = False
        self.aggregator_stopped = False
        self.listeners = [] if listeners is None else listeners
        asyncio.ensure_future(self._reader())
        asyncio.ensure_future(self._aggregator())

    def stop(self):
        '''
        Set cache-depth to 0 in order to aggregate all the results in buffer.
        Aggregator will exit automatically when it observe that reader is
        stopped and the buffer is empty (so nothing will probably appear
        in the buffer)
        '''
        self.cache_depth = 0  # empty the cache
        self._stop = True

    async def _reader(self):
        '''
        Read everything from the queue until it empty, then sleep
        for half a second
        '''
        logger.info("Results reader started")
        while not self._stop:
            try:
                sample = self.results_queue.get_nowait()
                self.results.setdefault(sample.ts, []).append(sample)
            except queue.Empty:
                await asyncio.sleep(0.5)
        logger.info("Results reader stopped")
        self.reader_stopped = True

    async def _aggregator(self):
        '''
        Sleep before next aggregation is needed (aggregations performed
        once each second), grab the oldest data from the buffer maintaning
        its size, aggregate it and send results to listeners by calling publish

        The aggregate() function will also write raw samples to a file
        '''
        start_time = time.time()
        while not (self.reader_stopped and len(self.results) == 0):
            work_time = time.time() - start_time
            logger.debug("Last aggregation took %02d µs", work_time * 1000000)
            delay = 1 - work_time
            if delay > 0:
                await asyncio.sleep(delay)
            start_time = time.time()
            for _ in range(len(self.results) - self.cache_depth):
                smallest_key = min(self.results.keys())
                ts, aggr = self.aggregate(
                    smallest_key, self.results.pop(smallest_key))
                if aggr:
                    self.publish(ts, aggr)
        logger.info("Results aggregator stopped")
        self.aggregator_stopped = True

    def publish(self, ts, aggr):
        '''
        Send aggregated data to the listeners
        '''
        logger.debug("Publishing aggregated data for %s:\n%s", ts, aggr)
        self.aggregated_results[ts] = aggr
        [l.publish(ts, aggr) for l in self.listeners]

    def _stat_for_df(self, df):
        '''
        Collect stat for a dataframe
        '''
        return {
            "samples": len(df),
            "delay": {
                "avg": df.delay.mean(),
                "quantiles": q_to_dict(df.delay.quantile(
                    [0, .25, .5, .75, .9, .99, 1])),
            },
            "rt": {
                "avg": df.rt.mean(),
                "quantiles": q_to_dict(df.rt.quantile([0, .25, .5, .75, .8, .85, .9, .95, .98, .99, 1])),
                'min': int(df.rt.min()),
                'max': int(df.rt.max())
            }
        }

    def aggregate(self, ts, samples):
        '''
        Convert samples to dataframe, save raw samples to a file,
        compute some statistics and return aggregated data
        '''
        if ts in self.aggregated_results:
            logger.warning(
                "%s already aggregated. Some data points lost."
                "Try increasing aggregator cachesize")
            return ts, None
        df = pd.DataFrame(samples, columns=Sample._fields)
        df.to_csv(self.raw_file, sep='\t', index=False, header=self.first_write)
        self.first_write = False  # write headers only in the beginning

        aggr = {
            "rps": len(df),
            "overall": self._stat_for_df(df),
        }
        return ts, aggr


class LoggingListener(object):
    def publish(self, ts, data):
        rt_stats = data.get('overall').get('rt')
        logger.info(
            "{ts} {rps} RPS, mean RT: {rt_avg:.3f} ms, 99% < {rt_q99:.3f} ms".format(
                ts=arrow.get(ts).to(tz.gettz()).format('HH:mm:ss'),
                rps=data.get('rps'),
                rt_avg=rt_stats.get('avg') / 1000,
                rt_q99=rt_stats.get('quantiles').get(99) / 1000
            )
        )


class TADWriter(object):

    def __init__(self, testdir):
        # testdir=os.path.join(os.getcwd(), time.strftime("%Y%m%d-%H%M%S"))
        # os.mkdir(testdir)

        self.data_and_stats_stream = io.open(os.path.join(testdir, 'test_data.tad'), mode='w')

    def publish(self, ts, data):
        quantiles = data['overall']['rt']['quantiles']
        quantiles_keys = [50, 75, 80, 85, 90, 95, 98, 99, 100]
        quantiles_values = [quantiles[k] for k in quantiles_keys]

        tank_aggregated_data = {
            "tagged": {},
            "overall": {
                "size_in": {"max": 0, "total": 0, "len": 2, "min": 0},
                "latency": {"max": 0, "total": 0, "len": 2, "min": 0},
                "interval_real": {
                    "q": {
                        "q": quantiles_keys,
                        "value": quantiles_values
                    },
                    "min": data['overall']['rt']['min'],
                    "max": data['overall']['rt']['max'],
                    "len": 2,
                    "hist": {"data": [], "bins": []},
                    "total": 1598170
                },
                "interval_event": {"max": 0, "total": 0, "len": 2, "min": 0},
                "receive_time": {"max": 0, "total": 0, "len": 2, "min": 0},
                "connect_time": {"max": 0, "total": 0, "len": 2, "min": 0},
                "proto_code": {
                    "count": {
                        "200": 0
                    }
                },
                "size_out": {"max": 0, "total": 0, "len": 2, "min": 0},
                "send_time": {"max": 0, "total": 0, "len": 2, "min": 0},
                "net_code": {
                    "count": {
                        "0": 0
                    }
                }
            },
            "ts": ts
        }


        self.data_and_stats_stream.write(
            '%s\n' % json.dumps({
                'data': tank_aggregated_data,
                'stats': {"metrics": {
                    "instances": 1,
                    "reqps": data.get('rps')
                },
                "ts": ts}
            }))


class AggregatorFactory(FactoryBase):
    ''' Factory that produces aggregators '''

    FACTORY_NAME = "aggregator"

    def __init__(self, component_factory, testdir):
        super().__init__(component_factory)
        self.results = CachingAggregator(
            self.event_loop,
            listeners=[LoggingListener(), TADWriter(testdir)])

    def get(self, key):
        if key in self.factory_config:
            return self.results
        else:
            raise ConfigurationError(
                "Configuration for %s schedule not found" % key)
