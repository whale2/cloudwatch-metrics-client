import asyncio
import datetime
import functools
import logging
import math
from collections import OrderedDict
from contextlib import contextmanager, asynccontextmanager
from typing import Union

import aioboto3

log = logging.getLogger(__name__.split('.')[0])


class CloudWatchAsyncMetrics:

    dimensions = None
    client = None
    namespace = None
    reporter = None

    @classmethod
    def with_namespace(cls, namespace):
        cls.namespace = namespace
        return cls

    @classmethod
    def with_client(cls, client):
        cls.client = client
        return cls

    @classmethod
    def setup_client(cls):
        if cls.client is None:
            cls.client = aioboto3.client('cloudwatch')
        return cls

    @classmethod
    def with_reporter(cls, reporter):
        cls.reporter = reporter
        return cls

    @classmethod
    async def put_metric(cls, **metric_data):
        try:
            return await cls.reporter.put_metric(**metric_data)
        except AttributeError:
            return False

    @classmethod
    async def put_statistic(cls, name, dimensions, value, unit=None):
        try:
            return await cls.reporter.put_statistic(name, dimensions, value, unit)
        except AttributeError:
            return False

    @classmethod
    async def send_metric(cls, **metric_data):

        try:
            if metric_data.get('Timestamp') is None:
                metric_data['Timestamp'] = datetime.datetime.now()
            cls.setup_client()
            return await cls.client.put_metric_data(
                Namespace=cls.namespace,
                MetricData=[{**metric_data}]
            )
        except AttributeError:
            return False

    @classmethod
    def with_monitored_dimension(cls, dimension, value):
        if cls.dimensions is None:
            cls.dimensions = {}
        cls.dimensions[dimension] = value
        return cls

    @classmethod
    def monitored_task(cls, func, name='transaction'):

        @contextmanager
        def monitor():
            previous_dimensions = cls.dimensions
            cls.dimensions = None
            start = datetime.datetime.now()
            yield
            elapsed = datetime.datetime.now() - start
            asyncio.run_coroutine_threadsafe(
                cls.put_statistic(
                    name=name, dimensions=cls.dimensions, value=elapsed.microseconds, unit='Microseconds'),
                asyncio.get_event_loop()
            )
            cls.dimensions = previous_dimensions

        @asynccontextmanager
        async def async_monitor():
            previous_dimensions = cls.dimensions
            cls.dimensions = None
            start = datetime.datetime.now()
            yield
            elapsed = datetime.datetime.now() - start
            await cls.put_statistic(
                name=name, dimensions=cls.dimensions, value=elapsed.microseconds, unit='Microseconds')
            cls.dimensions = previous_dimensions

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not asyncio.iscoroutinefunction(func):
                with monitor():
                    return func(*args, **kwargs)
            else:
                async def tmp():
                    async with async_monitor():
                        return await func(*args, **kwargs)

                return tmp()

        return wrapper


class CloudWatchAsyncMetricReporter:

    MAX_METRICS_PER_REPORT = 20

    def __init__(self, report_interval=30):

        self.metrics = {}
        self.statistics = {}
        self.report_interval = report_interval
        self.sleep_task = None
        self.report_task = None

        self.lock = asyncio.Lock()

        self.stopped = False

    async def run(self):
        if self.report_interval:
            self.report_task = asyncio.create_task(self.report())
            log.debug('Reporting metrics to CloudWatch every {} sec'.format(self.report_interval))

    def stop(self):
        if self.sleep_task is not None:
            self.sleep_task.cancel()
        self.stopped = True

    async def put_metric(self, **metric_data):
        async with self.lock:
            name = metric_data['MetricName']
            dimensions = metric_data.get('Dimensions')
            metric_id = Metric.generate_id(name, dimensions)
            metric = self.metrics.get(metric_id)
            if metric is None:
                self.metrics[metric_id] = MetricSeries(name=name, dimensions=dimensions, unit=metric_data.get('Unit'))
                metric = self.metrics[metric_id]
            metric.add_value(metric_data['Value'])

        return True

    async def put_statistic(self, name, dimensions, value, unit=None):
        async with self.lock:
            metric_id = Metric.generate_id(name, dimensions)
            stat = self.statistics.get(metric_id)
            if stat is None:
                self.statistics[metric_id] = StatisticSeries(name=name, dimensions=dimensions, unit=unit)
                stat = self.statistics[metric_id]
            stat.add_value(value)

        return True

    async def report(self):

        while True:
            try:
                if self.stopped:
                    log.debug('reporter stopped')
                    return
                self.sleep_task = asyncio.create_task(asyncio.sleep(self.report_interval))
                try:
                    await self.sleep_task
                except asyncio.CancelledError:
                    log.debug('sleep cancelled; reporter stopped')
                    return
                if len(self.metrics) + len(self.statistics) == 0:
                    log.debug('nothing to report')
                    continue
                await self._report()
            except Exception as e:
                log.error(e)

    def _calculate_statistics(self) -> []:
        statistics = [stat.to_repr() for name, stat in self.statistics.items()]
        self.statistics = {}
        return list(filter(None.__ne__, statistics))

    def _calculate_metrics(self) -> []:
        metrics = [data.to_repr() for name, data in self.metrics.items()]
        self.metrics = {}
        return list(filter(None.__ne__, metrics))

    async def _report(self):
        CloudWatchAsyncMetrics.setup_client()
        async with self.lock:
            num_metrics = len(self.metrics) + len(self.statistics)
            metric_data = self._calculate_metrics() + self._calculate_statistics()
            for n in range(math.ceil(len(metric_data) / CloudWatchAsyncMetricReporter.MAX_METRICS_PER_REPORT)):
                response = await CloudWatchAsyncMetrics.client.put_metric_data(
                    Namespace=CloudWatchAsyncMetrics.namespace,
                    MetricData=metric_data[n * CloudWatchAsyncMetricReporter.MAX_METRICS_PER_REPORT:
                                           (n + 1) * CloudWatchAsyncMetricReporter.MAX_METRICS_PER_REPORT]
                )
                if response.get('ResponseMetadata', {}).get('HTTPStatusCode') != 200:
                    log.warning('Failed reporting metrics to CloudWatch; response={}'.format(
                        response
                    ))
            log.debug('Reported {} metrics to CloudWatch'.format(num_metrics))

    async def flush(self):
        await self._report()


class MetricDimension:

    def __init__(self, dimensions):

        self.dimensions = OrderedDict(dimensions) if dimensions is not None else None
        self.dim_id = MetricDimension.generate_id(self.dimensions)

    def to_repr(self) -> Union[list, None]:
        if self.dimensions is None:
            return None
        return [{'Name': name, 'Value': value} for name, value in self.dimensions.items()]

    @staticmethod
    def generate_id(dimensions) -> str:
        if dimensions is not None:
            return '&'.join(['%s=%s' % (a, b) for a, b in dimensions.items()])
        else:
            return ''


class Metric:

    def __init__(self, name, dimensions, value, unit=None):

        self.name = name
        self.dimensions = MetricDimension(dimensions)
        self.value = value
        self.unit = unit

        self.metric_id = Metric.generate_id(self.name, self.dimensions)

    def to_repr(self) -> dict:
        data = {
            'MetricName': self.name,
            'Timestamp': datetime.datetime.now(),
            'Value': self.value
        }
        dimensions = self.dimensions.to_repr()
        if dimensions is not None:
            data['Dimensions'] = dimensions
        if self.unit is not None:
            data['Unit'] = self.unit
        return data

    @staticmethod
    def generate_id(name: str, dimensions) -> str:
        if isinstance(dimensions, MetricDimension):
            return '%s?%s' % (name, dimensions.dim_id)
        else:
            return '%s?%s' % (name, MetricDimension.generate_id(dimensions))


class MetricSeries:

    def __init__(self, name, dimensions=None, unit=None):

        self.metric = Metric(name=name, dimensions=dimensions, value={}, unit=unit)
        self.metric_id = self.metric.metric_id

    def add_value(self, value) -> None:
        values = self.metric.value
        count = values.get(value, 0) + 1
        values[value] = count

    def to_repr(self) -> Union[dict, None]:
        data = self.metric.to_repr()
        del data['Value']
        data['Values'] = []
        data['Counts'] = []
        for value, count in self.metric.value.items():
            data['Values'].append(value)
            data['Counts'].append(count)

        return data


class StatisticSeries(MetricSeries):

    def __init__(self, name, dimensions=None, unit=None):

        super().__init__(name=name, dimensions=dimensions, unit=unit)
        self.metric.value = []

    def to_repr(self) -> Union[dict, None]:
        if len(self.metric.value) == 0:
            return None
        data = self.metric.to_repr()
        del data['Value']
        data['StatisticValues'] = {
            'SampleCount': len(self.metric.value),
            'Sum': sum(self.metric.value),
            'Minimum': min(self.metric.value),
            'Maximum': max(self.metric.value)
        }

        return data

    def add_value(self, value) -> None:
        self.metric.value.append(value)
