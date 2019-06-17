import datetime
import functools
import logging
import math
import threading
from contextlib import contextmanager

import boto3

from cloudwatch_metrics_client.aiocloudwatch import CloudWatchAsyncMetrics, Metric, MetricSeries, StatisticSeries

log = logging.getLogger(__name__.split('.')[0])


class CloudWatchSyncMetrics(CloudWatchAsyncMetrics):

    @classmethod
    def setup_client(cls):
        cls.client = boto3.client('cloudwatch')
        return cls

    @classmethod
    def put_metric(cls, **metric_data):
        try:
            return cls.reporter.put_metric(**metric_data)
        except AttributeError:
            return False

    @classmethod
    def put_statistic(cls, name, dimensions, value, unit=None):
        try:
            return cls.reporter.put_statistic(name, dimensions, value, unit)
        except AttributeError:
            return False

    @classmethod
    def send_metric(cls, **metric_data):

        try:
            if metric_data.get('Timestamp') is None:
                metric_data['Timestamp'] = datetime.datetime.now()
            return cls.client.put_metric_data(
                Namespace=cls.namespace,
                MetricData=[{**metric_data}]
            )
        except AttributeError:
            return False

    @classmethod
    def monitored_task(cls, func, name='transaction'):

        @contextmanager
        def monitor():
            previous_dimensions = cls.dimensions
            cls.dimensions = None
            start = datetime.datetime.now()
            yield
            elapsed = datetime.datetime.now() - start
            cls.put_statistic(
                    name=name, dimensions=cls.dimensions, value=elapsed.microseconds, unit='Microseconds')
            cls.dimensions = previous_dimensions

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with monitor():
                return func(*args, **kwargs)
        return wrapper


class CloudWatchSyncMetricReporter:

    MAX_METRICS_PER_REPORT = 20

    def __init__(self, report_interval=30):

        self.metrics = {}
        self.statistics = {}
        self.report_interval = report_interval
        self.timer = None
        self.report_task = None

        self.lock = threading.Lock()

        self.stopped = False

    def run(self):
        if self.report_interval:
            self.report_task = threading.Thread(target=self.report)
            self.report_task.start()
            log.debug('Reporting metrics to CloudWatch every {} sec'.format(self.report_interval))

    def stop(self):
        if self.timer is not None:
            self.timer.set()
        self.stopped = True

    def put_metric(self, **metric_data):
        with self.lock:
            name = metric_data['MetricName']
            dimensions = metric_data.get('Dimensions')
            metric_id = Metric.generate_id(name, dimensions)
            metric = self.metrics.get(metric_id)
            if metric is None:
                self.metrics[metric_id] = MetricSeries(name=name, dimensions=dimensions, unit=metric_data.get('Unit'))
                metric = self.metrics[metric_id]
            metric.add_value(metric_data['Value'])

        return True

    def put_statistic(self, name, dimensions, value, unit=None):
        with self.lock:
            metric_id = Metric.generate_id(name, dimensions)
            stat = self.statistics.get(metric_id)
            if stat is None:
                self.statistics[metric_id] = StatisticSeries(name=name, dimensions=dimensions, unit=unit)
                stat = self.statistics[metric_id]
            stat.add_value(value)

        return True

    def report(self):
        while True:
            try:
                self.timer = threading.Event()
                self.stopped = self.timer.wait(self.report_interval)
                if self.stopped:
                    log.debug('reporter stopped')
                    return

                if CloudWatchSyncMetrics.client is None:
                    CloudWatchSyncMetrics.setup_client()
                    log.debug('Configured CloudWatch client')
                if len(self.metrics) + len(self.statistics) == 0:
                    log.debug('nothing to report')
                    continue
                self._report()
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

    def _report(self):
        with self.lock:
            num_metrics = len(self.metrics) + len(self.statistics)
            metric_data = self._calculate_metrics() + self._calculate_statistics()
            for n in range(math.ceil(len(metric_data) / CloudWatchSyncMetricReporter.MAX_METRICS_PER_REPORT)):
                response = CloudWatchSyncMetrics.client.put_metric_data(
                    Namespace=CloudWatchAsyncMetrics.namespace,
                    MetricData=metric_data[n * CloudWatchSyncMetricReporter.MAX_METRICS_PER_REPORT:
                                           (n + 1) * CloudWatchSyncMetricReporter.MAX_METRICS_PER_REPORT]
                )
                if response.get('ResponseMetadata', {}).get('HTTPStatusCode') != 200:
                    log.warning('Failed reporting metrics to CloudWatch; response={}'.format(
                        response
                    ))
            log.debug('Reported {} metrics to CloudWatch'.format(num_metrics))

    def flush(self):
        self._report()