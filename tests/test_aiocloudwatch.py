import asyncio
import datetime
from time import sleep

import aioboto3
from unittest import TestCase
from mock import MagicMock

from src.cloudwatch_metrics_client.aiocloudwatch import CloudWatchAsyncMetrics, CloudWatchAsyncMetricReporter, MetricDimension, Metric, \
    MetricSeries, StatisticSeries


class TestAsyncCloudWatchReporter(TestCase):

    def setUp(self) -> None:
        self.dimensions = {'Dimension1': 'Test1', 'Dimension2': 'Test2'}

    def test_metric_dimension(self):

        dim = MetricDimension({'Dimension': 'Test'})
        self.assertListEqual([{'Name': 'Dimension', 'Value':'Test'}], dim.to_repr())
        self.assertEqual('Dimension=Test', dim.dim_id)

        dim = MetricDimension(dimensions=None)
        self.assertIsNone(dim.to_repr())
        self.assertEqual('', dim.dim_id)

        dim = MetricDimension(self.dimensions)
        self.assertListEqual([{'Name': 'Dimension1', 'Value': 'Test1'}, {'Name': 'Dimension2', 'Value': 'Test2'}],
                             dim.to_repr())
        self.assertEqual('Dimension1=Test1&Dimension2=Test2', dim.dim_id)

    def test_metric(self):

        metric = Metric(name='Metric0', dimensions=self.dimensions, value=15, unit='furlong')
        repr = metric.to_repr()
        self.assertEqual('Metric0', repr['MetricName'])
        self.assertEqual(15, repr['Value'])
        self.assertEqual(2, len(repr['Dimensions']))
        self.assertEqual('furlong', repr['Unit'])
        self.assertEqual('Metric0?Dimension1=Test1&Dimension2=Test2', metric.metric_id)

    def test_metric_series(self):

        metric_series = MetricSeries(name='Series0', dimensions=self.dimensions)
        for n in range(5):
            metric_series.add_value(n)
        metric_series.add_value(2)
        metric_series.add_value(10)
        metric_series.add_value(2)
        repr = metric_series.to_repr()
        self.assertEqual('Series0',repr['MetricName'])
        self.assertEqual(2, len(repr['Dimensions']))
        self.assertListEqual([0, 1, 2, 3, 4, 10], repr['Values'])
        self.assertListEqual([1, 1, 3, 1, 1, 1], repr['Counts'])

    def test_statistic_series(self):

        statistic_series = StatisticSeries(name='Series0', dimensions=self.dimensions)
        for n in range(5):
            statistic_series.add_value(n)
        statistic_series.add_value(2)
        statistic_series.add_value(10)
        statistic_series.add_value(2)
        repr = statistic_series.to_repr()
        self.assertEqual('Series0',repr['MetricName'])
        self.assertEqual(2, len(repr['Dimensions']))
        self.assertEqual(8, repr['StatisticValues']['SampleCount'])
        self.assertEqual(24, repr['StatisticValues']['Sum'])
        self.assertEqual(0, repr['StatisticValues']['Minimum'])
        self.assertEqual(10, repr['StatisticValues']['Maximum'])


class TestCloudwatch(TestCase):

    def setUp(self) -> None:

        aioboto3.client = MagicMock()

        self.reporter = CloudWatchAsyncMetricReporter(report_interval=None)

        CloudWatchAsyncMetrics.with_namespace('test_namespace').with_reporter(self.reporter)
        CloudWatchAsyncMetrics.setup_client()

    def test_recording_metric(self):

        async def test():
            await CloudWatchAsyncMetrics.put_metric(MetricName='test_metric', Value=100)

            metrics = list(self.reporter.metrics.values())[0].to_repr()

            self.assertEqual('test_metric', metrics['MetricName'])
            self.assertEqual(100, metrics['Values'][0])
            self.assertEqual(1, metrics['Counts'][0])
            self.assertIsNone(metrics.get('Dimensions'))
            self.assertIsInstance(metrics['Timestamp'], datetime.datetime)

        asyncio.get_event_loop().run_until_complete(test())

    def test_async_decorator(self):

        async def test():
            await task()

        @CloudWatchAsyncMetrics.monitored_task
        async def task():
            await asyncio.sleep(0.1)

        asyncio.get_event_loop().run_until_complete(test())
        metrics = self.reporter.statistics
        repr = metrics['transaction?'].to_repr()
        self.assertEqual('transaction', repr['MetricName'])
        self.assertIsNone(repr.get('Dimensions'))
        self.assertEqual(1, repr['StatisticValues']['SampleCount'])
        self.assertLess(100000, repr['StatisticValues']['Sum'])
        self.assertGreater(150000, repr['StatisticValues']['Sum'])
        self.assertEqual('Microseconds', repr['Unit'])

    def test_async_decorator_with_dimensions(self):

        async def test():
            await task()
            metrics = list(self.reporter.statistics.values())[0].to_repr()
            self.assertEqual('transaction', metrics['MetricName'])
            self.assertDictEqual({'Name': 'Test_Dimension', 'Value': 'ValueX'}, metrics['Dimensions'][0])
            self.assertEqual(1, metrics['StatisticValues']['SampleCount'])
            self.assertLess(100000, metrics['StatisticValues']['Sum'])
            self.assertGreater(150000, metrics['StatisticValues']['Sum'])
            self.assertEqual('Microseconds', metrics['Unit'])

            await task2()
            metrics = list(self.reporter.statistics.values())[1].to_repr()
            self.assertEqual('transaction', metrics['MetricName'])
            self.assertDictEqual({'Name': 'Test_Dimension', 'Value': 'ValueY'}, metrics['Dimensions'][0])
            self.assertEqual(1, metrics['StatisticValues']['SampleCount'])
            self.assertLess(100000, metrics['StatisticValues']['Sum'])
            self.assertGreater(150000, metrics['StatisticValues']['Sum'])
            self.assertEqual('Microseconds', metrics['Unit'])

        @CloudWatchAsyncMetrics.monitored_task
        async def task():
            CloudWatchAsyncMetrics.with_monitored_dimension('Test_Dimension', 'ValueX')
            await asyncio.sleep(0.1)

        @CloudWatchAsyncMetrics.monitored_task
        async def task2():
            CloudWatchAsyncMetrics.with_monitored_dimension('Test_Dimension', 'ValueY')
            await asyncio.sleep(0.1)

        asyncio.get_event_loop().run_until_complete(test())
        metrics = list(self.reporter.statistics.values())
        self.assertEqual(2, len(metrics))


    def test_sync_decorator_in_coro(self):

        @CloudWatchAsyncMetrics.monitored_task
        def task():
            sleep(0.15)

        async def test():
            task()
            await asyncio.sleep(0.1)

        asyncio.get_event_loop().run_until_complete(test())

        metrics = list(self.reporter.statistics.values())[0].to_repr()
        self.assertEqual('transaction', metrics['MetricName'])
        self.assertIsNone(metrics.get('Dimensions'))
        self.assertEqual(1, metrics['StatisticValues']['SampleCount'])
        self.assertLess(150000, metrics['StatisticValues']['Sum'])
        self.assertGreater(200000, metrics['StatisticValues']['Sum'])

    def test_reporter(self):

        self.stored_kwargs = None

        async def put_data(**kwargs):
            self.stored_kwargs = kwargs
            return {'ResponseMetadata': { 'HTTPStatusCode': 200 }}

        CloudWatchAsyncMetrics.client = MagicMock()
        CloudWatchAsyncMetrics.client.put_metric_data = put_data

        async def test():
            reporter = CloudWatchAsyncMetricReporter(report_interval=0.5)
            CloudWatchAsyncMetrics.with_reporter(reporter)
            await reporter.run()
            await CloudWatchAsyncMetrics.put_metric(MetricName='test_metric_2', Value=50)
            self.assertIsNone(self.stored_kwargs)
            await asyncio.sleep(0.7)
            self.assertEqual('test_metric_2', self.stored_kwargs['MetricData'][0]['MetricName'])
            self.assertEqual(50, self.stored_kwargs['MetricData'][0]['Values'][0])
            self.assertEqual(1, self.stored_kwargs['MetricData'][0]['Counts'][0])

        asyncio.get_event_loop().run_until_complete(test())
