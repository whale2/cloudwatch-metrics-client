import datetime
import time

import boto3
from unittest import TestCase
from mock import MagicMock

from src.cloudwatch_metrics_client.cloudwatch import CloudWatchSyncMetrics, CloudWatchSyncMetricReporter


class TestCloudwatch(TestCase):

    def setUp(self) -> None:

        boto3.client = MagicMock()

        self.reporter = CloudWatchSyncMetricReporter(report_interval=None)

        CloudWatchSyncMetrics.with_namespace('test_namespace').with_reporter(self.reporter)
        CloudWatchSyncMetrics.setup_client()

    def test_recording_metric(self):

        CloudWatchSyncMetrics.put_metric(MetricName='test_metric', Value=100)

        metrics = list(self.reporter.metrics.values())[0].to_repr()

        self.assertEqual('test_metric', metrics['MetricName'])
        self.assertEqual(100, metrics['Values'][0])
        self.assertEqual(1, metrics['Counts'][0])
        self.assertIsNone(metrics.get('Dimensions'))
        self.assertIsInstance(metrics['Timestamp'], datetime.datetime)

    def test_sync_decorator(self):

        @CloudWatchSyncMetrics.monitored_task
        def task():
            time.sleep(0.1)

        task()
        metrics = self.reporter.statistics
        repr = metrics['transaction?'].to_repr()
        self.assertEqual('transaction', repr['MetricName'])
        self.assertIsNone(repr.get('Dimensions'))
        self.assertEqual(1, repr['StatisticValues']['SampleCount'])
        self.assertLess(100000, repr['StatisticValues']['Sum'])
        self.assertGreater(150000, repr['StatisticValues']['Sum'])
        self.assertEqual('Microseconds', repr['Unit'])

    def test_sync_decorator_with_dimensions(self):

        def test():
            task()
            metrics = list(self.reporter.statistics.values())[0].to_repr()
            self.assertEqual('transaction', metrics['MetricName'])
            self.assertDictEqual({'Name': 'Test_Dimension', 'Value': 'ValueX'}, metrics['Dimensions'][0])
            self.assertEqual(1, metrics['StatisticValues']['SampleCount'])
            self.assertLess(100000, metrics['StatisticValues']['Sum'])
            self.assertGreater(150000, metrics['StatisticValues']['Sum'])
            self.assertEqual('Microseconds', metrics['Unit'])

            task2()
            metrics = list(self.reporter.statistics.values())[1].to_repr()
            self.assertEqual('transaction', metrics['MetricName'])
            self.assertDictEqual({'Name': 'Test_Dimension', 'Value': 'ValueY'}, metrics['Dimensions'][0])
            self.assertEqual(1, metrics['StatisticValues']['SampleCount'])
            self.assertLess(100000, metrics['StatisticValues']['Sum'])
            self.assertGreater(150000, metrics['StatisticValues']['Sum'])
            self.assertEqual('Microseconds', metrics['Unit'])

        @CloudWatchSyncMetrics.monitored_task
        def task():
            CloudWatchSyncMetrics.with_monitored_dimension('Test_Dimension', 'ValueX')
            time.sleep(0.1)

        @CloudWatchSyncMetrics.monitored_task
        def task2():
            CloudWatchSyncMetrics.with_monitored_dimension('Test_Dimension', 'ValueY')
            time.sleep(0.1)

        test()
        metrics = list(self.reporter.statistics.values())
        self.assertEqual(2, len(metrics))

    def test_reporter(self):

        self.stored_kwargs = None

        def put_data(**kwargs):
            self.stored_kwargs = kwargs
            return {'ResponseMetadata': {'HTTPStatusCode': 200}}

        CloudWatchSyncMetrics.client = MagicMock()
        CloudWatchSyncMetrics.client.put_metric_data = put_data

        def test():
            reporter = CloudWatchSyncMetricReporter(report_interval=0.5)
            CloudWatchSyncMetrics.with_reporter(reporter)
            reporter.run()
            CloudWatchSyncMetrics.put_metric(MetricName='test_metric_2', Value=50)
            self.assertIsNone(self.stored_kwargs)
            time.sleep(0.7)
            reporter.stop()
            self.assertEqual('test_metric_2', self.stored_kwargs['MetricData'][0]['MetricName'])
            self.assertEqual(50, self.stored_kwargs['MetricData'][0]['Values'][0])
            self.assertEqual(1, self.stored_kwargs['MetricData'][0]['Counts'][0])

        test()
