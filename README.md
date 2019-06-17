# cloudwatch-metrics-client
Python CloudWatch metrics Client library utilising asyncio (along with sync variant)

This is yet another Python CloudWatch client. While there are alternatives, they are
either not supporting asyncio or not actively maintained.

https://github.com/awslabs/cloudwatch-fluent-metrics

https://github.com/peterdemin/awsme

The client collects whatever metrics you feed it with and reports to CloudWatch regularly, though
you can use it for ad-hoc reporting as well

NOTE: aioboto3 is not in the requirements, in case you only need sync version. If running under Python 3.6, you'll
need async_generator>=1.10 as well

Usage:

```python
import asyncio
from cloudwatch_metrics_client.aiocloudwatch import CloudWatchAsyncMetrics, CloudWatchAsyncMetricReporter

async def setup():
  
  reporter = CloudWatchAsyncMetricReporter(report_interval=REPORT INTERVAL)        # seconds
  CloudWatchAsyncMetrics.with_namespace('<YOUR NAMESPACE').with_reporter(reporter)
  # .with_client(aioboto3_cloudwatch_client) is you're not happy with default client
  await reporter.run()
  
  
@CloudWatchAsyncMetrics.monitored_task
async def process_request(request):
  
  # processing logic...
  #
  # request_type = ... 
  # request_specific_data = 
  
  # Set details on monitored call
  CloudWatchAsyncMetrics\
    .with_monitored_dimension('TypeOfRequest', request_type)\
    .with_monitored_dimension('SpecificAttribute', request_specific_data)
  
  # Put particular metric
  await CloudWatchAsyncMetrics.put_metric(
        MetricName='particular-metric-name',
        Unit='Count',
        Dimensions={'Kind': 'metric-kind', 'Sort': 'metric-sort'},
        Value=value
  )
  
  # Record statistical value
  await CloudWatchAsyncMetrics.put_statistic(
        MetricName='particular-metric-name',
        Unit='Count',
        Dimensions={'Kind': 'metric-kind', 'Sort': 'metric-sort'},
        Value=value
   
  # Elapsed time will be recorded  
```

Elapsed time of each call of `process_request` coroutine will be recorded in internal data structure and aggregated 
statistics will be regularly sent to CloudWatch.
Individual metrics could be collected with `put_metric` call - they too would be stored for a while and then sent
to CloudWatch. `put_metric` keeps each unique value and sends list of them along with number of occurrencies of each
value, `put_statistics` only sends aggregated data over a bunch of metrics - Sum, SampleCount, Min and Max
(See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudwatch.html#CloudWatch.Client.put_metric_data for details)

Reporter has async `flush` method for flushing metric data still not sent to CloudWatch. If regular reporting is not 
needed, just don't call `run` coro and instead call `flush` when it is time to report metrics.

Sync version works in the very same way, it utilises separate thread for reporting.

```python

from cloudwatch_metrics_client.cloudwatch import CloudWatchSyncMetrics, CloudWatchSyncMetricReporter

def setup():

  reporter = CloudWatchSyncMetricReporter(report_interval=REPORT INTERVAL)        # seconds
  CloudWatchSyncMetrics.with_namespace('<YOUR NAMESPACE').with_reporter(reporter)
  reporter.run()

@CloudWatchSyncMetrics.monitored_task  
def process_request(request):

# Same here, but with CloudWatchSync*


```  