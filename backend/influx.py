import os

from influxdb import InfluxDBClient, SeriesHelper


dsn = os.environ.get('INFLUXDB_DSN', None)
if dsn:
    client = InfluxDBClient.from_DSN(dsn)
else:
    client = InfluxDBClient(
        os.environ.get('INFLUXDB_HOST'),
        int(os.environ.get('INFLUXDB_PORT', 8086)),
        os.environ.get('INFLUXDB_USERNAME', ''),
        os.environ.get('INFLUXDB_PASSWORD', ''),
        os.environ.get('INFLUXDB_DATABASE', ''),
    )


class TaskStats(SeriesHelper):
    class Meta:
        client = client

        series_name = 'celery_task'

        fields = [
            'avg_exec_in_millis',
            'avg_wait_in_millis',
            'count',
            'max_exec_in_millis',
            'max_wait_in_millis',
        ]

        tags = ['name', 'worker', 'state'] 

        bulk_size = 500
        autocommit = True



class QueueStats(SeriesHelper):
    class Meta:
        client = client

        series_name = 'celery_queue'

        fields = ['count']

        tags = ['queue']

        bulk_size = 100
        autocommit = True
