Docker image to track celery tasks directly into InfluxDB

Required environment variables:
* INFLUXDB_DSN - dns for influxdb (`influxdb://<username>:<password>@<host>:<port>/<db>`). If
  mising, will use next ones:
* INFLUXDB_HOST
* INFLUXDB_PORT
* INFLUXDB_USERNAME
* INFLUXDB_PASSWORD
* INFLUXDB_DATABASE
* CELERY_BROKER_URL - Broker url as expected by selery. You want it to be the same as your main app
* FREQUENCY - frequency which app is taking snapshots of celery state

Next series are created:
* celery_tasks:
  Tags:
  * name
  * worker
  * state

  Fields:
  * avg_exec_in_millis
  * avg_wait_in_millis
  * count
  * max_exec_in_millis
  * max_wait_in_millis
* celery_queue:
  Tags:
  * name

  Fields:
  * count
