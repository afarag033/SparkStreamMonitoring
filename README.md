# Spark Stream Monitoring
This package can be used as a SparkStreamingListener implementation to write logs about streaming query events so, these logs can be analyzed and used for monitoring and alerting purposes

## Requirements
pyspark `3.4.0` or later

## How to setup
* Clone the github repo
* Change location to the repo directory
* run `pip3 install .`

## How to use

Import stream monitoring

```
from spark_stream_monitoring import SparkStreamMonitoring
```
Initialize and add the monitoring object as a listener to your spark session streams 

```
spark_stream_monitoring = SparkStreamMonitoring()
spark.streams.addListener(spark_stream_monitoring)
```