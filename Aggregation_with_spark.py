from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.appName("Spark-Transformation").getOrCreate()
"""Reading the click event data."""
clickDF = spark.read.parquet("/home/mithun/click_data.parquet")
clickDF.createOrReplaceTempView("table")

"""Since the column timestamp is in string format and we need to perform certain operations on it, the column is converted to timestamp format."""
df = spark.sql("select *, cast(timestamp as timestamp) as t1 from table")
df.createOrReplaceTempView("table")

"""Using a window function lead on column t1 to find the amount of time the user spent in each webpage."""
df = spark.sql("select *, lead(t1, 1, null) over (partition by user_id order by t1) as t2 from table")
df.createOrReplaceTempView("table")

"""Using in-built function datediff to find the time spent in seconds and later converting it to minutes."""
df = spark.sql("select *, round(datediff(second, t1, t2) / 60, 2) as minutes_spent from table")

"""Grouping the records by country and url, performing aggregation functions - count, countDistinct and average on
click_event_id, user_id, minutes_spent respectively to find click counts, unique users count and minutes spent."""
df = df.groupBy("country", "url").agg(count("click_event_id").alias("click_count"), avg("minutes_spent").alias("average_minutes_spent"), countDistinct("user_id").alias("unique_users_count"))

""""Loading the dataframe to elastic search server hosted on localhost:9200."""
df.write.format('org.elasticsearch.spark.sql').option('es.nodes', 'localhost').option('es.port', 9200).option('es.resource', '%s/%s' % ('test', 'data')).save()
