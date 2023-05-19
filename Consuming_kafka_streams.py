from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.appName("Spark-Kafka").getOrCreate()
"""Reading click data streams from kafka server topic stream."""
clickStreamJson = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("startingOffsets", "latest").option("subscribe", "stream").load()
"""The data under the column value are in binary format, so it is been casted to string format."""
clickStreamStringJson = clickStreamJson.selectExpr("CAST(value as STRING)")
"""Creating a schema to flatten out nested JSON string."""
schema = StructType([
    StructField("click_event_id", IntegerType(), False),
    StructField("click_data", StructType([
        StructField("user_id", StringType(), False),
        StructField("url", StringType(), False),
        StructField("timestamp", StringType(), False)
    ]), False),
    StructField("geo_data", StructType([
        StructField("city", StringType(), False),
        StructField("country", StringType(), False),
        StructField("ip_address", StringType(), False)
    ]), False),
    StructField("user_agent_data", StructType([
        StructField("device", StringType(), False),
        StructField("browser", StringType(), False)
    ]), False)
])
"""Imposing schema on the column value."""
clickStreamDF = clickStreamStringJson.select(from_json(col("value"), schema).alias("data")).select("data.*")
clickStreamDF = clickStreamDF.select("click_event_id", "click_data.user_id", "click_data.url", "click_data.timestamp", "geo_data.city", "geo_data.country", "geo_data.ip_address", "user_agent_data.device", "user_agent_data.browser")
"""Writing the read events as a parquet file."""
clickStreamDF.writeStream.format('parquet').option("path", f"/home/mithun/click_data.parquet").option("checkpointLocation", "/home/mithun/checkpoint_stream").outputMode("append").start().awaitTermination()
