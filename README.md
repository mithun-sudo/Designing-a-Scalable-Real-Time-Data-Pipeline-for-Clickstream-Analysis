# Designing-a-Scalable-Real-Time-Data-Pipeline-for-Clickstream-Analysis
This project implements a real-time data pipeline for clickstream analysis using Kafka, Spark, and Elasticsearch.

### Brief:
1. A python program [generating_click_streams.py](https://github.com/mithun-sudo/Designing-a-Scalable-Real-Time-Data-Pipeline-for-Clickstream-Analysis/blob/main/generating_click_streams.py) that generates click event streams.
2. Kafka server hosted on localhost:9092 acts as a buffer storage for the streaming data.
3. Spark streaming API [Consuming_kafka_streams.py](https://github.com/mithun-sudo/Designing-a-Scalable-Real-Time-Data-Pipeline-for-Clickstream-Analysis/blob/main/Consuming_kafka_streams.py) reads data from kafka server, flattens out nested json and the resulting data is written to a storage layer at granular level.
4. As per requirements, the transformations are performed on the data using Spark [Aggregation_with_spark.py](https://github.com/mithun-sudo/Designing-a-Scalable-Real-Time-Data-Pipeline-for-Clickstream-Analysis/blob/main/Aggregation_with_spark.py) and loaded to elasticsearch localhost:9200. 


### Scope for improvements:
1. If in future at production level the webpage attracts huge amount of tractions, then we can set up multiple kafka servers specific for each country and split up the traffic.
2. This project can be replicated in cloud services. EC2 instances or Azure VM can be used to host kafka and elasticsearch servers. AWS S3 or ADLS can be used as storage layer. A trigger can be scheduled in such a way that spark applications can read the data file at a particular time interval, perform aggregations and load that into elastic search.
3. At production level we will be getting data throughout the year, at that time we can partition the streaming data by year, month, day and store it in storage layer. Reading the data and performing transformation can be done efficiently if partitioned.
4. We can also setup multiple kafka servers and increase the replication factor of the topic. By doing so we improve the fault tolerance of our data pipeline.

