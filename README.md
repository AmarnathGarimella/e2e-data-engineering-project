# e2e-data-engineering-project

In this Project data is being read periodically from API of `randomuser.me` using Apache Airflow.

The data is then produced into Apache Kafka. After that using Apache Spark Streaming we consume the data from Kafka.

Finally we will persist the data in Apache Cassandra.

# Requirements
Python3, Java8, Docker, Apache Spark3.2.0 Downloaded in local.
