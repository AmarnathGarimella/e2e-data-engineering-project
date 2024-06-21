from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import logging
import os

spark_jars =  ("{},{},{},{},{}".format(os.getcwd() + "/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar",  
                                      os.getcwd() + "/jars/kafka-clients-3.4.1.jar", 
                                      os.getcwd() + "/jars/spark-streaming-kafka-0-10-assembly_2.12-3.4.1.jar", 
                                      os.getcwd() + "/jars/commons-pool-1.5.4.jar",  
                                      os.getcwd() + "/jars/spark-token-provider-kafka-0-10_2.12-3.4.1.jar"))




def create_keyspace(session):
    # create keyspace
    session.execute(
        """
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
        """
    )

    logging.info("Keyspace created successfully")


def create_table(session):
    #create table

    session.execute("""
                        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
                            id UUID PRIMARY KEY,
                            first_name TEXT,
                            last_name TEXT,
                            gender TEXT,
                            address TEXT,
                            post_code TEXT,
                            dob TEXT,
                            email TEXT,
                            username TEXT,
                            password TEXT,
                            registered_date TEXT,
                            phone TEXT,
                            picture TEXT);
                    """)
    logging.info("Created table successfully")


def insert_data(session, **kwargs):
    # insert data into the table

    logging.info("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    dob = kwargs.get('dob')
    email = kwargs.get('email')
    username = kwargs.get('username')
    password = kwargs.get('password')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, dob, email, username, password, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, dob, email, username, password, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')


def create_spark_connection():
    # creating spark connection
    try:
        spark_conn = SparkSession.builder \
        .master("spark://localhost:7077") \
        .appName('SparkDataStreaming') \
        .config('spark.jars.packages', 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1," 
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                "org.apache.spark:spark-streaming-kafka-0-10-assembly_2.12:3.4.1,"
                "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.4.1,"
                "org.apache.commons:commons-pool2:2.11.1,"
                "org.apache.kafka:kafka-clients:3.4.1"
                ) \
        .config('spark.cassandra.connection.host','localhost') \
        .config("spark.sql.shuffle.partitions", 1) \
        .getOrCreate()

        spark_conn.sparkContext.setLogLevel("INFO")
        logging.info("Spark Connection Created Successfully!!!")
        return spark_conn
    except Exception as e :
        logging.error(f"Error Occurred when creating spark session: {e}")
        return None
    
    
        

def create_cassandra_connection():
    # creating cassandra connection
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        return session
    except Exception as e:
        logging.error(f"Unable to connect to cassandra {e}")
        return None
    

def connect_kafka(spark_session):
    spark_df = None
    try:
        spark_df = (spark_session.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'create_user') \
            .option('startingOffsets', 'earliest') \
            .load())
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("password", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


if __name__ == "__main__":
    spark_conn = create_spark_connection()


    if spark_conn is not None:
        df = connect_kafka(spark_conn)
        # data = [("John", 30), ("Doe", 25), ("Jane", 28)]
        # columns = ["Name", "Age"]
        # df = spark_conn.createDataFrame(data, columns)
        # df.show()
    #     cassandra_session = create_cassandra_connection()
    #     selected_df = create_selection_df_from_kafka(df)

    #     if cassandra_session is not None:
    #         create_keyspace(cassandra_session)
    #         create_table(cassandra_session)

    #         logging.info("Streaming is being started...")

    #         streaming_query = (selected_df.writeStream.format("org.apache.spark.sql.cassandra")
    #                            .option('checkpointLocation', '/tmp/checkpoint')
    #                            .option('keyspace', 'spark_streams')
    #                            .option('table', 'created_users')
    #                            .start())

    #         streaming_query.awaitTermination()



