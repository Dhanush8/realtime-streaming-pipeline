import uuid
import time
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, length
from pyspark.sql.types import StructType, StructField, StringType


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_stream
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace Created Successfully.")


def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_stream.users_created (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            postcode TEXT,
            email TEXT,
            username TEXT,
            dob TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT
        );
    """)
    print("Table Created Successfully.")


def wait_for_cassandra(session, retries=10, delay=5):
    for i in range(retries):
        try:
            session.execute("SELECT now() FROM system.local")
            print("Cassandra is ready.")
            return True
        except Exception as e:
            print(f"Waiting for Cassandra to be ready... ({i+1}/{retries})")
            time.sleep(delay)
    raise Exception("Cassandra did not respond in time.")


def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        print("Connected to Cassandra.")
        wait_for_cassandra(session)
        return session
    except Exception as e:
        print(f"Failed to Connect to Cassandra: {e}")
        return None


def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages',
                    "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoint") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        print("Spark Session Created Successfully.")
        return spark
    except Exception as e:
        print(f"Failed to Create Spark Session: {e}")
        return None


def connect_to_kafka(spark):
    try:
        df = spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:29092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .option("kafka.request.timeout.ms", "60000") \
            .load()
        print("Connected to Kafka stream.")
        return df
    except Exception as e:
        print(f"Failed to Connect to Kafka: {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("postcode", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    df = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .filter(col("id").isNotNull()) \
        .filter(length("id") == 36)

    print("Schema Applied and Valid UUID Filtering Completed.")
    return df


if __name__ == "__main__":
    spark_connection = create_spark_connection()

    if spark_connection is not None:
        kafka_df = connect_to_kafka(spark_connection)

        if kafka_df is not None:
            selection_df = create_selection_df_from_kafka(kafka_df)
            cass_session = create_cassandra_connection()

            if cass_session is not None:
                create_keyspace(cass_session)
                cass_session.set_keyspace('spark_stream')
                create_table(cass_session)

                query = selection_df.writeStream \
                    .format("org.apache.spark.sql.cassandra") \
                    .option("checkpointLocation", "/tmp/spark_checkpoint") \
                    .option("keyspace", "spark_stream") \
                    .option("table", "users_created") \
                    .outputMode("append") \
                    .start()

                print("Spark Structured Streaming Started.")
                query.awaitTermination()