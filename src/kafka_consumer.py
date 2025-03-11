from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils import spark_schema, get_validation_condition
import os
import time

# Configurable Kafka settings
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "nyc_taxi_stream")
PROCESSED_TOPIC = os.getenv("PROCESSED_TOPIC", "nyc_taxi_processed")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "nyc_taxi_dlq")
POSTGRES_JDBC = os.getenv("POSTGRES_JDBC", "jdbc:postgresql://postgres:5432/airflow")
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PWD = os.getenv("POSTGRES_PWD", "airflow")
CHECKPOINT_LOCATION = "/tmp/checkpoints/main"

def create_spark_session():
    """Create and configure a Spark session with monitoring"""
    return SparkSession.builder \
        .appName("KafkaConsumer") \
        .config("spark.master", "spark://spark-master:7077") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.memory", "1G") \
        .config("spark.cores.max", "1") \
        .config("spark.scheduler.mode", "FAIR") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.sql.streaming.minBatchesToRetain", 2) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.executor.extraJavaOptions", "-Dcom.sun.management.jmxremote") \
        .getOrCreate()

def process_stream():
    """Main streaming processing function"""
    spark = create_spark_session()

    def process_batch(df, batch_id):
        """Process each batch of Kafka messages"""
        if df.isEmpty():
            return

        # Parse and validate messages
        processed_df = df.select(
            F.from_json(F.col("value").cast("string"), spark_schema).alias("data")
        ).select("data.*") \
         .withColumn("is_valid", get_validation_condition())

        # Split into valid and invalid streams
        valid_df = processed_df.filter(F.col("is_valid"))
        invalid_df = processed_df.filter(~F.col("is_valid"))

        # Write valid records to processed topic
        if not valid_df.isEmpty():
            valid_df.write \
                .format("jdbc") \
                .option("url", POSTGRES_JDBC) \
                .option("dbtable", "processed_messages") \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PWD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()

        # Write invalid records to DLQ
        if not invalid_df.isEmpty():
            invalid_df.select(
                F.to_json(F.struct([F.col(c) for c in spark_schema.fieldNames()])).alias("value")
            ).write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BROKER) \
                .option("topic", DLQ_TOPIC) \
                .save()

    # Start Kafka stream processing
    spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", SOURCE_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("kafka.enable.idempotence", "true") \
        .option("transactional.id", "main_stream_producer") \
        .load() \
        .writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .start() \
        .awaitTermination()

    # Monitor streams
    while True:
        print("=== Stream Status ===")
        for q in spark.streams.active:
            print(f"{q.name}: {q.status}")
        time.sleep(60)

if __name__ == "__main__":
    process_stream()
