from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils import spark_schema, apply_corrections
import os
import time
from datetime import datetime

# Configurable Kafka settings
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "nyc_taxi_dlq")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "nyc_taxi_stream")
FAILED_TOPIC = os.getenv("FAILED_TOPIC", "nyc_taxi_failed")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
CHECKPOINT_LOCATION = f"/tmp/checkpoints/dlq"

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
        .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true") \
        .config("spark.sql.streaming.stateStore.providerClass", 
              "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.executor.extraJavaOptions", "-Dcom.sun.management.jmxremote") \
        .getOrCreate()

def process_dlq_stream():
    """Process DLQ stream with retry logic"""
    spark = create_spark_session()

    def process_batch(df, batch_id):
        """Process each batch of DLQ messages"""
        if df.isEmpty():
            return

        # Parse and process records
        processed_df = df.select(
            F.from_json(F.col("value").cast("string"), spark_schema).alias("data")
        ).select("data.*") \
         .transform(apply_corrections) \
         .withColumn("retry_count", F.col("retry_count") + 1) \

        # Split into retry and failed streams
        retry_df = processed_df.filter(
            (F.col("retry_count") < MAX_RETRIES)
        )
        failed_df = processed_df.filter(
            (F.col("retry_count") >= MAX_RETRIES)
        )

        # Write retries back to main stream
        if not retry_df.isEmpty():
            retry_df.select(F.to_json(F.struct([F.col(c) for c in spark_schema.fieldNames()])).alias("value")) \
                .write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BROKER) \
                .option("topic", SOURCE_TOPIC) \
                .save()

        # Write failures to final DLQ
        if not failed_df.isEmpty():
            failed_df.select(F.to_json(F.struct([F.col(c) for c in spark_schema.fieldNames()])).alias("value")) \
                .write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BROKER) \
                .option("topic", FAILED_TOPIC) \
                .save()

    # Start DLQ processing
    spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", DLQ_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load() \
        .writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .start() \
        .awaitTermination()
    
    while True:
        print("=== Stream Status ===")
        for q in spark.streams.active:
            print(f"{q.name}: {q.status}")
        time.sleep(60)

if __name__ == "__main__":
    process_dlq_stream()