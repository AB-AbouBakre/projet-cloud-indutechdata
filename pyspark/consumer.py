"""Analyse en temps réel les tickets du topic Redpanda client_tickets."""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, from_json, max as spark_max, to_timestamp
from pyspark.sql.types import StringType, StructField, StructType


BROKERS = os.getenv("REDPANDA_BROKERS", "redpanda:9092")
TOPIC = os.getenv("REDPANDA_TOPIC", "client_tickets")
CHECKPOINT_DIR = os.getenv(
    "SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints/ticket-insights"
)

TICKET_SCHEMA = StructType(
    [
        StructField("ticket_id", StringType(), nullable=False),
        StructField("client_id", StringType(), nullable=False),
        StructField("created_at", StringType(), nullable=False),
        StructField("request", StringType(), nullable=False),
        StructField("request_type", StringType(), nullable=False),
        StructField("priority", StringType(), nullable=False),
    ]
)


def main() -> None:
    spark = (
        SparkSession.builder.appName("InduTechDataTicketAnalysis")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_messages = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_tickets = (
        raw_messages.select(
            from_json(col("value").cast("string"), TICKET_SCHEMA).alias("ticket"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition"),
            col("offset"),
        )
        .select("ticket.*", "kafka_timestamp", "partition", "offset")
        .withColumn("created_at", to_timestamp("created_at"))
    )

    valid_tickets = parsed_tickets.filter(
        col("ticket_id").isNotNull()
        & col("client_id").isNotNull()
        & col("created_at").isNotNull()
        & col("request").isNotNull()
        & col("request_type").isNotNull()
        & col("priority").isNotNull()
    )

    insights = valid_tickets.groupBy("request_type", "priority").agg(
        count("*").alias("ticket_count"),
        spark_max("created_at").alias("last_ticket_at"),
    )

    query = (
        insights.writeStream.outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="5 seconds")
        .start()
    )

    print(f"Analyse du topic {TOPIC} via {BROKERS}")
    print("Arrêt du traitement : Ctrl+C")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Arrêt demandé par l'utilisateur.")
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()

