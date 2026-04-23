import os
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    count,
    sum as spark_sum,
    avg as spark_avg,
    to_json,
    struct,
    current_timestamp,
    expr,
    udf,
    lag,
    when,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DoubleType,
)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "web_activity"
HDFS_BASE_PATH = os.getenv("HDFS_OUTPUT_PATH", "/user/netflix/analytics")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/user/netflix/checkpoint")

SCHEMA = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("action", StringType(), True),
        StructField("movie_id", StringType(), True),
        StructField("genre", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("rating_value", IntegerType(), True),
    ]
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("NetflixStreamingAnalytics")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.stateStore.stateValidation", "false")
        .getOrCreate()
    )


def get_kafka_stream(spark: SparkSession, duration: str = "5 minutes"):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 1000)
        .load()
    )


def parse_events(df):
    return df.select(
        from_json(col("value").cast("string"), SCHEMA).alias("data"),
        col("timestamp").alias("event_time"),
    ).select("data.*", "event_time")


def calculate_trending_genres(df, window_duration: str = "5 minutes"):
    windowed = (
        df.filter(col("genre").isNotNull())
        .groupBy(window(col("event_time"), window_duration), col("genre"))
        .agg(count("*").alias("activity_count"))
    )

    return windowed.orderBy(col("activity_count").desc()).limit(10)


def calculate_engagement_score(df, window_duration: str = "5 minutes"):
    base_agg = df.groupBy(
        window(col("event_time"), window_duration), col("movie_id"), col("genre")
    ).agg(
        count(when(col("action") == "play", 1)).alias("views"),
        count(when(col("action") == "like", 1)).alias("likes"),
        spark_avg(when(col("action") == "rate", col("rating_value"))).alias(
            "avg_rating"
        ),
    )

    base_agg = base_agg.fillna(0, ["views", "likes", "avg_rating"])

    return (
        base_agg.withColumn(
            "engagement_score",
            col("views") * 1.0 + col("likes") * 2.0 + col("avg_rating") * 5.0,
        )
        .orderBy(col("engagement_score").desc())
        .limit(20)
    )


def sink_to_hdfs(df, output_path: str, mode: str = "append"):
    date_str = datetime.now().strftime("%Y-%m-%d")
    action_type = df.select("action").first() if not df.isEmpty() else "activity"

    return (
        df.writeStream.format("parquet")
        .option("path", f"{output_path}/dt={date_str}/action={action_type}")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/parquet_{action_type}")
        .trigger(processingTime="30 seconds")
        .outputMode(mode)
        .start()
    )


def console_output(df, name: str):
    return (
        df.writeStream.format("console")
        .option("truncate", "false")
        .option("numRows", "20")
        .trigger(processingTime="30 seconds")
        .queryName(name)
        .start()
    )


def run_streaming_analytics():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Starting Netflix Streaming Analytics")

    try:
        raw_stream = get_kafka_stream(spark)
        events = parse_events(raw_stream)

        logger.info("Processing trending genres...")
        trending_genres = calculate_trending_genres(events)

        query_trending = (
            trending_genres.writeStream.format("console")
            .option("truncate", "false")
            .trigger(processingTime="30 seconds")
            .queryName("Trending Genres")
            .start()
        )

        logger.info("Processing engagement scores...")
        engagement_scores = calculate_engagement_score(events)

        query_engagement = (
            engagement_scores.writeStream.format("console")
            .option("truncate", "false")
            .trigger(processingTime="30 seconds")
            .queryName("Engagement Scores")
            .start()
        )

        logger.info(f"Sinking to HDFS: {HDFS_BASE_PATH}")
        hdfs_sink = (
            events.writeStream.format("parquet")
            .option(
                "path", f"{HDFS_BASE_PATH}/raw/dt={datetime.now().strftime('%Y-%m-%d')}"
            )
            .option("checkpointLocation", f"{CHECKPOINT_PATH}/raw")
            .trigger(processingTime="30 seconds")
            .partitionBy("action")
            .outputMode("append")
            .start()
        )

        logger.info("All streaming queries started")
        logger.info("Awaiting termination...")

        spark.streams.awaitAnyTermination()

    except Exception as e:
        logger.error(f"Streaming analytics error: {e}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")


def process_mock_data():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    mock_data = [
        {
            "user_id": "u1",
            "action": "play",
            "movie_id": "A",
            "genre": "Sci-Fi",
            "timestamp": datetime.now().isoformat(),
            "rating_value": None,
        },
        {
            "user_id": "u2",
            "action": "like",
            "movie_id": "A",
            "genre": "Sci-Fi",
            "timestamp": datetime.now().isoformat(),
            "rating_value": None,
        },
        {
            "user_id": "u3",
            "action": "rate",
            "movie_id": "A",
            "genre": "Sci-Fi",
            "timestamp": datetime.now().isoformat(),
            "rating_value": 4,
        },
        {
            "user_id": "u4",
            "action": "play",
            "movie_id": "B",
            "genre": "Action",
            "timestamp": datetime.now().isoformat(),
            "rating_value": None,
        },
        {
            "user_id": "u1",
            "action": "play",
            "movie_id": "B",
            "genre": "Action",
            "timestamp": datetime.now().isoformat(),
            "rating_value": None,
        },
        {
            "user_id": "u5",
            "action": "like",
            "movie_id": "C",
            "genre": "Thriller",
            "timestamp": datetime.now().isoformat(),
            "rating_value": None,
        },
        {
            "user_id": "u2",
            "action": "rate",
            "movie_id": "D",
            "genre": "Romance",
            "timestamp": datetime.now().isoformat(),
            "rating_value": 5,
        },
        {
            "user_id": "u3",
            "action": "play",
            "movie_id": "E",
            "genre": "Horror",
            "timestamp": datetime.now().isoformat(),
            "rating_value": None,
        },
        {
            "user_id": "u4",
            "action": "like",
            "movie_id": "F",
            "genre": "Comedy",
            "timestamp": datetime.now().isoformat(),
            "rating_value": None,
        },
        {
            "user_id": "u5",
            "action": "rate",
            "movie_id": "G",
            "genre": "Documentary",
            "timestamp": datetime.now().isoformat(),
            "rating_value": 5,
        },
    ]

    df = spark.createDataFrame(mock_data)

    logger.info("=== Trending Genres (Last 5 Minutes) ===")
    trending = calculate_trending_genres(df)
    trending.show()

    logger.info("=== Engagement Scores ===")
    engagement = calculate_engagement_score(df)
    engagement.show()

    logger.info(f"Results would be sunk to: {HDFS_BASE_PATH}")
    logger.info("Parquet structure: dt=YYYY-MM-DD/action=play|like|rate/")

    spark.stop()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--mock":
        logger.info("Running with mock data...")
        process_mock_data()
    else:
        logger.info("Starting Kafka streaming mode...")
        run_streaming_analytics()
