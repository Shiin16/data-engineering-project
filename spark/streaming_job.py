from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = (
    SparkSession.builder
    .appName("KafkaEcommerceStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Define schema (defense in depth)
event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
])

# Read from Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "ecommerce-events")
    .option("startingOffsets", "latest")
    .load()
)

# Parse JSON value
parsed_df = (
    kafka_df
    .select(from_json(col("value").cast("string"), event_schema).alias("data"))
    .select("data.*")
)

# Convert timestamps
final_df = (
    parsed_df
    .withColumn("event_timestamp", to_timestamp("event_timestamp"))
    .withColumn("event_date", date_format("event_timestamp", "yyyy-MM-dd"))
)

# Write to Parquet
query = (
    final_df.writeStream
    .format("parquet")
    .option("path", "/opt/spark-data/processed")
    .option("checkpointLocation", "/opt/spark-data/checkpoints")
    .outputMode("append")
    .start()
)

query.awaitTermination()
