# Event-Driven E-Commerce Data Pipeline

An end-to-end streaming data pipeline that simulates e-commerce user activity and processes it in real time using Kafka and Spark.

## Overview
A Python producer generates schema-validated user events (view, add_to_cart, purchase) and publishes them to Apache Kafka. Spark Structured Streaming consumes these events, applies transformations, and writes analytics-ready Parquet files partitioned by date.

## Architecture
Python Producer → Apache Kafka → Spark Structured Streaming → Parquet Data Lake

## Tech Stack
Python · Apache Kafka · Apache Spark · Docker · Parquet · JSON

## Features
- Event-driven, decoupled architecture
- Schema validation for data quality
- Fault-tolerant stream processing
- Columnar Parquet output with Snappy compression

## How to Run

### 1. Clone the Repository
git clone https://github.com/<your-username>/event-driven-data-pipeline.git
cd event-driven-data-pipeline

2. Start Kafka
docker compose up -d

3. Create Kafka Topic
docker exec -it kafka kafka-topics --create \
--topic ecommerce-events \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1

4. Start the Event Producer
python -m producer.event_producer

5. Start Spark Structured Streaming
MSYS_NO_PATHCONV=1 docker run -it --rm \
  --network="dataengineeringproject_default" \
  -v "$(pwd)/data:/opt/spark-data" \
  -v "$(pwd)/spark:/opt/spark-app" \
  apache/spark:3.5.0 \
  /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/ivy \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark-app/streaming_job.py
6. Verify Output
After ~30–60 seconds:

ls data/processed
