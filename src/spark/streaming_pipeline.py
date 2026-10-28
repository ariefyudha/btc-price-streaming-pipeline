import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, 
    max as _max, min as _min, sum as _sum, first, last, to_json, struct
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Konfigurasi Kafka
# Menggunakan 'kafka:9092' karena script ini dijalankan di dalam container Docker yang sama dengan network compose
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
INPUT_TOPIC = "btc-trades"
VALID_OUTPUT_TOPIC = "btc-candles"
DLQ_TOPIC = "btc-dead-letter"

def get_spark_session():
    # Menyiapkan session Spark dengan package kafka-sql
    return SparkSession.builder \
        .appName("RealTimeTradingETL") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

def get_schema():
    return StructType([
        StructField("trade_id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("Mulai membaca aliran data (stream) dari Kafka...")

    # 1. Data Collection & ETL: Membaca dari Kafka
    raw_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Data dari Kafka berformat binari, kita cast ke string lalu parse sebagai JSON
    schema = get_schema()
    parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_val") \
        .select(from_json(col("json_val"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("timestamp")))

    # 2. Data Validation: Mengecek data yang valid (harga & volume harus > 0)
    valid_data = parsed_stream.filter((col("price") > 0) & (col("volume") > 0))
    
    # 3. Data Routing (Dead Letter Queue): Mengambil data kotor untuk dikirim ke tempat lain
    invalid_data = parsed_stream.filter((col("price") <= 0) | (col("volume") <= 0))

    # 4. Stateful Operations & Aggregation: Menghitung OHLC dan total volume 
    # menggunakan "Tumbling Window" berdurasi 10 Detik (agar cepat terlihat di demo)
    ohlc_aggregated = valid_data \
        .withWatermark("timestamp", "2 seconds") \
        .groupBy(
            window(col("timestamp"), "10 seconds"),
            col("symbol")
        ) \
        .agg(
            first("price").alias("open"),
            _max("price").alias("high"),
            _min("price").alias("low"),
            last("price").alias("close"),
            _sum("volume").alias("total_volume")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("total_volume")
        )

    # 5. Output Sink: Menulis hasil agregasi (candle) kembali ke Kafka & Console
    # Siapkan format JSON untuk dikirim ke Kafka
    kafka_output_df = ohlc_aggregated.selectExpr("to_json(struct(*)) AS value")

    candles_query = kafka_output_df \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", VALID_OUTPUT_TOPIC) \
        .option("checkpointLocation", "/app/src/checkpoints/candles_v2") \
        .outputMode("append") \
        .start()

    # Print hasil validasi valid_data ke console sebagai demo di kelas (update mode agar terlihat)
    console_query = ohlc_aggregated \
        .writeStream \
        .format("console") \
        .outputMode("update") \
        .option("truncate", "false") \
        .option("checkpointLocation", "/app/src/checkpoints/console_v2") \
        .start()

    # Mengirim data yang invalid ke Dead Letter Queue Topic di Kafka
    dlq_kafka_df = invalid_data.selectExpr("to_json(struct(*)) AS value")
    dlq_query = dlq_kafka_df \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", DLQ_TOPIC) \
        .option("checkpointLocation", "/app/src/checkpoints/dlq_v2") \
        .outputMode("append") \
        .start()

    print("Pipeline berjalan! Menunggu terminasi...")
    
    # Tunggu semua query berjalan tanpa henti
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
