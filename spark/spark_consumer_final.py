from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KafkaToPostgres") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("coin_id",    StringType()),
    StructField("symbol",     StringType()),
    StructField("price_usd",  DoubleType()),
    StructField("change_24h", DoubleType()),
    StructField("market_cap", LongType()),
    StructField("volume_24h", LongType()),
    StructField("fetched_at", StringType()),
])

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto-prices") \
    .option("startingOffsets", "latest") \
    .load()

parsed = raw.select(
    F.from_json(F.col("value").cast("string"), schema).alias("data")
).select("data.*")

JDBC_URL = "jdbc:postgresql://postgres:5432/cryptodb"
JDBC_PROPS = {
    "user":     "cryptouser",
    "password": "cryptopass",
    "driver":   "org.postgresql.Driver",
}

def write_to_postgres(batch_df, batch_id):
    count = batch_df.count()
    if count == 0:
        return
    batch_df.write.jdbc(
        url=JDBC_URL,
        table="crypto_prices",
        mode="append",
        properties=JDBC_PROPS,
    )
    print(f"[batch={batch_id}] {count} ")

query = parsed.writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()