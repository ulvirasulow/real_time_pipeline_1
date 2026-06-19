# Real-time Streaming Pipeline 
### CoinGecko API → Kafka → Spark → PostgreSQL → Grafana


---

## Ümumi Arxitektura

```
CoinGecko API → Kafka Producer → Kafka Broker → Spark Streaming → PostgreSQL → Grafana
```

---

## Modul 1 — CoinGecko API 

### Brauzerdə aç

```
https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd
```

```json
{"bitcoin": {"usd": 67432.12}}
```

```
https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=bitcoin,ethereum,solana
```

### `api_test.py`

```python
import json
import requests
from datetime import datetime, timezone

url = "https://api.coingecko.com/api/v3/coins/markets"
params = {
    "vs_currency": "usd",
    "ids": "bitcoin,ethereum,solana",
    "order": "market_cap_desc",
    "sparkline": False,
}

resp = requests.get(url, params=params, timeout=10)
data = resp.json()

print(f"{'Symbol':<8} {'Qiymət':>14} {'24h':>8}")
print("-" * 34)
for coin in data:
    print(f"{coin['symbol'].upper():<8} "
          f"${coin['current_price']:>13,.2f} "
          f"{coin['price_change_percentage_24h']:>+7.2f}%")

coin = data[0]
msg = {
    "coin_id":    coin["id"],
    "symbol":     coin["symbol"].upper(),
    "price_usd":  coin["current_price"],
    "change_24h": coin["price_change_percentage_24h"],
    "market_cap": coin["market_cap"],
    "volume_24h": coin["total_volume"],
    "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
}
print(json.dumps(msg, indent=2))
```

```bash
pip install requests
python api_test.py
```


## Modul 2 — Kafka (15 dəq)

### Qovluq strukturu

```
kafka-demo/
└── docker-compose.yml
```

### `kafka-demo/docker-compose.yml`


```bash
docker compose up -d
docker compose ps   
```

### Topic yarat

```bash
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic crypto-prices \
  --partitions 3 \
  --replication-factor 1
```

### Konsol ilə test

**Terminal 1 — Producer:**
```bash
docker exec -it kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic crypto-prices \
  --property "parse.key=true" \
  --property "key.separator=:"
```

Mesaj göndər:
```
bitcoin:{"coin_id":"bitcoin","price_usd":67432.12}
ethereum:{"coin_id":"ethereum","price_usd":3521.45}
```

**Terminal 2 — Consumer:**
```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic crypto-prices \
  --from-beginning \
  --property "print.key=true"
```

**Kafka UI:** http://localhost:8080

### `m2_kafka_producer.py`

```python
import json
import time
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer

BOOTSTRAP = "localhost:9094"
TOPIC     = "crypto-prices"
COINS     = "bitcoin,ethereum,solana"
INTERVAL  = 10

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",
)

def fetch():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    r = requests.get(url, params={
        "vs_currency": "usd",
        "ids": COINS,
    }, timeout=10)
    return r.json()


cycle = 0
while True:
    cycle += 1
    coins = fetch()
    for coin in coins:
        msg = {
            "coin_id":    coin["id"],
            "symbol":     coin["symbol"].upper(),
            "price_usd":  coin["current_price"],
            "change_24h": coin["price_change_percentage_24h"],
            "market_cap": coin["market_cap"],
            "volume_24h": coin["total_volume"],
            "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        producer.send(TOPIC, key=msg["coin_id"], value=msg)
    producer.flush()
    time.sleep(INTERVAL)
```

```bash
pip install kafka-python
python m2_kafka_producer.py
```

---

## Modul 3 — Spark Structured Streaming


```
spark-demo/
├── docker-compose.yml
└── spark_consumer.py
```


### `spark-demo/spark_consumer.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KafkaConsumer") \
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

query = parsed.writeStream \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
```

### 

```bash
docker cp spark_consumer.py spark:/opt/spark/spark_consumer.py

docker exec -it spark bash

/opt/spark/bin/spark-submit \
  --master local[2] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/spark/spark_consumer.py
```

---

## Modul 4 — PostgreSQL Sink (25 dəq)

### Qovluq strukturu

```
postgres-demo/
└── docker-compose.yml
```

### `postgres-demo/docker-compose.yml`

```yaml
services:
  postgres:
    image: postgres:16-alpine
    container_name: postgres
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: cryptodb
      POSTGRES_USER: cryptouser
      POSTGRES_PASSWORD: cryptopass
    networks:
      - kafka-net

networks:
  kafka-net:
    external: true
    name: kafka_default
```

```bash
docker compose up -d
```

### Cədvəl yarat

```bash
docker exec -it postgres psql -U cryptouser -d cryptodb
```

```sql
CREATE TABLE crypto_prices (
    id          SERIAL PRIMARY KEY,
    coin_id     TEXT,
    symbol      TEXT,
    price_usd   NUMERIC(20,8),
    change_24h  NUMERIC(10,4),
    market_cap  BIGINT,
    volume_24h  BIGINT,
    fetched_at  TEXT,
    created_at  TIMESTAMPTZ DEFAULT now()
);
```

### `spark_consumer.py`-ni yenilə

```python
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
    print(f"[batch={batch_id}] {count} sətir yazıldı")

query = parsed.writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
```

### Çalışdır

```bash
exit

docker cp spark_consumer.py spark:/opt/spark/spark_consumer.py
docker exec -it spark bash

wget -q -O /tmp/postgresql.jar \
  https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

/opt/spark/bin/spark-submit \
  --master local[2] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  --jars /tmp/postgresql.jar \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/spark/spark_consumer.py
```

### PostgreSQL-də yoxla

```bash
docker exec -it postgres psql -U cryptouser -d cryptodb
```

```sql
SELECT symbol, price_usd, change_24h, created_at
FROM crypto_prices
ORDER BY id DESC
LIMIT 5;
```

---

## Qovluq strukturu (tam)

```
streaming-pipeline/
├── kafka-demo/
│   ├── docker-compose.yml
│   └── m2_kafka_producer.py
├── spark-demo/
│   ├── docker-compose.yml
│   └── spark_consumer.py
├── postgres-demo/
│   └── docker-compose.yml
├── grafana-demo/
│   └── docker-compose.yml
└── m1_api_test.py
```
