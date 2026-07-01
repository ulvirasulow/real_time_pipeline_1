# Real-Time Crypto Streaming Pipeline

CoinGecko API-dan real vaxtda kripto valyuta məlumatları toplayıb emal edən, saxlayan və vizuallaşdıran end-to-end data pipeline-ı.

## Arxitektura

```
CoinGecko API
      │
      ▼
Kafka Producer          ← kafka_python_producer.py (hər 4 san.)
      │
      ▼
Kafka Broker            ← topic: crypto-prices
      │
      ▼
Spark Streaming         ← spark_consumer_final.py (hər 10 san. batch)
      │
      ▼
PostgreSQL              ← cryptodb.crypto_prices
      │
      ├──▶ Grafana       ← real-time dashboard
      │
      └──▶ Airflow       ← gündəlik: cryptodb.daily_prices
```

## Servislər

| Servis | Texnologiya | UI / Port |
|---|---|---|
| Mesaj brokerı | Kafka (KRaft) | — |
| Kafka idarəetmə | Kafka UI | http://localhost:8080 |
| Stream prosessinq | Apache Spark 3.5.6 | — |
| Verilənlər bazası | PostgreSQL 15 | localhost:5432 |
| Dashboard | Grafana | http://localhost:3000 |
| İş planlaşdırıcı | Apache Airflow | http://localhost:8081 |

## Qovluq strukturu

```
real_time_pipeline_1/
├── kafka/
│   ├── docker-compose.yaml       # Kafka broker + Kafka UI
│   └── kafka_python_producer.py  # CoinGecko → Kafka producer
├── spark/
│   ├── docker-compose.yaml       # Spark konteyneri
│   └── spark_consumer_final.py   # Kafka → PostgreSQL stream consumer
├── postgres/
│   └── docker-compose.yaml       # PostgreSQL
├── grafana/
│   └── docker-compose.yaml       # Grafana
├── airflow/
│   ├── docker-compose.yaml       # Airflow (webserver + scheduler)
│   ├── dags/
│   │   └── daily_prices_dag.py   # Gündəlik transform DAG-ı
└── dashboard/
    └── dashboard.png             # Grafana dashboard screenshot
```

## İşə salmaq

Servisləri **aşağıdakı ardıcıllıqla** ayrıca qaldırın — hamısı `kafka_default` şəbəkəsindən istifadə edir, ona görə Kafka mütləq birinci olmalıdır:

```bash
# 1. Kafka (şəbəkəni bu yaradır)
cd kafka && docker compose up -d && cd ..

# 2. PostgreSQL
cd postgres && docker compose up -d && cd ..

# 3. Spark
cd spark && docker compose up -d && cd ..

# 4. Grafana
cd grafana && docker compose up -d && cd ..

# 5. Airflow
cd airflow && docker compose up -d && cd ..
```

Kafka tam hazır olmadan digərlərini qaldırsanız şəbəkə xətası alına bilər.

## Pipeline-ı işə salmaq

**1. Producer-ı başlat** (Kafka konteynerinin xaricindən, local mühitdən):

```bash
cd kafka
pip install kafka-python requests
python kafka_python_producer.py
```

Hər 4 saniyədə bir CoinGecko-dan `bitcoin`, `ethereum`, `solana` məlumatlarını çəkib `crypto-prices` topic-inə yazır.

**2. Spark consumer-ı başlat:**

```bash
docker exec -it spark bash

# Konteyner içindən:
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3 \
  /path/to/spark_consumer_final.py
```

Hər 10 saniyədə bir Kafka-dan batch oxuyub `crypto_prices` cədvəlinə yazır.

**3. Airflow DAG-ı aktiv et:**

`http://localhost:8081` → `daily_prices_transform` DAG-ını tap → toggle ilə aktiv et.

Hər gün gecə yarısı (00:00 UTC) avtomatik işləyir. Manual işlətmək üçün "Trigger DAG" düyməsini istifadə et.

## Verilənlər bazası

**`cryptodb.crypto_prices`** — Spark tərəfindən yazılır (real-time):

| Sütun | Tip | Açıqlama |
|---|---|---|
| `coin_id` | TEXT | Sikkə adı (bitcoin, ethereum...) |
| `symbol` | TEXT | Ticker (btc, eth...) |
| `price_usd` | DOUBLE | O anki qiymət (USD) |
| `change_24h` | DOUBLE | 24 saatlıq dəyişmə (%) |
| `market_cap` | BIGINT | Bazar kapitalizasiyası |
| `volume_24h` | BIGINT | 24 saatlıq ticarət həcmi |
| `fetched_at` | TEXT | Məlumat çəkilmə vaxtı |

**`cryptodb.daily_prices`** — Airflow tərəfindən yazılır (gündəlik):

| Sütun | Tip | Açıqlama |
|---|---|---|
| `price_date` | DATE | Tarix |
| `coin_id` | TEXT | Sikkə adı |
| `avg_price_usd` | NUMERIC | Günlük orta qiymət |
| `min_price_usd` | NUMERIC | Günün minimu |
| `max_price_usd` | NUMERIC | Günün maksimumu |
| `close_price_usd` | NUMERIC | Günün sonuncu qiyməti |
| `avg_change_24h` | NUMERIC | Orta dəyişmə faizi |
| `avg_market_cap` | NUMERIC | Orta bazar kap. |
| `avg_volume_24h` | NUMERIC | Orta həcm |

## Giriş məlumatları

| Servis | URL | İstifadəçi | Parol |
|---|---|---|---|
| Kafka UI | http://localhost:8080 | — | — |
| Grafana | http://localhost:3000 | admin | admin123 |
| Airflow | http://localhost:8081 | admin | admin |
| PostgreSQL | localhost:5432 | cryptouser | cryptopass |

## Dayandırmaq

```bash
# Bütün servisləri dayandır (datanı saxlayır)
for dir in kafka postgres spark grafana airflow; do
  cd $dir && docker compose down && cd ..
done

# Bütün datanı da sil (sıfırdan başlamaq üçün)
for dir in kafka postgres spark grafana airflow; do
  cd $dir && docker compose down -v && cd ..
done
```
