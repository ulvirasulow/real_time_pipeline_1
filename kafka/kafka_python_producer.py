import json
import time
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer

BOOTSTRAP = "localhost:9094"   
TOPIC     = "crypto-prices"
COINS     = "bitcoin,ethereum,solana"
INTERVAL  = 4

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",
)

def fetch():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    try:
        r = requests.get(url, params={
            "vs_currency": "usd",
            "ids": COINS,
        }, timeout=10)
        
        if r.status_code != 200:
            print(f"API xetasi: {r.status_code}, limit doldu")
            return None
            
        data = r.json()
        
        if isinstance(data, list):
            return data
        else:
            print("xeta")
            return None
            
    except Exception as e:
        print(f"xeta: {e}")
        return None

cycle = 0
while True:
    cycle += 1
    coins = fetch()
    
    if not coins:
        print(f"[{cycle}] data alinmadi gozlenilir")
        time.sleep(INTERVAL * 2)
        continue

    for coin in coins:
        try:
            msg = {
                "coin_id":    coin["id"],
                "symbol":     coin["symbol"].upper(),
                "price_usd":  coin["current_price"],
                "change_24h": coin.get("price_change_percentage_24h", 0),
                "market_cap": coin.get("market_cap", 0),
                "volume_24h": coin.get("total_volume", 0),
                "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            producer.send(TOPIC, key=msg["coin_id"], value=msg)
            print(f"[{cycle}] gonderildi: {msg['symbol']} = ${msg['price_usd']:,.2f}")
        except (KeyError, TypeError) as e:
            print(f"xeta: {e}")
            continue

    producer.flush()
    time.sleep(INTERVAL)