import json
import time
import random
import os
import uuid
import urllib.request
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = 'btc-trades'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_real_market_data():
    """Mengambil data asli (historical 1-menit) dari Yahoo Finance."""
    print("Mengunduh data market asli dari Yahoo Finance...")
    url = 'https://query1.finance.yahoo.com/v8/finance/chart/BTC-USD?range=1d&interval=1m'
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    
    try:
        response = urllib.request.urlopen(req)
        data = json.loads(response.read())
        
        result = data['chart']['result'][0]
        timestamps = result['timestamp']
        quotes = result['indicators']['quote'][0]
        close_prices = quotes['close']
        volumes = quotes['volume']
        
        # Gabungkan menjadi list of dicts
        market_data = []
        for i in range(len(timestamps)):
            if close_prices[i] is not None:
                market_data.append({
                    "price": float(close_prices[i]),
                    "volume": float(volumes[i]) if volumes[i] is not None else random.uniform(0.1, 2.0)
                })
        print(f"Berhasil mengunduh {len(market_data)} baris data asli.")
        return market_data
    except Exception as e:
        print(f"Gagal mengunduh data: {e}. Menggunakan fallback data.")
        return [{"price": 60000.0, "volume": 1.5}]

def generate_trade_data():
    """Memutar ulang (replay) data market asli seolah-olah real-time."""
    market_data = fetch_real_market_data()
    
    while True:
        # Looping terus menerus mensimulasikan stream tak terbatas
        for data_point in market_data:
            # 5% kemungkinan menghasilkan data kotor (Dirty Data) untuk simulasi Validasi ETL
            is_dirty_data = random.random() < 0.05
            
            if is_dirty_data:
                price = -1.0 # Harga tidak valid
                vol = 0.0 # Volume tidak valid
            else:
                price = round(data_point['price'], 2)
                vol = round(data_point['volume'], 4)
    
            trade = {
                "trade_id": str(uuid.uuid4()),
                "symbol": "BTC-USD",
                "price": price,
                "volume": vol,
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }
    
            try:
                producer.send(TOPIC_NAME, trade)
                print(f"Mengirim: {trade}")
            except Exception as e:
                print(f"Gagal mengirim ke Kafka: {e}")
                
            time.sleep(random.uniform(0.1, 1.0))

def main():
    print(f"Menghubungkan ke Kafka broker di {KAFKA_BROKER}...")
    print(f"Mulai stream data transaksi BTC ke topik '{TOPIC_NAME}'...")
    generate_trade_data()

if __name__ == "__main__":
    main()
