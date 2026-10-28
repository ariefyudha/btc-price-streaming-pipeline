import json
import os
import time
from flask import Flask, render_template, Response, request
from kafka import KafkaConsumer

app = Flask(__name__)

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPIC_NAME = "btc-candles"

def get_kafka_consumer():
    # Loop retries to wait for kafka to start
    for _ in range(10):
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='earliest'
            )
            return consumer
        except Exception as e:
            print(f"Waiting for Kafka broker... {e}")
            time.sleep(2)
    raise Exception("Failed to connect to Kafka.")

@app.route('/')
def index():
    return render_template('index.html')

@app.after_request
def add_header(response):
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '-1'
    return response

@app.route('/log_error', methods=['POST'])
def log_error():
    print("FRONTEND ERROR:", request.json, flush=True)
    return "OK", 200

def stream_events():
    consumer = get_kafka_consumer()
    print("Dashboard terhubung ke Kafka. Menunggu data candle...")
    for message in consumer:
        # message.value merupakan dictionary dari JSON yang dihasilkan oleh Spark
        data = message.value
        yield f"data: {json.dumps(data)}\n\n"

@app.route('/stream')
def stream():
    response = Response(stream_events(), mimetype="text/event-stream")
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['X-Accel-Buffering'] = 'no'
    return response

if __name__ == '__main__':
    # Jalankan pada port 5001, 0.0.0.0 agar bisa diakses dari host
    app.run(host='0.0.0.0', port=5001)
