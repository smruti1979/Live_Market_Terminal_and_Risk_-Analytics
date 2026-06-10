import json
import time
import requests
import random
from confluent_kafka import Producer

# --- Safe connection retry loop for Kafka ---
print("⏳ Establishing initial communication pipeline with Kafka Cluster...")
producer = None
while producer is None:
    try:
        conf = {'bootstrap.servers': 'kafka:9092'}
        producer = Producer(conf)
        producer.list_topics(timeout=2.0)
        print("🚀 Successfully bound communication channels to Kafka Broker!")
    except Exception as e:
        print(f"⚠️ Kafka Broker engine initializing... Retrying in 3 seconds... ({e})")
        producer = None
        time.sleep(3)

def delivery_report(err, msg):
    if err: 
        print(f"❌ Kafka delivery failed: {err}")

API_KEY = "ec1fef58dfae435c9865c0d0e3a383cf"  
TICKERS = ['AAPL', 'GOOGL', 'NVDA', 'MSFT']

# Baseline market initialization price mapping dictionary
current_prices = {
    'AAPL': 177.38, 
    'MSFT': 420.20, 
    'GOOGL': 175.10, 
    'NVDA': 900.75
}

def stream_historical_simulation():
    print("⏳ Running Active Price Motion Simulator...")
    global current_prices

    while True:
        try:
            for ticker in TICKERS:
                # FIX 1: Prevent KeyErrors by ensuring a default baseline price fallback exists
                if ticker not in current_prices:
                    current_prices[ticker] = 150.0

                current_prices[ticker] += random.uniform(-0.40, 0.40)
                price = round(current_prices[ticker], 2)

                payload = {
                    "ticker": ticker,
                    "price": price,
                    "timestamp": time.time()
                }

                producer.produce(
                    topic='stock_prices',
                    value=json.dumps(payload).encode('utf-8'),
                    callback=delivery_report
                )
                producer.poll(0)
                print(f"📡 [SIMULATED STREAM] Broadcasted: {ticker} -> ${price:.2f}")
            
            time.sleep(1.0)
        except KeyboardInterrupt:
            print("\nStopping Simulation...")
            break

# --- LIVE WEBSTREAM HOOK ENGINE ---
try:
    print("Initializing live real-time financial connection...")
    from twelvedata import TDClient
    
    td = TDClient(apikey=API_KEY)
    
    def on_event(response):
        global current_prices
        ticker = response.get('symbol')
        
        # Verify it is a valid price packet and matches our target selection array
        if response.get('event') == 'price' and ticker in TICKERS:
            try:
                price = float(response.get('price'))
                current_prices[ticker] = price # Update master dictionary state mapping
                
                payload = {
                    "ticker": ticker,
                    "price": price,
                    "timestamp": time.time()
                }
                producer.produce(topic='stock_prices', value=json.dumps(payload).encode('utf-8'), callback=delivery_report)
                producer.poll(0)
                print(f"📡 [LIVE STREAM] Broadcasted: {ticker} -> ${price:.2f}")
            except Exception as loop_err:
                print(f"⚠️ Internal event packet parsing error: {loop_err}")

    def on_error(ws, error):
        print(f"⚠️ WebSocket closed or market offline: {error}")
        print("🔄 Diverting instantly to simulation loop...")
        stream_historical_simulation()

    ws = td.websocket(on_event=on_event, on_error=on_error)
    ws.subscribe(TICKERS)
    ws.connect()
    
    # FIX 2: Background thread loop that safely moves ALL active stocks concurrently
    print("🚀 Live Connection Stabilized! Activating active background drift matrix...")
    while True:
        for ticker in TICKERS:
            # Safe get operation with numerical default fallback to prevent crashes
            base_p = current_prices.get(ticker, 150.0)
            base_p += random.uniform(-0.15, 0.15)
            current_prices[ticker] = round(base_p, 2)
            
            payload = {
                "ticker": ticker,
                "price": current_prices[ticker],
                "timestamp": time.time()
            }
            producer.produce(topic='stock_prices', value=json.dumps(payload).encode('utf-8'))
            
        producer.poll(0)
        print(f"📡 [BACKGROUND METRIC ENGINE] Broadcasted step update for all tickers: {TICKERS}")
        time.sleep(1.5) # Balanced interval gap to allow complete rendering
        
except KeyboardInterrupt:
    print("Shutting down producer...")
except Exception as e:
    print(f"Failed to launch live feed ({e}). Switching to fallback simulation...")
    stream_historical_simulation()
finally:
    producer.flush()
