import json
import time
import sqlite3
from collections import deque
from confluent_kafka import Consumer, KafkaException, KafkaError

conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'stock-analytics-v11',  
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True
}

consumer = Consumer(conf)
consumer.subscribe(['stock_prices'])

db_conn = sqlite3.connect('kafka_analytics.db', timeout=5.0, isolation_level=None)
db_conn.execute("PRAGMA journal_mode=WAL;") 
cursor = db_conn.cursor()

cursor.execute('CREATE TABLE IF NOT EXISTS stock_history (id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT, current_price REAL, moving_avg_60s REAL, timestamp REAL)')
cursor.execute('CREATE TABLE IF NOT EXISTS flash_alerts (id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT, old_price REAL, new_price REAL, drop_percentage REAL, timestamp REAL)')
cursor.execute('CREATE TABLE IF NOT EXISTS engine_config (id INTEGER PRIMARY KEY, window_seconds INTEGER, drop_threshold REAL)')
cursor.execute('INSERT OR IGNORE INTO engine_config (id, window_seconds, drop_threshold) VALUES (1, 60, 2.0)')

price_windows = {}

# FIX: Cache configuration settings to stop querying SQLite on every loop iteration
cursor.execute("SELECT window_seconds, drop_threshold FROM engine_config WHERE id = 1")
config_row = cursor.fetchone()
window_size = config_row[0] if config_row else 60
drop_threshold_percent = config_row[1] if config_row else 2.0
last_config_check = time.time()

print("🚀 Dynamic Financial Risk Engine Active & Monitoring...")
try:
    while True:
        msg = consumer.poll(0.01) # Optimize poll response latency
        
        if msg is None: 
            continue

        if msg.error(): 
            if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                continue
            else:
                raise KafkaException(msg.error())
        
        current_time = time.time()
        
        # FIX: Check config every 10 seconds instead of every single tick
        if current_time - last_config_check > 10:
            cursor.execute("SELECT window_seconds, drop_threshold FROM engine_config WHERE id = 1")
            config_row = cursor.fetchone()
            window_size = config_row[0] if config_row else 60
            drop_threshold_percent = config_row[1] if config_row else 2.0
            last_config_check = current_time

        data = json.loads(msg.value().decode('utf-8'))
        ticker = data['ticker']
        price = float(data['price'])
        
        if ticker not in price_windows:
            price_windows[ticker] = deque()
            
        price_windows[ticker].append((current_time, price))
        
        while price_windows[ticker] and (current_time - price_windows[ticker][0][0] > window_size):
            price_windows[ticker].popleft()
            
        active_window = price_windows[ticker]
        
        if len(active_window) >= 2:
            opening_window_price = active_window[0][1]
            price_change_pct = ((price - opening_window_price) / opening_window_price) * 100
            
            if price_change_pct <= -drop_threshold_percent:
                abs_drop = abs(price_change_pct)
                print(f"🚨 ALERT TRIGGERED: {ticker} dropped {abs_drop:.2f}%")
                cursor.execute('''
                    INSERT INTO flash_alerts (ticker, old_price, new_price, drop_percentage, timestamp)
                    VALUES (?, ?, ?, ?, ?)
                ''', (ticker, opening_window_price, price, abs_drop, current_time))
        
        prices = [p[1] for p in active_window]
        moving_avg = sum(prices) / len(prices) if prices else price
        
        cursor.execute('INSERT INTO stock_history (ticker, current_price, moving_avg_60s, timestamp) VALUES (?, ?, ?, ?)', 
                       (ticker, price, moving_avg, current_time))
        print(f"✅ Metric logged: {ticker} -> Price: ${price:.2f} | MA: ${moving_avg:.2f}")
        
except KeyboardInterrupt:
    print("Shutting down risk metric system...")
finally:
    consumer.close()
    db_conn.close()
