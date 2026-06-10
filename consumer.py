import json
import time
import sqlite3
from collections import deque
from confluent_kafka import Consumer, KafkaException

# Configure the Consumer
conf = {
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'analytics-group-v6',   # Incremented to process from scratch
    'auto.offset.reset': 'earliest',    
    'enable.auto.commit': True
}

consumer = Consumer(conf)
consumer.subscribe(['transactions'])

# Initialize Database Connection
db_conn = sqlite3.connect('kafka_analytics.db', isolation_level=None) # Auto-commit mode enabled
cursor = db_conn.cursor()

# Create Tables
cursor.execute('''
    CREATE TABLE IF NOT EXISTS transaction_ledger (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        amount REAL,
        timestamp REAL
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS user_profiles (
        user_id INTEGER PRIMARY KEY,
        all_time_total REAL,
        last_window_total REAL,
        last_window_avg REAL,
        last_updated REAL
    )
''')

# In-Memory State for Window Tracking
WINDOW_SECONDS = 60
sliding_windows = {}
all_time_totals = {}

print(f"Listening and saving to database... [Sliding Window: {WINDOW_SECONDS}s]")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            raise KafkaException(msg.error())

        try:
            data = json.loads(msg.value().decode('utf-8'))
            user_id = data.get('user_id')
            amount = data.get('amount')
            tx_time = data.get('timestamp', time.time())

            if user_id is None or amount is None:
                continue

            current_time = time.time()

            # 1. Log Raw Event to Ledger Table
            cursor.execute('''
                INSERT INTO transaction_ledger (user_id, amount, timestamp)
                VALUES (?, ?, ?)
            ''', (user_id, amount, tx_time))

            # 2. Update In-Memory Window Calculations
            all_time_totals[user_id] = all_time_totals.get(user_id, 0.0) + amount

            if user_id not in sliding_windows:
                sliding_windows[user_id] = deque()
            
            sliding_windows[user_id].append((tx_time, amount))

            while sliding_windows[user_id] and (current_time - sliding_windows[user_id][0][0] > WINDOW_SECONDS):
                sliding_windows[user_id].popleft()

            active_window = sliding_windows[user_id]
            window_count = len(active_window)
            window_total = sum(tx[1] for tx in active_window)
            window_avg = window_total / window_count if window_count > 0 else 0.0

            # 3. Save Metrics Snapshot to Profiles Table (Upsert pattern)
            cursor.execute('''
                INSERT INTO user_profiles (user_id, all_time_total, last_window_total, last_window_avg, last_updated)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    all_time_total = excluded.all_time_total,
                    last_window_total = excluded.last_window_total,
                    last_window_avg = excluded.last_window_avg,
                    last_updated = excluded.last_updated
            ''', (user_id, all_time_totals[user_id], window_total, window_avg, current_time))

            print(f"💾 Saved: User {user_id} | Total: ${all_time_totals[user_id]:.2f} | 60s Avg: ${window_avg:.2f}")

        except json.JSONDecodeError:
            print("Invalid JSON received")
        except Exception as e:
            print(f"Processing error: {e}")

except KeyboardInterrupt:
    print("\nShutting down consumer...")

finally:
    consumer.close()
    db_conn.close()
