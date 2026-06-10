import json
import time
import random
from confluent_kafka import Producer

# Configuration for the Kafka Producer connecting to our local broker
conf = {'bootstrap.servers': '127.0.0.1:9092'} 

producer = Producer(conf)

def delivery_report(err, msg):
    """ Callback triggered when a message is successfully delivered or fails. """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Produced to topic {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# Simulate a live data stream
topic_name = "transactions"
user_ids = [101, 102, 103, 104, 105]

print("Starting transaction stream...")
try:
    while True:
        # Create a mock transaction dictionary
        transaction = {
            "user_id": random.choice(user_ids),
            "amount": round(random.uniform(5.0, 1500.0), 2),
            "timestamp": time.time()
        }
        
        # Convert dictionary to JSON string, encode to bytes, and send to Kafka
        producer.produce(
            topic=topic_name,
            value=json.dumps(transaction).encode('utf-8'),
            callback=delivery_report
        )
        
        # Flush batches the messages for efficiency
        producer.poll(1) 
        time.sleep(0.5) # Send 2 messages per second
        
except KeyboardInterrupt:
    print("Stopping stream.")
finally:
    # Ensure all messages are sent before closing
    producer.flush()