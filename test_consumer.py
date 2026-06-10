import sys
from confluent_kafka import Consumer, KafkaError

def print_assignment(consumer, partitions):
    print(f"✅ SUCCESS: Consumer assigned to partitions: {partitions}")

conf = {
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'diagnostic-group-unique-1',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'debug': 'consumer,cgrp' # Forces Kafka to log its internal connection attempts
}

print("Initializing diagnostic consumer...")
try:
    consumer = Consumer(conf)
    # The on_assign callback will tell us if Kafka even acknowledges this consumer
    consumer.subscribe(['transactions'], on_assign=print_assignment)
    print("Listening... Press Ctrl+C to stop.")
    
    for _ in range(20): # Poll 20 times then exit automatically
        msg = consumer.poll(1.0)
        if msg is None:
            print("... Polled but got no data (Timeout) ...")
            continue
        if msg.error():
            print(f"❌ KAFKA ERROR: {msg.error()}")
        else:
            print(f"🎉 SUCCESS! Received message: {msg.value().decode('utf-8')}")
            
except Exception as e:
    print(f"❌ CRITICAL EXCEPTION: {e}")
finally:
    if 'consumer' in locals():
        consumer.close()
