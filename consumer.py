from confluent_kafka import Consumer
import json

consumer_config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "order-tracker",
        "auto.offset.reset": "earliest"
        }
consumer = Consumer(consumer_config)
consumer.subscribe(["orders"])

print("subscribed to the 'orders' topic")
try: 
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"error bro {msg.error()}")
            continue

        value = msg.value().decode("utf-8")
        order = json.loads(value)
        print(f"some order bro:\nproduct: {order['product']} from user: {order['user']}")
except: 
    print("Consumer stopped")
finally:
    consumer.close()
