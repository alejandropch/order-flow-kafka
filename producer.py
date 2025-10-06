from confluent_kafka import Producer
import uuid
import json

producer_config = {
        "bootstrap.servers":"localhost:9092"
    }

def report (err, msg): 
    if err:
        print(f"error: {err}") 
    else:
        print(f"success!: {msg.value().decode('utf-8')}")
        print(f"{dir(msg)}")

producer = Producer(producer_config)

order = {
        "order_id": str(uuid.uuid4()),
        "user": "Alejandro",
        "product": "this is a product"
    }

to_send = json.dumps(order).encode("utf-8")

producer.produce(
        topic = 'orders',
        value = to_send,
        callback = report
    )

producer.flush()


