from confluent_kafka import Producer
import uuid
import json

producerConfig = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(producerConfig)

def deliveryReport(err, msg):
    # Debugging prints
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered {msg.value().decode("utf-8")}")
        print(f"Delivered to {msg.topic()} : partition {msg.partition()} : at offset {msg.offset()}")



order = {
    "order_id": str(uuid.uuid4()),
    "user": "kev",
    "item": "potato ball ",
    "quantity": 33
}

value = json.dumps(order).encode("utf-8")

# Kafka automatically makes orders topic if not exist
producer.produce(
    topic="orders",
    value=value,
    callback=deliveryReport
    ) 
producer.flush() # forces events to kafka

# docker exec -it kafka_broker kafka-topics --bootstrap-server localhost:9092 --describe --topic orders