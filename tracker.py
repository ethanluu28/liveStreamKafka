from confluent_kafka import Consumer
import json


consumerConfig = {
    'bootstrap.servers': 'localhost:9092',
    "group.id": "order-tracker",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumerConfig)

consumer.subscribe(["orders"]) # can be multiple topics, add to list

print("Consumer is running and subscribed to orders topic")

try:
    while True:
        msg = consumer.poll(1.0) # checking if new msg
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: ", msg.error())
            continue

        value = msg.value().decode("utf-8")
        order = json.loads(value)

        print(f"Package order received: {order['quantity']} x {order['item']} from {order['user']}")
except KeyboardInterrupt:
    print("\n Closing consumer connection")

finally:
    # Always close gracefully
    consumer.close()