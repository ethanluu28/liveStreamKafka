from confluent_kafka import Producer
import uuid
import time
import json


class runProducer:
    # Streams vehicles routes through Kafka

    def __init__(
            self,
            vehicleID: str,
            route: list[tuple[float,float]],
            topic: str = "vehicle_positions",
            bootstrapServers: str = "localhost:9092",
            sendIntervalSec: float = 0.2,
    ):
        self.vehicleID = vehicleID
        self.route = route
        self.topic = topic
        self.bootstrapServers = bootstrapServers
        self.sendIntervalSec = sendIntervalSec

        self.delivered = 0
        self.failed = 0

        self.producer = Producer(
            {
                'bootstrap.servers': self.bootstrapServers,
                'linger.ms': 5,      # small batching window to help throughput
                'compression.type': 'lz4'
            }
        )


    def deliveryReport(self, err, msg):
        if err:
            self.failed += 1
            print(f"[{self.vehicleID}] delivery failed: {err}")
        else:
            self.delivered += 1
 
    def run(self):
        print(f"[{self.vehicleID}] streaming {len(self.route)} points to '{self.topic}'")
 
        for i, (lat, lon) in enumerate(self.route):
            payload = {
                "vehicle_id": self.vehicleID,
                "lat": lat,
                "lon": lon,
                "seq": i,
                "ts": time.time(),
            }
            self.producer.produce(
                topic=self.topic,
                key=self.vehicleID,        # keeps this vehicle's updates on one partition, in order
                value=json.dumps(payload).encode("utf-8"),
                callback=self.deliveryReport,
            )
            self.producer.poll(0)
 
            if self.sendIntervalSec:
                time.sleep(self.sendIntervalSec)
 
        self.producer.flush()
        print(f"[{self.vehicleID}] done. delivered={self.delivered} failed={self.failed}")
    

if __name__ == '__main__':
    with open("route.json") as f:
        route = json.load(f)
 
    vehicle = runProducer(vehicleID="vehicle-1", route=route)
    vehicle.run()

"""
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
"""
# docker exec -it kafka_broker kafka-topics --bootstrap-server localhost:9092 --describe --topic orders