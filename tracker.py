import asyncio
import json
import threading
import time
from queue import Queue, Empty
 
from confluent_kafka import Consumer
import websockets

class PositionConsumer:
    # Consumer class to create more and decode when necessary. 
    # Pushes onto queue to share with others
 
    def __init__(
        self,
        queue: "Queue[str]",
        topic: str = "vehicle_positions",
        groupID: str = "map-tracker",
        bootstrap_servers: str = "localhost:9092",
        name: str = "consumer-1",
    ):
        self.queue = queue
        self.topic = topic
        self.groupID = groupID
        self.bootstrap_servers = bootstrap_servers
        self.name = name
 
        self.received = 0
    
    def start(self):
        # spins up background polling thread
        threading.Thread(target=self.pollLoop, daemon=True).start()

    def pollLoop(self):
        consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.groupID,
            'auto.offset.reset': 'earliest',
        })

        consumer.subscribe([self.topic])

        print(f"[{self.name}] subscribed to '{self.topic}' (group: {self.groupID})")

        msgCount = 0
        lastReport = time.time()

        try:
            while True:
                msg = consumer.poll(1.0) # checking if new msg
                if msg is None:
                    continue
                if msg.error():
                    print("Consumer error: ", msg.error())
                    continue

                data = json.loads(msg.value().decode("utf-8"))
                data["latency_ms"] = round((time.time() - data["ts"]) * 1000, 2)

                # Useful when more than one consumer
                data["_partition"] = msg.partition()

                self.queue.put(json.dumps(data))
                self.received += 1

                msgCount += 1
                now = time.time()
                if now - lastReport >= 1.0:
                    print(f"[{self.name}] {msgCount} msg/s | partition {msg.partition()} | latency {data['latency_ms']}ms")
                    msgCount = 0
                    lastReport = now
        finally:
            consumer.close()

class MapBroadcaster:
    # Live map display class that inputs a shared queue and fans each message over a websocket
 
    def __init__(
        self,
        queue: "Queue[str]",
        ws_host: str = "localhost",
        ws_port: int = 8765,
    ):
        self.queue = queue
        self.ws_host = ws_host
        self.ws_port = ws_port
        self.connected_clients: set = set()
 
    async def _broadcast_loop(self):
        # Bridges sync data to async events
        loop = asyncio.get_event_loop()
        while True:
            try:
                msg = await loop.run_in_executor(None, self.queue.get, True, 1.0)
            except Empty:
                continue
            if self.connected_clients:
                await asyncio.gather(
                    *[client.send(msg) for client in self.connected_clients],
                    return_exceptions=True,
                )
 
    async def _handler(self, websocket):
        # Adds new websocket per webbrowser opened
        self.connected_clients.add(websocket)
        print(f"Browser connected ({len(self.connected_clients)} total)")
        try:
            async for _ in websocket:
                pass  # this server doesn't expect messages from the browser
        finally:
            # Disconnects when closed
            self.connected_clients.discard(websocket)
            print(f"Browser disconnected ({len(self.connected_clients)} total)")
 
    async def _serve(self):
        # Block starts websocket server and clean shutdown when closed
        async with websockets.serve(self._handler, self.ws_host, self.ws_port):
            print(f"WebSocket server running on ws://{self.ws_host}:{self.ws_port}")
            await self._broadcast_loop()
 
    def run(self):
        # Called from outside to run
        asyncio.run(self._serve())
 
 
if __name__ == "__main__":
    shared_queue: "Queue[str]" = Queue()
    # Can add more consumers if wanted here
    consumers = [
        PositionConsumer(queue=shared_queue, name="consumer-1"),
    ]
    for c in consumers:
        c.start()
 
    broadcaster = MapBroadcaster(queue=shared_queue)
    broadcaster.run()
    
"""
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
"""