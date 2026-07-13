# Kafka live vehicle tracker

A small project to test how fast a Kafka pipeline can move location data end
to end, using a simulated vehicle driving a real route and a live map that
updates as messages arrive.

## How it works

A producer streams GPS-style coordinates into Kafka. A consumer reads them
back out and pushes them over a WebSocket to a browser, which plots the
vehicle's position on a live map in real time.

```
get_route.py -> route.json -> producer.py -> Kafka -> tracker.py -> map.html
```

- `get_route.py` fetches a real, road-following route between two points
  (via the OSRM routing API) and saves it as coordinates in `route.json`.
- `producer.py` streams those coordinates into Kafka, one message per
  point, simulating a vehicle moving along the route.
- `tracker.py` consumes those messages from Kafka and rebroadcasts them
  over a WebSocket (`ws://localhost:8765`). Built to scale on the consumer
  side: you can run multiple `PositionConsumer` instances against a shared
  queue, feeding a single `MapBroadcaster`.
- `map.html` is a live Leaflet map that connects to that WebSocket and
  plots the vehicle's position and trail as updates arrive, along with a
  small stats readout (rate, latency).
- `docker-compose.yaml` runs a single-node Kafka broker (KRaft mode, no
  Zookeeper needed).

## Prerequisites

- Docker
- Python 3.10+
- `pip install confluent-kafka requests websockets`

## Running it

Run these in order. Steps 3 and 5 block, so use separate terminals.

1. **Start Kafka**
   ```bash
   docker compose up -d
   ```

2. **Generate a route** (creates `route.json`)
   ```bash
   python get_route.py
   ```

3. **Start the tracker** (leave this running)
   ```bash
   python tracker.py
   ```

4. **Open `map.html`** in a browser (double-click it, or `open map.html` /
   `start map.html` / `xdg-open map.html`). Confirm the status box shows
   `live` before continuing — the map must be open and connected *before*
   the producer runs, or those messages are broadcast to no one and lost.

5. **Run the producer**
   ```bash
   python producer.py
   ```
- `producer.py`'s `sendIntervalSec` controls the simulated update rate
  (default ~5/s). Set it to `0` to fire messages as fast as possible, for
  a real throughput test.

   Watch the marker move on the map, and watch `tracker.py`'s terminal for
   throughput (msg/s) and latency, printed once a second.


- To scale consumption, add more `PositionConsumer(...)` entries in
  `tracker.py`'s `__main__` block. Same `group_id` splits Kafka partitions
  between them (parallel throughput); a different `group_id` gives each
  one its own independent copy of the stream.

