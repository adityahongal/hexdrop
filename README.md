# hexdrop

Rider and fleet location ingest platform. Device telemetry arrives over MQTT at
the edge, is bridged into Kafka keyed by rider so each rider's events stay
ordered, aggregated into H3 hex cells in Redis, and served as a heatmap.

## Why the pipeline has both brokers

MQTT is transport, Kafka is storage. MQTT handles many constrained devices on
unreliable links but keeps no log, so nothing can be replayed. Kafka keeps an
ordered, retained log that many consumers can read independently at their own
pace, but is not built for tens of thousands of device connections. Each does
the job it was designed for.

```
devices --MQTT--> mosquitto --bridge--> Kafka --> aggregator --> Redis --> API
 QoS 0, LWT,      per-device      keyed by      H3 cell      sorted set   heatmap
 retained status  ACLs            rider id      per ping     per cell
```

## Design decisions

**Kafka messages are keyed by rider id.** Kafka guarantees ordering within a
partition, not across them, so keying by rider puts one rider's pings on one
partition and keeps them in sequence while different riders process in parallel.
With three partitions and three riders the assignment is visible in the
aggregator output.

**GPS is published at QoS 0, status at QoS 1 and retained.** A dropped position
is replaced a second later and is not worth a retry on a metered mobile link.
Status is different: it is a state change, so it is acknowledged, and retained
so a dashboard opening later immediately sees which riders are online.

**Offline detection uses Last Will and Testament.** Each device registers a will
at connect time, so if it stops responding within the keep-alive window the
broker publishes `offline` on its behalf. Retained plus LWT means current fleet
state is available to any subscriber the moment it connects.

**Each device has its own credentials and a topic ACL.** A device may only
publish under `fleet/<its own id>/#`. A shared fleet credential could not be
revoked for one compromised device without reprovisioning every other one.

**The H3 cell is the aggregation key.** Caching or grouping by raw latitude and
longitude has effectively unbounded cardinality. A hex cell id at a fixed
resolution is bounded, so counts per cell stay small and cheap. Resolution 8 is
roughly 0.7 km² per cell; changing the resolution is the scaling knob.

**The aggregator commits offsets manually** and routes unparseable messages to
`rider-location.dlq`. Kafka has no built-in dead letter queue, and an
uncommitted offset on a poison message stalls that partition indefinitely.

## Running it

```bash
docker compose up -d
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic rider-location \
  --partitions 3 --replication-factor 1

npm install
npm run bridge      # MQTT -> Kafka
npm run aggregator  # Kafka -> H3 -> Redis
npm run api         # http://localhost:3000/heatmap

RIDER_ID=rider-001 npm run device
RIDER_ID=rider-002 npm run device
RIDER_ID=rider-003 npm run device
```

```
GET /heatmap?limit=20     hex cells by ping count, with centre and boundary
GET /riders/:id/cell      last known cell for one rider
```

Consumer lag per partition:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group aggregator
```

## Tracing

Set `OTEL_ENABLED=1` with an OTLP collector on `localhost:4318`. The bridge
injects trace context into Kafka message headers, so the producer and the
consumer appear under one trace id despite being separate processes either side
of a broker.

## Stack

Node.js · MQTT (Mosquitto) · Apache Kafka (KRaft) · Redis · H3 · Express ·
OpenTelemetry · Docker Compose
