// Kafka consumer: maps each ping to an H3 hex cell and counts it in Redis
import "./otel.js";
import { Kafka } from "kafkajs";
import { latLngToCell } from "h3-js";
import { createClient } from "redis";

const TOPIC = "rider-location";
const RESOLUTION = 8; // roughly 0.7 km2 per cell

const redis = createClient({ url: "redis://localhost:6379" });
await redis.connect();

const kafka = new Kafka({ clientId: "hexdrop-aggregator", brokers: ["localhost:9092"] });
const consumer = kafka.consumer({ groupId: "aggregator" });
const producer = kafka.producer();

await consumer.connect();
await producer.connect();
await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

await consumer.run({
  autoCommit: false,
  eachMessage: async ({ topic, partition, message }) => {
    try {
      const { riderId, lat, lng } = JSON.parse(message.value.toString());
      const cell = latLngToCell(lat, lng, RESOLUTION);

      // the hex cell is the key: bounded cardinality, unlike raw lat/lng
      await redis.zIncrBy("heatmap", 1, cell);
      await redis.hSet("rider:cell", riderId, cell);

      console.log(`p${partition} ${riderId} -> ${cell}`);
    } catch (err) {
      // park the bad message, otherwise the offset never advances and this partition stalls
      await producer.send({
        topic: `${topic}.dlq`,
        messages: [{ key: message.key, value: message.value }],
      });
      console.error("[aggregator] parked poison message:", err.message);
    }

    await consumer.commitOffsets([
      { topic, partition, offset: (Number(message.offset) + 1).toString() },
    ]);
  },
});
