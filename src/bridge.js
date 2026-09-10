// MQTT to Kafka bridge: republishes device telemetry into the durable log, keyed by rider
import "./otel.js";
import mqtt from "mqtt";
import { Kafka } from "kafkajs";
import { context, propagation } from "@opentelemetry/api";

const TOPIC = "rider-location";

const kafka = new Kafka({ clientId: "hexdrop-bridge", brokers: ["localhost:9092"] });
const producer = kafka.producer();
await producer.connect();

const client = mqtt.connect("mqtt://localhost:1883", {
  // stable id and persistent session so the broker holds QoS 1 subscriptions
  // across a restart. GPS is QoS 0 by design, so those pings are still dropped
  // while the bridge is down - see README on where the durability boundary sits
  clientId: "bridge",
  clean: false,
  keepalive: 30,
  reconnectPeriod: 2000 + Math.floor(Math.random() * 3000),
  username: "bridge",
  password: "bridge-secret",
});

client.on("connect", () => {
  console.log("[bridge] connected, subscribing to fleet/+/gps");
  client.subscribe("fleet/+/gps", { qos: 0 });
});

client.on("message", async (topic, payload) => {
  const [, riderId] = topic.split("/");

  // carry the current trace across the broker boundary so producer and consumer share one trace id
  const headers = {};
  propagation.inject(context.active(), headers);

  try {
    await producer.send({
      topic: TOPIC,
      // same rider always hashes to the same partition, so that rider's events stay ordered
      messages: [{ key: riderId, value: payload.toString(), headers }],
    });
  } catch (err) {
    // without this the rejection is unhandled and takes the whole bridge down
    console.error("[bridge] kafka send failed, ping dropped:", err.message);
  }
});

client.on("error", (err) => console.error("[bridge]", err.message));

const shutdown = async () => {
  client.end(true);
  await producer.disconnect().catch(() => {});
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
