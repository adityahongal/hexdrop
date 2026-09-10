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
  clientId: "bridge",
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

  await producer.send({
    topic: TOPIC,
    // same rider always hashes to the same partition, so that rider's events stay ordered
    messages: [{ key: riderId, value: payload.toString(), headers }],
  });
});

client.on("error", (err) => console.error("[bridge]", err.message));
