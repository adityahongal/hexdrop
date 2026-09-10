// Simulated rider device: publishes GPS over MQTT and announces liveness via Last Will
import mqtt from "mqtt";

const riderId = process.env.RIDER_ID || "rider-001";
const statusTopic = `fleet/${riderId}/status`;

// Pune city centre, each device drifts around its own start point
let lat = 18.5204 + (Math.random() - 0.5) * 0.05;
let lng = 73.8567 + (Math.random() - 0.5) * 0.05;

const client = mqtt.connect("mqtt://localhost:1883", {
  clientId: riderId,
  username: riderId,
  password: `${riderId}-secret`,
  clean: false,
  keepalive: 30,
  reconnectPeriod: 2000 + Math.floor(Math.random() * 3000),
  will: {
    topic: statusTopic,
    payload: JSON.stringify({ riderId, status: "offline" }),
    qos: 1,
    retain: true,
  },
});

client.on("connect", () => {
  console.log(`[${riderId}] connected`);

  // retained so a dashboard opening later sees current state immediately
  client.publish(statusTopic, JSON.stringify({ riderId, status: "online" }), {
    qos: 1,
    retain: true,
  });

  // QoS 0: a dropped ping is cheaper than retrying it, the next one is a second away
  setInterval(() => {
    lat += (Math.random() - 0.5) * 0.002;
    lng += (Math.random() - 0.5) * 0.002;
    client.publish(
      `fleet/${riderId}/gps`,
      JSON.stringify({ riderId, lat, lng, ts: Date.now() }),
      { qos: 0 }
    );
  }, 1000);
});

client.on("error", (err) => console.error(`[${riderId}]`, err.message));
