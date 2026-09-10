// Read API: serves the hex heatmap and the last known cell for a rider
import "./otel.js";
import express from "express";
import { createClient } from "redis";
import { cellToLatLng, cellToBoundary } from "h3-js";

const redis = createClient({ url: "redis://localhost:6379" });
await redis.connect();

const app = express();

app.get("/heatmap", async (req, res) => {
  const limit = Number(req.query.limit) || 20;
  const rows = await redis.zRangeWithScores("heatmap", 0, -1, { REV: true });

  res.json(
    rows.slice(0, limit).map(({ value: cell, score }) => {
      const [lat, lng] = cellToLatLng(cell);
      return { cell, count: score, center: { lat, lng }, boundary: cellToBoundary(cell) };
    })
  );
});

app.get("/riders/:id/cell", async (req, res) => {
  const cell = await redis.hGet("rider:cell", req.params.id);
  if (!cell) return res.status(404).json({ error: "unknown rider" });
  const [lat, lng] = cellToLatLng(cell);
  res.json({ riderId: req.params.id, cell, center: { lat, lng } });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`[api] http://localhost:${PORT}/heatmap`));
