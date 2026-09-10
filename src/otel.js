// OpenTelemetry bootstrap: import this first so instrumentation can patch modules before they load
import { NodeSDK } from "@opentelemetry/sdk-node";
import { getNodeAutoInstrumentations } from "@opentelemetry/auto-instrumentations-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";

// off by default so the pipeline runs without a collector; set OTEL_ENABLED=1 with Tempo up
if (process.env.OTEL_ENABLED === "1") {
  const sdk = new NodeSDK({
    serviceName: process.env.OTEL_SERVICE_NAME || "service",
    traceExporter: new OTLPTraceExporter({
      url: process.env.OTEL_EXPORTER_OTLP_ENDPOINT || "http://localhost:4318/v1/traces",
    }),
    instrumentations: [getNodeAutoInstrumentations()],
  });
  sdk.start();
  process.on("SIGTERM", () => sdk.shutdown());
}
