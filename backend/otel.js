/**
 * OpenTelemetry bootstrap for the Node processes (API + outbox relay).
 *
 * Load it BEFORE the application, with a preload flag:
 *   node --import ./backend/otel.js backend/workers/outboxRelay.js
 *
 * The repo is ESM ("type": "module"), so require-style auto-instrumentation is not enough:
 * module.register() installs the import-in-the-middle loader hook that lets the instrumentations
 * patch modules loaded through `import`. Requires Node >= 18.19 / 20.6.
 *
 * No-op when OTEL_SDK_DISABLED=true.
 */
import path from "node:path";
import { register } from "node:module";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { diag, DiagConsoleLogger, DiagLogLevel } from "@opentelemetry/api";
import { NodeSDK } from "@opentelemetry/sdk-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { HttpInstrumentation } from "@opentelemetry/instrumentation-http";
import { ExpressInstrumentation } from "@opentelemetry/instrumentation-express";
import { MongoDBInstrumentation } from "@opentelemetry/instrumentation-mongodb";
import { KafkaJsInstrumentation } from "@opentelemetry/instrumentation-kafkajs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Same .env as backend/index.js and backend/workers/outboxRelay.js (dotenv never overrides real env vars).
dotenv.config({ path: path.join(__dirname, "../.env") });

if (String(process.env.OTEL_SDK_DISABLED).toLowerCase() === "true") {
  console.log("[otel] OTEL_SDK_DISABLED=true; tracing is off");
} else {
  // Optional SDK self-diagnostics: OTEL_LOG_LEVEL=debug|info|warn|error
  const levels = {
    error: DiagLogLevel.ERROR,
    warn: DiagLogLevel.WARN,
    info: DiagLogLevel.INFO,
    debug: DiagLogLevel.DEBUG,
    verbose: DiagLogLevel.VERBOSE,
    all: DiagLogLevel.ALL,
  };
  const diagLevel = levels[String(process.env.OTEL_LOG_LEVEL || "").toLowerCase()];
  if (diagLevel !== undefined) diag.setLogger(new DiagConsoleLogger(), diagLevel);

  // Jaeger all-in-one only ingests traces: never try to export metrics or logs.
  process.env.OTEL_TRACES_EXPORTER ||= "otlp";
  process.env.OTEL_METRICS_EXPORTER ||= "none";
  process.env.OTEL_LOGS_EXPORTER ||= "none";

  // One service name per process, without needing per-OS env syntax in package.json scripts.
  const entry = process.argv[1] ? path.basename(process.argv[1]) : "";
  process.env.OTEL_SERVICE_NAME ||= entry === "outboxRelay.js" ? "outbox-relay" : "shopify-backend";

  // Hook must be registered before any application module is imported.
  register("@opentelemetry/instrumentation/hook.mjs", import.meta.url);

  const sdk = new NodeSDK({
    // Reads OTEL_EXPORTER_OTLP_ENDPOINT / OTEL_EXPORTER_OTLP_TRACES_ENDPOINT (default http://localhost:4318).
    traceExporter: new OTLPTraceExporter(),
    instrumentations: [
      new HttpInstrumentation({
        ignoreIncomingRequestHook: (req) => {
          const url = req.url || "";
          return (
            req.method === "OPTIONS" ||
            url === "/api/health" ||
            url === "/healthz" ||
            url === "/livez"
          );
        },
      }),
      new ExpressInstrumentation(),
      // requireParentSpan: the relay polls Mongo every ~300ms when idle; without a parent those
      // would each become a parentless trace. Spans under an HTTP request (createOrder) still appear.
      new MongoDBInstrumentation({ requireParentSpan: true }),
      new KafkaJsInstrumentation(),
    ],
  });

  sdk.start();

  const endpoint =
    process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT ||
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT ||
    "http://localhost:4318";
  console.log(`[otel] tracing enabled service=${process.env.OTEL_SERVICE_NAME} endpoint=${endpoint}`);

  // Flush buffered spans on shutdown. Registered before the app's own handlers (preload), so at the
  // moment a signal fires, listenerCount > 1 means the app owns the exit (the relay does) and we
  // only flush; otherwise (the API has no SIGTERM handler) we flush and exit ourselves.
  let flushing = false;
  for (const signal of ["SIGTERM", "SIGINT"]) {
    process.on(signal, () => {
      if (flushing) return;
      flushing = true;
      const appOwnsExit = process.listenerCount(signal) > 1;
      sdk
        .shutdown()
        .catch((err) => console.error(`[otel] shutdown error: ${err?.message || err}`))
        .finally(() => {
          if (!appOwnsExit) process.exit(0);
        });
    });
  }
}