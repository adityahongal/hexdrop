// Registers the OpenTelemetry ESM loader hook.
// Without this the auto-instrumentations silently patch nothing in an ESM project.
import { register } from "node:module";
import { pathToFileURL } from "node:url";

register("@opentelemetry/instrumentation/hook.mjs", pathToFileURL("./"));
