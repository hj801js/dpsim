"""DPsim worker — consumes dpsim-api AMQP messages, runs dpsimpy, returns results.

Features:
  - Credential / endpoint discovery via environment variables (validated at
    startup via check_config()).
  - Redis simulation state tracking (status, error, warnings).
  - Dead-letter queue: messages that fail AMQP_MAX_RETRY times are parked on
    dpsim-worker-queue.dlq instead of being silently dropped.
  - Structured JSON logging.
  - Prefetch 1 (see note on AMQP_PREFETCH below — Logger.set_log_dir is a
    process-global that cannot be shared across concurrent jobs).
  - Graceful SIGTERM: stops consuming, lets the in-flight job finish.
  - CIM SystemTopology is rebuilt per job. Do NOT cache — see docs/20.

Production extensions: multiprocessing workers, Prometheus metrics,
OpenTelemetry tracing, object-store upload in place of the in-memory
file-service stub.
"""
from __future__ import annotations

import contextvars
import glob
import json
import logging
import os
import signal
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Any

import pika
import redis
import requests

try:
    from prometheus_client import (
        Counter as _PromCounter,
        Histogram as _PromHistogram,
        Gauge as _PromGauge,
        start_http_server as _prom_start_http_server,
    )
    _PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover — optional at dev time
    _PROMETHEUS_AVAILABLE = False

try:
    import boto3
    from botocore.client import Config as _BotoConfig
    _BOTO_AVAILABLE = True
except ImportError:  # pragma: no cover
    _BOTO_AVAILABLE = False

# P2.2 — OpenTelemetry. Opt-in via OTEL_EXPORTER_OTLP_ENDPOINT; gracefully
# degrades to no-op tracer when the library or the collector isn't there.
try:
    from opentelemetry import trace as _otel_trace
    from opentelemetry.sdk.resources import Resource as _OtelResource
    from opentelemetry.sdk.trace import TracerProvider as _OtelTracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor as _OtelBatchSpanProcessor
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
        OTLPSpanExporter as _OtelHttpExporter,
    )
    from opentelemetry.propagate import extract as _otel_extract, inject as _otel_inject
    _OTEL_AVAILABLE = True
except ImportError:  # pragma: no cover
    _OTEL_AVAILABLE = False

import dpsimpy


# ---------------------------------------------------------------------------
# Configuration (all env-driven; sensible fallbacks for local brew services)
# ---------------------------------------------------------------------------
AMQP_HOST = os.environ.get("AMQP_HOST", "localhost")
AMQP_PORT = int(os.environ.get("AMQP_PORT", 5672))
AMQP_USER = os.environ.get("AMQP_USER", "guest")
AMQP_PASS = os.environ.get("AMQP_PASS", "guest")
AMQP_VHOST = os.environ.get("AMQP_VHOST", "/")
AMQP_QUEUE = os.environ.get("AMQP_QUEUE", "dpsim-worker-queue")
AMQP_DLQ = AMQP_QUEUE + ".dlq"
AMQP_MAX_RETRY = int(os.environ.get("AMQP_MAX_RETRY", 3))

# Prefetch is intentionally 1. dpsimpy.Logger.set_log_dir() is a process-global,
# so concurrent jobs in the same process would race on the log directory and
# write each other's CSV. To scale out, run multiple worker processes (each
# with its own dpsimpy state) rather than raising prefetch here. See docs/21.
AMQP_PREFETCH = int(os.environ.get("AMQP_PREFETCH", 1))

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
FILE_SERVICE_URL = os.environ.get("FILE_SERVICE_URL", "http://127.0.0.1:18080")

# MinIO / S3-compatible object store (Phase 2.4). Opt-in — if MINIO_ENDPOINT
# is set, CSV results upload goes through boto3 in addition to the legacy
# file-service. Bucket must exist; worker does not create it.
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")  # e.g. http://localhost:9000
MINIO_BUCKET   = os.environ.get("MINIO_BUCKET", "dpsim-results")
MINIO_ACCESS   = os.environ.get("MINIO_ACCESS_KEY", "dpsim")
MINIO_SECRET   = os.environ.get("MINIO_SECRET_KEY", "dpsim12345")

# Prometheus metrics HTTP port. 0 disables the exporter (e.g. pytest runs).
PROMETHEUS_PORT = int(os.environ.get("DPSIM_PROMETHEUS_PORT", 9109))

JOBS_DIR = Path(os.environ.get("DPSIM_JOBS_DIR", "/tmp/dpsim_jobs"))
JOBS_DIR.mkdir(parents=True, exist_ok=True)

# Hours to keep per-job output directories. Set to 0 to disable purging.
# The worker sweeps on startup and then hourly on a daemon thread.
JOBS_RETENTION_HOURS = float(os.environ.get("DPSIM_JOBS_RETENTION_HOURS", 24))

# DPsim build dir to find bundled CIM test data. Override to your clone.
DPSIM_BUILD = Path(os.environ.get(
    "DPSIM_BUILD_DIR",
    Path(__file__).resolve().parents[2] / "build",
))

DOMAIN_MAP = {
    "SP": dpsimpy.Domain.SP,
    "DP": dpsimpy.Domain.DP,
    "EMT": dpsimpy.Domain.EMT,
}

# Known CIM bundles, keyed by the file-service model_id (tail of the model
# URL, with any extension stripped). Extend this dict to accept more models.
CIM_BUNDLES = {
    "wscc9":    sorted(glob.glob(str(DPSIM_BUILD / "_deps/cim-data-src/WSCC-09/WSCC-09/*.xml"))),
    "ieee14":   sorted(glob.glob(str(DPSIM_BUILD / "_deps/cim-data-src/IEEE-14/*.xml"))),
    "ieee39":   sorted(glob.glob(str(DPSIM_BUILD / "_deps/cim-data-src/IEEE-39/*.xml"))),
    "cigre_mv": sorted(glob.glob(str(
        DPSIM_BUILD / "_deps/cim-data-src/CIGRE_MV/NEPLAN/"
                      "CIGRE_MV_no_tapchanger_noLoad1_LeftFeeder_With_LoadFlow_Results/*.xml"
    ))),
    # Matpower single-file cases. Each XML bundles EQ+TP in one, which is
    # exactly what CIMReader prefers when there's no multi-file profile.
    "matpower_case9":   sorted(glob.glob(str(DPSIM_BUILD / "_deps/cim-data-src/Matpower_cases/case9.xml"))),
    "matpower_case14":  sorted(glob.glob(str(DPSIM_BUILD / "_deps/cim-data-src/Matpower_cases/case14.xml"))),
    "matpower_case300": sorted(glob.glob(str(DPSIM_BUILD / "_deps/cim-data-src/Matpower_cases/case300.xml"))),
}

MAX_FINAL_TIME_SEC = 30.0
MIN_TIMESTEP_SEC = 1e-5

# API convention: SimulationForm.timestep and .finaltime are integers and
# interpreted as MILLISECONDS. The dpsim C++ engine expects seconds, so we
# divide by 1000 at the worker boundary. Typical DP: 1 ms step, 1000 ms
# duration. Typical EMT: 0.01–0.1 ms step.
TIMEUNIT_MS_TO_SEC = 1e-3

# ZIP local-file-header magic. Must stay in lock-step with dpsim-api's
# ZIP_MAGIC in routes.rs — an upload the API accepts as a ZIP is resolved
# the same way here, and vice versa.
ZIP_MAGIC = b"PK\x03\x04"


# ---------------------------------------------------------------------------
# Prometheus metrics — counters / histogram / gauge all no-op if the client
# library is missing so unit tests run without the dep. Exporter starts in
# main(); pytest doesn't import main so nothing binds to a port.
# ---------------------------------------------------------------------------
class _NoopMetric:
    def labels(self, **_):
        return self
    def inc(self, *_a, **_k):
        pass
    def dec(self, *_a, **_k):
        pass
    def observe(self, *_a, **_k):
        pass
    def set(self, *_a, **_k):
        pass


if _PROMETHEUS_AVAILABLE:
    JOBS_TOTAL = _PromCounter(
        "dpsim_worker_jobs_total",
        "Total simulation jobs processed by the worker, labelled by outcome.",
        ["outcome", "domain"],
    )
    JOB_DURATION = _PromHistogram(
        "dpsim_worker_job_duration_seconds",
        "Wall-clock duration of sim.run() per job (not including upload).",
        ["domain"],
        buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
    )
    DLQ_TOTAL = _PromCounter(
        "dpsim_worker_dlq_total",
        "Messages that exhausted AMQP_MAX_RETRY and were parked on the DLQ.",
    )
    INFLIGHT = _PromGauge(
        "dpsim_worker_inflight_jobs",
        "Jobs currently being processed (0..AMQP_PREFETCH).",
    )
    QUEUE_DEPTH = _PromGauge(
        "dpsim_worker_queue_depth",
        "Messages waiting on an AMQP queue as seen by the worker's passive declare.",
        ["queue"],
    )
else:
    JOBS_TOTAL = _NoopMetric()
    JOB_DURATION = _NoopMetric()
    DLQ_TOTAL = _NoopMetric()
    INFLIGHT = _NoopMetric()
    QUEUE_DEPTH = _NoopMetric()


# ---------------------------------------------------------------------------
# OpenTelemetry (Jaeger-bound) tracer. No-op when OTEL_EXPORTER_OTLP_ENDPOINT
# is unset so `make up` stays portable.
# ---------------------------------------------------------------------------
class _NoopTracer:
    def start_as_current_span(self, *_a, **_kw):
        from contextlib import nullcontext
        return nullcontext(_NoopSpan())
    def start_span(self, *_a, **_kw):
        return _NoopSpan()


class _NoopSpan:
    def set_attribute(self, *_a, **_kw):
        pass
    def set_status(self, *_a, **_kw):
        pass
    def record_exception(self, *_a, **_kw):
        pass
    def end(self):
        pass
    def __enter__(self):
        return self
    def __exit__(self, *_):
        return False


_OTEL_ENDPOINT = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")

if _OTEL_AVAILABLE and _OTEL_ENDPOINT:
    # Resource.attributes land on every span — Jaeger uses service.name as
    # the top-level tree label.
    _otel_resource = _OtelResource.create({"service.name": "dpsim-worker"})
    _otel_provider = _OtelTracerProvider(resource=_otel_resource)
    # OTLP HTTP endpoint convention: <base>/v1/traces. We accept either the
    # bare base (http://jaeger:4318) or an explicit /v1/traces path.
    _otel_url = _OTEL_ENDPOINT.rstrip("/")
    if not _otel_url.endswith("/v1/traces"):
        _otel_url = f"{_otel_url}/v1/traces"
    _otel_provider.add_span_processor(
        _OtelBatchSpanProcessor(_OtelHttpExporter(endpoint=_otel_url))
    )
    _otel_trace.set_tracer_provider(_otel_provider)
    tracer = _otel_trace.get_tracer("dpsim-worker")
else:
    tracer = _NoopTracer()


# ---------------------------------------------------------------------------
# Structured JSON logging
# ---------------------------------------------------------------------------
class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)),
            "level": record.levelname,
            "msg": record.getMessage(),
        }
        # attach any extra context the caller passed in
        for k, v in record.__dict__.items():
            if k.startswith("ctx_"):
                payload[k[4:]] = v
        return json.dumps(payload, default=str)


logger = logging.getLogger("dpsim-worker")
logger.setLevel(logging.INFO)
h = logging.StreamHandler(sys.stdout)
h.setFormatter(JsonFormatter())
logger.addHandler(h)


# P2.2 trace-id propagation. dpsim-api stamps x-trace-id into the AMQP
# headers and inlines it in parameters; on_msg pulls whichever is present
# into this contextvar, and every log() / set_status() / set_progress()
# call pulls it back out — no plumbing through every function arg.
CURRENT_TRACE_ID: contextvars.ContextVar[str] = contextvars.ContextVar(
    "dpsim_trace_id", default="")


def log(msg: str, level: str = "info", **context: Any) -> None:
    tid = context.pop("trace_id", None) or CURRENT_TRACE_ID.get()
    if tid:
        context["trace_id"] = tid
    extra = {f"ctx_{k}": v for k, v in context.items()}
    getattr(logger, level)(msg, extra=extra)


# ---------------------------------------------------------------------------
# Redis helpers — simple status field per simulation id.
# dpsim-api writes the main record; we only touch `status` / `error`.
# Store uses a side-channel key so we don't fight Rust over JSON schema.
# ---------------------------------------------------------------------------
_redis = redis.from_url(REDIS_URL, decode_responses=True)


def set_status(
    sim_id: str | int,
    status: str,
    error: str | None = None,
    warnings: list[str] | None = None,
    progress: float | None = None,
) -> None:
    key = f"dpsim:sim:{sim_id}:status"
    value: dict[str, Any] = {"status": status}
    if error is not None:
        value["error"] = error
    if warnings:
        value["warnings"] = warnings
    if progress is not None:
        # Clamp to [0, 100] so UI never shows >100% or negative values.
        value["progress"] = max(0.0, min(100.0, float(progress)))
    tid = CURRENT_TRACE_ID.get()
    if tid:
        value["trace_id"] = tid
    _redis.set(key, json.dumps(value))
    log("status update", sim_id=sim_id, status=status, error=error or "",
        warnings=warnings or [], progress=value.get("progress", ""))


def set_progress(sim_id: str | int, progress: float) -> None:
    """Update only the `progress` field of the existing status key.

    Background progress watcher calls this every ~200ms while the simulator is
    running, so we avoid bouncing status back to "running" or dropping accrued
    warnings. Best-effort — a stale status (job completed in parallel) is
    merely overwritten with "running" and re-corrected by the completion path.
    """
    key = f"dpsim:sim:{sim_id}:status"
    try:
        raw = _redis.get(key)
        current = json.loads(raw) if raw else {"status": "running"}
    except Exception:
        current = {"status": "running"}
    current["progress"] = max(0.0, min(100.0, float(progress)))
    _redis.set(key, json.dumps(current))


def get_status(sim_id: str | int) -> dict[str, Any] | None:
    raw = _redis.get(f"dpsim:sim:{sim_id}:status")
    return json.loads(raw) if raw else None


class _ProgressWatcher:
    """Sample the CSV output file while dpsim runs and publish an approximate
    progress percentage to redis. Approximate because dpsimpy doesn't expose a
    step callback — we infer progress from row count vs expected rows.
    Runs as a daemon thread; stops when `stop()` is set or the file vanishes.
    """

    def __init__(self, sim_id: str | int, csv_path: Path,
                 expected_rows: int, poll_interval: float = 0.2) -> None:
        self.sim_id = sim_id
        self.csv_path = csv_path
        # +1 for the header row dpsim writes once at the start.
        self.expected_rows = max(1, expected_rows)
        self.poll_interval = poll_interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True,
                                         name=f"progress-{sim_id}")

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=1.0)

    def _loop(self) -> None:
        last_sent = -1.0
        while not self._stop.is_set():
            try:
                if self.csv_path.exists():
                    # Cheap row estimate: file size / avg_line_bytes is wrong
                    # for WSCC-9 (19 cols, ~300 bytes/line) vs demo (3 cols, ~60
                    # bytes). Use line count — file grows append-only.
                    with self.csv_path.open("rb") as f:
                        rows = sum(1 for _ in f) - 1  # minus header
                    pct = min(95.0, max(0.0, rows / self.expected_rows * 100.0))
                    # Cap at 95% during run; completion path writes 100%.
                    if pct - last_sent >= 1.0:
                        set_progress(self.sim_id, pct)
                        last_sent = pct
            except Exception:
                # Never let the watcher kill the job — best-effort only.
                pass
            if self._stop.wait(self.poll_interval):
                return


# ---------------------------------------------------------------------------
# CIM topology builder (no cache).
#
# A SystemTopology is single-use: dpsim mutates the inductor/capacitor
# internal state on each sim.run(), and a second sim.run() on the same
# object begins from that mutated state rather than the CIM SV snapshot.
# Reusing cached topologies produced fresh-looking first runs followed by
# severe voltage decay on subsequent jobs — see the root-cause analysis
# in docs/19_gui-extension-plan.md / docs/20_dp-decay-root-cause.md.
# Always parse the CIM files afresh per job.
# ---------------------------------------------------------------------------
def build_cim_topology(sim_name: str, files: list[str], domain: Any, freq: float = 60):
    reader = dpsimpy.CIMReader(sim_name)
    return reader.loadCIM(
        freq, list(files), domain,
        dpsimpy.PhaseType.Single,
        dpsimpy.GeneratorType.IdealVoltageSource,
    )


# ---------------------------------------------------------------------------
# Topology builders
# ---------------------------------------------------------------------------
MODELS_CACHE_DIR = Path(os.environ.get(
    "DPSIM_MODELS_CACHE_DIR", "/tmp/dpsim_models"))
MODELS_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _extract_model_id(url: str) -> str:
    """Strip query/fragment/extension from a file-service URL tail."""
    tail = url.rsplit("/", 1)[-1].lower()
    return tail.split("?", 1)[0].split("#", 1)[0].rsplit(".", 1)[0]


def _resolve_uploaded_model(model_id: str) -> list[str]:
    """Materialize an uploaded model onto local disk and return the list of
    .xml files to feed the CIMReader. Supports two body formats:

    * raw CIM XML — stored as `<cache>/<id>/<id>.xml`
    * ZIP of XMLs — extracted to `<cache>/<id>/*.xml`

    On repeat calls the cache directory is reused. Returns [] on any error
    so the caller can fall back to the demo topology with a clear warning.
    """
    import io
    import zipfile

    cache = MODELS_CACHE_DIR / model_id
    if cache.is_dir():
        existing = sorted(str(p) for p in cache.glob("*.xml"))
        if existing:
            return existing
    cache.mkdir(parents=True, exist_ok=True)

    # file-service's GET /api/files/<id> returns {data: {url: "<raw>"}}.
    # We hit that raw URL to fetch the bytes.
    try:
        meta = requests.get(
            f"{FILE_SERVICE_URL}/api/files/{model_id}", timeout=(3, 10),
        )
        meta.raise_for_status()
        raw_url = meta.json().get("data", {}).get("url")
        if not raw_url:
            return []
        # raw_url is relative to file-service (e.g. "/raw/<id>"). Resolve.
        if raw_url.startswith("/"):
            raw_url = f"{FILE_SERVICE_URL}{raw_url}"
        payload = requests.get(raw_url, timeout=(5, 30))
        payload.raise_for_status()
        body = payload.content
    except Exception:
        return []

    # Sniff the magic bytes.
    if body[:4] == ZIP_MAGIC:
        try:
            with zipfile.ZipFile(io.BytesIO(body)) as z:
                # Defense against ZIP slip: reject any entry whose resolved
                # path escapes the per-model cache dir (absolute paths,
                # `../..` traversal, symlinks). stdlib extractall doesn't
                # guard against this on Linux as of 3.11.
                cache_resolved = cache.resolve()
                for info in z.infolist():
                    dest = (cache / info.filename).resolve()
                    try:
                        dest.relative_to(cache_resolved)
                    except ValueError:
                        logger.warning(
                            "rejected zip entry outside cache dir: %s",
                            info.filename,
                        )
                        return []
                z.extractall(cache)
        except Exception:
            return []
    else:
        (cache / f"{model_id}.xml").write_bytes(body)

    # Collect XMLs at any depth (some bundles have a top-level folder).
    return sorted(str(p) for p in cache.rglob("*.xml"))


def _find_cim_bundle(payload: dict[str, Any]) -> tuple[str | None, list[str]]:
    """Resolve the payload's model URL list to a concrete CIM file set.

    Priority:
      1. hard-coded builds baked into dpsimpy (`CIM_BUNDLES`)
      2. uploaded bundle cached by file-service model_id
    Returns (token, files) — token is the cache subdir name or builtin key.
    """
    for url in payload.get("model", {}).get("url", []):
        model_id = _extract_model_id(url)
        # 1 — baked bundles
        files = CIM_BUNDLES.get(model_id)
        if files:
            return model_id, files
        # 2 — uploaded bundle
        uploaded = _resolve_uploaded_model(model_id)
        if uploaded:
            return model_id, uploaded
    return None, []


def _collapse_load_series(
    series: list[dict[str, Any]],
    sim_type: str,
    finaltime_sec: float,
) -> float | None:
    """P3.3 time-series MVP — reduce a (t_sec, factor) series to a scalar.

    Rules:
      * Powerflow is steady-state; the worst-case/conservative pick is max.
        Users running a Powerflow "profile" are almost always looking for
        peak-load stress response, so we emit that.
      * DP / EMT are time-stepped but dpsim's Python API doesn't expose a
        PreStep hook we can cleanly use to rescale per step. As a
        pragmatic stand-in we linearly interpolate the series value at
        finaltime_sec and apply that — i.e. the sim runs as if the load
        ramp had settled to its end-of-horizon point.
    """
    if not series:
        return None
    clean: list[tuple[float, float]] = []
    for pt in series:
        try:
            t = float(pt.get("t_sec", 0))
            f = float(pt.get("factor", 1.0))
        except (TypeError, ValueError):
            continue
        clean.append((t, f))
    if not clean:
        return None
    if sim_type.lower() == "powerflow":
        return max(f for _, f in clean)
    # Time-stepped: interpolate at finaltime.
    clean.sort(key=lambda p: p[0])
    if finaltime_sec <= clean[0][0]:
        return clean[0][1]
    if finaltime_sec >= clean[-1][0]:
        return clean[-1][1]
    for i in range(1, len(clean)):
        t0, f0 = clean[i - 1]
        t1, f1 = clean[i]
        if t0 <= finaltime_sec <= t1:
            if t1 == t0:
                return f0
            return f0 + (f1 - f0) * (finaltime_sec - t0) / (t1 - t0)
    return clean[-1][1]


def _apply_load_factor(files: list[str], factor: float, sim_id: str) -> tuple[list[str], str]:
    """P3.3 — scale every load's SvPowerFlow p/q by `factor`. Mutation at the
    CIM SV level so CIMReader sees pre-scaled values; downstream dpsim
    topology construction needs no extra code.

    Identifies loads by rdf:ID prefix "LOAD" (the convention CIMpp uses for
    every EnergyConsumer it serializes). Generators (GEN*) stay untouched so
    the slack bus still injects whatever the load draws.
    """
    import shutil
    import xml.etree.ElementTree as ET

    ET.register_namespace("rdf",    "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    ET.register_namespace("cim",    "http://iec.ch/TC57/2012/CIM-schema-cim16#")
    ET.register_namespace("md",     "http://iec.ch/TC57/61970-552/ModelDescription/1#")
    ET.register_namespace("entsoe", "http://entsoe.eu/Secretariat/ProfileExtension/2#")
    rdf_id = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}ID"

    if factor == 1.0 or factor <= 0:
        return files, "skipped"

    # Reuse the cim-outage dir if it already exists (outage + load factor can
    # stack cleanly — outage mutates EQ, load factor mutates SV).
    out_dir = JOBS_DIR / sim_id / "cim-outage"
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        new_files: list[str] = []
        for src in files:
            dst = out_dir / Path(src).name
            if not dst.exists():
                shutil.copyfile(src, dst)
            new_files.append(str(dst))
    except Exception:
        return files, "skipped"

    applied = False
    for path in new_files:
        try:
            tree = ET.parse(path)
            root = tree.getroot()
        except Exception:
            continue
        changed = False
        for sv in list(root):
            if not sv.tag.endswith("SvPowerFlow"):
                continue
            sv_id = sv.attrib.get(rdf_id, "")
            # Only scale loads. Gens (GEN*) keep their injections.
            if not sv_id.upper().startswith("LOAD"):
                continue
            for child in sv:
                if child.tag.endswith("SvPowerFlow.p") or child.tag.endswith("SvPowerFlow.q"):
                    try:
                        child.text = str(float(child.text) * factor)
                        changed = True
                    except (TypeError, ValueError) as exc:
                        # Non-numeric or missing child.text — not fatal, just
                        # means this entry stays unscaled. Log with the sv_id
                        # so operators can see which load was skipped.
                        logger.debug(
                            "load_factor skip sv=%s tag=%s text=%r: %s",
                            sv_id, child.tag, child.text, exc,
                        )
        if changed:
            tree.write(path, encoding="utf-8", xml_declaration=True)
            applied = True

    return new_files, ("applied" if applied else "not-found")


def _apply_outage(files: list[str], component_name: str, sim_id: str) -> tuple[list[str], str]:
    """P3.4 — copy the CIM bundle to a per-job dir and bump r/x of the target
    equipment by 1000× so the solver treats it as effectively open.

    Supports two element types:
      * cim:ACLineSegment    — bump ACLineSegment.r and .x
      * cim:PowerTransformerEnd — bump PowerTransformerEnd.r, .x, .r0, .x0
        (both ends share cim:IdentifiedObject.name = PowerTransformer.name,
         so one component_name matches both ends and scales the series
         impedance uniformly).

    Returns (new_files, status) where status is one of:
      * "applied"   — match found and mutated
      * "not-found" — bundle copied but no matching element
      * "skipped"   — something went wrong; original files returned unchanged

    Using a 1000× bump instead of rip-out-the-element because the latter
    would also require removing dangling Terminals and may desync
    TopologicalIsland references in the TP/SV files. r/x bump keeps the
    topology graph intact while making the element carry ~0 current.
    """
    import shutil
    import xml.etree.ElementTree as ET

    # Register namespaces globally so ET.write emits rdf:RDF + cim:* prefixes
    # exactly as CIMpp expects. Without this the root element comes out as
    # the "default" namespace (ns0:RDF) and the CIM parser aborts with
    # "Nobody knows the cim:RDF I've seen".
    ET.register_namespace("rdf",    "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    ET.register_namespace("cim",    "http://iec.ch/TC57/2012/CIM-schema-cim16#")
    ET.register_namespace("md",     "http://iec.ch/TC57/61970-552/ModelDescription/1#")
    ET.register_namespace("entsoe", "http://entsoe.eu/Secretariat/ProfileExtension/2#")

    # Some bundles use a different CIM version; accept any "CIM-schema-cim" NS.
    def is_cim_ns(tag: str) -> bool:
        return tag.startswith("{http://iec.ch/TC57/") and "CIM-schema" in tag

    out_dir = JOBS_DIR / sim_id / "cim-outage"
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        new_files: list[str] = []
        for src in files:
            dst = out_dir / Path(src).name
            shutil.copyfile(src, dst)
            new_files.append(str(dst))
    except Exception:
        return files, "skipped"

    # Suffixes of elements we can scale, and which child impedance fields to
    # multiply. Keep ACLineSegment first so a component name that collides
    # (unlikely but possible) prefers the line.
    outage_targets = [
        ("ACLineSegment",      ("ACLineSegment.r", "ACLineSegment.x")),
        ("PowerTransformerEnd", ("PowerTransformerEnd.r", "PowerTransformerEnd.x",
                                 "PowerTransformerEnd.r0", "PowerTransformerEnd.x0")),
    ]

    applied = False
    for path in new_files:
        try:
            tree = ET.parse(path)
            root = tree.getroot()
        except Exception:
            continue
        changed = False
        for seg in list(root):
            if not is_cim_ns(seg.tag):
                continue
            # Match any outage target whose tag ends with one of the suffixes.
            match_fields = None
            for suffix, fields in outage_targets:
                if seg.tag.endswith(suffix):
                    match_fields = fields
                    break
            if match_fields is None:
                continue
            name_el = None
            for child in seg:
                if child.tag.endswith("IdentifiedObject.name"):
                    name_el = child
                    break
            if name_el is None or (name_el.text or "").strip() != component_name:
                continue
            # Found the target. Bump the listed impedance fields by 1000×.
            for child in seg:
                local_tag = child.tag.rsplit("}", 1)[-1] if "}" in child.tag else child.tag
                if local_tag in match_fields:
                    try:
                        child.text = str(float(child.text) * 1000.0)
                        changed = True
                    except Exception:
                        pass
        if changed:
            tree.write(path, encoding="utf-8", xml_declaration=True)
            applied = True

    return new_files, ("applied" if applied else "not-found")


def _build_demo_topology(domain: Any = None):
    """Tiny two-bus demo circuit. Built in whichever simulation domain the
    caller asks for (DP / EMT / SP); shape is identical, only the module
    namespace differs. P3.1: EMT works on programmatic topologies even
    though dpsimpy.CIMReader + EMT still segfaults — so we surface EMT for
    demo users and only fall back to DP when CIM is the model source."""
    # Defaults to DP for backwards compat.
    dom = domain if domain is not None else dpsimpy.Domain.DP
    ns = {
        dpsimpy.Domain.DP:  dpsimpy.dp,
        dpsimpy.Domain.EMT: dpsimpy.emt,
        dpsimpy.Domain.SP:  dpsimpy.sp,
    }.get(dom, dpsimpy.dp)

    gnd = ns.SimNode.gnd
    n1 = ns.SimNode("n1")
    n2 = ns.SimNode("n2")
    vs = ns.ph1.VoltageSource("vs")
    vs.set_parameters(V_ref=complex(10000, 0), f_src=50)
    rline = ns.ph1.Resistor("r_line")
    rline.set_parameters(R=1.0)
    rload = ns.ph1.Resistor("r_load")
    rload.set_parameters(R=100.0)
    vs.connect([gnd, n1])
    rline.connect([n1, n2])
    rload.connect([n2, gnd])
    sys = dpsimpy.SystemTopology(50, [n1, n2], [vs, rline, rload])
    return sys, {"v1": n1, "v2": n2}, {"i_line": rline}


def clamp_params(p: dict[str, Any]) -> tuple[float, float, list[str]]:
    """Return (timestep_sec, finaltime_sec, warnings) from an API payload."""
    warnings: list[str] = []
    ts_raw = float(p.get("timestep", 1)) * TIMEUNIT_MS_TO_SEC
    ft_raw = float(p.get("finaltime", 100)) * TIMEUNIT_MS_TO_SEC

    ts = max(ts_raw, MIN_TIMESTEP_SEC)
    if ts != ts_raw:
        warnings.append(
            f"timestep clamped from {ts_raw*1000:g} ms to {ts*1000:g} ms (min {MIN_TIMESTEP_SEC*1000:g} ms)"
        )

    ft_min = 10 * ts
    ft = min(max(ft_raw, ft_min), MAX_FINAL_TIME_SEC)
    if ft != ft_raw:
        if ft_raw < ft_min:
            warnings.append(
                f"finaltime raised from {ft_raw*1000:g} ms to {ft*1000:g} ms (must be ≥ 10× timestep)"
            )
        else:
            warnings.append(
                f"finaltime clamped from {ft_raw:g} s to {ft:g} s (max {MAX_FINAL_TIME_SEC:g} s)"
            )
    return ts, ft, warnings


def run_simulation(payload: dict[str, Any]) -> dict[str, Any]:
    params = payload["parameters"]
    results_file = params.get("results_file", "anon")
    sim_id = params.get("simulation_id", results_file)

    job_dir = JOBS_DIR / results_file
    job_dir.mkdir(parents=True, exist_ok=True)

    set_status(sim_id, "running")

    dom_name = params.get("domain", "DP")
    domain = DOMAIN_MAP.get(dom_name, dpsimpy.Domain.DP)
    warnings: list[str] = []
    # EMT resolution (P3.1): EMT runs fine on programmatic topologies but
    # segfaults inside CIMpp. Decide the actual domain only after we know
    # whether CIM is the model source (that decision happens below when we
    # call _find_cim_bundle); default to the requested domain for now and
    # override to DP later if we're on the CIM path.
    actual_domain = domain

    timestep, finaltime, clamp_warnings = clamp_params(params)
    warnings.extend(clamp_warnings)

    # Load profile: dpsim-api wraps the URL as {"type":"url-list","url":[...]} when
    # the user supplied one, else leaves the field as an empty string. The worker
    # has no time-series Load support yet, so surface the fact to the user
    # instead of silently dropping their input.
    lp = payload.get("load_profile")
    lp_urls = lp.get("url") if isinstance(lp, dict) else None
    if lp_urls:
        warnings.append(
            f"load_profile ({lp_urls[0]}) ignored — time-series Load wiring "
            f"not yet implemented (docs/24 Phase 3.3)"
        )

    sim_name = f"job_{results_file}"
    # Logger.set_log_dir is a dpsimpy process-global; relying on AMQP_PREFETCH=1
    # to guarantee no two jobs race on it. Do not raise prefetch without moving
    # to a per-process isolation model.
    dpsimpy.Logger.set_log_dir(str(job_dir))
    logger_cim = dpsimpy.Logger(sim_name)

    token, cim_files = _find_cim_bundle(payload)
    # P3.4 — apply outage mutation before CIMReader parses the files.
    outage_target = params.get("outage_component")
    outage_status = None
    if cim_files and outage_target:
        cim_files, outage_status = _apply_outage(cim_files, outage_target, str(sim_id))
        if outage_status == "applied":
            warnings.append(f"outage applied: ACLineSegment '{outage_target}' r/x ×1000")
        elif outage_status == "not-found":
            warnings.append(f"outage target '{outage_target}' not found in CIM; running baseline")
        else:
            warnings.append(f"outage mutation skipped (io error); running baseline")
    # P3.3 — scalar load factor. Stacks with outage by reusing the cim-outage dir.
    # Optional time-series load_factor_series ([{t_sec, factor}, ...]) is
    # collapsed to an effective scalar here: max for steady-state Powerflow
    # (stress-test intent), end-of-run interpolated value for DP/EMT. True
    # per-step time-stepping isn't exposed by dpsim's Python API.
    load_factor = params.get("load_factor")
    series = params.get("load_factor_series")
    if series:
        sim_type_for_series = str(params.get("simulation_type", ""))
        effective = _collapse_load_series(series, sim_type_for_series, finaltime)
        if effective is not None:
            if load_factor is not None and float(load_factor) != 1.0:
                # Combine: multiply. User-set scalar × series-derived factor.
                effective = effective * float(load_factor)
            load_factor = effective
            warnings.append(
                f"load profile ({len(series)} points) collapsed to effective "
                f"×{effective:g} for {sim_type_for_series.lower()}"
            )
    load_factor_status = None
    if cim_files and load_factor is not None and float(load_factor) != 1.0:
        cim_files, load_factor_status = _apply_load_factor(cim_files, float(load_factor), str(sim_id))
        if load_factor_status == "applied":
            warnings.append(f"load factor applied: ×{float(load_factor):g} on all LOAD* SvPowerFlow entries")
        elif load_factor_status == "not-found":
            warnings.append(f"load factor skipped: no LOAD* SvPowerFlow entries in CIM")
        else:
            warnings.append(f"load factor skipped (io error)")
    if cim_files:
        # P3.1 — CIMReader + EMT segfaults inside dpsim/CIMpp (probed session 22).
        # Fall back to DP on the CIM path only; keep EMT available for demo.
        if dom_name == "EMT":
            actual_domain = dpsimpy.Domain.DP
            warnings.append(
                "EMT via CIM not supported (dpsim/CIMpp gap); ran DP. "
                "Use model_id=demo to exercise EMT with a programmatic topology."
            )
        suffix = ""
        if outage_status == "applied":
            suffix += f"+outage:{outage_target}"
        if load_factor_status == "applied":
            suffix += f"+load:{float(load_factor):g}x"
        source = f"cim:{token}{suffix}"
        # NOTE: sys is single-use. Each job rebuilds from CIM XML (docs/20).
        sys = build_cim_topology(sim_name, cim_files, actual_domain, freq=60)
        for i, node in enumerate(sys.nodes):
            logger_cim.log_attribute(f"v_n{i}", "v", node)
    else:
        # Programmatic demo topology — EMT here actually runs EMT (P3.1).
        source = f"demo-circuit:{dom_name.lower()}"
        sys, logged_nodes, logged_intfs = _build_demo_topology(actual_domain)
        for name, node in logged_nodes.items():
            logger_cim.log_attribute(name, "v", node)
        for name, comp in logged_intfs.items():
            logger_cim.log_attribute(name, "i_intf", comp)

    sim = dpsimpy.Simulation(sim_name, dpsimpy.LogLevel.info)
    sim.set_system(sys)
    sim.set_time_step(timestep)
    sim.set_final_time(finaltime)
    sim.set_domain(actual_domain)
    sim.add_logger(logger_cim)

    # H12: publish approximate progress while sim.run() blocks. Watcher reads
    # the partially-written CSV to estimate row count vs expected steps.
    csv_path = job_dir / f"{sim_name}.csv"
    expected_rows = max(1, int(finaltime / timestep))
    watcher = _ProgressWatcher(sim_id, csv_path, expected_rows)
    set_progress(sim_id, 0.0)
    watcher.start()
    INFLIGHT.inc()
    t0 = time.monotonic()
    with tracer.start_as_current_span("sim.run") as run_span:
        run_span.set_attribute("dpsim.timestep_sec", float(timestep))
        run_span.set_attribute("dpsim.finaltime_sec", float(finaltime))
        run_span.set_attribute("dpsim.topology_source", source)
        try:
            sim.run()
        finally:
            watcher.stop()
            JOB_DURATION.labels(domain=actual_domain.name).observe(time.monotonic() - t0)
            INFLIGHT.dec()

    upload_info: dict[str, Any] = {"uploaded": False}
    if csv_path.exists():
        with tracer.start_as_current_span("upload_results") as up_span:
            upload_info = _upload_results(csv_path, results_file)
            up_span.set_attribute("dpsim.upload.bytes", int(upload_info.get("bytes", 0)))
            up_span.set_attribute("dpsim.upload.minio", bool(upload_info.get("minio")))

    status = {
        "results_file": results_file,
        "simulation_id": sim_id,
        "status": "done",
        "topology_source": source,
        "requested_domain": dom_name,
        "actual_domain": actual_domain.name,
        "timestep_sec": timestep,
        "finaltime_sec": finaltime,
        "warnings": warnings,
        "artifacts": sorted(p.name for p in job_dir.iterdir()),
        "upload": upload_info,
    }
    (job_dir / "status.json").write_text(json.dumps(status, indent=2))
    set_status(sim_id, "done", warnings=warnings or None, progress=100.0)
    return status


# ---------------------------------------------------------------------------
# AMQP consumer with DLQ routing
# ---------------------------------------------------------------------------
_s3_client: Any = None


def _get_s3_client() -> Any:
    """Lazily construct the boto3 client. Returns None if MINIO disabled or
    the boto3 dep is missing. Caches across calls to avoid re-handshake."""
    global _s3_client
    if _s3_client is not None:
        return _s3_client
    if not (MINIO_ENDPOINT and _BOTO_AVAILABLE):
        return None
    _s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=_BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )
    return _s3_client


def _upload_results(csv_path: Path, results_file: str) -> dict[str, Any]:
    """Upload the simulation CSV to MinIO (if configured) and the legacy
    file-service (always). Returns the legacy upload_info dict; MinIO status
    is merged in under `minio` so the API payload is backward-compatible."""
    info: dict[str, Any] = {"uploaded": False}
    # Legacy file-service (dpsim-api reads from this).
    try:
        r = requests.put(
            f"{FILE_SERVICE_URL}/api/files/{results_file}",
            data=csv_path.read_bytes(),
            headers={"Content-Type": "text/csv"},
            timeout=(5, 60),
        )
        r.raise_for_status()
        info = {"uploaded": True, "bytes": csv_path.stat().st_size}
    except Exception as e:
        info = {"uploaded": False, "error": str(e)}
    # MinIO (object store of record for Phase 4 UI).
    s3 = _get_s3_client()
    if s3 is not None:
        key = f"{results_file}.csv"
        try:
            with csv_path.open("rb") as f:
                s3.put_object(
                    Bucket=MINIO_BUCKET, Key=key, Body=f,
                    ContentType="text/csv",
                )
            info["minio"] = {"bucket": MINIO_BUCKET, "key": key}
        except Exception as e:
            info["minio"] = {"error": str(e)[:200]}
    return info


def _retry_count(props: pika.spec.BasicProperties) -> int:
    headers = (props.headers or {}).copy()
    return int(headers.get("x-retry-count", 0))


def on_msg(ch, method, props, body: bytes) -> None:
    try:
        payload = json.loads(body)
    except Exception:
        log("non-JSON body rejected", level="warning", bytes=len(body))
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    # P2.2 — pick up the trace id the publisher set. Header wins; fall back
    # to the body copy (survives DLQ/requeue paths that drop headers).
    header_tid = (props.headers or {}).get("x-trace-id") if props else None
    payload_tid = payload.get("parameters", {}).get("trace_id")
    trace_id = str(header_tid or payload_tid or "")
    token = CURRENT_TRACE_ID.set(trace_id)

    sim_id = payload.get("parameters", {}).get("results_file", "?")
    domain_label = payload.get("parameters", {}).get("domain") or "unknown"
    # Cheap queue-depth snapshot: passive declare returns current message_count.
    # Done inline so we don't need a second AMQP connection + thread.
    try:
        q = ch.queue_declare(queue=AMQP_QUEUE, passive=True)
        QUEUE_DEPTH.labels(queue=AMQP_QUEUE).set(float(q.method.message_count))
        dlq = ch.queue_declare(queue=AMQP_DLQ, passive=True)
        QUEUE_DEPTH.labels(queue=AMQP_DLQ).set(float(dlq.method.message_count))
    except Exception:
        pass
    log("job received", sim_id=sim_id, domain=domain_label)

    # P2.2 — root span for the entire job. Attributes surface in Jaeger UI.
    # Continues the parent trace from the AMQP traceparent header so the
    # whole HTTP → AMQP → worker chain lands in a single Jaeger waterfall.
    parent_ctx = None
    if _OTEL_AVAILABLE and _OTEL_ENDPOINT:
        headers = (props.headers or {}) if props else {}
        # Only carry headers the W3C propagator cares about. AMQP often gives
        # us bytes; decode them so TraceContextTextMapPropagator sees strings.
        carrier = {}
        for k in ("traceparent", "tracestate"):
            v = headers.get(k)
            if isinstance(v, (bytes, bytearray)):
                v = v.decode("utf-8", errors="ignore")
            if v:
                carrier[k] = v
        if carrier:
            parent_ctx = _otel_extract(carrier)
    root_span = tracer.start_span("dpsim-worker.on_msg", context=parent_ctx)
    root_span.set_attribute("dpsim.sim_id", str(sim_id))
    root_span.set_attribute("dpsim.domain", str(domain_label))
    root_span.set_attribute("dpsim.trace_id_str", trace_id)
    ctx_token = None
    if _OTEL_AVAILABLE and _OTEL_ENDPOINT:
        from opentelemetry import context as _otel_context
        from opentelemetry.trace import set_span_in_context as _otel_set_span_in_context
        ctx_token = _otel_context.attach(_otel_set_span_in_context(root_span))
    try:
      try:
        status = run_simulation(payload)
        log("job complete", sim_id=sim_id, upload=status["upload"], source=status["topology_source"])
        JOBS_TOTAL.labels(outcome="done", domain=domain_label).inc()
        root_span.set_attribute("dpsim.topology_source", status["topology_source"])
        ch.basic_ack(delivery_tag=method.delivery_tag)
      except Exception as e:
        tb = traceback.format_exc()
        retry = _retry_count(props)
        log("job failed", level="error", sim_id=sim_id,
            error=str(e)[:200], retry=retry, traceback=tb[-500:])
        JOBS_TOTAL.labels(outcome="failed", domain=domain_label).inc()
        set_status(sim_id, "failed", error=str(e))

        # The nack/republish pair below must be atomic from the broker's
        # point of view: if we ack without successfully publishing the
        # retry/DLQ copy, the message is lost. Wrap each publish and fall
        # back to requeue=True on any pika error so the broker redelivers.
        if retry + 1 >= AMQP_MAX_RETRY:
            try:
                ch.basic_publish(
                    exchange="",
                    routing_key=AMQP_DLQ,
                    body=body,
                    properties=pika.BasicProperties(headers={
                        **(props.headers or {}),
                        "x-original-queue": AMQP_QUEUE,
                        "x-failure-reason": str(e)[:200],
                    }),
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)
                DLQ_TOTAL.inc()
                log("moved to DLQ", level="warning", sim_id=sim_id)
            except Exception as pub_err:
                log("DLQ publish failed — requeueing for broker redelivery",
                    level="error", sim_id=sim_id, error=str(pub_err)[:200])
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        else:
            try:
                ch.basic_publish(
                    exchange="",
                    routing_key=AMQP_QUEUE,
                    body=body,
                    properties=pika.BasicProperties(headers={
                        **(props.headers or {}),
                        "x-retry-count": retry + 1,
                    }),
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as pub_err:
                log("retry publish failed — requeueing for broker redelivery",
                    level="error", sim_id=sim_id, error=str(pub_err)[:200])
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    finally:
        CURRENT_TRACE_ID.reset(token)
        if ctx_token is not None:
            from opentelemetry import context as _otel_context
            _otel_context.detach(ctx_token)
        root_span.end()


def _sweep_job_dirs() -> int:
    """Remove job dirs under JOBS_DIR older than JOBS_RETENTION_HOURS.
    Returns the number of directories removed. Best-effort — never raises."""
    if JOBS_RETENTION_HOURS <= 0 or not JOBS_DIR.exists():
        return 0
    import shutil
    cutoff = time.time() - JOBS_RETENTION_HOURS * 3600
    removed = 0
    for entry in JOBS_DIR.iterdir():
        try:
            if not entry.is_dir():
                continue
            # mtime reflects last write — good enough proxy for "finished".
            if entry.stat().st_mtime < cutoff:
                shutil.rmtree(entry, ignore_errors=True)
                removed += 1
        except Exception:
            continue
    return removed


def _start_retention_thread() -> None:
    def _loop() -> None:
        while True:
            try:
                n = _sweep_job_dirs()
                if n:
                    log("retention sweep", removed=n, dir=str(JOBS_DIR),
                        hours=JOBS_RETENTION_HOURS)
            except Exception as e:
                log("retention sweep failed",
                    level="warning", error=str(e)[:120])
            time.sleep(3600)  # hourly
    t = threading.Thread(target=_loop, daemon=True, name="jobs-retention")
    t.start()


def check_config() -> None:
    """Fail fast with an actionable error if env/services are misconfigured."""
    # Redis
    try:
        _redis.ping()
    except Exception as e:
        raise SystemExit(f"[fatal] cannot reach Redis at {REDIS_URL}: {e}")
    # AMQP — ping upfront so failure is adjacent to the other config checks
    # instead of surfacing later in main() after "redis OK" is logged.
    try:
        _amqp_probe = pika.BlockingConnection(pika.ConnectionParameters(
            host=AMQP_HOST, port=AMQP_PORT, virtual_host=AMQP_VHOST,
            credentials=pika.PlainCredentials(AMQP_USER, AMQP_PASS),
            socket_timeout=3, blocked_connection_timeout=3,
        ))
        _amqp_probe.close()
    except Exception as e:
        raise SystemExit(
            f"[fatal] cannot reach AMQP at {AMQP_HOST}:{AMQP_PORT}{AMQP_VHOST}: {e}"
        )
    # File service (HEAD /healthz; fall back to GET). Soft-fail if unreachable,
    # since smoke.sh asserts upload path separately; here we only warn.
    try:
        r = requests.get(f"{FILE_SERVICE_URL}/healthz", timeout=3)
        if r.status_code != 200:
            log("file-service /healthz non-200",
                level="warning", url=FILE_SERVICE_URL, code=r.status_code)
    except Exception as e:
        log("file-service unreachable — uploads will fail",
            level="warning", url=FILE_SERVICE_URL, error=str(e)[:120])
    # CIM bundles exist?
    for token, files in CIM_BUNDLES.items():
        if not files:
            log("CIM bundle empty — requests referencing it will fall through to demo",
                level="warning", bundle=token, build_dir=str(DPSIM_BUILD))


def main() -> None:
    check_config()

    # Sweep on startup (in case the worker was down for long enough that the
    # retention window closed on in-flight jobs) then schedule hourly sweeps.
    removed = _sweep_job_dirs()
    if removed:
        log("retention sweep (startup)", removed=removed,
            hours=JOBS_RETENTION_HOURS)
    _start_retention_thread()

    # Prometheus exporter on :9109 (default). Scraped by the local prometheus
    # install. Skipped when DPSIM_PROMETHEUS_PORT=0 or the client lib missing.
    if _PROMETHEUS_AVAILABLE and PROMETHEUS_PORT > 0:
        try:
            _prom_start_http_server(PROMETHEUS_PORT)
            log("prometheus exporter up", port=PROMETHEUS_PORT)
        except OSError as e:
            log("prometheus exporter failed to bind — continuing without metrics",
                level="warning", port=PROMETHEUS_PORT, error=str(e)[:120])

    creds = pika.PlainCredentials(AMQP_USER, AMQP_PASS)
    params = pika.ConnectionParameters(
        host=AMQP_HOST, port=AMQP_PORT, virtual_host=AMQP_VHOST,
        credentials=creds,
    )
    try:
        conn = pika.BlockingConnection(params)
    except Exception as e:
        raise SystemExit(
            f"[fatal] cannot connect to AMQP at {AMQP_HOST}:{AMQP_PORT}{AMQP_VHOST}: {e}"
        )
    ch = conn.channel()
    ch.queue_declare(queue=AMQP_QUEUE, durable=False)
    ch.queue_declare(queue=AMQP_DLQ, durable=False)
    ch.basic_qos(prefetch_count=AMQP_PREFETCH)
    ch.basic_consume(queue=AMQP_QUEUE, on_message_callback=on_msg)
    log("worker starting", queue=AMQP_QUEUE, dlq=AMQP_DLQ,
        prefetch=AMQP_PREFETCH, jobs_dir=str(JOBS_DIR))

    def _shutdown(signum, _frame):
        log("signal received — stopping consumer", signal=signum)
        try:
            ch.stop_consuming()
        except Exception as e:
            log("stop_consuming raised — forcing exit",
                level="warning", error=str(e)[:120])
            raise SystemExit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass
        log("worker stopped")


if __name__ == "__main__":
    main()
