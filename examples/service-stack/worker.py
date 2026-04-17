"""DPsim worker — consumes dpsim-api AMQP messages, runs dpsimpy, returns results.

Features:
  - Credential / endpoint discovery via environment variables.
  - Redis simulation state tracking (status, error).
  - Dead-letter queue: messages that fail 3 times are parked on
    dpsim-worker-queue.dlq instead of being silently dropped (C + B4).
  - CIM topology LRU cache so repeat submissions of the same model_id
    don't re-parse XML (F).
  - Structured JSON logging (D).
  - Prefetch 2 for modest concurrency (C).

This is a reference implementation that sits between dpsim-api and
dpsimpy. A production deployment would add:
  - multiprocessing.Pool to run multiple jobs in parallel per worker
  - metrics export (Prometheus)
  - distributed tracing (OpenTelemetry)
  - object-store upload instead of in-memory file-service
"""
from __future__ import annotations

import glob
import json
import logging
import os
import sys
import time
import traceback
from collections import OrderedDict
from pathlib import Path
from typing import Any

import pika
import redis
import requests

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

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
FILE_SERVICE_URL = os.environ.get("FILE_SERVICE_URL", "http://127.0.0.1:18080")

JOBS_DIR = Path(os.environ.get("DPSIM_JOBS_DIR", "/tmp/dpsim_jobs"))
JOBS_DIR.mkdir(parents=True, exist_ok=True)

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

# Known CIM bundles, keyed by substring that may appear in a model URL.
# Extend this dict to accept more models.
CIM_BUNDLES = {
    "wscc9": sorted(glob.glob(str(DPSIM_BUILD / "_deps/cim-data-src/WSCC-09/WSCC-09/*.xml"))),
    "ieee39": sorted(glob.glob(str(DPSIM_BUILD / "_deps/cim-data-src/IEEE-39/*.xml"))),
}

MAX_FINAL_TIME_SEC = 30.0
MIN_TIMESTEP_SEC = 1e-5


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


def log(msg: str, level: str = "info", **context: Any) -> None:
    extra = {f"ctx_{k}": v for k, v in context.items()}
    getattr(logger, level)(msg, extra=extra)


# ---------------------------------------------------------------------------
# Redis helpers — simple status field per simulation id.
# dpsim-api writes the main record; we only touch `status` / `error`.
# Store uses a side-channel key so we don't fight Rust over JSON schema.
# ---------------------------------------------------------------------------
_redis = redis.from_url(REDIS_URL, decode_responses=True)


def set_status(sim_id: str | int, status: str, error: str | None = None) -> None:
    key = f"dpsim:sim:{sim_id}:status"
    value = {"status": status}
    if error is not None:
        value["error"] = error
    _redis.set(key, json.dumps(value))
    log("status update", sim_id=sim_id, status=status, error=error or "")


def get_status(sim_id: str | int) -> dict[str, Any] | None:
    raw = _redis.get(f"dpsim:sim:{sim_id}:status")
    return json.loads(raw) if raw else None


# ---------------------------------------------------------------------------
# CIM topology cache — expensive parse once, reuse across identical jobs.
# Keyed by tuple of sorted file paths.
# ---------------------------------------------------------------------------
_CIM_CACHE: "OrderedDict[tuple, Any]" = OrderedDict()
_CIM_CACHE_MAX = 8


def cached_cim_topology(sim_name: str, files: list[str], domain: Any, freq: float = 60):
    key = tuple(sorted(files)) + (domain.name,)
    hit = _CIM_CACHE.pop(key, None)
    if hit is not None:
        _CIM_CACHE[key] = hit  # mark MRU
        log("cim cache hit", key_len=len(files), domain=domain.name)
        return hit
    reader = dpsimpy.CIMReader(sim_name)
    sys = reader.loadCIM(
        freq, list(files), domain,
        dpsimpy.PhaseType.Single,
        dpsimpy.GeneratorType.IdealVoltageSource,
    )
    _CIM_CACHE[key] = sys
    while len(_CIM_CACHE) > _CIM_CACHE_MAX:
        _CIM_CACHE.popitem(last=False)
    log("cim cache miss", key_len=len(files), domain=domain.name)
    return sys


# ---------------------------------------------------------------------------
# Topology builders
# ---------------------------------------------------------------------------
def _find_cim_bundle(payload: dict[str, Any]) -> tuple[str | None, list[str]]:
    for url in payload.get("model", {}).get("url", []):
        tail = url.rsplit("/", 1)[-1].lower()
        for token, files in CIM_BUNDLES.items():
            if token in tail and files:
                return token, files
    return None, []


def _build_demo_topology():
    gnd = dpsimpy.dp.SimNode.gnd
    n1 = dpsimpy.dp.SimNode("n1")
    n2 = dpsimpy.dp.SimNode("n2")
    vs = dpsimpy.dp.ph1.VoltageSource("vs")
    vs.set_parameters(V_ref=complex(10000, 0), f_src=50)
    rline = dpsimpy.dp.ph1.Resistor("r_line")
    rline.set_parameters(R=1.0)
    rload = dpsimpy.dp.ph1.Resistor("r_load")
    rload.set_parameters(R=100.0)
    vs.connect([gnd, n1])
    rline.connect([n1, n2])
    rload.connect([n2, gnd])
    sys = dpsimpy.SystemTopology(50, [n1, n2], [vs, rline, rload])
    return sys, {"v1": n1, "v2": n2}, {"i_line": rline}


def clamp_params(p: dict[str, Any]) -> tuple[float, float]:
    ts = float(p.get("timestep", 1e-3))
    ft = float(p.get("finaltime", 0.1))
    if ft > MAX_FINAL_TIME_SEC * 1000:
        ft = ft / 1000.0
    ts = max(ts, MIN_TIMESTEP_SEC)
    ft = min(max(ft, 10 * ts), MAX_FINAL_TIME_SEC)
    return ts, ft


def run_simulation(payload: dict[str, Any]) -> dict[str, Any]:
    params = payload["parameters"]
    results_file = params.get("results_file", "anon")
    sim_id = params.get("simulation_id", results_file)

    job_dir = JOBS_DIR / results_file
    job_dir.mkdir(parents=True, exist_ok=True)

    set_status(sim_id, "running")

    dom_name = params.get("domain", "DP")
    domain = DOMAIN_MAP.get(dom_name, dpsimpy.Domain.DP)
    actual_domain = domain if dom_name != "EMT" else dpsimpy.Domain.DP
    timestep, finaltime = clamp_params(params)

    sim_name = f"job_{results_file}"
    dpsimpy.Logger.set_log_dir(str(job_dir))
    logger_cim = dpsimpy.Logger(sim_name)

    token, cim_files = _find_cim_bundle(payload)
    if cim_files:
        source = f"cim:{token}"
        sys = cached_cim_topology(sim_name, cim_files, actual_domain, freq=60)
        for i, node in enumerate(sys.nodes):
            logger_cim.log_attribute(f"v_n{i}", "v", node)
    else:
        source = "demo-circuit"
        sys, logged_nodes, logged_intfs = _build_demo_topology()
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
    sim.run()

    csv_path = job_dir / f"{sim_name}.csv"
    upload_info = {"uploaded": False}
    if csv_path.exists():
        try:
            r = requests.put(
                f"{FILE_SERVICE_URL}/api/files/{results_file}",
                data=csv_path.read_bytes(),
                headers={"Content-Type": "text/csv"},
                timeout=10,
            )
            r.raise_for_status()
            upload_info = {"uploaded": True, "bytes": csv_path.stat().st_size}
        except Exception as e:
            upload_info = {"uploaded": False, "error": str(e)}

    status = {
        "results_file": results_file,
        "simulation_id": sim_id,
        "status": "done",
        "topology_source": source,
        "requested_domain": dom_name,
        "actual_domain": actual_domain.name,
        "timestep_sec": timestep,
        "finaltime_sec": finaltime,
        "artifacts": sorted(p.name for p in job_dir.iterdir()),
        "upload": upload_info,
    }
    (job_dir / "status.json").write_text(json.dumps(status, indent=2))
    set_status(sim_id, "done")
    return status


# ---------------------------------------------------------------------------
# AMQP consumer with DLQ routing
# ---------------------------------------------------------------------------
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

    sim_id = payload.get("parameters", {}).get("results_file", "?")
    log("job received", sim_id=sim_id, domain=payload.get("parameters", {}).get("domain"))

    try:
        status = run_simulation(payload)
        log("job complete", sim_id=sim_id, upload=status["upload"], source=status["topology_source"])
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        tb = traceback.format_exc()
        retry = _retry_count(props)
        log("job failed", level="error", sim_id=sim_id,
            error=str(e)[:200], retry=retry, traceback=tb[-500:])
        set_status(sim_id, "failed", error=str(e))
        if retry + 1 >= AMQP_MAX_RETRY:
            # Park on DLQ.
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
            log("moved to DLQ", level="warning", sim_id=sim_id)
        else:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            ch.basic_publish(
                exchange="",
                routing_key=AMQP_QUEUE,
                body=body,
                properties=pika.BasicProperties(headers={
                    **(props.headers or {}),
                    "x-retry-count": retry + 1,
                }),
            )


def main() -> None:
    creds = pika.PlainCredentials(AMQP_USER, AMQP_PASS)
    params = pika.ConnectionParameters(
        host=AMQP_HOST, port=AMQP_PORT, virtual_host=AMQP_VHOST,
        credentials=creds,
    )
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue=AMQP_QUEUE, durable=False)
    ch.queue_declare(queue=AMQP_DLQ, durable=False)
    prefetch = int(os.environ.get("AMQP_PREFETCH", 2))
    ch.basic_qos(prefetch_count=prefetch)
    ch.basic_consume(queue=AMQP_QUEUE, on_message_callback=on_msg)
    log("worker starting", queue=AMQP_QUEUE, dlq=AMQP_DLQ,
        prefetch=prefetch, jobs_dir=str(JOBS_DIR))
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
