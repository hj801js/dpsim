"""Numerical regression harness — pins WSCC-9 bus voltages so any silent change
in dpsimpy / CIM reader / topology builder shows up immediately.

This runs a real simulation against the live dpsim-api stack; the pytest uses
`pytest.skip` when the API is down so `make check` stays backend-independent.
Run explicitly with `make e2e-regression`.
"""
from __future__ import annotations

import json
import math
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
API = "http://localhost:8000"

# Lock both systems we trust today. Adding a new system = drop a
# <name>_reference.json next to this file and add the name below.
REFERENCE_CASES = ["wscc9", "ieee39"]


def _post(path: str, body: dict) -> dict:
    req = urllib.request.Request(
        f"{API}{path}",
        method="POST",
        headers={"Content-Type": "application/json"},
        data=json.dumps(body).encode(),
    )
    return json.loads(urllib.request.urlopen(req).read())


def _get(path: str) -> dict:
    return json.loads(urllib.request.urlopen(f"{API}{path}").read())


def _api_up() -> bool:
    try:
        urllib.request.urlopen(f"{API}/healthz", timeout=1).read()
        return True
    except Exception:
        return False


def _run_and_check(case: str) -> None:
    if not _api_up():
        pytest.skip("dpsim-api not reachable on :8000 — run `make up` first")
    ref_path = HERE / f"{case}_reference.json"
    reference = json.loads(ref_path.read_text())
    spec = reference["simulation"]
    tol_t0 = reference["tolerance_t0_pct"] / 100.0
    tol_tl = reference["tolerance_tlast_pct"] / 100.0

    sid = _post("/simulation", {
        "simulation_type": "Powerflow",
        "model_id": spec["model"],
        "load_profile_id": "None",
        "domain": spec["domain"],
        "solver": "MNA",
        "timestep": spec["timestep_ms"],
        "finaltime": spec["finaltime_ms"],
    })["simulation_id"]

    csv = ""
    for _ in range(60):
        csv = _get(f"/simulation/{sid}").get("results_data", "")
        if csv and csv.count("\n") > 2:
            break
        time.sleep(0.25)
    assert csv, f"no CSV returned for sim {sid} after 15s"

    lines = [l.strip() for l in csv.splitlines() if l.strip()]
    header = [x.strip() for x in lines[0].split(",")]
    first = [float(x) for x in lines[1].split(",")]
    last  = [float(x) for x in lines[-1].split(",")]

    failures: list[str] = []
    for bus, ref in reference["buses"].items():
        re_i = header.index(f"{bus}.re")
        im_i = header.index(f"{bus}.im")
        got_t0   = math.hypot(first[re_i], first[im_i])
        got_last = math.hypot(last[re_i],  last[im_i])
        if abs(got_t0 - ref["t0"]) / ref["t0"] > tol_t0:
            failures.append(
                f"{bus} t0: got {got_t0:.2f}, ref {ref['t0']:.2f} "
                f"(diff {abs(got_t0-ref['t0'])/ref['t0']*100:.2f}% > {tol_t0*100}%)"
            )
        if abs(got_last - ref["tlast"]) / ref["tlast"] > tol_tl:
            failures.append(
                f"{bus} tlast: got {got_last:.2f}, ref {ref['tlast']:.2f} "
                f"(diff {abs(got_last-ref['tlast'])/ref['tlast']*100:.2f}% > {tol_tl*100}%)"
            )
    assert not failures, "{} regression (sid={}):\n  {}".format(case, sid, "\n  ".join(failures))


@pytest.mark.parametrize("case", REFERENCE_CASES)
def test_dp_matches_reference(case: str) -> None:
    _run_and_check(case)
