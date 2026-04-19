"""Assert the Python catalog generator (ops/gen-model-catalog.py) and the
Rust /topology endpoint produce the same bus/branch graph for every baked
bundle.

Two implementations exist because:
  * ops/gen-model-catalog.py produces the compile-time TS catalogs the
    web UI ships with — instant render, no API round-trip.
  * dpsim-api/src/topology.rs serves uploaded model ids at runtime and
    has to parse CIM entirely server-side.

They parse the same XML shape — any divergence is a drift bug. This test
catches it early. Skips the network case if /topology isn't live.

Run:
    API=http://localhost:8000 pytest tests/test_parser_equivalence.py
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[4]
OPS_DIR   = REPO_ROOT / "ops"
sys.path.insert(0, str(OPS_DIR))

# Import the generator's internals directly — no subprocess.
import importlib.util
_spec = importlib.util.spec_from_file_location(
    "gen_model_catalog",
    OPS_DIR / "gen-model-catalog.py",
)
assert _spec and _spec.loader
gen = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gen)  # type: ignore

API = os.environ.get("API", "http://localhost:8000")

# Restrict to models that have working baked paths both sides.
CASES = [
    ("wscc9",    gen.CATALOG_SPECS[0][1]),
    ("ieee14",   gen.CATALOG_SPECS[1][1]),
    ("ieee39",   gen.CATALOG_SPECS[2][1]),
    ("cigre_mv", gen.CATALOG_SPECS[3][1]),
    # Matpower single-file cases — also dual-path covered.
    ("matpower_case9",   gen.CATALOG_SPECS[4][1]),
    ("matpower_case14",  gen.CATALOG_SPECS[5][1]),
    ("matpower_case300", gen.CATALOG_SPECS[6][1]),
]


def _python_graph(globs: list[str]) -> set[tuple]:
    """Run the Python generator and return a set of hashable branch records."""
    import glob as _g
    eq_paths: list[str] = []
    for g in globs:
        eq_paths.extend(_g.glob(g))
    parsed = gen._parse_bundle(eq_paths)
    out = set()
    for rid, (kind, name) in parsed["elements"].items():
        buses = gen._element_buses(rid, parsed)
        if len(buses) < 2:
            continue
        out.add((name, frozenset((buses[0], buses[1])), kind))
    return out


def _rust_graph(model_id: str) -> set[tuple]:
    """Fetch /topology/<id> and return the same shape."""
    try:
        req = urllib.request.Request(f"{API}/topology/{model_id}")
        with urllib.request.urlopen(req, timeout=5) as resp:
            payload = json.loads(resp.read().decode())
    except urllib.error.URLError as e:
        pytest.skip(f"/topology endpoint unreachable: {e}")
    except urllib.error.HTTPError as e:
        if e.code == 401:
            pytest.skip("/topology behind auth (DPSIM_AUTH_REQUIRED=1)")
        raise
    out = set()
    for b in payload.get("branches", []):
        out.add((b["name"], frozenset((b["bus_from"], b["bus_to"])), b["kind"]))
    return out


@pytest.mark.parametrize("model_id,globs", CASES, ids=[c[0] for c in CASES])
def test_parsers_agree(model_id: str, globs: list[str]) -> None:
    py_graph   = _python_graph(globs)
    rust_graph = _rust_graph(model_id)
    if not py_graph:
        pytest.skip(f"no XML matched {globs}")
    # Set-equality; tuple-wise diff shows up in pytest output on failure.
    missing_in_rust = py_graph - rust_graph
    missing_in_py   = rust_graph - py_graph
    assert not missing_in_rust, (
        f"{model_id}: Rust /topology missing branches that Python found: "
        f"{sorted(missing_in_rust)[:3]}"
    )
    assert not missing_in_py, (
        f"{model_id}: Python generator missing branches that Rust found: "
        f"{sorted(missing_in_py)[:3]}"
    )
