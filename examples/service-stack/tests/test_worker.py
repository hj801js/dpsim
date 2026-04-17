"""Unit tests for worker.py logic that does not need AMQP or file-service.

Run from the service-stack directory::

    cd examples/service-stack
    python3 -m pytest tests/ -v

These tests exist to lock the `clamp_params` warning contract, the
`_find_cim_bundle` routing, and the single-use invariant of the CIM
builder documented in docs/20. They deliberately avoid importing pika
or redis; worker's module-level `_redis = redis.from_url(...)` connects
lazily on first use (ping), so these tests will run offline.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

# Make worker.py importable without installing the package.
HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))

import worker  # noqa: E402


# ---------------------------------------------------------------------------
# clamp_params — warnings contract
# ---------------------------------------------------------------------------
class TestClampParams:
    def test_nominal_input_has_no_warnings(self):
        ts, ft, warnings = worker.clamp_params({"timestep": 1, "finaltime": 1000})
        assert ts == 0.001
        assert ft == 1.0
        assert warnings == []

    def test_zero_timestep_is_clamped_up(self):
        ts, _, warnings = worker.clamp_params({"timestep": 0, "finaltime": 1000})
        assert ts == worker.MIN_TIMESTEP_SEC
        assert len(warnings) == 1
        assert "timestep clamped" in warnings[0]

    def test_finaltime_below_ten_times_timestep_is_raised(self):
        _, ft, warnings = worker.clamp_params({"timestep": 1, "finaltime": 5})
        assert ft == 0.01  # 10 * 1 ms
        assert any("finaltime raised" in w for w in warnings)

    def test_finaltime_over_max_is_capped(self):
        _, ft, warnings = worker.clamp_params({"timestep": 1, "finaltime": 100_000})
        assert ft == worker.MAX_FINAL_TIME_SEC
        assert any("finaltime clamped" in w for w in warnings)

    def test_non_numeric_timestep_raises(self):
        with pytest.raises(ValueError):
            worker.clamp_params({"timestep": "oops", "finaltime": 100})


# ---------------------------------------------------------------------------
# _find_cim_bundle — URL → known CIM set routing
# ---------------------------------------------------------------------------
class TestFindCimBundle:
    def test_unknown_model_returns_none(self):
        token, files = worker._find_cim_bundle({"model": {"url": ["unknown"]}})
        assert token is None
        assert files == []

    def test_missing_model_key_returns_none(self):
        token, files = worker._find_cim_bundle({})
        assert token is None
        assert files == []

    def test_wscc9_url_routes_to_bundle(self, monkeypatch):
        monkeypatch.setitem(worker.CIM_BUNDLES, "wscc9", ["/fake/WSCC_A.xml"])
        token, files = worker._find_cim_bundle({
            "model": {"url": ["https://example.com/models/wscc9.zip"]},
        })
        assert token == "wscc9"
        assert files == ["/fake/WSCC_A.xml"]

    def test_empty_bundle_is_ignored(self, monkeypatch):
        """If CIM_BUNDLES[token] is [] (build hasn't fetched CIM data yet),
        treat as unknown rather than matching with empty file list."""
        monkeypatch.setitem(worker.CIM_BUNDLES, "wscc9", [])
        token, files = worker._find_cim_bundle({
            "model": {"url": ["wscc9"]},
        })
        assert token is None


# ---------------------------------------------------------------------------
# set_status — writes redis sidechannel key with warnings when supplied
# ---------------------------------------------------------------------------
class TestSetStatus:
    def test_set_status_writes_warnings(self, monkeypatch):
        captured = {}

        class FakeRedis:
            def set(self, key, value):
                captured["key"] = key
                captured["value"] = value

        monkeypatch.setattr(worker, "_redis", FakeRedis())
        worker.set_status("abc123", "done", warnings=["timestep clamped from 0 ms to 0.01 ms"])

        assert captured["key"] == "dpsim:sim:abc123:status"
        payload = json.loads(captured["value"])
        assert payload["status"] == "done"
        assert payload["warnings"] == ["timestep clamped from 0 ms to 0.01 ms"]

    def test_set_status_omits_empty_warnings(self, monkeypatch):
        captured = {}

        class FakeRedis:
            def set(self, key, value):
                captured["value"] = value

        monkeypatch.setattr(worker, "_redis", FakeRedis())
        worker.set_status("abc", "running")
        payload = json.loads(captured["value"])
        assert "warnings" not in payload
        assert "error" not in payload


# ---------------------------------------------------------------------------
# Invariant documentation — build_cim_topology must not cache.
# A regression here (reintroducing an LRU) would break the WSCC-9
# same-job×2 smoke test; this pytest keeps the intent visible in code.
# ---------------------------------------------------------------------------
def test_build_cim_topology_always_returns_fresh_object(monkeypatch):
    """Two consecutive calls must produce distinct objects.

    Regression guard for docs/20 — if someone reintroduces an LRU
    cache for SystemTopology, this test fails and the smoke test
    catches the voltage collapse on the second job.
    """
    calls = {"loadCIM": 0}

    class FakeReader:
        def __init__(self, _name):
            pass

        def loadCIM(self, *args, **kwargs):
            calls["loadCIM"] += 1
            return object()  # fresh identity every time

    class FakeDpsimpy:
        class PhaseType:
            Single = "single"

        class GeneratorType:
            IdealVoltageSource = "ivs"

        CIMReader = FakeReader

    monkeypatch.setattr(worker, "dpsimpy", FakeDpsimpy)

    a = worker.build_cim_topology("job_a", ["/fake.xml"], domain="DP")
    b = worker.build_cim_topology("job_b", ["/fake.xml"], domain="DP")

    assert a is not b
    assert calls["loadCIM"] == 2
