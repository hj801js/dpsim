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

    def test_substring_does_not_match(self, monkeypatch):
        """A model_id that contains a known token as substring must not
        route there. Regression for C4 (docs/22)."""
        monkeypatch.setitem(worker.CIM_BUNDLES, "wscc9", ["/fake/WSCC_A.xml"])
        token, _ = worker._find_cim_bundle({
            "model": {"url": ["https://example.com/models/wscc9-broken"]},
        })
        assert token is None

    def test_uploaded_model_reads_cache(self, monkeypatch, tmp_path):
        """If CIM_BUNDLES has no match, `_find_cim_bundle` should return
        files from the uploaded model cache when present. Pre-populate the
        cache so the function never needs to hit file-service."""
        monkeypatch.setattr(worker, "MODELS_CACHE_DIR", tmp_path)
        cache = tmp_path / "abc123"
        cache.mkdir()
        (cache / "EQ.xml").write_text("<cim/>")
        (cache / "TP.xml").write_text("<cim/>")
        token, files = worker._find_cim_bundle({
            "model": {"url": ["http://fs/api/files/abc123"]},
        })
        assert token == "abc123"
        assert sorted(Path(f).name for f in files) == ["EQ.xml", "TP.xml"]


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
        assert "progress" not in payload

    def test_set_status_includes_progress_when_given(self, monkeypatch):
        captured = {}

        class FakeRedis:
            def set(self, key, value):
                captured["value"] = value

        monkeypatch.setattr(worker, "_redis", FakeRedis())
        worker.set_status("abc", "done", progress=100.0)
        payload = json.loads(captured["value"])
        assert payload["progress"] == 100.0

    def test_set_status_clamps_progress(self, monkeypatch):
        captured = {}

        class FakeRedis:
            def set(self, key, value):
                captured["value"] = value

        monkeypatch.setattr(worker, "_redis", FakeRedis())
        worker.set_status("abc", "running", progress=150.0)
        assert json.loads(captured["value"])["progress"] == 100.0
        worker.set_status("abc", "running", progress=-5.0)
        assert json.loads(captured["value"])["progress"] == 0.0


# ---------------------------------------------------------------------------
# set_progress — preserves status/error/warnings, only touches `progress`
# ---------------------------------------------------------------------------
class TestSetProgress:
    def test_set_progress_preserves_warnings(self, monkeypatch):
        state = {"value": json.dumps({
            "status": "running",
            "warnings": ["timestep clamped"],
        })}

        class FakeRedis:
            def get(self, key):
                return state["value"]

            def set(self, key, value):
                state["value"] = value

        monkeypatch.setattr(worker, "_redis", FakeRedis())
        worker.set_progress("abc", 42.5)
        payload = json.loads(state["value"])
        assert payload["warnings"] == ["timestep clamped"]
        assert payload["status"] == "running"
        assert payload["progress"] == 42.5


# ---------------------------------------------------------------------------
# Invariant documentation — build_cim_topology must not cache.
# A regression here (reintroducing an LRU) would break the WSCC-9
# same-job×2 smoke test; this pytest keeps the intent visible in code.
# ---------------------------------------------------------------------------
def test_apply_outage_bumps_target_rx(monkeypatch, tmp_path):
    """P3.4 — _apply_outage multiplies r and x by 1000 for the named
    ACLineSegment, leaves the rest untouched, and reports 'applied'."""
    src = tmp_path / "WSCC-09_EQ.xml"
    src.write_text(
        '<?xml version="1.0" encoding="utf-8"?>'
        '<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" '
        '         xmlns:cim="http://iec.ch/TC57/2012/CIM-schema-cim16#">'
        '<cim:ACLineSegment rdf:ID="LINE75">'
        '<cim:IdentifiedObject.name>LINE75</cim:IdentifiedObject.name>'
        '<cim:ACLineSegment.r>10.0</cim:ACLineSegment.r>'
        '<cim:ACLineSegment.x>50.0</cim:ACLineSegment.x>'
        '</cim:ACLineSegment>'
        '<cim:ACLineSegment rdf:ID="LINE96">'
        '<cim:IdentifiedObject.name>LINE96</cim:IdentifiedObject.name>'
        '<cim:ACLineSegment.r>20.0</cim:ACLineSegment.r>'
        '<cim:ACLineSegment.x>60.0</cim:ACLineSegment.x>'
        '</cim:ACLineSegment>'
        '</rdf:RDF>'
    )
    monkeypatch.setattr(worker, "JOBS_DIR", tmp_path)
    new_files, status = worker._apply_outage([str(src)], "LINE75", "job-x")
    assert status == "applied"
    assert len(new_files) == 1 and Path(new_files[0]).name == "WSCC-09_EQ.xml"
    content = Path(new_files[0]).read_text()
    assert "10000.0" in content, "LINE75 r should be 10.0 * 1000"
    assert "50000.0" in content, "LINE75 x should be 50.0 * 1000"
    # LINE96 values preserved (not 20000/60000).
    assert ">20.0<" in content and ">60.0<" in content


def test_apply_outage_not_found(monkeypatch, tmp_path):
    """Outage component not present → status 'not-found', unchanged copy."""
    src = tmp_path / "EQ.xml"
    src.write_text(
        '<?xml version="1.0"?>'
        '<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" '
        '         xmlns:cim="http://iec.ch/TC57/2012/CIM-schema-cim16#">'
        '<cim:ACLineSegment rdf:ID="X"><cim:IdentifiedObject.name>X</cim:IdentifiedObject.name>'
        '<cim:ACLineSegment.r>1</cim:ACLineSegment.r></cim:ACLineSegment>'
        '</rdf:RDF>'
    )
    monkeypatch.setattr(worker, "JOBS_DIR", tmp_path)
    _, status = worker._apply_outage([str(src)], "DOES_NOT_EXIST", "job-y")
    assert status == "not-found"


def test_sweep_job_dirs_respects_retention(monkeypatch, tmp_path):
    """P2.6 retention — dirs older than JOBS_RETENTION_HOURS go, newer stay."""
    import os, time
    old = tmp_path / "stale_job"
    new = tmp_path / "fresh_job"
    old.mkdir()
    new.mkdir()
    # Backdate the old dir by 48h.
    past = time.time() - 48 * 3600
    os.utime(old, (past, past))

    monkeypatch.setattr(worker, "JOBS_DIR", tmp_path)
    monkeypatch.setattr(worker, "JOBS_RETENTION_HOURS", 24.0)
    removed = worker._sweep_job_dirs()
    assert removed == 1
    assert not old.exists()
    assert new.exists()


def test_sweep_disabled_by_zero_retention(monkeypatch, tmp_path):
    """JOBS_RETENTION_HOURS=0 opts out of purging entirely."""
    import os, time
    (tmp_path / "ancient").mkdir()
    past = time.time() - 365 * 24 * 3600
    os.utime(tmp_path / "ancient", (past, past))

    monkeypatch.setattr(worker, "JOBS_DIR", tmp_path)
    monkeypatch.setattr(worker, "JOBS_RETENTION_HOURS", 0.0)
    assert worker._sweep_job_dirs() == 0
    assert (tmp_path / "ancient").exists()


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
