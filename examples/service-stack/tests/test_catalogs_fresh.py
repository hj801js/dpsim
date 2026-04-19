"""Regression: the auto-generated `dpsim-web/src/lib/catalogs/*.ts`
files must match what `ops/gen-model-catalog.py --all` would produce
right now. Drift here means either the generator changed or someone
edited the output by hand — both bad.

This test re-runs the generator into a temp directory and diff's the
resulting TS text against the committed files. Any mismatch fails
loudly with the diff printed.

Run:
    pytest tests/test_catalogs_fresh.py
"""
from __future__ import annotations

import difflib
import importlib.util
import io
import sys
from pathlib import Path

import pytest

REPO_ROOT        = Path(__file__).resolve().parents[4]
CATALOGS_DIR     = REPO_ROOT / "dpsim-web" / "src" / "lib" / "catalogs"
GEN_SCRIPT       = REPO_ROOT / "ops" / "gen-model-catalog.py"

_spec = importlib.util.spec_from_file_location("gen_model_catalog", GEN_SCRIPT)
assert _spec and _spec.loader
gen = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gen)  # type: ignore


@pytest.mark.parametrize("model_id,globs", gen.CATALOG_SPECS, ids=[c[0] for c in gen.CATALOG_SPECS])
def test_catalog_matches_generator(model_id: str, globs: list[str], tmp_path: Path) -> None:
    out = tmp_path / f"{model_id}.ts"
    try:
        gen.generate(model_id, globs, out)
    except FileNotFoundError as e:
        pytest.skip(f"{model_id}: source XMLs missing ({e})")

    committed = CATALOGS_DIR / f"{model_id}.ts"
    if not committed.exists():
        pytest.fail(f"{model_id}: no committed catalog at {committed} — run `make gen-catalogs`")

    fresh    = out.read_text()
    expected = committed.read_text()
    if fresh != expected:
        diff = "\n".join(difflib.unified_diff(
            expected.splitlines(), fresh.splitlines(),
            fromfile=str(committed), tofile="fresh",
            lineterm="",
        ))
        pytest.fail(
            f"{model_id}: committed catalog does not match a fresh generator run.\n"
            f"Run `make gen-catalogs` and commit the result.\n\n{diff}"
        )
