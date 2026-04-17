# macOS Apple Silicon Port (hj801js fork)

This fork adds the minimum patches needed for DPsim to build and run on
macOS (Apple Clang 21 on arm64) inside a conda-forge Python 3.11
environment, plus a local service-stack demo matching the upstream
sogno-platform topology.

Upstream tracking: <https://github.com/sogno-platform/dpsim>

## TL;DR

```bash
brew install miniforge cmake pybind11 eigen spdlog fmt nlohmann-json \
             graphviz redis rabbitmq rust
conda env create -f environment.yml   # or equivalent
conda activate dpsim

mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DFETCH_CIMPP=ON \
         -DWITH_VILLAS=OFF -DWITH_RT=OFF -DWITH_GSL=OFF \
         -DWITH_GRAPHVIZ=OFF -DWITH_SUNDIALS=OFF -DWITH_OPENMP=OFF \
         -DFETCH_SUITESPARSE=ON -DFETCH_PYBIND=OFF
cmake --build . --target dpsimpy -j$(sysctl -n hw.ncpu)
python3 -c 'import dpsimpy; print("OK")'
```

Companion repo (REST + queue): `hj801js/dpsim-api`.

Full service stack demo + end-to-end smoke:
`examples/service-stack/` (README there).

## Fix branches in this fork (9)

Each of the following is on its own branch and is a candidate for an
individual upstream PR.

| Branch | Problem | Fix |
|---|---|---|
| `fix/clang-template-keyword` | Apple Clang 21 rejects dependent-name `.lpNorm<>()` without `template` keyword in `MNASolver.cpp`. | Insert `template` keyword per C++ standard. |
| `fix/filesystem-standard-header` | `<experimental/filesystem>` unavailable on recent libc++. | Detect via `__has_include(<filesystem>)` first, fall back for older toolchains. |
| `fix/eigen-version-range` | CMake `find_package(Eigen3 3.4 REQUIRED)` rejects Eigen 5.x now shipped via conda-forge. | Switch to `find_package(Eigen3 3.4...5.99 REQUIRED)` range syntax. |
| `fix/logger-setlogdir-macos` | `Logger::setLogDir` hard-codes `/tmp/...` paths with a Linux-specific conditional; macOS gets uninitialised path. | Add `__APPLE__` branch. |
| `fix/matpower-series-scalar` | `matpower.py` compares a `pandas.Series` to a scalar and hits `ValueError: truth value ambiguous`. | Use `.iloc[0]` to extract scalar. |
| `fix/notebook-syngen-dq-api` | SynGenDQ notebook calls the mixed PF/DP wrapper API that dpsimpy no longer exposes. | Split into explicit PF and DP setup + reconcile object naming (2 commits). |
| `fix/eigen-internal-header-removal` | `DecouplingLineEMT_Ph3.cpp` includes `<Eigen/src/Core/Array.h>` — an internal Eigen path. Breaks when Eigen rearranges. | Remove include; the public header is already transitively present. |
| `fix/init-with-powerflow-name-check` | `initWithPowerflow` segfaults silently when the supplied PF system lacks a component with the expected name. | Raise `RuntimeError` with the missing name. |
| `feat/service-stack-examples` | No runnable reference for the sogno-platform (REST + queue + worker) pipeline on a developer laptop. | Add `examples/service-stack/` with worker, file-service stub, compose file, smoke script, and macOS CI template. |

## Known-tricky subtleties

### SystemTopology is single-use (see docs/20 in the companion workspace)

A `dpsimpy.SystemTopology` produced by `CIMReader::loadCIM` contains
capacitor / inductor / generator state that `Simulation::run()` mutates
in place. Calling `run()` a second time on the same topology object
begins from the mutated state rather than the CIM SV snapshot, and for
DP with small timesteps (< 1 s) this manifests as voltages decaying to
zero within a few thousand integration steps.

A worker that caches parsed topologies for throughput will therefore
produce correct-looking first runs followed by collapsing subsequent
jobs. The fix is to re-parse the CIM on every job (CIMpp parse of
WSCC-9 is ~20 ms, so per-job reparsing is not a real bottleneck).

The `examples/service-stack/smoke.sh` includes a regression test
(`regression "first"` / `regression "second"`) that submits the same
WSCC-9 job twice and asserts bus magnitudes stay near the Anderson
reference — do not remove it.

### Prefetch must be 1

`dpsimpy.Logger.set_log_dir()` writes a process-global. Two concurrent
jobs in one worker process would race on the log directory. The worker
pins `AMQP_PREFETCH=1`; to scale, run multiple worker processes rather
than raising prefetch.

## Verification

| Check | Expected | Where |
|---|---|---|
| `cmake --build . --target dpsimpy` | succeeds on macos-14 arm64 | `.github/workflows/ci-macos.yml` |
| `import dpsimpy; dpsimpy.CIMReader` | resolves | same |
| `examples/Notebooks/matpower-case9.ipynb` | voltages match MATPOWER reference to 0.01 % | manual |
| `examples/service-stack/smoke.sh` | 8 OKs incl. DLQ regression | manual or CI |
| 3-source cross-check of WSCC-9 | dpsim ≡ CIM SV ≈ MATPOWER (0.005 % max) | see companion workspace docs/20 |

## Contact

Jimmy Kim · jimmykim07@gmail.com · GitHub `hj801js`
