# hj801js/dpsim — Changelog

Delta from upstream `sogno-platform/dpsim@master` (as of 2026-04-17).
All items below are on their own branch for individual upstream review.

## Build & portability (macOS arm64)

- **Apple Clang 21 template-keyword fix** (`MNASolver.cpp`).
  C++ standard requires `template` when invoking a dependent-name
  member function template; Apple Clang 21 now enforces this.
  Branch: `fix/clang-template-keyword`.

- **`<filesystem>` header selection** (dpsim + dpsim-villas).
  `<experimental/filesystem>` is gone from recent libc++. Use
  `__has_include(<filesystem>)` to prefer the standard header and
  keep the experimental path only as a fallback.
  Branch: `fix/filesystem-standard-header`.

- **Eigen version range** (CMakeLists).
  `find_package(Eigen3 3.4 REQUIRED)` refused conda-forge Eigen 5.x.
  Switched to range syntax `3.4...5.99`.
  Branch: `fix/eigen-version-range`.

- **Remove internal-Eigen header include** (`DecouplingLineEMT_Ph3.cpp`).
  `<Eigen/src/Core/Array.h>` is an Eigen internal path that is
  already included transitively via public headers. Dropping it
  avoids a build break if Eigen restructures.
  Branch: `fix/eigen-internal-header-removal`.

- **`Logger::setLogDir` macOS branch**.
  The existing Linux conditional left macOS with an uninitialised
  default path, producing logs in the working directory. Added
  `__APPLE__` branch with `/tmp` fallback.
  Branch: `fix/logger-setlogdir-macos`.

## Python / examples correctness

- **matpower scalar comparison** (`python/dpsim/matpower.py`).
  `pandas.Series` compared to scalar raised the familiar
  `ValueError: The truth value of a Series is ambiguous`. Used
  `.iloc[0]` to extract the scalar.
  Branch: `fix/matpower-series-scalar`.

- **SynGenDQ notebook API split**
  (`examples/Notebooks/.../Compare_EMT_SynGenDQ7odTrapez_DP_SynGenTrStab_SMIB_Fault.ipynb`).
  The old single-call wrapper is gone; split into explicit PF setup
  and DP setup, with object-naming reconciled between the two so
  `initWithPowerflow` finds its matching component. 2 commits.
  Branch: `fix/notebook-syngen-dq-api`.

## Runtime ergonomics

- **`initWithPowerflow` name-mismatch error**.
  Previously, a name mismatch between the PF system and the dynamic
  system produced a silent segfault with no diagnostics. Now throws
  `RuntimeError` naming the missing component.
  Branch: `fix/init-with-powerflow-name-check`.

## Local service-stack demo

- **`examples/service-stack/`** (new).
  Reference implementation of the sogno-platform topology that runs
  on a developer laptop with nothing but brew + cargo:
  - `worker.py` — pika consumer, rebuilds CIM SystemTopology per
    job (see `docs/macos-port.md` on why caching is unsafe),
    structured JSON logging, DLQ parking, SIGTERM graceful shutdown.
  - `file_service_stub.py` — in-memory HTTP file service that
    matches sogno's API surface.
  - `docker-compose.yaml` — alternative all-in-one startup.
  - `smoke.sh` — 8-step end-to-end assertion harness, including a
    same-job×2 regression that catches SystemTopology-cache reuse
    and a DLQ-routing regression for the worker exception handler.
  - `ci-macos.yml.example` → `.github/workflows/ci-macos.yml`
    activated in this fork.
  Branch: `feat/service-stack-examples`.

## Documentation

- **`docs/macos-port.md`** — summary of this changelog plus
  knowledge-transfer notes (SystemTopology single-use invariant,
  prefetch=1 invariant) so a fresh clone of this fork is
  self-describing.

---

Contact: Jimmy Kim · jimmykim07@gmail.com · `hj801js`
