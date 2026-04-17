# DPsim service stack — local reference

Three-tier reference to expose DPsim as a REST service:

```
client (curl) ──► dpsim-api (Rust, :8000) ──► RabbitMQ ──► worker.py ──► dpsimpy
                                │                              │
                                ▼                              ▼
                       file-service-stub (:18080) ◄──── CSV upload
                                ▲
                                └── GET /simulation/<id>  returns results_data
```

Works out-of-the-box on macOS and Linux via Homebrew or Docker Compose.

## Files

| File | Role |
|---|---|
| `worker.py` | AMQP consumer, runs dpsimpy.Simulation, uploads CSV back, tracks status in Redis |
| `file_service_stub.py` | Minimal in-memory sogno-file-service replacement |
| `smoke.sh` | End-to-end smoke test — submits a demo circuit and a CIM WSCC-9 job, verifies CSV comes back |
| `docker-compose.yaml` | Brings up redis + rabbitmq + stub + dpsim-api + worker |

## Prerequisites

- DPsim built with CIM support (`-DFETCH_CIMPP=ON`) so `dpsimpy.CIMReader` is available:
  ```bash
  cd ../../build && rm -rf *
  cmake .. -DCMAKE_BUILD_TYPE=Release \
    -DFETCH_SUITESPARSE=ON -DFETCH_PYBIND=OFF -DFETCH_CIMPP=ON \
    -DWITH_VILLAS=OFF -DWITH_RT=OFF -DWITH_GSL=OFF \
    -DWITH_GRAPHVIZ=OFF -DWITH_SUNDIALS=OFF -DWITH_OPENMP=OFF \
    -DDPSIM_BUILD_EXAMPLES=OFF -DDPSIM_BUILD_DOC=OFF
  cmake --build . --target dpsimpy -j$(nproc 2>/dev/null || sysctl -n hw.ncpu)
  ```
- A clone of `hj801js/dpsim-api` (or the upstream equivalent once patches land). Two local-friendliness patches are required until merged upstream:
  - `fix/configurable-service-urls` — env-configurable `REDIS_URL` / `FILE_SERVICE_URL`
  - `fix/amqp-payload-hardcoded` — stops overwriting user simulation params with fixed values

## Native macOS / Linux run (no Docker)

```bash
# 1) services via Homebrew
brew install redis rabbitmq
brew services start redis
brew services start rabbitmq

# 2) stub file service + worker
pip install pika redis requests flask
python3 examples/service-stack/file_service_stub.py &
python3 examples/service-stack/worker.py &

# 3) dpsim-api
cd /path/to/dpsim-api
cargo build --release
export AMQP_ADDR='amqp://guest:guest@localhost:5672/%2f'
export REDIS_URL='redis://localhost:6379/'
export FILE_SERVICE_URL='http://127.0.0.1:18080'
./target/release/dpsim-api &

# 4) smoke test
bash /path/to/dpsim/examples/service-stack/smoke.sh
```

## Docker Compose (opinionated)

```bash
cd examples/service-stack
docker compose up -d --build
bash smoke.sh
docker compose down
```

## Environment variables

### Worker (`worker.py`)

| Variable | Default | Meaning |
|---|---|---|
| `AMQP_HOST` / `AMQP_PORT` / `AMQP_USER` / `AMQP_PASS` / `AMQP_VHOST` | localhost/5672/guest/guest// | RabbitMQ connection |
| `AMQP_QUEUE` | `dpsim-worker-queue` | Source queue for jobs |
| `AMQP_PREFETCH` | 2 | Concurrent jobs per worker |
| `AMQP_MAX_RETRY` | 3 | Before moving to `<queue>.dlq` |
| `REDIS_URL` | `redis://localhost:6379/0` | For status tracking |
| `FILE_SERVICE_URL` | `http://127.0.0.1:18080` | Where to PUT result CSVs |
| `DPSIM_JOBS_DIR` | `/tmp/dpsim_jobs` | Local scratch for logs/CSV |
| `DPSIM_BUILD_DIR` | `<repo>/build` | Used to locate bundled CIM test data |

### File service stub (`file_service_stub.py`)

| Variable | Default |
|---|---|
| `FILE_SERVICE_HOST` | 127.0.0.1 |
| `FILE_SERVICE_PORT` | 18080 |

## Submitting jobs

### Plain demo (voltage divider)
```bash
curl -X POST http://localhost:8000/simulation \
  -H 'Content-Type: application/json' \
  -d '{"simulation_type":"Powerflow","model_id":"demo","load_profile_id":"None",
       "domain":"DP","solver":"MNA","timestep":1,"finaltime":2}'
```

### CIM WSCC-9 bus
```bash
curl -X POST http://localhost:8000/simulation \
  -H 'Content-Type: application/json' \
  -d '{"simulation_type":"Powerflow","model_id":"wscc9","load_profile_id":"None",
       "domain":"DP","solver":"MNA","timestep":1,"finaltime":2}'
```

Poll results:
```bash
curl http://localhost:8000/simulation/<id> | jq .results_data
```

## Job state (Redis side-channel)

The worker writes a status blob to `dpsim:sim:<id>:status` so the caller can
distinguish queued / running / done / failed without re-fetching CSV:

```bash
redis-cli get dpsim:sim:42:status
# {"status":"done"}  or  {"status":"failed","error":"..."}
```

## Dead-letter queue

Messages that fail `AMQP_MAX_RETRY` times land on `<queue>.dlq`. Inspect
via the RabbitMQ management UI (http://localhost:15672) or:

```bash
curl -u guest:guest "http://localhost:15672/api/queues/%2f/dpsim-worker-queue.dlq" | jq .messages
```

## Known limitations

- `docker-compose.yaml` expects a host-built `dpsimpy.*.so` mounted in; the
  compose file does not compile DPsim itself.
- The stub file service is in-memory only; restarting it loses all jobs.
- CIM bundle recognition is by substring match on the `model_id` token. Add
  your own bundles by editing `CIM_BUNDLES` in `worker.py`.
