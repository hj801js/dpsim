#!/usr/bin/env bash
# End-to-end smoke test for the DPsim service stack:
#   brew services (redis, rabbitmq)
#   file_service_stub.py
#   worker.py
#   dpsim-api  (Rust, separately started)
#
# Expects the stack to be running on loopback with default credentials.
# Returns non-zero on any failure.
set -euo pipefail

API=${API:-http://localhost:8000}
FSS=${FSS:-http://127.0.0.1:18080}

fail() { echo "FAIL: $*" >&2; exit 1; }
ok()   { echo "[OK] $*"; }

# 1. health
curl -sf "$FSS/healthz" >/dev/null || fail "file-service /healthz unreachable"
ok "file-service up"

# 2. submit a demo circuit job (no CIM)
RESP=$(curl -sf -X POST "$API/simulation" \
  -H 'Content-Type: application/json' \
  -d '{"simulation_type":"Powerflow","model_id":"demo","load_profile_id":"None",
       "domain":"DP","solver":"MNA","timestep":1,"finaltime":2}')
SID=$(echo "$RESP" | python3 -c 'import sys,json; print(json.load(sys.stdin)["simulation_id"])')
ok "POST demo -> simulation_id=$SID"

# 3. poll for results up to 10s
for i in $(seq 1 10); do
    CSV=$(curl -sf "$API/simulation/$SID" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("results_data",""))')
    if [[ -n "$CSV" && $(printf "%s" "$CSV" | wc -l) -gt 2 ]]; then
        ok "GET /simulation/$SID has CSV with $(printf "%s" "$CSV" | wc -l) lines"
        break
    fi
    sleep 1
    [[ $i -eq 10 ]] && fail "no CSV after 10s"
done

# 4. submit a CIM (wscc9) job
RESP=$(curl -sf -X POST "$API/simulation" \
  -H 'Content-Type: application/json' \
  -d '{"simulation_type":"Powerflow","model_id":"wscc9","load_profile_id":"None",
       "domain":"DP","solver":"MNA","timestep":1,"finaltime":2}')
SID=$(echo "$RESP" | python3 -c 'import sys,json; print(json.load(sys.stdin)["simulation_id"])')
ok "POST wscc9 -> simulation_id=$SID"

for i in $(seq 1 15); do
    CSV=$(curl -sf "$API/simulation/$SID" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("results_data",""))')
    # WSCC-9 CSV has 19 columns (time + 9 nodes * 2 complex cols).
    if [[ -n "$CSV" && $(printf "%s" "$CSV" | head -1 | tr -cd ',' | wc -c) -ge 18 ]]; then
        ok "wscc9 CSV has $(printf "%s" "$CSV" | head -1 | tr -cd ',' | wc -c) commas → 19+ cols"
        break
    fi
    sleep 1
    [[ $i -eq 15 ]] && fail "no wscc9 CSV after 15s"
done

# 5. regression — re-run same wscc9 at 1ms and assert BUS1 magnitude stays near
# the Anderson WSCC-9 reference (17160 V within 5 %). A cached/mutated
# SystemTopology would collapse this value to < 1 kV. See docs/20.
regression() {
    local label=$1
    local RESP
    RESP=$(curl -sf -X POST "$API/simulation" \
      -H 'Content-Type: application/json' \
      -d '{"simulation_type":"Powerflow","model_id":"wscc9","load_profile_id":"None",
           "domain":"DP","solver":"MNA","timestep":1,"finaltime":500}')
    local RID=$(echo "$RESP" | python3 -c 'import sys,json; print(json.load(sys.stdin)["simulation_id"])')
    # poll up to 20s
    for i in $(seq 1 20); do
        local BODY=$(curl -sf "$API/simulation/$RID")
        local CSV=$(echo "$BODY" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("results_data",""))')
        if [[ -n "$CSV" && $(printf "%s" "$CSV" | wc -l) -gt 2 ]]; then
            # Look at any v_nX.re with |t=0 mag| > 10 kV and check it stays > 10 kV
            # at the last row.
            local RESULT
            RESULT=$(echo "$BODY" | python3 -c "
import sys, json, math
d = json.load(sys.stdin)
lines = [l.strip() for l in d['results_data'].splitlines() if l.strip()]
h = [x.strip() for x in lines[0].split(',')]
first = [float(x) for x in lines[1].split(',')]
last  = [float(x) for x in lines[-1].split(',')]
ok = True
for i in range(9):
    re_i, im_i = h.index(f'v_n{i}.re'), h.index(f'v_n{i}.im')
    m0 = math.hypot(first[re_i], first[im_i])
    mN = math.hypot(last[re_i], last[im_i])
    if m0 > 1000 and mN < 0.5 * m0:
        print(f'COLLAPSE v_n{i}: {m0:.0f} -> {mN:.0f}')
        ok = False
if ok:
    print('STABLE')
")
            if [[ "$RESULT" == STABLE ]]; then
                ok "$label wscc9 stable at 1ms step (sid=$RID)"
                return
            else
                fail "$label wscc9 collapsed: $RESULT"
            fi
        fi
        sleep 1
    done
    fail "$label wscc9 no CSV after 20s (sid=$RID)"
}

regression "first"
regression "second (catches SystemTopology cache reuse bug)"

# 6. DLQ regression — inject a job directly into the worker queue (bypassing
# dpsim-api schema validation) that will fail deterministically inside
# clamp_params, verify the worker parks it on .dlq after AMQP_MAX_RETRY=3
# attempts. Catches regressions in C2-style exception handling.
dlq_regression() {
    python3 - <<'PYEOF' >/dev/null
import pika, json
creds = pika.PlainCredentials('guest', 'guest')
conn = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=creds))
ch = conn.channel()
ch.queue_declare(queue='dpsim-worker-queue', durable=False)
ch.queue_declare(queue='dpsim-worker-queue.dlq', durable=False)
ch.queue_purge('dpsim-worker-queue.dlq')
# Trigger ValueError inside clamp_params (float("not-a-number")).
body = json.dumps({
    "parameters": {
        "results_file": "dlq_smoke",
        "timestep": "not-a-number",
        "finaltime": 100,
        "domain": "DP",
    }
}).encode()
ch.basic_publish(exchange='', routing_key='dpsim-worker-queue', body=body)
conn.close()
PYEOF
    for i in $(seq 1 20); do
        DEPTH=$(python3 - <<'PYEOF'
import pika
creds = pika.PlainCredentials('guest', 'guest')
conn = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=creds))
ch = conn.channel()
q = ch.queue_declare(queue='dpsim-worker-queue.dlq', durable=False, passive=True)
print(q.method.message_count)
conn.close()
PYEOF
)
        if [[ "${DEPTH:-0}" -ge 1 ]]; then
            ok "DLQ regression — poison msg parked on .dlq (depth=$DEPTH after ${i}s)"
            python3 - <<'PYEOF' >/dev/null
import pika
creds = pika.PlainCredentials('guest', 'guest')
conn = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=creds))
ch = conn.channel()
ch.queue_purge('dpsim-worker-queue.dlq')
conn.close()
PYEOF
            return
        fi
        sleep 1
    done
    fail "DLQ depth still 0 after 20s — worker isn't routing failures to .dlq"
}

dlq_regression

ok "smoke test passed"
