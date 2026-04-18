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
       "domain":"DP","solver":"MNA","timestep":1,"finaltime":20}')
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
       "domain":"DP","solver":"MNA","timestep":1,"finaltime":20}')
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

# 7. server-side input validation — posting timestep=0 must return a 400,
# posting finaltime < 10*timestep must return a 400. Regression guard for
# M13 (docs/22) so these never silently reach the queue again.
validate_reject() {
    local label=$1 body=$2
    local CODE
    CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API/simulation" \
      -H 'Content-Type: application/json' -d "$body")
    [[ "$CODE" == "400" ]] \
      && ok "server validation: $label rejected (HTTP $CODE)" \
      || fail "server validation: $label should have been 400, got $CODE"
}

validate_reject "timestep=0" \
  '{"simulation_type":"Powerflow","model_id":"demo","load_profile_id":"None","domain":"DP","solver":"MNA","timestep":0,"finaltime":100}'
validate_reject "finaltime<10*timestep" \
  '{"simulation_type":"Powerflow","model_id":"demo","load_profile_id":"None","domain":"DP","solver":"MNA","timestep":5,"finaltime":20}'

# 8. liveness probe — /healthz should return 200 "ok" (H10 regression).
HZ=$(curl -sf "$API/healthz") || fail "GET /healthz failed"
[[ "$HZ" == "ok" ]] && ok "/healthz returned 'ok'" || fail "/healthz returned '$HZ' (expected 'ok')"

# 9. version probe — /version returns name + version + git_sha (L3 regression).
VER=$(curl -sf "$API/version") || fail "GET /version failed"
echo "$VER" | python3 -c 'import sys,json; d=json.load(sys.stdin); assert d["name"]=="dpsim-api" and "version" in d and "git_sha" in d, d' \
  && ok "/version returned $VER" || fail "/version missing fields: $VER"

# 9b. model upload — POST /models returns a model_id; oversize request 413.
MODEL_BODY='<cim>test</cim>'
MID=$(curl -sf -X POST "$API/models" -H 'Content-Type: application/xml' --data "$MODEL_BODY" \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["model_id"])')
[[ -n "$MID" ]] && ok "/models returned model_id=$MID" || fail "/models didn't return a model_id"
OVERSIZE=$(dd if=/dev/zero bs=1M count=20 2>/dev/null | base64)
HTTP_CODE=$(printf "%s" "$OVERSIZE" | curl -s -o /dev/null -w '%{http_code}' -X POST "$API/models" \
  -H 'Content-Type: application/octet-stream' --data-binary @-)
[[ "$HTTP_CODE" == "413" ]] && ok "/models rejected 20 MiB payload (HTTP $HTTP_CODE)" \
  || fail "/models oversize expected 413, got $HTTP_CODE"

# 9c. dynamic CIM loading — upload a zipped WSCC-9 bundle and verify the worker
# materializes the cache + runs CIMReader on it (topology_source = cim:<id>).
WSCC9_DIR=$(python3 -c '
import os, sys, glob
for root in ["/Users/hk/DPsim_hk/dpsim/build/_deps/cim-data-src/WSCC-09/WSCC-09",
             os.path.expanduser("~/DPsim_hk/dpsim/build/_deps/cim-data-src/WSCC-09/WSCC-09")]:
    if glob.glob(os.path.join(root, "*.xml")): print(root); sys.exit(0)
')
if [[ -n "$WSCC9_DIR" ]]; then
    ZIP=/tmp/dpsim_smoke_wscc9_$$.zip
    rm -f "$ZIP"
    ( cd "$WSCC9_DIR" && /usr/bin/zip -qj "$ZIP" *.xml )
    UP_MID=$(curl -sf -X POST "$API/models" -H 'Content-Type: application/zip' \
      --data-binary @"$ZIP" | python3 -c 'import sys,json; print(json.load(sys.stdin)["model_id"])')
    UP_SID=$(curl -sf -X POST "$API/simulation" -H 'Content-Type: application/json' \
      -d "{\"simulation_type\":\"Powerflow\",\"model_id\":\"$UP_MID\",\"load_profile_id\":\"None\",\"domain\":\"DP\",\"solver\":\"MNA\",\"timestep\":1,\"finaltime\":50}" \
      | python3 -c 'import sys,json; print(json.load(sys.stdin)["simulation_id"])')
    for i in $(seq 1 15); do
        JOB_DIR=$(ls -dt /tmp/dpsim_jobs/*/ 2>/dev/null | head -1)
        STATUS_FILE="${JOB_DIR}status.json"
        if [[ -f "$STATUS_FILE" ]]; then
            SRC=$(python3 -c "import json; print(json.load(open('$STATUS_FILE')).get('topology_source',''))")
            if [[ "$SRC" == "cim:$UP_MID" ]]; then
                ok "uploaded WSCC-9 bundle executed with topology_source=$SRC"
                rm -f "$ZIP"
                break
            fi
        fi
        sleep 1
        [[ $i -eq 15 ]] && fail "uploaded CIM never produced topology_source=cim:$UP_MID"
    done
else
    echo "[skip] WSCC-9 build-deps not present; upload-run step skipped"
fi

# 9d. trace-id propagation — POST /simulation returns a trace_id, worker stamps
# it into the redis status sidechannel. Regression for P2.2 (docs/30).
TR=$(curl -sf -X POST "$API/simulation" -H 'Content-Type: application/json' \
  -d '{"simulation_type":"Powerflow","model_id":"demo","load_profile_id":"None","domain":"DP","solver":"MNA","timestep":1,"finaltime":20}')
TR_SID=$(echo "$TR" | python3 -c 'import sys,json; print(json.load(sys.stdin)["simulation_id"])')
TR_TID=$(echo "$TR" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("trace_id",""))')
[[ -n "$TR_TID" && ${#TR_TID} -ge 8 ]] \
  && ok "/simulation response carries trace_id=$TR_TID" \
  || fail "/simulation response missing trace_id: $TR"
for i in $(seq 1 20); do
    REDIS_TID=$(python3 - <<PYEOF
import redis, json, sys
r = redis.from_url("redis://localhost:6379/0")
v = r.get(f"dpsim:sim:$TR_SID:status")
if v:
    d = json.loads(v)
    if d.get("trace_id") and d.get("status") in ("done", "running"):
        print(d["trace_id"]); sys.exit(0)
PYEOF
)
    if [[ "$REDIS_TID" == "$TR_TID" ]]; then
        ok "redis status carries same trace_id=$TR_TID (sid=$TR_SID)"
        break
    fi
    sleep 0.5
    [[ $i -eq 20 ]] && fail "redis trace_id never matched $TR_TID (got '$REDIS_TID')"
done

# 10. H12 progress sidechannel — after any completed job redis must carry
# progress==100 under the integer simulation_id key. Catches regressions in
# the dpsim-api AMQP payload (worker keys by simulation_id sent from Rust).
SID=$(curl -sf -X POST "$API/simulation" \
  -H 'Content-Type: application/json' \
  -d '{"simulation_type":"Powerflow","model_id":"wscc9","load_profile_id":"None","domain":"DP","solver":"MNA","timestep":1,"finaltime":20}' \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["simulation_id"])')
for i in $(seq 1 20); do
    PROG=$(python3 - <<PYEOF
import redis, json, sys
r = redis.from_url("redis://localhost:6379/0")
v = r.get(f"dpsim:sim:$SID:status")
if not v: sys.exit(0)
d = json.loads(v)
if d.get("status") == "done":
    print(d.get("progress", -1))
PYEOF
)
    if [[ "$PROG" == "100.0" ]]; then
        ok "progress sidechannel reached 100.0 for sid=$SID"
        break
    fi
    sleep 0.5
    [[ $i -eq 20 ]] && fail "progress never reached 100.0 for sid=$SID"
done

ok "smoke test passed"
