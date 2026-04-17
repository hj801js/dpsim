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

ok "smoke test passed"
