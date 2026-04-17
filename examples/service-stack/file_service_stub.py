"""Minimal sogno-file-service stand-in used by the local DPsim stack demo.

Endpoints used by dpsim-api:
  POST /api/files           -> {"data":{"fileID":"<new>"}}
  GET  /api/files/<fid>     -> {"data":{"url":"<self>/raw/<fid>"}}

Endpoints used by worker.py to push results:
  PUT  /api/files/<fid>     body: raw bytes  -> {"ok": true}
  GET  /raw/<fid>           -> raw bytes

Service health:
  GET  /healthz             -> {"status":"ok", "files": N}

Everything lives in-memory; restarting the process clears the store.
Replace with MinIO/S3 for anything resembling production.
"""
from flask import Flask, jsonify, request, Response
import os
import uuid

app = Flask(__name__)
STORE: dict[str, bytes] = {}

HOST = os.environ.get("FILE_SERVICE_HOST", "127.0.0.1")
PORT = int(os.environ.get("FILE_SERVICE_PORT", 18080))


def _self_base() -> str:
    return f"http://{HOST}:{PORT}"


@app.post("/api/files")
def post_file():
    fid = uuid.uuid4().hex[:8]
    STORE[fid] = b""
    return jsonify({"data": {"fileID": fid}})


@app.get("/api/files/<fid>")
def get_file_meta(fid):
    return jsonify({"data": {"url": f"{_self_base()}/raw/{fid}"}})


@app.put("/api/files/<fid>")
def put_file(fid):
    STORE[fid] = request.get_data() or b""
    return jsonify({"ok": True, "fileID": fid, "bytes": len(STORE[fid])})


@app.get("/raw/<fid>")
def raw(fid):
    data = STORE.get(fid)
    if data is None:
        return ("not found", 404)
    return Response(data, mimetype="text/csv")


@app.get("/healthz")
def healthz():
    return jsonify({"status": "ok", "files": len(STORE)})


@app.get("/api/_debug/files")
def debug_list():
    return jsonify({fid: len(buf) for fid, buf in STORE.items()})


if __name__ == "__main__":
    app.run(host=HOST, port=PORT, threaded=True)
