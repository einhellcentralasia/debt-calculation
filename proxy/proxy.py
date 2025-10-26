from flask import Flask, request, jsonify, Response
import os
import requests
from urllib.parse import quote

app = Flask(__name__)

# ----- Config from env (Render) -----
ONEC_USER = os.getenv("ONEC_USER")
ONEC_PASS = os.getenv("ONEC_PASS")
PROXY_TOKEN = os.getenv("PROXY_TOKEN")
ONEC_BASE_URL = os.getenv(
    "ONEC_BASE_URL",
    "https://buh.uchet.kz/M2luthgarimilonal0126/odata/standard.odata",
).rstrip("/")

# Poka-yoke: minimal validation
def check_env():
    missing = [k for k, v in {
        "ONEC_USER": ONEC_USER,
        "ONEC_PASS": ONEC_PASS,
        "PROXY_TOKEN": PROXY_TOKEN,
        "ONEC_BASE_URL": ONEC_BASE_URL,
    }.items() if not v]
    if missing:
        return f"Missing env vars: {', '.join(missing)}"
    return None

@app.route("/", methods=["GET"])
def health():
    err = check_env()
    if err:
        return jsonify({"status": "misconfigured", "error": err}), 500
    return jsonify({"status": "ok"}), 200

@app.route("/odata", methods=["GET"])
def odata_proxy():
    # Auth to use the proxy
    token = request.headers.get("X-Proxy-Token")
    if token != PROXY_TOKEN:
        return jsonify({"error": "Forbidden"}), 403

    # Required param: path to collection, e.g. "Catalog_Контрагенты"
    raw_path = request.args.get("path")
    if not raw_path:
        return jsonify({"error": "Missing 'path'"}), 400

    # Encode non-ASCII safely; keep characters used by OData filters intact
    safe_chars = "/()_-$',:= "  # allow OData operators in query params
    path_encoded = quote(raw_path, safe=safe_chars)

    # Forward ALL query params except 'path' to 1C (e.g., $top, $skip, $filter)
    forward_params = {k: v for k, v in request.args.items() if k != "path"}
    # Prefer Atom (v3)
    forward_params.setdefault("$format", "atom")

    target_url = f"{ONEC_BASE_URL}/{path_encoded}"

    try:
        resp = requests.get(
            target_url,
            params=forward_params,
            auth=(ONEC_USER, ONEC_PASS),
            headers={"Accept": "application/atom+xml", "User-Agent": "1C-Proxy/1.0"},
            timeout=90,
        )
        # Pass through status + content-type
        ctype = resp.headers.get("Content-Type", "application/xml")
        return Response(resp.content, status=resp.status_code, content_type=ctype)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
