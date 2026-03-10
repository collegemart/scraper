"""BharatFare Universal Scraper Dashboard.

Run with: python dashboard.py
Opens at: http://localhost:5000
"""

import csv
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from flask import Flask, Response, jsonify, render_template, request, send_file

app = Flask(__name__)

BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

SCRAPY_EXE = Path(__file__).parent.parent / ".venv" / "Scripts" / "scrapy.exe"
if not SCRAPY_EXE.exists():
    SCRAPY_EXE = "scrapy"

# ── State ──────────────────────────────────────────────────

run_counter = 0
runs = {}  # id -> run state dict
lock = threading.Lock()


def _new_run(run_id, url, max_pages):
    parsed = urlparse(url)
    domain = (parsed.netloc or parsed.path.split("/")[0]).replace("www.", "")
    return {
        "id": run_id,
        "url": url,
        "domain": domain,
        "status": "running",      # running, finished, error, stopped
        "pid": None,
        "process": None,
        "csv_file": None,
        "log_file": None,
        "pages_crawled": 0,
        "pages_per_min": 0,
        "items_scraped": 0,
        "items_per_min": 0,
        "max_pages": max_pages,
        "progress": 0,
        "eta_seconds": None,
        "started_at": time.time(),
        "elapsed": 0,
        "error": None,
    }


# ── Background threads ─────────────────────────────────────

def _tail_log(run_id, log_path):
    """Parse Scrapy logstats from log file."""
    pattern = re.compile(
        r"Crawled (\d+) pages? \(at (\d+) pages?/min\),\s*"
        r"scraped (\d+) items? \(at (\d+) items?/min\)"
    )
    while True:
        with lock:
            run = runs.get(run_id)
            if not run or run["status"] != "running":
                break

        try:
            if log_path.exists():
                text = log_path.read_text(encoding="utf-8", errors="ignore")
                matches = pattern.findall(text)
                if matches:
                    last = matches[-1]
                    pages, ppm, items, ipm = int(last[0]), int(last[1]), int(last[2]), int(last[3])
                    with lock:
                        r = runs[run_id]
                        r["pages_crawled"] = pages
                        r["pages_per_min"] = ppm
                        r["items_scraped"] = items
                        r["items_per_min"] = ipm
                        est = r["max_pages"]
                        r["progress"] = min(99, int(pages / est * 100)) if est else 0
                        r["eta_seconds"] = int(max(0, est - pages) / ppm * 60) if ppm > 0 else None
                        if r["started_at"]:
                            r["elapsed"] = int(time.time() - r["started_at"])
        except Exception:
            pass
        time.sleep(2)


def _wait_process(run_id, proc):
    """Wait for subprocess to exit."""
    proc.wait()
    with lock:
        run = runs.get(run_id)
        if not run:
            return
        if run["status"] == "running":
            run["status"] = "finished" if proc.returncode == 0 else "error"
        run["progress"] = 100 if run["status"] == "finished" else run["progress"]
        run["pid"] = None
        run["process"] = None
        if run["started_at"]:
            run["elapsed"] = int(time.time() - run["started_at"])
        if proc.returncode and proc.returncode != 0:
            run["error"] = f"Exit code {proc.returncode}"

        csv_path = run.get("csv_file")
        if csv_path and Path(csv_path).exists():
            try:
                with open(csv_path, encoding="utf-8") as f:
                    run["items_scraped"] = max(0, sum(1 for _ in f) - 1)
            except Exception:
                pass


# ─── Routes ────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/scrape", methods=["POST"])
def start_scrape():
    global run_counter

    data = request.get_json(silent=True) or {}
    url = data.get("url", "").strip()
    if not url:
        return jsonify({"error": "Provide a URL"}), 400
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    max_pages = int(data.get("max_pages", 50))
    depth = int(data.get("depth", 2))
    follow = str(data.get("follow", "true"))
    scroll = str(data.get("scroll", "true"))

    with lock:
        run_counter += 1
        run_id = f"run_{run_counter}"

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file = OUTPUT_DIR / f"{run_id}_{ts}.csv"
    log_file = OUTPUT_DIR / f"{run_id}_{ts}.log"

    cmd = [
        str(SCRAPY_EXE), "crawl", "universal",
        "-a", f"url={url}",
        "-a", f"max_pages={max_pages}",
        "-a", f"depth={depth}",
        "-a", f"follow={follow}",
        "-a", f"scroll={scroll}",
        "-o", f"{csv_file}:csv",
        "-s", f"LOG_FILE={log_file}",
        "-s", "LOG_LEVEL=INFO",
    ]

    creation_flags = 0
    if sys.platform == "win32":
        creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP

    proc = subprocess.Popen(
        cmd,
        cwd=str(BASE_DIR),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=creation_flags,
    )

    with lock:
        run = _new_run(run_id, url, max_pages)
        run["pid"] = proc.pid
        run["process"] = proc
        run["csv_file"] = str(csv_file)
        run["log_file"] = str(log_file)
        runs[run_id] = run

    threading.Thread(target=_tail_log, args=(run_id, log_file), daemon=True).start()
    threading.Thread(target=_wait_process, args=(run_id, proc), daemon=True).start()

    return jsonify({"status": "started", "id": run_id, "pid": proc.pid})


@app.route("/api/runs")
def list_runs():
    result = []
    with lock:
        for rid, run in runs.items():
            result.append({
                "id": rid,
                "url": run["url"],
                "domain": run["domain"],
                "status": run["status"],
                "pages_crawled": run["pages_crawled"],
                "pages_per_min": run["pages_per_min"],
                "items_scraped": run["items_scraped"],
                "items_per_min": run["items_per_min"],
                "max_pages": run["max_pages"],
                "progress": run["progress"],
                "eta_seconds": run["eta_seconds"],
                "elapsed": run["elapsed"],
                "csv_file": os.path.basename(run["csv_file"]) if run.get("csv_file") else None,
                "error": run["error"],
            })
    return jsonify(list(reversed(result)))


@app.route("/api/runs/<run_id>/stop", methods=["POST"])
def stop_run(run_id):
    with lock:
        run = runs.get(run_id)
        if not run:
            return jsonify({"error": "Not found"}), 404
        if run["status"] != "running" or not run["process"]:
            return jsonify({"error": "Not running"}), 409
        proc = run["process"]

    try:
        if sys.platform == "win32":
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            proc.terminate()
        with lock:
            runs[run_id]["status"] = "stopped"
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({"status": "stopping"})


@app.route("/api/runs/<run_id>/items")
def get_items(run_id):
    with lock:
        run = runs.get(run_id)
        if not run:
            return jsonify({"error": "Not found"}), 404
        csv_file = run.get("csv_file")

    limit = request.args.get("limit", 50, type=int)

    if not csv_file or not Path(csv_file).exists():
        return jsonify([])

    try:
        rows = []
        with open(csv_file, encoding="utf-8", errors="ignore") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
        return jsonify(rows[-limit:])
    except Exception:
        return jsonify([])


@app.route("/api/runs/<run_id>/delete", methods=["POST"])
def delete_run(run_id):
    with lock:
        run = runs.get(run_id)
        if not run:
            return jsonify({"error": "Not found"}), 404
        if run["status"] == "running":
            return jsonify({"error": "Stop it first"}), 409
        del runs[run_id]
    return jsonify({"status": "deleted"})


@app.route("/api/files")
def list_files():
    files = []
    for f in sorted(OUTPUT_DIR.glob("*.csv"), key=os.path.getmtime, reverse=True):
        try:
            with open(f, encoding="utf-8") as fp:
                rows = max(0, sum(1 for _ in fp) - 1)
        except Exception:
            rows = 0
        files.append({
            "name": f.name,
            "size": f.stat().st_size,
            "rows": rows,
            "modified": datetime.fromtimestamp(f.stat().st_mtime).isoformat(),
        })
    return jsonify(files)


@app.route("/api/files/<name>/download")
def download_file(name):
    filepath = OUTPUT_DIR / name
    if not filepath.exists() or filepath.suffix != ".csv":
        return jsonify({"error": "File not found"}), 404
    return send_file(filepath, as_attachment=True)


@app.route("/api/events")
def sse_events():
    def generate():
        while True:
            data = []
            with lock:
                for rid, run in runs.items():
                    data.append({
                        "id": rid,
                        "url": run["url"],
                        "domain": run["domain"],
                        "status": run["status"],
                        "pages_crawled": run["pages_crawled"],
                        "pages_per_min": run["pages_per_min"],
                        "items_scraped": run["items_scraped"],
                        "items_per_min": run["items_per_min"],
                        "progress": run["progress"],
                        "eta_seconds": run["eta_seconds"],
                        "elapsed": run["elapsed"],
                        "error": run["error"],
                    })
            yield f"data: {json.dumps(list(reversed(data)))}\n\n"
            time.sleep(3)

    return Response(generate(), mimetype="text/event-stream")


if __name__ == "__main__":
    print()
    print("=" * 50)
    print("  BharatFare Universal Scraper")
    print("  Open: http://localhost:5000")
    print("=" * 50)
    print()
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
