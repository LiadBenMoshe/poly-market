"""Local dashboard: trade history, wins/losses, PnL after fees and total money.

Started automatically by run.py (DASHBOARD_PORT, 0 disables), or on its own:
    python dashboard.py        ->  http://127.0.0.1:8050
"""
from __future__ import annotations

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from config import BASE_DIR, Settings, get_settings
from ledger import Ledger

logger = logging.getLogger(__name__)
PAGE = BASE_DIR / "dashboard.html"


def build_payload(settings: Settings) -> dict:
    ledger = Ledger(settings.data_dir / "trades.json", settings.paper_bankroll_usdc)
    status_path = settings.data_dir / "status.json"
    status = json.loads(status_path.read_text(encoding="utf-8")) if status_path.exists() else {}
    open_exposure = ledger.open_exposure()
    if status.get("mode") == "live":
        cash = float(status.get("cash_usdc") or 0.0)
        start = None  # unknown for a live wallet; PnL comes from the ledger
    else:
        cash = ledger.paper_bankroll()
        start = settings.paper_bankroll_usdc
    return {
        "mode": status.get("mode") or ("paper" if settings.dry_run else "live"),
        "bot_updated_at": status.get("updated_at"),
        "bot_running": status.get("running", False),
        "cash_usdc": cash,
        "open_exposure_usdc": open_exposure,
        "total_usdc": cash + open_exposure,
        "starting_usdc": start,
        "stats": ledger.stats(),
        "trades": sorted(ledger.trades, key=lambda t: t["opened_at"], reverse=True),
    }


class _Handler(BaseHTTPRequestHandler):
    settings: Settings

    def do_GET(self) -> None:  # noqa: N802
        if self.path.startswith("/api/data"):
            body = json.dumps(build_payload(self.settings)).encode()
            ctype = "application/json"
        elif self.path in ("/", "/index.html"):
            body = PAGE.read_bytes()
            ctype = "text/html; charset=utf-8"
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args) -> None:  # keep the bot's console clean
        pass


def serve_in_background(settings: Settings) -> ThreadingHTTPServer | None:
    if not settings.dashboard_port:
        return None
    handler = type("Handler", (_Handler,), {"settings": settings})
    try:
        server = ThreadingHTTPServer((settings.dashboard_host, settings.dashboard_port), handler)
    except OSError as exc:
        logger.warning("dashboard not started (port %s busy?): %s", settings.dashboard_port, exc)
        return None
    threading.Thread(target=server.serve_forever, name="dashboard", daemon=True).start()
    logger.info("dashboard: http://%s:%s", settings.dashboard_host, settings.dashboard_port)
    return server


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    s = get_settings()
    handler = type("Handler", (_Handler,), {"settings": s})
    print(f"Dashboard: http://{s.dashboard_host}:{s.dashboard_port}")
    ThreadingHTTPServer((s.dashboard_host, s.dashboard_port), handler).serve_forever()
