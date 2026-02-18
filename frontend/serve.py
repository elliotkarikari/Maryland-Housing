#!/usr/bin/env python3
"""
Simple HTTP server for Maryland Viability Atlas frontend.
Serves static files and enables CORS for local development.
"""

import http.server
import socketserver
import os
from pathlib import Path
import json

# Configuration
PORT = int(os.getenv("FRONTEND_PORT", os.getenv("PORT", "3000")))
FRONTEND_DIR = Path(__file__).parent
PROJECT_ROOT = FRONTEND_DIR.parent
DOTENV_PATH = PROJECT_ROOT / ".env"


def load_dotenv(path: Path) -> dict:
    """Load simple KEY=VALUE pairs from .env without importing extra deps."""
    values = {}
    if not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if not key:
            continue

        if (
            len(value) >= 2
            and ((value[0] == value[-1] == '"') or (value[0] == value[-1] == "'"))
        ):
            value = value[1:-1]

        values[key] = value

    return values

class CORSRequestHandler(http.server.SimpleHTTPRequestHandler):
    """HTTP request handler with CORS enabled"""

    def do_GET(self):
        if self.path in ("/runtime-config.js", "/frontend/runtime-config.js"):
            self.serve_runtime_config()
            return
        super().do_GET()

    def end_headers(self):
        # Enable CORS
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate')
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def serve_runtime_config(self):
        env_values = load_dotenv(DOTENV_PATH)
        payload = {
            "MAPBOX_ACCESS_TOKEN": env_values.get("MAPBOX_ACCESS_TOKEN", ""),
            "ATLAS_API_BASE_URL": env_values.get("ATLAS_API_BASE_URL", "")
        }
        body = (
            "window.ATLAS_RUNTIME_CONFIG = "
            + json.dumps(payload)
            + ";\n"
            + "if (!window.MAPBOX_ACCESS_TOKEN && window.ATLAS_RUNTIME_CONFIG.MAPBOX_ACCESS_TOKEN) { "
            + "window.MAPBOX_ACCESS_TOKEN = window.ATLAS_RUNTIME_CONFIG.MAPBOX_ACCESS_TOKEN; }\n"
            + "if (!window.ATLAS_API_BASE_URL && window.ATLAS_RUNTIME_CONFIG.ATLAS_API_BASE_URL) { "
            + "window.ATLAS_API_BASE_URL = window.ATLAS_RUNTIME_CONFIG.ATLAS_API_BASE_URL; }\n"
        ).encode("utf-8")

        self.send_response(200)
        self.send_header("Content-Type", "application/javascript; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class ReusableTCPServer(socketserver.TCPServer):
    """TCP server configured to allow immediate port reuse after restart."""

    allow_reuse_address = True


def main():
    os.chdir(FRONTEND_DIR)

    with ReusableTCPServer(("", PORT), CORSRequestHandler) as httpd:
        print(f"""
╔════════════════════════════════════════════════════════════╗
║  Maryland Growth & Family Viability Atlas                 ║
║  Frontend Server Running                                   ║
╚════════════════════════════════════════════════════════════╝

🌐 Map Interface: http://localhost:{PORT}
🗺️  API Endpoint:  http://localhost:8000

📊 Data Status:
   - Counties: 24 Maryland counties
   - Primary Layer: synthesis_grouping
   - Current Classification: high_uncertainty (all areas)

💡 Instructions:
   1. Ensure API server is running: uvicorn src.api.main:app
   2. Open http://localhost:{PORT} in your browser
   3. Click on any county to see detailed analysis

Press Ctrl+C to stop the server.
        """)

        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n\n👋 Server stopped. Goodbye!\n")

if __name__ == "__main__":
    main()
