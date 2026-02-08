#!/usr/bin/env python3
"""
Development server for Maryland Viability Atlas frontend.
Serves static files with CORS and auto-reload on file changes.

Usage:
    python frontend/serve.py              # live-reload enabled
    python frontend/serve.py --no-reload  # static server only
"""

import http.server
import socketserver
import os
import sys
import json
import threading
import time
from pathlib import Path

# Configuration
PORT = 3000
FRONTEND_DIR = Path(__file__).parent
WATCH_EXTENSIONS = {'.html', '.css', '.js', '.json'}
RELOAD_DEBOUNCE_MS = 300

# Track file modification times
_file_versions = {}
_version_counter = 0
_version_lock = threading.Lock()


def scan_files():
    """Scan frontend files and return latest mtime."""
    latest = 0
    for ext in WATCH_EXTENSIONS:
        for f in FRONTEND_DIR.glob(f'*{ext}'):
            try:
                mtime = f.stat().st_mtime
                if mtime > latest:
                    latest = mtime
            except OSError:
                pass
    return latest


def watch_files():
    """Background thread that watches for file changes."""
    global _version_counter
    last_mtime = scan_files()

    while True:
        time.sleep(0.5)
        current_mtime = scan_files()
        if current_mtime > last_mtime:
            last_mtime = current_mtime
            with _version_lock:
                _version_counter += 1
                v = _version_counter
            print(f"  \033[36m↻\033[0m File change detected (v{v}), browsers will reload")


# Small JS snippet injected into HTML responses for auto-reload
RELOAD_SCRIPT = """
<!-- live-reload -->
<script>
(function() {
  let v = 0;
  async function poll() {
    try {
      const r = await fetch('/__reload');
      const d = await r.json();
      if (v && d.v !== v) { location.reload(); return; }
      v = d.v;
    } catch(e) {}
    setTimeout(poll, 800);
  }
  poll();
})();
</script>
"""


class LiveReloadHandler(http.server.SimpleHTTPRequestHandler):
    """HTTP handler with CORS, no-cache, and live-reload support."""

    enable_reload = True

    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate')
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        # Reload polling endpoint
        if self.path == '/__reload':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            with _version_lock:
                v = _version_counter
            self.wfile.write(json.dumps({"v": v}).encode())
            return

        # For HTML files, inject the reload script
        if self.enable_reload and (self.path == '/' or self.path.endswith('.html')):
            # Resolve the file path
            if self.path == '/':
                file_path = FRONTEND_DIR / 'index.html'
            else:
                file_path = FRONTEND_DIR / self.path.lstrip('/')

            if file_path.exists():
                content = file_path.read_text(encoding='utf-8')
                # Inject reload script before </body>
                if '</body>' in content:
                    content = content.replace('</body>', RELOAD_SCRIPT + '</body>')
                encoded = content.encode('utf-8')

                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.send_header('Content-Length', str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)
                return

        super().do_GET()

    def log_message(self, format, *args):
        # Suppress reload poll spam
        if '/__reload' in str(args):
            return
        super().log_message(format, *args)


def main():
    enable_reload = '--no-reload' not in sys.argv
    LiveReloadHandler.enable_reload = enable_reload

    os.chdir(FRONTEND_DIR)

    # Start file watcher
    if enable_reload:
        watcher = threading.Thread(target=watch_files, daemon=True)
        watcher.start()

    reload_status = "\033[32m● live-reload on\033[0m" if enable_reload else "○ static mode"

    with socketserver.TCPServer(("", PORT), LiveReloadHandler) as httpd:
        print(f"""
╔════════════════════════════════════════════════════════════╗
║  Maryland Growth & Family Viability Atlas                 ║
║  Frontend Server Running                                   ║
╚════════════════════════════════════════════════════════════╝

🌐 Map Interface: http://localhost:{PORT}
🗺️  API Endpoint:  http://localhost:8000
{reload_status}

Edit index.html, map.js, or any .css/.js file and the
browser will automatically reload — no restart needed.

Press Ctrl+C to stop the server.
        """)

        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n\n👋 Server stopped. Goodbye!\n")


if __name__ == "__main__":
    main()
