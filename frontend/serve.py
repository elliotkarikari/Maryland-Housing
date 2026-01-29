#!/usr/bin/env python3
"""
Simple HTTP server for Maryland Viability Atlas frontend.
Serves static files and enables CORS for local development.
"""

import http.server
import socketserver
import os
from pathlib import Path

# Configuration
PORT = 3000
FRONTEND_DIR = Path(__file__).parent

class CORSRequestHandler(http.server.SimpleHTTPRequestHandler):
    """HTTP request handler with CORS enabled"""

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

def main():
    os.chdir(FRONTEND_DIR)

    with socketserver.TCPServer(("", PORT), CORSRequestHandler) as httpd:
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
