#!/usr/bin/env python3

import json
import os
import subprocess
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


PORT = int(os.environ.get('PORT', '8765'))
HOST = os.environ.get('HOST', '0.0.0.0')
PYTHON_BIN = os.environ.get('CURATORR_ANALYZER_PYTHON', sys.executable or 'python3')
SCRIPT_PATH = os.environ.get(
    'CURATORR_ANALYZER_SCRIPT',
    os.path.join(os.path.dirname(__file__), 'analyze-track-features.py'),
)


def write_json(handler, status, payload):
    body = json.dumps(payload).encode('utf-8')
    handler.send_response(status)
    handler.send_header('Content-Type', 'application/json')
    handler.send_header('Content-Length', str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class AnalyzerHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def do_GET(self):
        if self.path.rstrip('/') == '/healthz':
            return write_json(self, 200, {
                'ok': True,
                'service': 'curatorr-analyzer-sidecar',
                'scriptPath': SCRIPT_PATH,
            })
        return write_json(self, 404, { 'ok': False, 'error': 'not-found' })

    def do_POST(self):
        if self.path.rstrip('/') != '/analyze':
            return write_json(self, 404, { 'ok': False, 'error': 'not-found' })

        try:
            length = int(self.headers.get('Content-Length', '0') or '0')
        except ValueError:
            length = 0
        try:
            raw = self.rfile.read(length) if length > 0 else b'{}'
            payload = json.loads(raw.decode('utf-8') or '{}')
        except Exception:
            return write_json(self, 400, { 'ok': False, 'error': 'invalid-json' })

        input_path = str(payload.get('inputPath') or '').strip()
        output_path = str(payload.get('outputPath') or '').strip()
        if not input_path or not output_path:
          return write_json(self, 400, { 'ok': False, 'error': 'input-output-required' })

        command = [
            PYTHON_BIN,
            SCRIPT_PATH,
            '--input',
            input_path,
            '--output',
            output_path,
        ]
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=False)
        except Exception as exc:
            return write_json(self, 500, {
                'ok': False,
                'error': 'spawn-failed',
                'message': str(exc),
            })

        if result.returncode != 0:
            return write_json(self, 500, {
                'ok': False,
                'error': 'analysis-failed',
                'returnCode': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr,
            })

        return write_json(self, 200, {
            'ok': True,
            'returnCode': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'outputPath': output_path,
        })


def main():
    server = ThreadingHTTPServer((HOST, PORT), AnalyzerHandler)
    print(f'[curatorr-analyzer] listening on http://{HOST}:{PORT}', flush=True)
    server.serve_forever()


if __name__ == '__main__':
    main()
