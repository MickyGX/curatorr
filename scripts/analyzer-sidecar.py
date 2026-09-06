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
DEFAULT_MAX_DURATION_SECONDS = os.environ.get('CURATORR_ANALYZER_MAX_DURATION_SECONDS', '180')


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

        try:
            track_delay_ms = max(0, int(payload.get('trackDelayMs') or 0))
        except (TypeError, ValueError):
            track_delay_ms = 0
        raw_max_duration_seconds = payload.get('maxDurationSeconds')
        if raw_max_duration_seconds is None or str(raw_max_duration_seconds).strip() == '':
            raw_max_duration_seconds = DEFAULT_MAX_DURATION_SECONDS
        max_duration_seconds = str(raw_max_duration_seconds).strip()

        command = [
            PYTHON_BIN,
            SCRIPT_PATH,
            '--input',
            input_path,
            '--output',
            output_path,
        ]
        if track_delay_ms > 0:
            command += ['--track-delay-ms', str(track_delay_ms)]
        if max_duration_seconds:
            command += ['--max-duration-seconds', max_duration_seconds]
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=False)
        except Exception as exc:
            return write_json(self, 500, {
                'ok': False,
                'error': 'spawn-failed',
                'message': str(exc),
            })

        if result.stderr:
            print(result.stderr, end='', file=sys.stderr, flush=True)

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
