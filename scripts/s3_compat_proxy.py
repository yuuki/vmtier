"""Loopback S3 test proxy: no conditional requests, no response ETag.

This is a compatibility/failure-injection test fixture, not an S3 implementation
or production proxy. It forwards only to the harness's disposable Moto server.
"""
import base64
import hashlib
import http.server
import threading
import urllib.error
import urllib.request

HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


class S3CompatibilityProxy:
    def __init__(self, upstream):
        self.upstream = upstream.rstrip('/')
        self.lock = threading.Lock()
        self.unavailable = threading.Event()
        self.counts = {'requests': 0, 'conditional_requests_rejected': 0,
                       'etag_headers_removed': 0, 'put_requests': 0,
                       'put_content_md5': 0, 'bad_content_md5': 0,
                       'range_requests': 0, 'unavailable_requests': 0,
                       'unavailable_get_requests': 0}
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def _count(self, key):
                with owner.lock:
                    owner.counts[key] += 1

            def _error(self, status, code):
                body = f'<Error><Code>{code}</Code><Message>vmtier test S3 proxy</Message></Error>'.encode()
                self.send_response(status)
                self.send_header('Content-Type', 'application/xml')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                if self.command != 'HEAD':
                    self.wfile.write(body)

            def _body(self):
                if self.headers.get('Transfer-Encoding', '').lower() == 'chunked':
                    chunks, total = [], 0
                    while True:
                        size = int(self.rfile.readline().split(b';', 1)[0], 16)
                        if not size:
                            while self.rfile.readline() not in (b'\r\n', b'\n', b''):
                                pass
                            break
                        total += size
                        if total > 128 << 20:
                            raise ValueError('test request too large')
                        chunks.append(self.rfile.read(size))
                        if self.rfile.read(2) != b'\r\n':
                            raise ValueError('invalid chunk framing')
                    return b''.join(chunks)
                length = int(self.headers.get('Content-Length', '0'))
                if not 0 <= length <= 128 << 20:
                    raise ValueError('test request too large')
                return self.rfile.read(length)

            def _forward(self):
                self._count('requests')
                if any(name.lower().startswith(('if-', 'x-amz-copy-source-if-'))
                       for name in self.headers):
                    self._count('conditional_requests_rejected')
                    self.close_connection = True
                    self._error(501, 'NotImplemented')
                    return
                if self.headers.get('Range'):
                    self._count('range_requests')
                try:
                    body = self._body()
                    if owner.unavailable.is_set():
                        self._count('unavailable_requests')
                        if self.command == 'GET':
                            self._count('unavailable_get_requests')
                        self._error(503, 'VMTierInjectedBlobOutage')
                        return
                    if self.command == 'PUT':
                        self._count('put_requests')
                        content_md5 = self.headers.get('Content-MD5')
                        if content_md5:
                            self._count('put_content_md5')
                            if content_md5 != base64.b64encode(hashlib.md5(body).digest()).decode():
                                self._count('bad_content_md5')
                                self._error(400, 'BadDigest')
                                return
                    headers = {k: v for k, v in self.headers.items()
                               if k.lower() not in ('connection', 'transfer-encoding', 'content-length', 'host')}
                    req = urllib.request.Request(owner.upstream + self.path, data=body if self.command in ('PUT', 'POST') else None,
                                                 headers=headers, method=self.command)
                    try:
                        response = HTTP.open(req, timeout=15)
                    except urllib.error.HTTPError as exc:
                        response = exc
                    with response:
                        result = response.read()
                        self.send_response(response.status)
                        for key, value in response.headers.items():
                            if key.lower() == 'etag':
                                self._count('etag_headers_removed')
                            elif key.lower() not in ('connection', 'transfer-encoding', 'content-length', 'server', 'date'):
                                self.send_header(key, value)
                        if self.command == 'HEAD':
                            length = response.headers.get('Content-Length', '0')
                        else:
                            length = str(len(result))
                        self.send_header('Content-Length', length)
                        self.end_headers()
                        if self.command != 'HEAD':
                            self.wfile.write(result)
                except (urllib.error.URLError, TimeoutError):
                    self._error(502, 'ServiceUnavailable')
                except (BrokenPipeError, ConnectionResetError):
                    pass
                except ValueError:
                    self._error(400, 'InvalidRequest')

            do_GET = do_PUT = do_HEAD = do_DELETE = do_POST = _forward

            def log_message(self, *args):
                pass

        self.server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}'

    def snapshot(self):
        with self.lock:
            return dict(self.counts)

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
