#!/usr/bin/env python3
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO

"""
HTTP-server
"""
class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.end_headers()
        self.wfile.write(b'Hello, world!')
        
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        body = self.rfile.read(content_length)
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')        
        self.end_headers()
        response = BytesIO()
        response.write(b'This is POST request. ')
        response.write(b'Received: ')
        response.write(body)
        self.wfile.write(response.getvalue())

def run_HTTP_server(server_class=HTTPServer, handler_class=SimpleHTTPRequestHandler):
    httpd = HTTPServer(('', 8080), SimpleHTTPRequestHandler)
    httpd.serve_forever()

def main():
    """
    The Main
    """
    #while True:
    #    time.sleep(10)

    run_HTTP_server();

if __name__ == '__main__':
    main()
