#!/usr/bin/env python3
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
        response = BytesIO()        
        response.write(b'This is GET request. ')
        response.write(b'Original URL:')
        response.write(self.path.encode('utf-8'))
        response.write(b' \n')
        self.wfile.write(response.getvalue())


    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        body = self.rfile.read(content_length)
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')        
        self.end_headers()
        response = BytesIO()
        response.write(b'This is POST request. ')
        response.write(b'Body: ')
        response.write(body)
        response.write(b' \n')        
        self.wfile.write(response.getvalue())



def run_HTTP_server(server_class=HTTPServer, handler_class=SimpleHTTPRequestHandler):
    master_port = 8080
    httpd = HTTPServer(('', master_port), SimpleHTTPRequestHandler)
    httpd.serve_forever()
    
if __name__ == '__main__':
    run_HTTP_server()

