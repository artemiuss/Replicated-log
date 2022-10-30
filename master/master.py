#!/usr/bin/env python3
import sys, os, json, time, logging
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO

def get_config(working_dir_path, key):
    with open(os.path.join(working_dir_path, 'config', 'config.json')) as json_file:
        try:
            dict_conf = json.load(json_file)
            return dict_conf[key]
        except:
            raise

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
    logging.info('Master host has been started')
    logging.info('script_dir_path: ' + script_dir_path)
    mapping = get_mapping(script_dir_path)
    delimiter = get_config(script_dir_path, "OutputFileDelimiter")
    file_extestion = get_config(script_dir_path, "OutputFileExtension")
    output_dir = get_config(script_dir_path, "OutputDir")
    output_path = os.path.join(script_dir_path, output_dir)

    #while True:
    #    time.sleep(10)

    run_HTTP_server();

if __name__ == '__main__':
    log_dir = get_config(script_dir_path, "LogDir")
    logfile_name = datetime.now().strftime('random_data_gen_%Y-%m-%d_%H-%M-%S.log')
    logfile_path = os.path.join(script_dir_path, log_dir, logfile_name)
    logging.basicConfig(filename=logfile_path, format='%(asctime)s %(message)s', level=logging.INFO)
    main()