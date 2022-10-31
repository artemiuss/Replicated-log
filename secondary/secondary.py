import logging
import json
import os
import time
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO
from tkinter import Tk

from airflow.utils import yaml

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)


class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    command_list = []

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.end_headers()
        self.wfile.write(json.dumps(self.command_list).encode())

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_body = self.rfile.read(content_length)

        self.command_list.append(str(post_body))
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.end_headers()

        response = BytesIO()
        response.write(b'This is POST request. ')
        response.write(b'Received: ')
        response.write(post_body)
        self.wfile.write(response.getvalue())


def keep_running():
    return True


def run_HTTP_server(server_class=HTTPServer, handler_class=SimpleHTTPRequestHandler):

    log_dir = config['log_dir']
    logfile_name = datetime.now().strftime('secondary_%Y-%m-%d_%H-%M-%S.log')
    logfile_path = os.path.join(log_dir, logfile_name)
    print(logfile_path)
    logging.basicConfig(filename=logfile_path, format='%(asctime)s %(message)s', level=logging.INFO)

    logging.info('Secondary host has been started')

    httpd = HTTPServer(('', config['port']), SimpleHTTPRequestHandler)
    while keep_running():
        time.sleep(config['delay'])
        httpd.handle_request()


if __name__ == '__main__':

    run_HTTP_server()

