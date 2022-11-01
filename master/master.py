#!/usr/bin/env python3
import sys, os, json, time, logging
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO

def get_config(key):
    """
    Read config
    """
    with open('config.json') as json_file:
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
        logging.info(f'{self.address_string()} GET = List messages: ')
        self.wfile.write(b'List of messages:')

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        body = self.rfile.read(content_length)
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')        
        self.end_headers()
        response = BytesIO()
        response.write(b'Received:\n' + body)
        self.wfile.write(response.getvalue())
        logging.info(f'{self.address_string()} POST = Append message: {body.decode("utf-8")}')

    def log_message(self, format, *args):
        #logging.info("%s %s" % (self.address_string(),format%args))
        pass

def run_HTTP_server(server_class=HTTPServer, handler_class=SimpleHTTPRequestHandler):
    hosts = get_config("Hosts")
    master_port = [e.get("port") for e in hosts if e.get("type") == "master"][0]
    httpd = HTTPServer(('', master_port), SimpleHTTPRequestHandler)
    logging.info(f'HTTP server started and listening on {master_port}')
    httpd.serve_forever()

"""
Main
"""
def main():
    """
    The Main
    """
    logging.info('Master host has been started')
    #while True:
    #    time.sleep(10)

    run_HTTP_server();

if __name__ == '__main__':
    log_dir = get_config("LogDir")
    logfile_name = datetime.now().strftime('master_%Y-%m-%d_%H-%M-%S.log')
    logfile_path = os.path.join(log_dir, logfile_name)
    logging.basicConfig(filename=logfile_path, format='%(asctime)s %(message)s', level=logging.INFO)
    main()