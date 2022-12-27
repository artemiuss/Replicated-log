#!/usr/bin/env python3
import sys, os, json, time, logging
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from jsonschema import validate
from tabulate import tabulate

def get_config(key):
    """
    Read config
    """
    with open(os.path.join(script_path,'config.json')) as json_file:
        try:
            dict_conf = json.load(json_file)
            return dict_conf[key]
        except:
            raise

"""
HTTP-server
"""
# Processing Simultaneous/Asynchronous Requests with Python BaseHTTPServer
# https://stackoverflow.com/a/12651298
class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    pass

class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        logging.info(f'{self.address_string()} requested list of messages')
        try:
            if log_list:
                log_list_fmt = [ 
                                    {
                                        "id": msg.get("id"),
                                        "msg": msg.get("msg"),
                                        "replicated_ts" : datetime.utcfromtimestamp(msg.get("replicated_ts")).strftime("%Y-%m-%d %H:%M:%S.%f")
                                    } 
                                for msg in log_list 
                                ]
                log_list_str = tabulate(log_list_fmt, headers="keys", tablefmt="simple_grid")
                response = 'The replication log:\n' + log_list_str
            else:
                response = 'The replication log is empty'
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Server', 'Secondary')
            self.end_headers()
            response = response + '\n'
            self.wfile.write(response.encode('utf-8'))
            logging.info(response)
        except Exception as e:
            response = f"Exception: {e}"
            self.send_response(500)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Server', 'Secondary')
            self.end_headers()
            response = response + '\n'
            self.wfile.write(response.encode('utf-8'))
            logging.error(f"Exception: {e}", stack_info=debug)

    def do_POST(self):
        logging.info(f'{self.address_string()} sent a request to replicate message')
        try:
            content_length = int(self.headers['Content-Length'])
            body = self.rfile.read(content_length).decode("utf-8")
            msg_dict = json.loads(body)
            
            logging.info(f"Received message \"" + msg_dict["msg"] + "\" has been added to log with id: " + str(msg_dict["id"]))
            
            response = f"The message msg_id = " + str(msg_dict["id"]) +", msg = \"" + msg_dict["msg"] + "\" has been succesfully replicated"            
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Server', 'Secondary')
            self.end_headers()
            response = response + '\n'
            self.wfile.write(response.encode('utf-8'))
            logging.info(response)   
        except Exception as e:
            response = f"Exception: {e}"
            self.send_response(500)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Server', 'Secondary')
            self.end_headers()
            response = response + '\n'
            self.wfile.write(response.encode('utf-8'))  
            logging.error(response, stack_info=debug)

    def log_message(self, format, *args):
        pass


def run_HTTP_server(server_class=ThreadedHTTPServer, handler_class=SimpleHTTPRequestHandler):
    secondary_port = [e.get("port") for e in hosts if e.get("type") == "secondary" and e.get("id") == int(secondary_id)][0]
    httpd = ThreadedHTTPServer(('', secondary_port), SimpleHTTPRequestHandler)
    logging.info(f'HTTP server started and listening on {secondary_port}')
    httpd.serve_forever()

# Init for shared variables
script_path = os.path.dirname(os.path.realpath(__file__))
hosts = get_config("Hosts")
master_host = [e.get("port") for e in hosts if e.get("type") == "master"][0]
log_list = []

def main():
    """
    The Main
    """
    logging.info('Secondary host has been started')
    try:
        run_HTTP_server();
    except Exception as e:
        logging.error(f"Exception: {e}", stack_info=debug)
        raise

if __name__ == '__main__':
    sys_ags = sys.argv
    if len(sys.argv) == 1:
        sys.exit("Please provide the Sedondary id (1 or 2) as first argument")
    secondary_id = sys_ags[1]

    debug = get_config("debug")
    logfile_name = datetime.now().strftime('secondary.log')
    logfile_path = os.path.join(script_path, logfile_name)
    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(message)s',
        level=logging.INFO,
        handlers=[
            logging.FileHandler(logfile_path),
            logging.StreamHandler()
        ]
    )
    main()
   