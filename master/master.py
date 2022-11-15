#!/usr/bin/env python3
import sys, os, json, time, logging, requests
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from jsonschema import validate
from tabulate import tabulate

from multiprocessing.pool import ThreadPool
import multiprocessing_logging

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

def replicate_msg(msg):
    logging.info(f'Replicating the message on every Secondary server')

    for secondary_host in secondary_hosts:
        logging.info(f'Replicating the message on {secondary_host.get("name")} ({secondary_host.get("hostname")}:{secondary_host.get("port")})')
        api_url = f'https:// {secondary_host.get("hostname")}:{secondary_host.get("port")}'
        request_body = {"userId": 1, "title": "Buy milk", "completed": False}
        response = requests.post(api_url, json=request_body)
        #print(response.json())
        #ack

"""
HTTP-server
"""
class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        logging.info(f'{self.address_string()} requested list of messages')
        try:
            if log_list:
                log_list_fmt = [ 
                                    {
                                        "id": msg.get("id"),
                                        "msg": msg.get("msg"),
                                        "created_ts" : datetime.utcfromtimestamp(msg.get("created_ts")).strftime("%Y-%m-%d %H:%M:%S.%f")
                                    } 
                                for msg in log_list 
                                ]
                log_list_str = tabulate(log_list_fmt, headers="keys")
                response = log_list_str
            else:
                response= 'The replication log is empty'
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Server', 'Master')
            self.end_headers()            
            self.wfile.write(response.encode('utf-8'))
            logging.info(response)
        except Exception as e:
            response = f"Exception: {e}"
            self.send_response(500)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Server', 'Master')
            self.end_headers()
            self.wfile.write(response.encode('utf-8'))     
            logging.error(f"Exception: {e}", stack_info=debug)

    def do_POST(self):
        logging.info(f'{self.address_string()} sent a request to append message')
        #time.sleep(5)        
        try:
            content_length = int(self.headers['Content-Length'])
            body = self.rfile.read(content_length).decode("utf-8")
            body_dict = json.loads(body)
            msg = body_dict.get("msg")

            #Validate input
            try:
                post_request_schema = {
                    "type": "object",
                    "properties": {
                        "name": {"msg": "string"},
                    },
                    "required": ["msg"],
                }                
                validate(instance=body_dict, schema=post_request_schema)

                # get last message id
                log_list_last_id = log_list[-1].get("id") if log_list else 0
                # append new message to log
                msg_id = log_list_last_id + 1
                ts = time.time()
                log_list.append({"id": msg_id, "msg": msg, "created_ts" : ts})
                logging.info(f'Received message \"{msg}\" has been added to log with id: {msg_id}')
                # trying to replicate message on every Secondary server
                logging.info(f'Replicating message id = {msg_id}, msg = \"{msg}\" on ...')

                response = f'Received message id = {msg_id}, msg = \"{msg}\" has been succesfully replicated'
                self.send_response(200)
                self.send_header('Content-Type', 'text/plain; charset=utf-8')
                self.send_header('Server', 'Master')
                self.end_headers()
                self.wfile.write(response.encode('utf-8'))
                logging.info(f'Received message id = {msg_id}, msg = \"{msg}\" has been succesfully replicated')

                #if
                #else
                #response = f'Failed to replicate message: id = {msg_id}, msg = \"{msg}\"'
                #self.send_response(599)
                #self.send_header('Content-Type', 'text/plain; charset=utf-8')
                #self.send_header('Server', 'Master')
                #self.end_headers()
                #self.wfile.write(response.encode('utf-8'))
                #logging.info(f'Failed to replicate message: id = {msg_id}, msg = \"{msg}\"')
            except Exception as e:
                response = f"Invalid POST request. Server accepts HTTP POST requests containing JSON of the following schema: {{\"msg\": \"message\"}}. Exception: {e}"               
                self.send_response(400)
                self.send_header('Content-Type', 'text/plain; charset=utf-8')
                self.send_header('Server', 'Master')
                self.end_headers()
                self.wfile.write(response.encode('utf-8'))
                logging.error(f"Invalid POST request. Received message {body} has incorrect form. Exception: {e}", stack_info=debug)
        except Exception as e:
            response = f"Exception: {e}"            
            self.send_response(500)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Server', 'Master')
            self.end_headers()
            self.wfile.write(response.encode('utf-8'))  
            logging.error(response, stack_info=debug)

    def log_message(self, format, *args):
        pass

def run_HTTP_server(server_class=HTTPServer, handler_class=SimpleHTTPRequestHandler):
    master_port = [e.get("port") for e in hosts if e.get("type") == "master"][0]
    httpd = HTTPServer(('', master_port), SimpleHTTPRequestHandler)
    logging.info(f'HTTP server started and listening on {master_port}')
    httpd.serve_forever()


# Init for shared variables
script_path = os.path.dirname(os.path.realpath(__file__))
hosts = get_config("Hosts")
secondary_hosts = list(filter(lambda host: host.get("type") == "secondary" and host.get("active") == 1, hosts))

log_list = []
"""
ts = time.time()
log_list = [
    {
        "id": 1, 
        "msg": "msg1", 
        "created_ts": ts, 
        "repl_status":
        [
            {
                "host_id": "1", 
                "ack": False,
                "ack_ts": None
            },
            {
                "host_id": "2", 
                "ack": False,
                "ack_ts": None
            }
        ]
    }
]
"""

def main():
    """
    The Main
    """
    logging.info('Master host has been started')
    try:
        run_HTTP_server();
    except Exception as e:
        logging.error(f"Exception: {e}", stack_info=debug)
        raise
    
if __name__ == '__main__':
    debug = get_config("debug")
    log_dir = get_config("LogDir")
    #logfile_name = datetime.now().strftime('master_%Y-%m-%d_%H-%M-%S.log')
    logfile_name = datetime.now().strftime('master.log')
    logfile_path = os.path.join(script_path, log_dir, logfile_name)
    logging.basicConfig(filename=logfile_path, format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    main()
