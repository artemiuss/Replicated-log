#!/usr/bin/env python3
import sys, os, json, time, logging, requests
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from jsonschema import validate
from tabulate import tabulate
from threading import Lock
import multiprocessing

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
    # create a lock
    lock = Lock()

    manager = multiprocessing.Manager()
    repl_status_dict = manager.dict()

    def do_GET(self):
        logging.info(f'[GET] {self.address_string()} requested list of messages')
        try:
            if log_list:
                log_list_sort = sorted(log_list, key=lambda msg: msg['id'])
                log_list_fmt = [ 
                                    {
                                        "id": msg.get("id"),
                                        "msg": msg.get("msg"),
                                        "w": msg.get("w"),
                                        "replicated_ts": datetime.utcfromtimestamp(msg.get("replicated_ts")).strftime("%Y-%m-%d %H:%M:%S.%f") if msg.get("replicated_ts") != None else "NOT REPLICATED"
                                    } 
                                for msg in log_list_sort
                                ]
                log_list_str = tabulate(log_list_fmt, headers="keys", tablefmt="simple_grid")
                response = 'The replication log:\n' + log_list_str
            else:
                response = 'The replication log is empty'
            logging.info('[GET] ' + response)
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Server', 'Master')
            self.end_headers()
            response = response + '\n'
            self.wfile.write(response.encode('utf-8'))
        except Exception as e:
            logging.error('[GET] ' + response, stack_info=debug)
            response = f"Exception: {e}"
            self.send_response(500)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Server', 'Master')
            self.end_headers()
            response = response + '\n'
            self.wfile.write(response.encode('utf-8'))

    def do_POST(self):
        def replicate_msg(secondary_host, msg_dict):
            url = f'http://{secondary_host.get("hostname")}:{secondary_host.get("port")}'    
            host_id = secondary_host.get("id")        
            process = multiprocessing.current_process()
            logging.info(f"[POST] [process {process.pid}] START {process.name}")
            sleep_delay = 1
            while True:
                try:
                    # https://requests.readthedocs.io/en/latest/user/advanced/#timeouts
                    response = requests.post(url, json=msg_dict, timeout=(3.5,None)) # (connect timeout, read timeout)
                    self.repl_status_dict[(msg_dict["id"],secondary_host["id"])] = response.status_code
                    
                    if response.status_code == 200:
                        logging.info(f"[POST] [process {process.pid}] The message msg_id = " + str(msg_dict["id"]) +", msg = \"" + msg_dict["msg"] + "\" has been succesfully replicated on " + secondary_host.get("name"))
                        break
                except (requests.ConnectionError, requests.Timeout) as e:
                    logging.info(f"[POST] [process {process.pid}] " + secondary_host.get("name") + f" not available. Retrying in {sleep_delay}s ...")
                except Exception as e:
                    logging.error(f"[POST] [process {process.pid}] Exception: {e}")
                finally:
                    time.sleep(sleep_delay)
                    # "smart" delays logic
                    if sleep_delay < 60:
                        sleep_delay = sleep_delay + 1
                    else:
                        sleep_delay = 1
            logging.info(f"[POST] [process {process.pid}] END {process.name}")

        logging.info(f'[POST] {self.address_string()} sent a request to append message')
        try:
            content_length = int(self.headers['Content-Length'])
            body = self.rfile.read(content_length).decode("utf-8")
            body_dict = json.loads(body)
            msg = body_dict.get("msg")
            w = body_dict.get("w") or 3

            #Validate input
            try:
                post_request_schema = {
                    "type": "object",
                    "properties": {
                        "msg": {"type": "string"},
                        "w": {"type": "integer"}, 
                    },
                    "required": ["msg"]
                }
                validate(instance=body_dict, schema=post_request_schema)
            except Exception as e:
                response = f"Invalid POST request. Server accepts HTTP POST requests containing JSON of the following schema: {{\"msg\": \"message\"}}. Exception: {e}"               
                self.send_response(400)
                self.send_header('Content-Type', 'text/plain; charset=utf-8')
                self.send_header('Server', 'Master')
                self.end_headers()
                response = response + '\n'
                self.wfile.write(response.encode('utf-8'))
                logging.error(f"[POST] Invalid POST request. Received message {body} has incorrect form. Exception: {e}", stack_info=debug)

            # acquire the lock
            self.lock.acquire()
            log_list_last_id = max(log_list, key=lambda msg:msg['id']).get('id') if log_list else 0
            msg_id = log_list_last_id + 1
            msg_dict = {"id": msg_id, "msg": msg, "replicated_ts" : None, "w": w}
            # append new message to log
            log_list.append(msg_dict)
            logging.info(f"[POST] Received message \"" + msg_dict["msg"] + "\" has been added to log with id: " + str(msg_dict["id"]))
            # release the lock
            self.lock.release()
            
            logging.info(f'[POST] Replicating the message')
            for secondary_host in secondary_hosts:
                self.repl_status_dict[(msg_dict["id"],secondary_host["id"])] = None
                p = multiprocessing.Process(target=replicate_msg, 
                                            name="Replicating msg_id = " + str(msg_dict["id"]) +", msg = \"" + msg_dict["msg"] + "\" on " + secondary_host.get("name"),
                                            args=(secondary_host, msg_dict))
                p.start()                            

            #check replication results
            while True:
                ack_count = 0 
                for secondary_host in secondary_hosts:
                    if self.repl_status_dict[(msg_dict["id"],secondary_host["id"])] == 200:
                        ack_count = ack_count + 1 
                if ack_count >= w-1:
                    break
                time.sleep(1)

            #set ts
            log_list[msg_id-1]["replicated_ts"] = time.time()

            response = f"The message msg_id = " + str(msg_dict["id"]) +", msg = \"" + msg_dict["msg"] + "\" has been succesfully replicated"
            logging.info('[POST] ' + response)
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Server', 'Master')
            self.end_headers()
            response = response + '\n'
            self.wfile.write(response.encode('utf-8'))
        except Exception as e:
            logging.error('[POST] ' + response, stack_info=debug)
            response = f"Failed to replicate message: msg_id = {msg_id}, msg = \"{msg}\". Exception: {e}"
            self.send_response(500)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Server', 'Master')
            self.end_headers()
            response = response + '\n'
            self.wfile.write(response.encode('utf-8'))  

    def log_message(self, format, *args):
        pass

def run_HTTP_server(server_class=ThreadedHTTPServer, handler_class=SimpleHTTPRequestHandler):
    master_port = [e.get("port") for e in hosts if e.get("type") == "master"][0]
    httpd = ThreadedHTTPServer(('', master_port), SimpleHTTPRequestHandler)
    logging.info(f'HTTP server started and listening on {master_port}')
    httpd.serve_forever()

# Init for shared variables
script_path = os.path.dirname(os.path.realpath(__file__))
hosts = get_config("Hosts")
secondary_hosts = list(filter(lambda host: host.get("type") == "secondary" and host.get("active") == 1, hosts))

log_list = []

def main():
    """
    The Main
    """
    logging.info('Master host has been started')
    try:
        run_HTTP_server()
    except Exception as e:
        logging.error(f"Exception: {e}", stack_info=debug)
        raise
    
if __name__ == '__main__':
    debug = get_config("debug")
    logfile_name = datetime.now().strftime('master.log')
    logfile_path = os.path.join(script_path, logfile_name)
    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(message)s',
        level=logging.DEBUG if debug else logging.INFO,
        handlers=[
            logging.FileHandler(logfile_path),
            logging.StreamHandler()
        ]
    )
    main()
   
