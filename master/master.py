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
    # https://stackoverflow.com/questions/18279941/maintaining-log-file-from-multiple-threads-in-python
    lock = Lock()

    manager = multiprocessing.Manager()
    repl_status_dict = manager.dict()

    def do_GET(self):
        logging.info(f'[GET] {self.address_string()} requested list of messages')
        try:
            if log_list:
                log_list_fmt = [ 
                                    {
                                        "id": msg.get("id"),
                                        "msg": msg.get("msg"),
                                        "w": msg.get("w"),
                                        "replicated_ts": datetime.utcfromtimestamp(msg.get("replicated_ts")).strftime("%Y-%m-%d %H:%M:%S.%f") if msg.get("replicated_ts") != None else "NOT REPLICATED"
                                    } 
                                for msg in log_list
                                ]
                log_list_str = tabulate(log_list_fmt, headers="keys", tablefmt="simple_grid")
                response = 'The replication log:\n' + log_list_str
            else:
                response = 'The replication log is empty'
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Server', 'Master')
            self.end_headers()
            response = response + '\n'
            self.wfile.write(response.encode('utf-8'))
            logging.info('[GET] ' + response)
        except Exception as e:
            response = f"Exception: {e}"
            self.send_response(500)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Server', 'Master')
            self.end_headers()
            response = response + '\n'
            self.wfile.write(response.encode('utf-8'))
            logging.error('[GET] ' + response, stack_info=debug)

    def do_POST(self):
        def replicate_msg(secondary_host, msg_dict):
            url = f'http://{secondary_host.get("hostname")}:{secondary_host.get("port")}'    
            host_id = secondary_host.get("id")        
            process = multiprocessing.current_process()
            logging.info(f"[POST] START process [{process.pid}] {process.name}")
            try:
                response = requests.post(url, json=msg_dict)
                self.repl_status_dict[(msg_dict["id"],secondary_host["id"])] = response.status_code
                if response.status_code == 200:
                    logging.info(f"[POST]       process [{process.pid}] The message msg_id = " + str(msg_dict["id"]) +", msg = \"" + msg_dict["msg"] + "\" has been succesfully replicated on " + secondary_host.get("name"))
                else:
                    logging.info(f"[POST]       process [{process.pid}] Failed to replicate message msg_id = " + str(msg_dict["id"]) +", msg = \"" + msg_dict["msg"] + "\" on " + secondary_host.get("name"))
            except Exception as e:
                logging.error(f"[POST] [{process.pid}]: Exception: {e}", stack_info=debug)
            finally:
                logging.info(f"[POST] END   process [{process.pid}] {process.name}")

        logging.info(f'[POST] {self.address_string()} sent a request to append message')
        try:
            content_length = int(self.headers['Content-Length'])
            body = self.rfile.read(content_length).decode("utf-8")
            body_dict = json.loads(body)
            msg = body_dict.get("msg")
            w = body_dict.get("w")

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
            log_list_last_id = log_list[-1].get("id") if log_list else 0
            msg_id = log_list_last_id + 1
            msg_dict = {"id": msg_id, "msg": msg, "replicated_ts" : None, "w": w}
            # append new message to log
            log_list.append(msg_dict)
            logging.info(f"[POST] Received message \"" + msg_dict["msg"] + "\" has been added to log with id: " + str(msg_dict["id"]))
            # release the lock
            self.lock.release()
            
            #prepare replication
            is_repl_failed = False            
            #manager = multiprocessing.Manager()
            #repl_status_dict = manager.dict()
            procs = []
            for secondary_host in secondary_hosts:
                self.repl_status_dict[(msg_dict["id"],secondary_host["id"])] = None
                p = multiprocessing.Process(target=replicate_msg, 
                                            name="Replicating msg_id = " + str(msg_dict["id"]) +", msg = \"" + msg_dict["msg"] + "\" on " + secondary_host.get("name"),
                                            args=(secondary_host, msg_dict))
                procs.append(p)

            if w is None:
                # Blocking mode (write concern has not been specified)
                logging.info(f'[POST] Replicating the message in the BLOCKING MODE on every Secondary server')
                # trying to replicate message on every Secondary server
                for proc in procs:
                    proc.start()
                for proc in procs:
                    proc.join()
                #check replication results
                for secondary_host in secondary_hosts:
                    if self.repl_status_dict[(msg_dict["id"],secondary_host["id"])] != 200:
                        is_repl_failed = True
                        break
            else:
                # Async mode
                logging.info(f'[POST] Replicating the message in the ASYNC MODE with Write Concern parameter specified')
                # trying to replicate message on every Secondary server
                for proc in procs:
                    proc.start()
                #check replication results
                while True:
                    ack_count = 0 
                    for secondary_host in secondary_hosts:
                        if self.repl_status_dict[(msg_dict["id"],secondary_host["id"])] == 200:
                            ack_count = ack_count + 1 
                    if ack_count >= w-1:
                        break
                    time.sleep(1)                    

            if is_repl_failed == False:
                #set ts
                log_list[msg_id-1]["replicated_ts"] = time.time()

                response = f"The message msg_id = " + str(msg_dict["id"]) +", msg = \"" + msg_dict["msg"] + "\" has been succesfully replicated"
                self.send_response(200)
                self.send_header('Content-Type', 'text/plain; charset=utf-8')
                self.send_header('Server', 'Master')
                self.end_headers()
                response = response + '\n'
                self.wfile.write(response.encode('utf-8'))
                logging.info('[POST] ' + response)
            else:
                response = f'Failed to replicate message: msg_id = {msg_id}, msg = \"{msg}\"'
                self.send_response(599)
                self.send_header('Content-Type', 'text/plain; charset=utf-8')
                self.send_header('Server', 'Master')
                self.end_headers()
                response = response + '\n'
                self.wfile.write(response.encode('utf-8'))
                logging.info('[POST] ' + response)                                       
        except Exception as e:
            response = f"Exception: {e}"
            self.send_response(500)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Server', 'Master')
            self.end_headers()
            response = response + '\n'
            self.wfile.write(response.encode('utf-8'))  
            logging.error('[POST] ' + response, stack_info=debug)

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
        run_HTTP_server();
    except Exception as e:
        logging.error(f"Exception: {e}", stack_info=debug)
        raise
    
if __name__ == '__main__':
    debug = get_config("debug")
    logfile_name = datetime.now().strftime('master.log')
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
   
