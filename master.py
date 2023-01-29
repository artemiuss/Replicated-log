#!/usr/bin/env python3
import sys, os, json, time, logging, requests, threading
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from jsonschema import validate
from tabulate import tabulate
from random import randint

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
CountDownLatch implementation
https://superfastpython.com/thread-countdown-latch/
simple countdown latch, starts closed then opens once count is reached
"""
class CountDownLatch():

    # constructor
    def __init__(self, count):
        # store the count
        self.count = count
        # control access to the count and notify when latch is open
        self.condition = threading.Condition()

    # count down the latch by one increment
    def count_down(self):
        # acquire the lock on the condition
        with self.condition:
            # check if the latch is already open
            if self.count == 0:
                return
            # decrement the counter
            self.count -= 1
            # check if the latch is now open
            if self.count == 0:
                # notify all waiting threads that the latch is open
                self.condition.notify_all()

    # wait for the latch to open
    def wait(self):
        # acquire the lock on the condition
        with self.condition:
            # check if the latch is already open
            if self.count == 0:
                return
            # wait to be notified when the latch is open
            self.condition.wait()


"""
HTTP-server
"""
# Processing Simultaneous/Asynchronous Requests with Python BaseHTTPServer
# https://stackoverflow.com/a/12651298
class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    pass

class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    # create a lock
    lock = threading.Lock()

    def do_GET(self):
        try:
            if self.path == '/health':
                logging.info(f'[GET] {self.address_string()} requested secondaries health status')      
                secondary_health_fmt = [
                                            {
                                                "secondary_name" : secondary_host.get("name"),
                                                "health_check_status" : secondary_statuses[secondary_host["id"]]
                                            } 
                                        for secondary_host in secondary_hosts     
                                        ]
                secondary_health_str = tabulate(secondary_health_fmt, headers="keys", tablefmt="simple_grid")
                response = "Secondaries health status:\n" + secondary_health_str
            else:
                logging.info(f'[GET] {self.address_string()} requested list of messages')    

                if log_list:
                    log_list_fmt = [ 
                                        {
                                            "id" : msg.get("id"),
                                            "msg" : msg.get("msg"),
                                            "w" : msg.get("w"),
                                            "replicated_ts" : datetime.utcfromtimestamp(msg.get("replicated_ts")).strftime("%Y-%m-%d %H:%M:%S.%f") if msg.get("replicated_ts") != None else "NOT REPLICATED"
                                        } 
                                    for msg in log_list
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

    def replicate_msg(self, latch, secondary_host, msg_dict):
        url = f'http://{secondary_host.get("hostname")}:{secondary_host.get("port")}'
        thread_name =  threading.current_thread().name
        logging.info(f"[POST] START {thread_name}")
        sleep_delay = 0
        while True:
            time.sleep(sleep_delay)
            try:
                if secondary_locks[secondary_host["id"]] is not None:
                    secondary_locks[secondary_host["id"]].wait()
                # https://requests.readthedocs.io/en/latest/user/advanced/#timeouts
                response = requests.post(url, json=msg_dict, timeout=(3.5,None)) # (connect timeout, read timeout)
                if response.status_code == 200:
                    latch.count_down()
                    logging.info(f"[POST] {thread_name}. The message has been succesfully replicated")
                    break
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                logging.info(f'[POST] {thread_name}. {secondary_host.get("name")} not available. Retrying in {sleep_delay}s ...')
            except Exception as e:
                logging.error(f"[POST] {thread_name}. An exception of type {type(e).__name__} occurred. Arguments: {e.args}")
            finally:
                # "smart" delays logic
                if sleep_delay < 60:
                    sleep_delay += randint(1, 10)
                else:
                    sleep_delay = randint(1, 10)
        logging.info(f"[POST] END {thread_name}")

    def do_POST(self):
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
            
            # Quorum check
            if not get_quorum():
                response = f"There is no Secondaries quorum. The Master has been switched into read-only mode and does not accept messages append requests"
                logging.info('[POST] ' + response)
                self.send_response(200)
                self.send_header('Content-Type', 'text/plain; charset=utf-8')
                self.send_header('Server', 'Master')
                self.end_headers()
                response = response + '\n'
                self.wfile.write(response.encode('utf-8'))
                return
            
            # acquire the lock
            self.lock.acquire()
            #log_list_last_index = max(log_list, key=lambda msg:msg['id']).get('id') if log_list else 0
            msg_id = len(log_list) + 1
            msg_dict = {"id": msg_id, "msg": msg, "replicated_ts" : None, "w": w}
            # add new message to log
            log_list.append(msg_dict)
            logging.info(f"[POST] Received message \"" + msg_dict["msg"] + "\" has been added to log with id: " + str(msg_dict["id"]))
            # release the lock
            self.lock.release()
            
            logging.info(f'[POST] Replicating the message')
            
            # create the countdown latch
            latch = CountDownLatch(w-1)
            
            for secondary_host in secondary_hosts:
                t = threading.Thread(
                                        target=self.replicate_msg, 
                                        name="Replicating msg_id = " + str(msg_dict["id"]) + " on " + secondary_host.get("name"),
                                        args=(latch, secondary_host, msg_dict)
                                    )
                t.start()
            
            # wait for the latch to close
            latch.wait()
            
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

def health_check(secondary_host):
    try:
        status_prev = secondary_statuses[secondary_host["id"]]
        url = f'http://{secondary_host.get("hostname")}:{secondary_host.get("port")}/health'
        response = requests.get(url, timeout=(3,1)) # (connect timeout, read timeout)
        request_failed = False
        
        if response.status_code == 200:
            secondary_statuses[secondary_host["id"]] = "Healthy"
            secondary_locks[secondary_host["id"]].count_down()
        else:
            request_failed = True
    except (requests.ConnectionError, requests.Timeout) as e:
        request_failed = True
    except Exception as e:
        logging.error(f'[Heartbeat check] Exception: {e}')
    finally:
        if request_failed:
            if secondary_statuses[secondary_host["id"]] is None or secondary_statuses[secondary_host["id"]] == "Healthy":
                secondary_statuses[secondary_host["id"]] = "Suspected"
            elif secondary_statuses[secondary_host["id"]] == "Suspected":
                secondary_statuses[secondary_host["id"]] = "Unhealthy"
        if secondary_statuses[secondary_host["id"]] != status_prev and not (secondary_statuses[secondary_host["id"]] == "Healthy" and status_prev is None):
            logging.info(f'[Heartbeat check] {secondary_host.get("name")} is in {secondary_statuses[secondary_host["id"]]} status')

def get_quorum():
    if any(value is None or value == "Healthy" for value in secondary_statuses.values()):
        return True
    else:
        return False

def heartbeats():
    time.sleep(5)
    quorum = None
    while True:
        threads = []
        for secondary_host in secondary_hosts:
            if secondary_statuses[secondary_host["id"]] is None or secondary_statuses[secondary_host["id"]] == "Healthy":
                secondary_locks[secondary_host["id"]] = CountDownLatch(1)
            t = threading.Thread(
                                    target=health_check,
                                    name="[Heartbeat check] Checking " + secondary_host.get("name") + " health status",
                                    args=(secondary_host,)
                                )
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        time.sleep(5)

        quorum_prev = quorum
        quorum = get_quorum()

        if not quorum and not quorum == quorum_prev:
            logging.info(f'[Heartbeat check] Master has been switched into read-only mode. Waiting for Secondaries quorum')
        if quorum and not quorum == quorum_prev and not quorum_prev is None:
            logging.info(f'[Heartbeat check] The Secondaries quorum has been restored. Master is ready to accept messages append requests')
        
# Init for shared variables
script_path = os.path.dirname(os.path.realpath(__file__))
hosts = get_config("Hosts")
secondary_hosts = list(filter(lambda host: host.get("type") == "secondary" and host.get("active") == 1, hosts))
secondary_statuses = {secondary_host["id"]:None for secondary_host in secondary_hosts}
secondary_locks = {secondary_host["id"]:None for secondary_host in secondary_hosts}
log_list = []

def main():
    """
    The Main
    """
    logging.info('Master host has been started')
    try:
        t = threading.Thread(target=heartbeats)
        t.start()

        run_HTTP_server()
    except Exception as e:
        logging.error(f"Exception: {e}", stack_info=debug)
        raise
    
if __name__ == '__main__':
    debug = get_config("debug")
    logfile_name = datetime.now().strftime('master.log')
    logfile_path = os.path.join(script_path, logfile_name)
    try:
        os.remove(logfile_name)
    except OSError:
        pass

    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(message)s',
        level=logging.DEBUG if debug else logging.INFO,
        handlers=[
            logging.FileHandler(logfile_path),
            logging.StreamHandler()
        ]
    )
    main()

