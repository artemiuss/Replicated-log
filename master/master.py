#!/usr/bin/env python3
import sys, os, json, time, logging, requests
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from jsonschema import validate

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
        #request_body = {"userId": 1, "title": "Buy milk", "completed": False}
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
            dict_log_str = json.dumps(dict_log, indent=2)
            response = b'List of messages:' + dict_log_str.encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Server', 'Master')
            self.end_headers()            
            self.wfile.write(response)
            logging.info(f'Returned list of messages: {dict_log_str}')
        except Exception as e:
            response = f"Exception: {e}".encode('utf-8')
            self.send_response(500)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Server', 'Master')
            self.end_headers()
            self.wfile.write(response)     
            logging.error(f"Exception: {e}", stack_info=debug)

    def do_POST(self):
        logging.info(f'{self.address_string()} sent a request to append message')
        post_request_schema = {
            "type": "object",
            "properties": {
                "name": {"msg": "string"},
            },
            "required": ["msg"],
        }
        
        try:
            content_length = int(self.headers['Content-Length'])
            body = self.rfile.read(content_length).decode("utf-8")

            #Validate input
            try:
                validate(instance=json.loads(body), schema=post_request_schema)
                response = f'Received message {body} has been added to log'.encode('utf-8')
                self.send_response(200)
                self.send_header('Content-Type', 'text/plain; charset=utf-8')
                self.send_header('Server', 'Master')
                self.end_headers()
                self.wfile.write(response)
                logging.info(f'Received message {body} has been added to log')    
            except Exception as e:
                response = f"Invalid POST request. Server accepts HTTP POST requests containing JSON of the following schema: {{\"msg\": \"message\"}}. Exception: {e}".encode('utf-8')                
                self.send_response(400)
                self.send_header('Content-Type', 'text/plain; charset=utf-8')
                self.send_header('Server', 'Master')
                self.end_headers()
                self.wfile.write(response)
                logging.error(f"Invalid POST request. Received message {body} has incorrect form. Exception: {e}", stack_info=debug)
        except Exception as e:
            response = f"Exception: {e}".encode('utf-8')            
            self.send_response(500)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Server', 'Master')
            self.end_headers()
            self.wfile.write(response)  
            logging.error(f"Exception: {e}", stack_info=debug)

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
dict_log = {}

"""
Main
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
    #%(funcName)s:%(lineno)d
    logging.basicConfig(filename=logfile_path, format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    main()
