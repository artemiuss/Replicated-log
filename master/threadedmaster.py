import logging, requests
import concurrent.futures

from master.master import secondary_hosts


def replicate_msg(msg):
    logging.info(f'Replicating the message on every Secondary server')
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []

        for secondary_host in secondary_hosts:
            logging.info(f'Replicating the message on {secondary_host.get("name")} ({secondary_host.get("hostname")}:{secondary_host.get("port")})')
            api_url = f'https://{secondary_host.get("hostname")}:{secondary_host.get("port")}'
            futures.append(executor.submit(requests.post(api_url, json=msg)))

        for future in concurrent.futures.as_completed(futures):
            print(future.result())




