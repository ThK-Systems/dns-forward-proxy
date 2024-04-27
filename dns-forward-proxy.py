import json
import signal
import socket
import logging
import threading
import time
from socketserver import ThreadingUDPServer, BaseRequestHandler


DEFAULT_TIMEOUT = "2"


def load_configuration(filename):
    with open(filename, 'r') as file:
        return json.load(file)


configuration_path = 'dns-forward-proxy.json'
configuration = load_configuration(configuration_path)

logging.basicConfig(filename=configuration["logfile"], level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info(f"Loading configuration from file '{configuration_path}': {configuration}")


class UDPRequestHandler(BaseRequestHandler):

    def handle(self):
        request_data = self.request[0].strip()
        response_data = forward_request(request_data)
        self.request[1].sendto(response_data, self.client_address)


def forward_request(request_data):
    for forwarder in configuration["forwarders"]:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as dns_socket:
                dns_socket.settimeout(int(forwarder.get("timeout", DEFAULT_TIMEOUT)))
                dns_socket.sendto(request_data, (forwarder["address"], int(forwarder.get("port", "53"))))
                response_data, _ = dns_socket.recvfrom(1024)
            return response_data
        except socket.timeout:
            logging.warning(f"Forwarder could not be connected: {forwarder}.")
            continue
    logging.error("No more forwarders are available")
    return b''


def main():
    bind_address = configuration["listen"].get("address", "0.0.0.0")
    bind_port = int(configuration["listen"].get("port", "53"))

    logging.info("Starting DNS-Forward-Proxy")
    server = ThreadingUDPServer((bind_address, bind_port), UDPRequestHandler)
    logging.info(f"Binding DNS-Forward-Proxy to {bind_address}:{bind_port}")
    thread = threading.Thread(target=server.serve_forever)  # that thread will start one more thread for each request
    thread.daemon = True  # exit the server thread when the main thread terminates
    thread.start()

    signal.signal(signal.SIGTERM, handle_sigterm)

    try:
        while 1:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()


def handle_sigterm(signum, frame):
    logging.info("Exiting DNS-Forward-Proxy")
    raise SystemExit


if __name__ == "__main__":
    main()
