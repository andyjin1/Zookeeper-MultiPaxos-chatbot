import threading
import socket
import json
import time
from queue import Queue
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Network:
    def __init__(self, node_id, nodes_info, network_server_info, message_handler):
        self.node_id = node_id
        self.nodes_info = nodes_info
        self.network_server_info = network_server_info
        self.message_handler = message_handler
        self.network_server_socket = None
        self.message_queue = Queue()
        self.active = True
        self.lock = threading.Lock()

        self.connect_to_network_server()
        threading.Thread(target=self.receive_messages_from_network_server, daemon=True).start()
        threading.Thread(target=self.process_incoming_messages, daemon=True).start()

    def connect_to_network_server(self):
        while self.active:
            try:
                self.network_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.network_server_socket.connect((self.network_server_info['host'], self.network_server_info['port']))
                # Send node_id to network server with delimiter
                initial_message = {'node_id': self.node_id}
                data = json.dumps(initial_message).encode() + b'\0'
                self.network_server_socket.sendall(data)
                print(f"Node {self.node_id} connected to Network Server.")
                break
            except Exception as e:
                print(f"Node {self.node_id} failed to connect to Network Server: {e}")
                time.sleep(1)

    def receive_messages_from_network_server(self):
        buffer = b''  # buffer to hold partial data
        while self.active:
            try:
                chunk = self.network_server_socket.recv(4096)
                if chunk:
                    buffer += chunk
                    # Split by delimiter '\0'
                    parts = buffer.split(b'\0')
                    # All complete messages end with '\0', so all parts except possibly the last are complete
                    for msg_bytes in parts[:-1]:
                        # Process each complete message
                        if msg_bytes.strip():
                            try:
                                message = json.loads(msg_bytes.decode())
                                self.message_queue.put(message)
                                #logging.info(f"Received message: {message}")
                            except json.JSONDecodeError as e:
                                logging.error(f"JSON decode error: {e}")
                    # The last part may be incomplete, keep it in buffer
                    buffer = parts[-1]
                else:
                    logging.warning("No data received, closing connection.")
                    break
            except socket.error as e:
                logging.error(f"Socket error: {e}")
                break
            except Exception as e:
                logging.error(f"Unexpected error: {e}")
                break

        if self.network_server_socket:
            self.network_server_socket.close()
        self.active = False
        logging.info(f"Node {self.node_id} disconnected from Network Server.")

    def send_message(self, recipient_id, message):
        if message['type'] in ["STATUS_REQUEST", "STATUS_RESPONSE"]:
            print(f"Routing {message['type']} to {recipient_id}.")
        message['to'] = recipient_id
        message['from'] = self.node_id
        data = json.dumps(message).encode() + b'\0'
        try:
            self.network_server_socket.sendall(data)
        except Exception as e:
            print(f"Error sending message to Network Server: {e}")

    # Route status messages to the correct recipient
    def send_message_prepare(self, recipient_id, message):
        if message['type'] in ["STATUS_REQUEST", "STATUS_RESPONSE"]:
            print(f"Routing {message['type']} to {recipient_id}.")
        message['to'] = recipient_id
        message['from'] = message['op_num']
        data = json.dumps(message).encode() + b'\0'
        try:
            self.network_server_socket.sendall(data)
        except Exception as e:
            print(f"Error sending message to Network Server: {e}")

    def broadcast_message(self, message):
        for node_id in self.nodes_info:
            if node_id != self.node_id:
                self.send_message(node_id, message)

    def process_incoming_messages(self):
        while self.active:
            message = self.message_queue.get()
            if message:
                self.message_handler(message)

    def stop(self):
        self.active = False
        if self.network_server_socket:
            self.network_server_socket.close()
        print("Network stopped.")

    def restart(self):
        self.active = True
        self.connect_to_network_server()
        threading.Thread(target=self.receive_messages_from_network_server, daemon=True).start()
        threading.Thread(target=self.process_incoming_messages, daemon=True).start()
        print("Network restarted.")
