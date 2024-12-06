# network.py

import threading
import socket
import json
import time
from queue import Queue

class Network:
    def __init__(self, node_id, nodes_info, network_server_info, message_handler):
        self.node_id = node_id
        self.nodes_info = nodes_info
        self.network_server_info = network_server_info
        self.message_handler = message_handler
        self.network_server_socket = None  # Connection to the network server
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
                # Send node_id to network server
                initial_message = {'node_id': self.node_id}
                self.network_server_socket.sendall(json.dumps(initial_message).encode())
                print(f"Node {self.node_id} connected to Network Server.")
                break
            except Exception as e:
                print(f"Node {self.node_id} failed to connect to Network Server: {e}")
                time.sleep(1)

    def receive_messages_from_network_server(self):
        while self.active:
            try:
                data = self.network_server_socket.recv(4096)
                if data:
                    message = json.loads(data.decode())
                    self.message_queue.put(message)
                else:
                    break
            except Exception as e:
                print(f"Error receiving messages from Network Server: {e}")
                break
        self.network_server_socket.close()
        self.active = False
        print(f"Node {self.node_id} disconnected from Network Server.")

    def send_message(self, recipient_id, message):
        # Include 'to' field in the message
        message['to'] = recipient_id
        message['from'] = self.node_id
        try:
            self.network_server_socket.sendall(json.dumps(message).encode())
        except Exception as e:
            print(f"Error sending message to Network Server: {e}")

    def broadcast_message(self, message):
        for node_id in self.nodes_info:
            if node_id != self.node_id:
                self.send_message(node_id, message)

    def process_incoming_messages(self):
        while self.active:
            message = self.message_queue.get()
            self.message_handler(message)

    def stop(self):
        self.active = False
        self.network_server_socket.close()
        print("Network stopped.")
