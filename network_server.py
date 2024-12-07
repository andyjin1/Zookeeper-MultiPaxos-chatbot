# network_server.py

import threading
import socket
import json
import time

import threading
import socket
import json
import time

class NetworkServer:
    def __init__(self, host, port, nodes_info):
        self.host = host
        self.port = port
        self.nodes_info = nodes_info
        self.node_sockets = {}
        self.node_addresses = {}
        self.links_status = {}
        self.active = True
        self.lock = threading.Lock()
        self.start_server()

    def start_server(self):
        threading.Thread(target=self.accept_connections, daemon=True).start()
        threading.Thread(target=self.command_interface, daemon=True).start()

    def accept_connections(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen()
        print("Network Server started and listening for node connections...")
        while self.active:
            conn, addr = server_socket.accept()
            threading.Thread(target=self.handle_node_connection, args=(conn,), daemon=True).start()

    def handle_node_connection(self, conn):
        try:
            node_id = self.read_delimited_message(conn)
            if node_id and node_id in self.nodes_info:
                with self.lock:
                    self.node_sockets[node_id] = conn
                    self.node_addresses[node_id] = self.nodes_info[node_id]
                    # Initialize links as up
                    for other_node_id in self.nodes_info:
                        if other_node_id != node_id:
                            self.links_status[(node_id, other_node_id)] = True
                            self.links_status[(other_node_id, node_id)] = True
                print(f"Node {node_id} connected to Network Server.")
                self.listen_to_node(node_id, conn)
            else:
                conn.close()
        except Exception as e:
            print(f"Error handling node connection: {e}")

    def read_delimited_message(self, conn):
        buffer = b''
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                return None
            buffer += chunk
            parts = buffer.split(b'\0')
            # We only expect one initial message for node_id
            if len(parts) > 1:
                msg_bytes = parts[0]
                buffer = parts[-1]  # leftover (if any)
                message = json.loads(msg_bytes.decode())
                return message.get('node_id')

    def listen_to_node(self, node_id, conn):
        buffer = b''
        while self.active:
            try:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                buffer += chunk
                parts = buffer.split(b'\0')
                for msg_bytes in parts[:-1]:
                    if msg_bytes.strip():
                        message = json.loads(msg_bytes.decode())
                        recipient_id = message.get('to')
                        if recipient_id and recipient_id in self.node_sockets:
                            with self.lock:
                                link_up = self.links_status.get((node_id, recipient_id), True)
                            if link_up:
                                threading.Thread(target=self.forward_message_with_delay, args=(recipient_id, message), daemon=True).start()
                            else:
                                print(f"Link between {node_id} and {recipient_id} is down. Message not forwarded.")
                        else:
                            print(f"Recipient {recipient_id} not connected.")
                buffer = parts[-1]
            except Exception as e:
                print(f"Error listening to node {node_id}: {e}")
                break
        with self.lock:
            if node_id in self.node_sockets:
                del self.node_sockets[node_id]
        conn.close()
        print(f"Node {node_id} disconnected.")

    def forward_message_with_delay(self, recipient_id, message):
        time.sleep(3)
        with self.lock:
            recipient_conn = self.node_sockets.get(recipient_id)
        if recipient_conn:
            try:
                data = json.dumps(message).encode() + b'\0'
                recipient_conn.sendall(data)
            except Exception as e:
                print(f"Error forwarding message to {recipient_id}: {e}")
        else:
            print(f"Recipient {recipient_id} not connected.")

    def command_interface(self):
        print("Network Server command interface started.")
        print("Commands:")
        print("fail_link <node1_id> <node2_id>")
        print("fix_link <node1_id> <node2_id>")
        print("crash_node <node_id>")
        print("help")
        while self.active:
            command = input("NetworkServer> ").strip()
            if command == '':
                continue
            parts = command.split()
            if parts[0] == 'fail_link' and len(parts) == 3:
                self.fail_link(parts[1], parts[2])
            elif parts[0] == 'fix_link' and len(parts) == 3:
                self.fix_link(parts[1], parts[2])
            elif parts[0] == 'crash_node' and len(parts) == 2:
                self.crash_node(parts[1])
            elif parts[0] == 'help':
                print("Commands:")
                print("fail_link <node1_id> <node2_id>")
                print("fix_link <node1_id> <node2_id>")
                print("crash_node <node_id>")
                print("help")
            else:
                print("Unknown command. Type 'help' for available commands.")

    def fail_link(self, node1_id, node2_id):
        with self.lock:
            self.links_status[(node1_id, node2_id)] = False
            self.links_status[(node2_id, node1_id)] = False
        print(f"Link between {node1_id} and {node2_id} failed.")

    def fix_link(self, node1_id, node2_id):
        with self.lock:
            self.links_status[(node1_id, node2_id)] = True
            self.links_status[(node2_id, node1_id)] = True
        print(f"Link between {node1_id} and {node2_id} fixed.")

    def crash_node(self, node_id):
        with self.lock:
            conn = self.node_sockets.get(node_id)
            if conn:
                conn.close()
                del self.node_sockets[node_id]
                print(f"Node {node_id} crashed.")
            else:
                print(f"Node {node_id} not connected.")

    def stop(self):
        self.active = False
        print("Network Server stopped.")

if __name__ == "__main__":
    from config import NODES_INFO, NETWORK_SERVER_INFO
    network_server = NetworkServer(NETWORK_SERVER_INFO['host'], NETWORK_SERVER_INFO['port'], NODES_INFO)
    # Keep the main thread alive
    while True:
        time.sleep(1)
