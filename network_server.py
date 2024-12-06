# network_server.py

import threading
import socket
import json
import time

class NetworkServer:
    def __init__(self, host, port, nodes_info):
        self.host = host
        self.port = port
        self.nodes_info = nodes_info
        self.node_sockets = {}  # node_id: socket
        self.node_addresses = {}  # node_id: (host, port)
        self.links_status = {}  # (node_id1, node_id2): True/False (link up/down)
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
            # First message from node is its node_id
            data = conn.recv(4096)
            if data:
                message = json.loads(data.decode())
                node_id = message.get('node_id')
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
                    # Start listening for messages from this node
                    self.listen_to_node(node_id, conn)
        except Exception as e:
            print(f"Error handling node connection: {e}")

    def listen_to_node(self, node_id, conn):
        while self.active:
            try:
                data = conn.recv(4096)
                if data:
                    message = json.loads(data.decode())
                    # The message should have 'to' field indicating the recipient
                    recipient_id = message.get('to')
                    if recipient_id and recipient_id in self.node_sockets:
                        # Check if link between sender and recipient is up
                        with self.lock:
                            link_up = self.links_status.get((node_id, recipient_id), True)
                        if link_up:
                            # Delay the message by 3 seconds
                            threading.Thread(target=self.forward_message_with_delay, args=(recipient_id, message), daemon=True).start()
                        else:
                            print(f"Link between {node_id} and {recipient_id} is down. Message not forwarded.")
                    else:
                        print(f"Recipient {recipient_id} not connected.")
                else:
                    break
            except Exception as e:
                print(f"Error listening to node {node_id}: {e}")
                break
        with self.lock:
            if node_id in self.node_sockets:
                del self.node_sockets[node_id]
        conn.close()
        print(f"Node {node_id} disconnected.")

    def forward_message_with_delay(self, recipient_id, message):
        time.sleep(3)  # 3-second delay
        with self.lock:
            recipient_conn = self.node_sockets.get(recipient_id)
        if recipient_conn:
            try:
                recipient_conn.sendall(json.dumps(message).encode())
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

if __name__ == "__main__":
    from config import NODES_INFO, NETWORK_SERVER_INFO
    network_server = NetworkServer(NETWORK_SERVER_INFO['host'], NETWORK_SERVER_INFO['port'], NODES_INFO)
    # Keep the main thread alive
    while True:
        time.sleep(1)
