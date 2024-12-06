# main.py

import sys
from config import NODES_INFO, NETWORK_SERVER_INFO
from node import Node
import signal
import sys

def handle_interrupt(signum, frame):
    print("Shutting down gracefully...")
    sys.exit(0)

signal.signal(signal.SIGINT, handle_interrupt)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python main.py <node_id>")
        sys.exit(1)
    node_id = sys.argv[1]
    if node_id not in NODES_INFO:
        print(f"Invalid node_id. Available nodes: {list(NODES_INFO.keys())}")
        sys.exit(1)
    node = Node(node_id, NODES_INFO, NETWORK_SERVER_INFO)
    node.user_interface()
