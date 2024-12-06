# paxos.py

import threading

class Paxos:
    def __init__(self, node_id, nodes_info, network, apply_operation_callback):
        self.node_id = node_id
        self.nodes_info = nodes_info
        self.network = network
        self.apply_operation = apply_operation_callback
        self.ballot_num = (0, self.node_id, 0)  # (seq_num, pid, op_num)
        self.accepted_num = None  # Highest accepted ballot number
        self.accepted_value = None  # Value accepted in highest ballot
        self.prepare_responses = {}  # Responses from acceptors
        self.accepted_responses = {}  # Responses from acceptors in accept phase
        self.proposed_operation = None
        self.lock = threading.Lock()
        self.leader = None  # Current leader node ID
        self.is_leader = False
        self.operation_queue = []
        self.op_num = 0  # Number of consensus operations executed
        self.num_operations_applied = 0  # Number of operations applied locally

    def start_election(self):
        print(f"Node {self.node_id} starting election.")
        with self.lock:
            print(f"Node {self.node_id} starting election.")
            self.ballot_num = (self.ballot_num[0] + 1, self.node_id, self.op_num)
            self.prepare_responses = {}
            message = {
                'type': 'PREPARE',
                'from': self.node_id,
                'ballot_num': self.ballot_num
            }
            print(f"PREPARE {self.ballot_num} sent by {self.node_id} to all nodes")
            self.network.broadcast_message(message)

    def handle_prepare(self, message):
        print(f"PREPARE {message['ballot_num']} received from {message['from']} by {self.node_id}")
        with self.lock:
            incoming_ballot = message['ballot_num']
            if self.num_operations_applied > incoming_ballot[2]:
                # Our op_num is larger; do not send PROMISE
                print(f"Node {self.node_id} rejects PREPARE from {message['from']} due to higher op_num.")
                return
            if self.accepted_num is None or incoming_ballot > self.accepted_num:
                self.accepted_num = incoming_ballot
                response = {
                    'type': 'PROMISE',
                    'from': self.node_id,
                    'to': message['from'],
                    'ballot_num': incoming_ballot,
                    'accepted_num': self.accepted_num,
                    'accepted_value': self.accepted_value,
                    'num_operations_applied': self.num_operations_applied
                }
                print(f"PROMISE {incoming_ballot} sent by {self.node_id} to {message['from']}")
                self.network.send_message(message['from'], response)

    def handle_promise(self, message):
        print(f"PROMISE {message['ballot_num']} received from {message['from']} by {self.node_id}")
        with self.lock:
            if self.leader is not None and self.leader != self.node_id:
                return
            self.prepare_responses[message['from']] = message
            got_majority = len(self.prepare_responses) >= (len(self.nodes_info) // 2) + 1

            # Store state changes in local variables
            became_leader = False
            if got_majority:
                self.leader = self.node_id
                self.is_leader = True
                print(f"Node {self.node_id} becomes leader.")
                became_leader = True

        # Lock is released here

        # Now that we are outside the lock, if we became leader, call process_next_operation()
        if became_leader:
            self.process_next_operation()

    def submit_operation(self, operation):

        print("Received operation:", operation)
        self.operation_queue.append(operation)
        is_leader = self.is_leader

        # Now we are outside the lock
        if is_leader:
            if len(self.operation_queue) == 1:
                # If no other operation is being processed
                self.process_next_operation()
        else:
            # Start election outside the lock
            print("Starting election to become leader.")
            self.start_election()

    def process_next_operation(self):
            if not self.operation_queue:
                return
            self.ballot_num = (self.ballot_num[0], self.node_id, self.op_num)
            self.proposed_operation = self.operation_queue[0]
            self.accepted_responses = {}
            accept_message = {
                'type': 'ACCEPT',
                'from': self.node_id,
                'ballot_num': self.ballot_num,
                'value': self.proposed_operation
            }
            print(f"ACCEPT {self.ballot_num} sent by {self.node_id} to all nodes with value {self.proposed_operation}")
            self.network.broadcast_message(accept_message)

    def handle_accept(self, message):
        print(f"ACCEPT {message['ballot_num']} received from {message['from']} by {self.node_id}")
        with self.lock:
            incoming_ballot = message['ballot_num']
            if self.num_operations_applied > incoming_ballot[2]:
                # Our op_num is larger; do not send ACCEPTED
                print(f"Node {self.node_id} rejects ACCEPT from {message['from']} due to higher op_num.")
                return
            if self.accepted_num is None or incoming_ballot >= self.accepted_num:
                self.accepted_num = incoming_ballot
                self.accepted_value = message['value']
                response = {
                    'type': 'ACCEPTED',
                    'from': self.node_id,
                    'to': message['from'],
                    'ballot_num': incoming_ballot,
                    'accepted_value': self.accepted_value
                }
                print(f"ACCEPTED {incoming_ballot} sent by {self.node_id} to {message['from']} for value {self.accepted_value}")
                self.network.send_message(message['from'], response)

    def handle_accepted(self, message):
        print(f"ACCEPTED {message['ballot_num']} received from {message['from']} by {self.node_id}")
        with self.lock:
            if not self.is_leader:
                return
            self.accepted_responses[message['from']] = message
            got_majority = len(self.accepted_responses) >= (len(self.nodes_info) // 2) + 1

            # If we got a majority, store the decision details
            if got_majority:
                decide_message = {
                    'type': 'DECIDE',
                    'from': self.node_id,
                    'value': self.proposed_operation,
                    'ballot_num': self.ballot_num
                }
                print(f"DECIDE {self.ballot_num} sent by {self.node_id} to all nodes for value {self.proposed_operation}")
                self.network.broadcast_message(decide_message)

            # Lock released here

            # Now call apply_decide outside the lock
                self.apply_decide(decide_message)

    def handle_decide(self, message):
        print(f"DECIDE {message['ballot_num']} received from {message['from']} by {self.node_id}")
        with self.lock:
            self.apply_decide(message)

    def apply_decide(self, message):
        #with self.lock:
            operation = message['value']
            self.apply_operation(operation)
            self.num_operations_applied += 1
            self.op_num = self.num_operations_applied
            if self.is_leader:
                # Remove the operation from the queue
                if self.operation_queue and self.operation_queue[0] == operation:
                    self.operation_queue.pop(0)
                # Start next operation if any
                self.process_next_operation()
            else:
                # If not leader, but have pending operations, start election
                if not self.is_leader and self.operation_queue:
                    self.start_election()
