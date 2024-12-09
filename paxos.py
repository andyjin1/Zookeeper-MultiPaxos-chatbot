# paxos.py
import threading
import time


def ballot_greater(b1, b2):
    # b1 and b2 are tuples: (seq, pid_str, op_num)
    seq1, pid_str1, op_num1 = b1
    seq2, pid_str2, op_num2 = b2

    # Convert pid_str to int
    pid1 = int(''.join(ch for ch in pid_str1 if ch.isdigit()))
    pid2 = int(''.join(ch for ch in pid_str2 if ch.isdigit()))

    if seq1 != seq2:
        return seq1 > seq2
    if pid1 != pid2:
        return pid1 > pid2
    return op_num1 > op_num2

def compare_ballots(b1, b2):
    """
    Compare two ballot numbers b1 and b2.
    Works for both tuples and lists.
    Returns:
        -1 if b1 < b2
         0 if b1 == b2
         1 if b1 > b2
    """
    # Extract values
    seq1, pid_str1, op_num1 = b1
    seq2, pid_str2, op_num2 = b2

    # Convert pid_str to int for comparison
    pid1 = int(''.join(ch for ch in pid_str1 if ch.isdigit()))
    pid2 = int(''.join(ch for ch in pid_str2 if ch.isdigit()))

    # Compare sequence numbers
    if seq1 != seq2:
        return -1 if seq1 < seq2 else 1

    # Compare proposer IDs
    if pid1 != pid2:
        return -1 if pid1 < pid2 else 1

    # Compare operation numbers
    if op_num1 != op_num2:
        return -1 if op_num1 < op_num2 else 1

    # Ballots are equal
    return 0


def server_name(node_id):
    # Convert 'node3' -> 'Server 3'
    pid = ''.join(ch for ch in node_id if ch.isdigit())
    return f"Server {pid}"

def format_ballot_num(ballot_num):
    # (seq, pid_str, op_num) -> "<seq,pid,op_num>"
    seq, pid_str, op_num = ballot_num
    pid = ''.join(ch for ch in pid_str if ch.isdigit())
    return f"<{seq},{pid},{op_num}>"

class Paxos:
    def __init__(self, node_id, nodes_info, network, apply_operation_callback, on_leader_callback=None):
        self.node_id = node_id
        self.nodes_info = nodes_info
        self.network = network
        self.apply_operation = apply_operation_callback
        self.on_leader_callback = on_leader_callback

        # Ballot number: (seq_num, pid, op_num)
        self.ballot_num = (0, self.node_id, 0)

        self.promised_ballot_num = None
        self.accepted_num = None
        self.accepted_value = None

        self.prepare_responses = {}
        self.accepted_responses = {}

        self.proposed_operation = None
        self.leader = None
        self.is_leader = False

        self.operation_queue = []
        self.num_operations_applied = 0  # number of decided ops applied locally


        self.decided_operations = set()

    def start_election(self):
        # Determine the next seq_num to use
        next_seq_num = max(self.ballot_num[0], self.promised_ballot_num[0] if self.promised_ballot_num else 0) + 1
        self.ballot_num = (next_seq_num, self.node_id, self.num_operations_applied)
        self.prepare_responses = {}

        print(f"{server_name(self.node_id)} starting election.")
        message = {
            'type': 'PREPARE',
            'from': self.node_id,
            'ballot_num': self.ballot_num
        }
        print(f"Sending PREPARE {format_ballot_num(self.ballot_num)} to ALL")
        self.network.broadcast_message(message)

    def handle_prepare(self, message):
        incoming_ballot = message['ballot_num']
        seq_num, pid_str, incoming_op_num = incoming_ballot
        sender = message['from']
        # Remove these lines that update self.ballot_num
        # seq, pid_str, _ = self.ballot_num
        # self.ballot_num = (seq_num, self.node_id, self.num_operations_applied)

        print(f"Received PREPARE {format_ballot_num(incoming_ballot)} from {server_name(sender)}")

        if self.num_operations_applied > incoming_op_num:
            print(f"{server_name(pid_str)} is behind. Sending STATUS_REQUEST to {server_name(self.leader)}.")
            status_request = {
                'type': 'STATUS_REQUEST',
                'from': pid_str,
                'to': self.leader,
                'op_num': pid_str,
                'ballot_num': self.ballot_num,  # Include the current ballot number
            }
            self.network.send_message_prepare(self.leader, status_request)
            return

        if self.promised_ballot_num is None or incoming_ballot >= self.promised_ballot_num:
            self.promised_ballot_num = incoming_ballot
            response = {
                'type': 'PROMISE',
                'from': self.node_id,
                'to': sender,
                'ballot_num': incoming_ballot,
                'accepted_num': self.accepted_num,
                'accepted_value': self.accepted_value
            }
            if self.accepted_value:
                print(
                    f"Sending PROMISE {format_ballot_num(incoming_ballot)} with previously accepted value to {server_name(sender)}")
            else:
                print(f"Sending PROMISE {format_ballot_num(incoming_ballot)} to {server_name(sender)}")
            self.network.send_message(sender, response)
        else:
            print(
                f"Ignoring PREPARE {format_ballot_num(incoming_ballot)} from {server_name(sender)}; already promised {format_ballot_num(self.promised_ballot_num)}")

    def handle_promise(self, message):
        sender = message['from']
        incoming_ballot = message['ballot_num']
        print(f"Received PROMISE {format_ballot_num(incoming_ballot)} from {server_name(sender)}")

        self.prepare_responses[sender] = message
        if len(self.prepare_responses) + 1 >= (len(self.nodes_info) // 2) + 1:
            if not self.is_leader:
                self.leader = self.node_id
                self.is_leader = True
                print(f"{server_name(self.node_id)} becomes leader.")
                if self.on_leader_callback:
                    # This will submit pending operations and call submit_operation for them,
                    # which in turn calls process_next_operation() as needed.
                    self.on_leader_callback()

    def handle_accept(self, message):
        incoming_ballot = message['ballot_num']
        seq_num, pid_str, incoming_op_num = incoming_ballot
        sender = message['from']
        operation = message['value']

        print(f"Received ACCEPT {format_ballot_num(incoming_ballot)} from {server_name(sender)} for {operation}")

        # Do not update self.ballot_num here either
        # seq, pid_str, _ = self.ballot_num
        # self.ballot_num = (seq_num, self.node_id, self.num_operations_applied)
        if self.is_leader and compare_ballots(incoming_ballot, self.ballot_num) > 0:
            print(
                f"{server_name(self.node_id)} invalidates leadership. Higher ballot {format_ballot_num(incoming_ballot)} observed.")
            self.is_leader = False
            self.leader = sender  # Update the known leader

            if self.proposed_operation:
                forward_message = {
                    'type': 'FORWARD',
                    'from': self.node_id,
                    'to': self.leader,
                    'operation': self.proposed_operation
                }
                print(
                    f"Forwarding invalidated operation to new leader {server_name(self.leader)}: {self.proposed_operation}")
                self.network.send_message(self.leader, forward_message)

        if self.num_operations_applied > incoming_op_num:
            print(
                f"Ignoring ACCEPT {format_ballot_num(incoming_ballot)} from {server_name(sender)}; local ops ({self.num_operations_applied}) > op_num ({incoming_op_num})")
            return
        if compare_ballots(incoming_ballot, self.ballot_num) < 0:
            print("Ignoring ACCEPT; incoming ballot less than current ballot.")
            return

        if self.promised_ballot_num is None or incoming_ballot >= self.promised_ballot_num:
            self.promised_ballot_num = incoming_ballot
            self.accepted_num = incoming_ballot
            self.accepted_value = operation
            response = {
                'type': 'ACCEPTED',
                'from': self.node_id,
                'to': sender,
                'ballot_num': incoming_ballot,
                'accepted_value': self.accepted_value
            }
            print(f"Sending ACCEPTED {format_ballot_num(incoming_ballot)} {operation} to {server_name(sender)}")
            self.network.send_message(sender, response)
        else:
            print(
                f"Ignoring ACCEPT {format_ballot_num(incoming_ballot)} from {server_name(sender)}; already promised {format_ballot_num(self.promised_ballot_num)}")

    def handle_accepted(self, message):
        sender = message['from']
        incoming_ballot = message['ballot_num']
        accepted_val = message['accepted_value']

        print(f"Received ACCEPTED {format_ballot_num(incoming_ballot)} from {server_name(sender)} for {accepted_val}")

        if not self.is_leader:
            return

        self.accepted_responses[sender] = message
        if len(self.accepted_responses) + 1 >= (len(self.nodes_info) // 2) + 1:
            # Use the stable key
            if hasattr(self, 'current_op_key') and self.current_op_key not in self.decided_operations:
                self.decided_operations.add(self.current_op_key)
                decide_message = {
                    'type': 'DECIDE',
                    'from': self.node_id,
                    'value': self.proposed_operation,
                    'ballot_num': self.ballot_num
                }
                print(f"Sending DECIDE {format_ballot_num(self.ballot_num)} for value {self.proposed_operation} to ALL")
                self.network.broadcast_message(decide_message)
                self.apply_decide(decide_message)
                ######CHANGE######
                #self.decided_operations.add(self.current_op_key)
            else:
                print(f"Operation {self.proposed_operation} already decided; not sending DECIDE.")

    def handle_decide(self, message):
        print(
            f"Received DECIDE {format_ballot_num(message['ballot_num'])} from {message['from']} for value {message['value']}")
        self.apply_decide(message)

        # Update leader information
        self.leader = message['from']
        if self.is_leader and self.leader != self.node_id:
            print(f"{server_name(self.node_id)} invalidates leadership. Leader is now {server_name(self.leader)}.")
            self.is_leader = False

        # Check if this node is lagging
        incoming_op_num = message['ballot_num'][2]
        if incoming_op_num > self.num_operations_applied:
            print(f"{server_name(self.node_id)} is behind. Sending STATUS_REQUEST to {server_name(self.leader)}.")
            status_request = {
                'type': 'STATUS_REQUEST',
                'from': self.node_id,
                'to': self.leader,
                'op_num': self.num_operations_applied,
                'ballot_num': self.ballot_num,  # Include the current ballot number
            }
            self.network.send_message(self.leader, status_request)

    def send_status_request(self, target_node):
        status_request = {
            'type': 'STATUS_REQUEST',
            'from': self.node_id,
            'to': self.leader,
            'op_num': self.num_operations_applied,
            'ballot_num': self.ballot_num,  # Include the current ballot number
        }
        self.network.send_message(target_node, status_request)

        # Start a timeout for the response
        threading.Timer(5.0, self.handle_status_request_timeout, args=(target_node,)).start()

    def handle_status_request_timeout(self, target_node):
        if target_node == self.leader:
            print(
                f"STATUS_REQUEST to leader {server_name(self.leader)} timed out. Trying another node or triggering election.")
            self.try_alternate_status_update()

    def try_alternate_status_update(self):
        # Broadcast STATUS_REQUEST to all nodes except self
        for node_id in self.nodes_info:
            if node_id != self.node_id:
                print(f"Broadcasting STATUS_REQUEST to {server_name(node_id)}.")
                self.send_status_request(node_id)

    def apply_decide(self, message):
        operation = message['value']
        sender = message['from']
        ballot_num = message['ballot_num']

        print(f"DECIDE {format_ballot_num(ballot_num)} from {server_name(sender)} for value {operation}")
        self.apply_operation(operation)
        self.num_operations_applied += 1
        seq, pid_str, _ = self.ballot_num
        self.ballot_num = (seq, self.node_id, self.num_operations_applied)

        # If this node is not leader, record who the leader is
        if not self.is_leader:
            self.leader = sender  # Now we know who the leader is

        if self.is_leader and self.operation_queue and self.operation_queue[0] == operation:
            self.operation_queue.pop(0)
            self.process_next_operation()

    def submit_operation(self, operation):
        # Add the new operation
        if self.is_leader:
            print(f"Submitting operation as leader: {operation}")
            self.operation_queue.append(operation)
            if len(self.operation_queue) == 1:
                self.process_next_operation()
        else:
            # Forward the operation to the leader
            if self.leader:
                print(f"Forwarding operation to leader {server_name(self.leader)}: {operation}")
                message = {
                    'type': 'FORWARD',
                    'from': self.node_id,
                    'to': self.leader,
                    'value': operation
                }
                self.network.send_message(self.leader, message)
            else:
                print("Cannot submit operation; no known leader.")

    def process_next_operation(self):
        if not self.operation_queue:
            print(f"{server_name(self.node_id)} has no operations to process.")
            return

        self.proposed_operation = self.operation_queue[0]
        # Create a stable key for this operation
        self.current_op_key = tuple(sorted(self.proposed_operation.items()))

        seq, pid_str, _ = self.ballot_num
        self.ballot_num = (seq, self.node_id, self.num_operations_applied)

        self.accepted_responses = {}
        accept_message = {
            'type': 'ACCEPT',
            'from': self.node_id,
            'ballot_num': self.ballot_num,
            'value': self.proposed_operation
        }
        print(f"Sending ACCEPT {format_ballot_num(self.ballot_num)} {self.proposed_operation} from {server_name(self.node_id)} to ALL")
        self.network.broadcast_message(accept_message)

