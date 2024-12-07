# paxos.py
import time


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

    def start_election(self):
        # Start a new election by incrementing seq_num in the ballot
        self.ballot_num = (self.ballot_num[0] + 1, self.node_id, self.num_operations_applied)
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
        seq, pid_str, _ = self.ballot_num
        self.ballot_num = (seq_num, self.node_id, self.num_operations_applied)
        print(f"Received PREPARE {format_ballot_num(incoming_ballot)} from {server_name(sender)}")
        # Check if we applied more ops than incoming_op_num
        if self.num_operations_applied > incoming_op_num:
            print(f"Ignoring PREPARE {format_ballot_num(incoming_ballot)} from {server_name(sender)}; local ops ({self.num_operations_applied}) > op_num ({incoming_op_num})")
            return

        # Only promise if incoming_ballot > promised_ballot_num
        if self.promised_ballot_num is None or incoming_ballot > self.promised_ballot_num:
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
                print(f"Sending PROMISE {format_ballot_num(incoming_ballot)} with previously accepted value to {server_name(sender)}")
            else:
                print(f"Sending PROMISE {format_ballot_num(incoming_ballot)} to {server_name(sender)}")
            self.network.send_message(sender, response)
        else:
            print(f"Ignoring PREPARE {format_ballot_num(incoming_ballot)} from {server_name(sender)}; already promised {format_ballot_num(self.promised_ballot_num)}")
    # paxos.py (Only small logic check)

    def handle_promise(self, message):
        sender = message['from']
        incoming_ballot = message['ballot_num']
        print(f"Received PROMISE {format_ballot_num(incoming_ballot)} from {server_name(sender)}")

        self.prepare_responses[sender] = message
        if len(self.prepare_responses) >= (len(self.nodes_info) // 2) + 1:
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

        if self.num_operations_applied > incoming_op_num:
            print(f"Ignoring ACCEPT {format_ballot_num(incoming_ballot)} from {server_name(sender)}; local ops ({self.num_operations_applied}) > op_num ({incoming_op_num})")
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
            print(f"Ignoring ACCEPT {format_ballot_num(incoming_ballot)} from {server_name(sender)}; already promised {format_ballot_num(self.promised_ballot_num)}")

    def handle_accepted(self, message):
        sender = message['from']
        incoming_ballot = message['ballot_num']
        accepted_val = message['accepted_value']

        print(f"Received ACCEPTED {format_ballot_num(incoming_ballot)} from {server_name(sender)} for {accepted_val}")

        if not self.is_leader:
            return

        self.accepted_responses[sender] = message
        if len(self.accepted_responses) >= (len(self.nodes_info) // 2) + 1:
            decide_message = {
                'type': 'DECIDE',
                'from': self.node_id,
                'value': self.proposed_operation,
                'ballot_num': self.ballot_num
            }
            print(f"Sending DECIDE {format_ballot_num(self.ballot_num)} for value {self.proposed_operation} to ALL")
            self.network.broadcast_message(decide_message)
            self.apply_decide(decide_message)

    def handle_decide(self, message):
        print(f"Received DECIDE {format_ballot_num(message['ballot_num'])} from {message['from']} for value {message['value']}")
        self.apply_decide(message)

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
        if self.is_leader:
            print(f"Submitting operation as leader: {operation}")
            self.operation_queue.append(operation)
            if len(self.operation_queue) == 1:
                self.process_next_operation()
        else:
            # Should not happen if logic in node is correct.
            # The node class handles forwarding if not leader.
            print(f"Cannot submit operation directly; not leader.")

    def process_next_operation(self):
        if not self.operation_queue:
            print(f"{server_name(self.node_id)} has no operations to process.")
            return

        self.proposed_operation = self.operation_queue[0]
        # Update ballot_num op_num with current num_operations_applied
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
