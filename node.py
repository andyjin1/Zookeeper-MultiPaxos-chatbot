# node.py
import os
import sys
import threading
import time
from network import Network
from paxos import Paxos, server_name, format_ballot_num, compare_ballots
from llm_integration import LLMIntegration

FORWARD_TIMEOUT = 10.0

class Node:
    def __init__(self, node_id, nodes_info, network_server_info):
        self.last_responses = {}
        self.node_id = node_id
        self.nodes_info = nodes_info
        self.kv_store = {}
        self.network = Network(node_id, nodes_info, network_server_info, self.handle_message)
        self.active = True
        self.num_operations_applied = 0

        self.in_election = False
        self.pending_operations = []

        self.forward_id_counter = 0
        self.pending_forwards = {}

        # Pass on_leader_elected callback to Paxos
        self.paxos = Paxos(node_id, nodes_info, self.network, self.apply_operation, self.on_leader_elected)
        self.llm_integration = LLMIntegration(node_id, self.network)

    def on_leader_elected(self):
        # Once leader is elected, we submit pending operations exactly once
        self.in_election = False
        # Submit all pending operations as leader
        while self.pending_operations:
            operation = self.pending_operations.pop(0)
            print(f"Submitting pending operation as leader: {operation}")
            self.paxos.submit_operation(operation)
        # Now pending_operations is cleared, so no duplicates

    def handle_message(self, message):
        msg_type = message['type']
        self.log_received_msg(msg_type, message)

        if msg_type == 'PREPARE':
            self.paxos.handle_prepare(message)
        elif msg_type == 'PROMISE':
            self.paxos.handle_promise(message)
        elif msg_type == 'ACCEPT':
            self.paxos.handle_accept(message)
        elif msg_type == 'ACCEPTED':
            self.paxos.handle_accepted(message)
        elif msg_type == 'DECIDE':
            self.paxos.handle_decide(message)
        elif msg_type == 'LLM_RESPONSE':
            self.llm_integration.handle_llm_response(message, self.present_llm_responses_to_user)
        elif msg_type == 'FORWARD':
            self.handle_forward(message)
        elif msg_type == 'ACK':
            self.handle_ack(message)
        elif msg_type == 'STATUS_REQUEST':
            self.handle_status_request(message)
        elif msg_type == 'STATUS_REQUEST_PREPARE':
            self.handle_status_request_prepare(message)
        elif msg_type == 'STATUS_RESPONSE':
            self.handle_status_response(message)
        elif msg_type == 'STATUS_RESPONSE':
            self.handle_status_response(message)
        elif msg_type == 'CRASH':
            self.handle_crash(message)
        elif msg_type == 'RECOVER_START':
            self.recover()
        else:
            print(f"Unknown message type received: {msg_type}")

    # def handle_status_request_prepare(self, message):
    #     sender = message['from']
    #     sender_op_num = message['op_num']
    #     sender_ballot_num = message.get('ballot_num', (0, self.node_id, 0))  # Default to a safe value if missing
    #
    #     print(
    #         f"Received STATUS_REQUEST from {server_name(sender)} with op_num {sender_op_num} and ballot {format_ballot_num(sender_ballot_num)}.")
    #
    #     if sender_op_num < self.num_operations_applied:
    #         print(f"Sending STATUS_RESPONSE to {server_name(sender)}.")
    #         response = {
    #             'type': 'STATUS_RESPONSE',
    #             'from': self.node_id,
    #             'to': sender,
    #             'op_num': self.num_operations_applied,
    #             'kv_store': self.kv_store,
    #             'ballot_num': self.ballot_num,  # Include the current ballot number
    #         }
    #         self.network.send_message(sender, response)
    #     else:
    #         print(f"{server_name(sender)} is up-to-date or ahead. No STATUS_RESPONSE sent.")

    def handle_status_request(self, message):
        sender = message['from']
        sender_op_num = message['op_num']
        sender_ballot_num = message.get('ballot_num', (0, self.node_id, 0))  # Default to a safe value if missing

        print(
            f"Received STATUS_REQUEST from {server_name(sender)} with op_num {sender_op_num} and ballot {format_ballot_num(sender_ballot_num)}.")

        if sender_op_num < self.num_operations_applied:
            print(f"Sending STATUS_RESPONSE to {server_name(sender)}.")
            response = {
                'type': 'STATUS_RESPONSE',
                'from': self.node_id,
                'to': sender,
                'op_num': self.num_operations_applied,
                'kv_store': self.kv_store,
                'ballot_num': self.ballot_num,  # Include the current ballot number
            }
            self.network.send_message(sender, response)
        else:
            print(f"{server_name(sender)} is up-to-date or ahead. No STATUS_RESPONSE sent.")

    def handle_status_response(self, message):
        sender = message['from']
        sender_op_num = message['op_num']
        sender_kv_store = message['kv_store']
        sender_ballot_num = message['ballot_num']

        print(
            f"Received STATUS_RESPONSE from {server_name(sender)} with op_num {sender_op_num} and ballot {format_ballot_num(sender_ballot_num)}.")

        # Update state if behind
        if sender_op_num > self.num_operations_applied:
            print(f"Updating op_num and kv_store from {server_name(sender)}.")
            self.kv_store.update(sender_kv_store)
            self.num_operations_applied = sender_op_num

        # Update ballot number to ensure it's not behind
        if compare_ballots(sender_ballot_num, self.ballot_num) > 0:
            print(f"Updating ballot_num to {format_ballot_num(sender_ballot_num)} from {server_name(sender)}.")
            self.ballot_num = (sender_ballot_num[0], self.node_id, self.num_operations_applied)
        else:
            print(f"Current ballot_num {format_ballot_num(self.ballot_num)} is ahead. No update needed.")

    def handle_forward(self, message):
        operation = message['operation']
        if self.paxos.is_leader:
            # We are leader, accept and ACK
            self.paxos.operation_queue.append(operation)
            print(f"Appending forwarded operation: {operation}")
            if len(self.paxos.operation_queue) == 1:
                self.paxos.process_next_operation()
            ack = {
                'type': 'ACK',
                'from': self.node_id,
                'to': message['from'],
                'forward_id': message.get('forward_id', None)
            }
            self.log_sending_msg("ACK", ack, to_node=message['from'])
            try:
                self.network.send_message(message['from'], ack)
            except Exception as e:
                print(f"Error sending ACK to {message['from']}: {e}")
        else:
            # We are not leader, must forward again or start election if no leader
            if self.paxos.leader is not None and self.paxos.leader != self.node_id:
                # Known leader different from self, forward again
                print("Forwarding forwarded operation to leader...")
                forward_id = self.get_next_forward_id()
                forward_msg = {
                    'type': 'FORWARD',
                    'from': self.node_id,
                    'to': self.paxos.leader,
                    'operation': operation,
                    'forward_id': forward_id
                }
                self.log_sending_msg("FORWARD", forward_msg, to_node=self.paxos.leader)
                self.network.send_message(self.paxos.leader, forward_msg)
                self.start_forward_timeout(forward_id, operation)
            else:
                # No known leader
                print("No known leader upon receiving forwarded operation, starting election")
                if not self.in_election:
                    self.in_election = True
                    self.paxos.start_election()
                self.pending_operations.append(operation)

    def handle_ack(self, message):
        forward_id = message.get('forward_id', None)
        if forward_id is not None and forward_id in self.pending_forwards:
            self.pending_forwards[forward_id]['timer'].cancel()
            del self.pending_forwards[forward_id]
            print("ACK: operation acknowledged by leader.")

    def apply_operation(self, operation):
        op_type = operation['op_type']
        context_id = operation['context_id']
        originator = operation.get('originator', self.node_id)

        if op_type == 'CREATE':
            self.kv_store[context_id] = ''
            print(f"NEW CONTEXT {context_id}")
        elif op_type == 'QUERY':
            query = operation['query']
            if context_id not in self.kv_store:
                self.kv_store[context_id] = ''
            self.kv_store[context_id] += f"Query: {query}\n"
            print(f"NEW QUERY on {context_id} with {self.kv_store[context_id]}")
            # Trigger LLM query
            self.llm_integration.set_originator(context_id, originator)
            threading.Thread(
                target=self.llm_integration.query_llm_and_send_response,
                args=(context_id, self.kv_store[context_id], originator),
                daemon=True
            ).start()
            # Wait for responses with a timeout
            threading.Thread(
                target=self.llm_integration.wait_for_responses,
                args=(context_id, self.present_llm_responses_to_user),
                daemon=True
            ).start()
        elif op_type == 'CHOOSE':
            answer = operation['answer']
            if context_id not in self.kv_store:
                self.kv_store[context_id] = ''
            self.kv_store[context_id] += f"Answer: {answer}\n"
            print(f"CHOSEN ANSWER on {context_id} with {answer}")
        self.num_operations_applied += 1

    def present_llm_responses_to_user(self, context_id, responses):
        self.last_responses[context_id] = responses
        print(f"Received responses for context {context_id}:")
        for idx, (node_id, resp) in enumerate(responses, start=1):
            print(f"{idx}: (from {node_id}) {resp}")
        print(f"To select an answer, please type at the main prompt: choose {context_id} <response_number>")

    def thread_llm_response(self, context_id, responses):
        threading.Thread(
            target=self.present_llm_responses_to_user,
            args=(context_id, responses),
            daemon=True
        ).start()

    def process_user_command(self, command):
        def handle_command():
            print(f"Received command: {command}")
            parts = command.strip().split(' ', 2)
            if not parts:
                print("Invalid command.")
                return
            cmd = parts[0]

            if cmd == 'create':
                if len(parts) < 2:
                    print("Usage: create <context_id>")
                    return
                context_id = parts[1]
                operation = {
                    'op_type': 'CREATE',
                    'context_id': context_id,
                    'originator': self.node_id
                }
                self.handle_new_operation(operation)

            elif cmd == 'query':
                if len(parts) < 3:
                    print("Usage: query <context_id> <query_string>")
                    return
                context_id = parts[1]
                query_str = parts[2]
                operation = {
                    'op_type': 'QUERY',
                    'context_id': context_id,
                    'query': query_str,
                    'originator': self.node_id
                }
                self.handle_new_operation(operation)

            elif cmd == 'choose':
                if len(parts) < 3:
                    print("Usage: choose <context_id> <response_number>")
                    return
                context_id = parts[1]
                try:
                    response_number = int(parts[2])
                except ValueError:
                    print("Response number must be an integer.")
                    return

                if context_id not in self.last_responses:
                    print("No responses available for this context.")
                    return
                responses = self.last_responses[context_id]
                if response_number < 1 or response_number > len(responses):
                    print("Invalid response number")
                    return

                chosen_node_id, chosen_answer = responses[response_number - 1]
                operation = {
                    'op_type': 'CHOOSE',
                    'context_id': context_id,
                    'answer': chosen_answer,
                    'originator': self.node_id
                }
                self.handle_new_operation(operation)

            elif cmd == 'view':
                if len(parts) < 2:
                    print("Usage: view <context_id>")
                    return
                context_id = parts[1]
                val = self.kv_store.get(context_id, 'Context not found')
                print(val)

            elif cmd == 'viewall':
                if not self.kv_store:
                    print("No contexts available.")
                for cid, context_val in self.kv_store.items():
                    print(f"{cid}: {context_val}")

            elif cmd == 'crash':
                self.network.stop()
                self.active = False
                print("Node crashed.")

            elif cmd == 'recover':
                # Reconnect
                self.network = Network(self.node_id, self.nodes_info, self.network.network_server_info,
                                       self.handle_message)
                print("Node recovered.")

            else:
                print("Unknown command.")

        threading.Thread(target=handle_command, daemon=True).start()

    def handle_new_operation(self, operation):
        if self.paxos.is_leader:
            # If we are already leader, submit directly
      #     print(f"Submitting operation as leader: {operation}")
            self.paxos.submit_operation(operation)
        else:
            # Not leader
            if self.paxos.leader is None:
                # No known leader, start election if not already in one
                if not self.in_election:
                    print("No known leader, starting election")
                    self.in_election = True
                    self.paxos.start_election()
                # Just store operation until we have a leader
                self.pending_operations.append(operation)
            else:
                # Known leader but not us
                if self.paxos.leader != self.node_id:
                    print("Forwarding operation to leader...")
                    forward_id = self.get_next_forward_id()
                    forward_msg = {
                        'type': 'FORWARD',
                        'from': self.node_id,
                        'to': self.paxos.leader,
                        'operation': operation,
                        'forward_id': forward_id
                    }
                    self.log_sending_msg("FORWARD", forward_msg, to_node=self.paxos.leader)
                    self.network.send_message(self.paxos.leader, forward_msg)
                    self.start_forward_timeout(forward_id, operation)
                else:
                    # This case shouldn't happen since leader == self.node_id means is_leader should be True
                    print("Inconsistent state: known leader is self but is_leader is False.")

    def get_next_forward_id(self):
        fid = self.forward_id_counter
        self.forward_id_counter += 1
        return fid

    def start_forward_timeout(self, forward_id, operation):
        def on_timeout():
            if forward_id in self.pending_forwards:
                print("TIMEOUT: no ACK received in time. Starting election...")
                del self.pending_forwards[forward_id]
                if not self.in_election:
                    self.in_election = True
                    self.paxos.start_election()
                self.pending_operations.append(operation)

        timer = threading.Timer(FORWARD_TIMEOUT, on_timeout)
        timer.start()
        self.pending_forwards[forward_id] = {
            'operation': operation,
            'timer': timer
        }

    def broadcast_status_request(self):
        status_request = {
            'type': 'STATUS_REQUEST',
            'from': self.node_id,
            'num_operations_applied': self.paxos.num_operations_applied,
            'ballot_num': self.paxos.ballot_num
        }
        print(f"Broadcasting STATUS_REQUEST from {server_name(self.node_id)}.")
        self.network.broadcast_message(status_request)

    def handle_status_request(self, message):
        status_response = {
            'type': 'STATUS_RESPONSE',
            'from': self.node_id,
            'num_operations_applied': self.paxos.num_operations_applied,
            'log': self.kv_store,  # Assuming kv_store holds applied operations
            'leader': self.paxos.leader
        }
        print(f"Sending STATUS_RESPONSE to {server_name(message['from'])}.")
        self.network.send_message(message['from'], status_response)

    def handle_status_response(self, message):
        sender_op_num = message['num_operations_applied']
        state = message['log']
        if sender_op_num > self.paxos.num_operations_applied:
            print(f"Updating state from {server_name(message['from'])} with op_num {sender_op_num}.")
            self.kv_store.update(state)
            self.paxos.num_operations_applied = sender_op_num
        else:
            print(f"Received state from {server_name(message['from'])} but no update needed.")
        if message.get('leader'):
            self.paxos.leader = message['leader']

    def recover(self):
        print(f"{server_name(self.node_id)} recovering...")
        self.active = True
        self.paxos = Paxos(
            self.node_id,
            self.nodes_info,
            self.network,
            self.apply_operation,
            self.on_leader_elected
        )
        self.kv_store = {}
        self.pending_operations = []
        self.leader = None

        # Restart the network listener
        self.network.restart()
        print(f"{server_name(self.node_id)} has recovered and is ready.")

    def start_network_listener(self):
        print(f"{server_name(self.node_id)} starting network listener...")
        self.network = Network(self.node_id, self.nodes_info, self.network.network_server_info, self.handle_message)
        print(f"{server_name(self.node_id)} is now listening for connections.")

    def handle_crash(self, message):
        self.crash()

    def crash(self):
        print(f"{server_name(self.node_id)} crashing...")
        self.network.stop()
        os._exit(0)  # Force exit in child process

    def user_interface(self):
        while self.active:
            command = input("> ")
            self.process_user_command(command)

    def start_user_interface(self):
        threading.Thread(target=self.user_interface, args=(), daemon=True).start()


    # In node.py, modify log_received_msg to skip detailed logging of protocol messages:

    def log_received_msg(self, msg_type, message):
        frm = message.get('from', 'UNKNOWN')

        # If it's a protocol message, let paxos.py handle detailed logging
        # Just print a simple summary or nothing at all:
        if msg_type in ["PREPARE", "PROMISE", "ACCEPT", "ACCEPTED", "DECIDE"]:
            # Skip or print a minimal one-liner:
            pass  # no logging here for these messages
        elif msg_type in ["FORWARD", "ACK"]:
            print(f"Received {msg_type} from {server_name(frm)}")
        elif msg_type == 'LLM_RESPONSE':
            print(f"Received LLM_RESPONSE from {server_name(frm)}")
        else:
            print(f"Received {msg_type} from {server_name(frm)}")

    def log_sending_msg(self, msg_type, message, to_node=None):
        if msg_type in ["PREPARE","PROMISE","ACCEPT","ACCEPTED","DECIDE"]:
            ballot = message.get('ballot_num')
            b_str = format_ballot_num(ballot) if ballot else ""
            tgt = server_name(message['to']) if ('to' in message and message['to'] != "ALL") else "ALL"
            if msg_type == "PROMISE":
                accepted_value = message.get('accepted_value')
                if accepted_value:
                    print(f"Sending {msg_type} {b_str} with previously accepted value to {tgt}")
                else:
                    print(f"Sending {msg_type} {b_str} Bottom Bottom to {tgt}")
            elif msg_type in ["ACCEPT","ACCEPTED","DECIDE"]:
                operation = message.get('value') or message.get('accepted_value')
                print(f"Sending {msg_type} {b_str} {operation} to {tgt}")
            elif msg_type == "PREPARE":
                print(f"Sending {msg_type} {b_str} to {tgt}")
        elif msg_type in ["FORWARD","ACK"]:
            tgt = server_name(message['to']) if 'to' in message else "ALL"
            print(f"Sending {msg_type} to {tgt}")
        else:
            tgt = server_name(message['to']) if 'to' in message else "ALL"
            print(f"Sending {msg_type} to {tgt}")
