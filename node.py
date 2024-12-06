# node.py

import threading
from network import Network
from paxos import Paxos
from llm_integration import LLMIntegration

class Node:
    def __init__(self, node_id, nodes_info, network_server_info):
        self.last_responses = {}
        self.node_id = node_id
        self.nodes_info = nodes_info
        self.kv_store = {}  # Replicated key-value store
        self.network = Network(node_id, nodes_info, network_server_info, self.handle_message)
        self.lock = threading.Lock()
        self.active = True
        self.num_operations_applied = 0  # Number of operations applied locally

        self.paxos = Paxos(node_id, nodes_info, self.network, self.apply_operation)
        self.llm_integration = LLMIntegration(node_id, self.network)

    def handle_message(self, message):
        msg_type = message['type']
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
        # Add other message types as needed

    def apply_operation(self, operation):
        op_type = operation['op_type']
        context_id = operation['context_id']
        originator = operation.get('originator', self.node_id)
        with self.lock:
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
                threading.Thread(target=self.llm_integration.query_llm_and_send_response, args=(context_id, self.kv_store[context_id], originator), daemon=True).start()
            elif op_type == 'CHOOSE':
                answer = operation['answer']
                if context_id not in self.kv_store:
                    self.kv_store[context_id] = ''
                self.kv_store[context_id] += f"Answer: {answer}\n"
                print(f"CHOSEN ANSWER on {context_id} with {answer}")
            # Update number of operations applied
            self.num_operations_applied += 1

    def present_llm_responses_to_user(self, context_id, responses):
        # Store the responses so that when the user issues a "choose" command later,
        # we can look up the chosen answer.
        self.last_responses[context_id] = responses

        print(f"Received responses for context {context_id}:")
        for idx, (node_id, resp) in enumerate(responses):
            print(f"{idx + 1}: (from node {node_id}) {resp}")

        # Instead of calling input() here, just instruct the user on what to do next.
        # The user_interface() loop will be continuously running and waiting for commands.
        # So the user can type: choose <context_id> <response_number>
        # at the main prompt (where they typed 'create', 'query', etc. before).
        print(f"To select an answer, please type at the main prompt: choose {context_id} <response_number>")

    # node.py

    import threading

    def process_user_command(self, command):
        def handle_command():
            print(f"Received command: {command}")  # Debugging print
            parts = command.strip().split(' ', 2)
            cmd = parts[0]

            if cmd == 'create':
                context_id = parts[1]
                operation = {
                    'op_type': 'CREATE',
                    'context_id': context_id,
                    'originator': self.node_id
                }
                print(f"Submitting operation: {operation}")  # Debugging print
                self.paxos.submit_operation(operation)

            elif cmd == 'query':
                context_id = parts[1]
                query_str = parts[2]
                operation = {
                    'op_type': 'QUERY',
                    'context_id': context_id,
                    'query': query_str,
                    'originator': self.node_id
                }
                print(f"Submitting operation: {operation}")  # Debugging print
                self.paxos.submit_operation(operation)

            elif cmd == 'choose':
                # Example: choose 1 2
                context_id = parts[1]
                response_number = int(parts[2])

                with self.lock:
                    if context_id not in self.last_responses:
                        print("No responses available for this context.")
                        return
                    responses = self.last_responses[context_id]
                    if response_number < 1 or response_number > len(responses):
                        print("Invalid response number")
                        return

                    chosen_node_id, chosen_answer = responses[response_number - 1]

                # Now submit a CHOOSE operation
                operation = {
                    'op_type': 'CHOOSE',
                    'context_id': context_id,
                    'answer': chosen_answer,
                    'originator': self.node_id
                }
                print(f"Submitting operation: {operation}")
                self.paxos.submit_operation(operation)

            elif cmd == 'view':
                context_id = parts[1]
                print(self.kv_store.get(context_id, 'Context not found'))

            elif cmd == 'viewall':
                for cid, context in self.kv_store.items():
                    print(f"{cid}: {context}")

            elif cmd == 'crash':
                self.network.stop()
                print("Node crashed.")

            elif cmd == 'recover':
                self.network = Network(self.node_id, self.nodes_info, self.network.network_server_info,
                                       self.handle_message)
                print("Node recovered.")
            else:
                print("Unknown command.")

        # Running the command handler in a thread is fine, as it prevents blocking the user_interface()
        threading.Thread(target=handle_command, daemon=True).start()

    def user_interface(self):
        # This method runs in the main thread and blocks on input(), which is normal.
        # Other threads (like Paxos message processing) run independently.
        # You do not need to thread this method; it's standard for the main loop to wait on input().
        while self.active:
            command = input("> ")
            self.process_user_command(command)

