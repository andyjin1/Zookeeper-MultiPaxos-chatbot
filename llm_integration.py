# llm_integration.py

import threading
from config import API_KEY, MODEL_NAME

try:
    import google.generativeai as genai
    genai.configure(api_key=API_KEY)
    model = genai.GenerativeModel(MODEL_NAME)
except ImportError:
    print("Please install the 'google-generativeai' package.")

class LLMIntegration:
    def __init__(self, node_id, network):
        self.node_id = node_id
        self.network = network
        self.lock = threading.Lock()
        self.llm_responses = {}  # Stores LLM responses per context
        self.originators = {}  # Tracks originator nodes per context

    def query_llm_and_send_response(self, context_id, context_text, originator_node_id):
        prompt_for_answer = "\nAnswer: "
        response = model.generate_content(context_text + prompt_for_answer)
        llm_response = response.text.strip()
        # Send response back to originator
        message = {
            'type': 'LLM_RESPONSE',
            'from': self.node_id,
            'to': originator_node_id,
            'context_id': context_id,
            'llm_response': llm_response
        }
        print(f"LLM_RESPONSE from {self.node_id} to {originator_node_id} for context {context_id}")
        self.network.send_message(originator_node_id, message)

    def handle_llm_response(self, message, present_responses_callback):
        context_id = message['context_id']
        llm_response = message['llm_response']
        with self.lock:
            if context_id not in self.llm_responses:
                self.llm_responses[context_id] = []
            self.llm_responses[context_id].append((message['from'], llm_response))
            # Check if responses from all nodes are received
            if len(self.llm_responses[context_id]) >= len(self.network.nodes_info):
                # Present options to the user
                responses = self.llm_responses[context_id]
                present_responses_callback(context_id, responses)
                # Clear stored responses
                del self.llm_responses[context_id]
                del self.originators[context_id]

    def set_originator(self, context_id, originator_node_id):
        with self.lock:
            self.originators[context_id] = originator_node_id
