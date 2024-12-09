import threading
import time
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
        self.timeout = 10  # Timeout in seconds

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
        sender = message['from']

        with self.lock:
            if context_id not in self.llm_responses:
                self.llm_responses[context_id] = []
            self.llm_responses[context_id].append((sender, llm_response))

    # def wait_for_responses(self, context_id, present_responses_callback):
    #     start_time = time.time()
    #     while time.time() - start_time < self.timeout:
    #         time.sleep(0.5)  # Check responses periodically
    #         with self.lock:
    #             if len(self.llm_responses.get(context_id, [])) >= (len(self.network.nodes_info) // 2) + 1:
    #                 break  # Majority quorum reached
    #
    #     # After timeout or quorum reached
    #     with self.lock:
    #         responses = self.llm_responses.pop(context_id, [])
    #     if responses:
    #         print(f"LLM responses received for context {context_id}: {responses}")
    #         present_responses_callback(context_id, responses)
    #     else:
    #         print(f"No responses received for context {context_id} within timeout.")

    def wait_for_responses(self, context_id, present_responses_callback):
        start_time = time.time()
        while time.time() - start_time < self.timeout:
            time.sleep(1)  # Check every 0.5 seconds
            # Note: We do NOT break if a majority is reached.
            # We simply keep collecting responses until timeout.
            # Remove any 'break' conditions related to majority.
            # This ensures we wait the entire timeout duration.

        # After the full timeout, print whatever we received.
        with self.lock:
            responses = self.llm_responses.pop(context_id, [])

        if responses:
            print(f"LLM responses received for context {context_id}: {responses}")
            present_responses_callback(context_id, responses)
        else:
            print(f"No responses received for context {context_id} within timeout.")

    def set_originator(self, context_id, originator_node_id):
        with self.lock:
            self.originators[context_id] = originator_node_id
