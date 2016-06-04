import os

# Explicit enum for logging message types
SSL_WRAP = 10  # Wrapping socket in SSL
CONNECTING = 19  # Connection attempts to the LogStash server
CONNECTED = 20  # Successful connection
DISCONNECTED = 30  # Disconnections
SEND_FAILED = 40  # Message sending failure
BUFFER_FULL = 48  # Buffer Full
BUFFER_FREED = 49  # Buffer Freed
CONFIG_FAILED = 50  # Init failure

# Defaults
DEFAULT_BATCH_INTERVAL = 5  # Seconds
DEFAULT_BATCH_SIZE = 4096  # Bytes
DEFAULT_ALOOMA_ENDPOINT = 'inputs.alooma.com'
DEFAULT_ALOOMA_PORT = 5001
DEFAULT_CA = os.path.dirname(os.path.realpath(__file__)) + '/alooma_ca'
DEFAULT_BUFFER_SIZE = 100000  # Events
DEFAULT_INPUT_LABEL = DEFAULT_INPUT_TYPE = 'Python SDK'

# Wrapper fieldname consts
WRAPPER_CALLING_FILE = 'calling_file'
WRAPPER_CALLING_LINE = 'calling_line'
WRAPPER_INPUT_TYPE = 'input_type'
WRAPPER_INPUT_LABEL = 'input_label'
WRAPPER_TOKEN = 'token'
WRAPPER_UUID = '@uuid'
WRAPPER_EVENT_TYPE = 'event_type'
WRAPPER_MESSAGE = 'message'
WRAPPER_REPORT_TIME = 'report_time'
