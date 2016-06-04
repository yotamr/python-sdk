import logging
import os

# Explicit enum for logging message types
LOG_SSL_WRAP = LOG_CONNECTING = LOG_EXIT_FLUSH = logging.DEBUG
LOG_TERMINATED = LOG_CONNECTED = LOG_BUFFER_FREED = logging.INFO
LOG_DISCONNECTED = LOG_FAILED_SEND = LOG_BUFFER_FULL = logging.ERROR
LOG_INIT_FAILED = logging.CRITICAL

# Sleep time consts
EMPTY_BATCH_SLEEP_TIME = 1  # Second


# Defaults
DEFAULT_BATCH_INTERVAL = 5  # Seconds
DEFAULT_BATCH_SIZE = 4096  # Bytes
DEFAULT_ALOOMA_ENDPOINT = 'inputs.alooma.com'
DEFAULT_ALOOMA_PORT = 5001
DEFAULT_CA = os.path.dirname(os.path.realpath(__file__)) + '/alooma_ca'
DEFAULT_BUFFER_SIZE = 100000  # Events
DEFAULT_INPUT_LABEL = 'Python SDK'

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
