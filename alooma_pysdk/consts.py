import logging

# Explicit enum for logging message types
LOG_SSL_WRAP = LOG_CONNECTING = LOG_EXIT_FLUSH = logging.DEBUG
LOG_TERMINATED = LOG_CONNECTED = LOG_BUFFER_FREED = logging.INFO
LOG_CANT_CONNECT = LOG_FAILED_SEND = LOG_BUFFER_FULL = logging.ERROR
LOG_INIT_FAILED = logging.CRITICAL

# Log messages
LOG_MSG_BUFFER_FULL = 'The buffer is full. Events will be discarded until ' \
                      'buffer space is freed'
LOG_MSG_BUFFER_FREED = 'The buffer is not full anymore, events will be ' \
                       'queued for reporting'
LOG_MSG_REPORT_AFTER_TERMINATION = 'Can\'t report events after termination'
LOG_MSG_BAD_EVENT = 'Received an invalid event of type "%s", the event was ' \
                    'discarded. Original event  = "%s"'
LOG_MSG_NEW_SERVER = 'Selected new server: "%s"'
LOG_MSG_CONNECTION_FAILED = 'Failed to connect to the Alooma server: %s'

LOG_MSG_BAD_PARAM_TOKEN = 'Invalid token (%s). Must be a string'
LOG_MSG_BAD_PARAM_BUFFER_SIZE = 'Invalid buffer size: %s'
LOG_MSG_BAD_PARAM_CALLBACK = 'Failure in callback function call: %s'
LOG_MSG_BAD_PARAM_EVENT_TYPE = 'Invalid event_type (%s). Must be either a ' \
                               'string or a callable. Instead given a %s'
LOG_MSG_BAD_PARAM_BATCH_SIZE = 'Invalid batch size (%s), must be an int ' \
                               '(in bytes)'
LOG_MSG_BAD_PARAM_BATCH_INTERVAL = 'Invalid batch interval (%s), must be an ' \
                                   'int or a float (in seconds)'
LOG_MSG_BAD_PARAM_BLOCKING = 'Invalid blocking parameter (%s), must be a ' \
                             'boolean'
LOG_MSG_BAD_PARAM_CALLBACK = 'Invalid callback: %s is not callable'
LOG_MSG_BAD_PARAM_SERVERS = 'Invalid server list (%s): must be a list of ' \
                            'servers or a str or unicode type representing ' \
                            'one server'

# General constants
EMPTY_BATCH_SLEEP_TIME = 2  # Seconds
NO_CONNECTION_SLEEP_TIME = 2  # Seconds
REST_URL_TEMPLATE = 'https://{host}/pysdk/{token}'
CONN_VALIDATION_URL_TEMPLATE = 'https://{host}/'
CONTENT_TYPE_JSON = {'Content-Type': 'application/json'}

# Defaults
DEFAULT_BATCH_INTERVAL = 5  # Seconds
DEFAULT_BATCH_SIZE = 500000  # Bytes
DEFAULT_ALOOMA_ENDPOINT = 'inputs.alooma.com'
DEFAULT_BUFFER_SIZE = 100000  # Events

# Wrapper fieldname consts
WRAPPER_CALLING_FILE = 'calling_file'
WRAPPER_CALLING_LINE = 'calling_line'
WRAPPER_UUID = '@uuid'
WRAPPER_EVENT_TYPE = 'event_type'
WRAPPER_MESSAGE = 'message'
WRAPPER_REPORT_TIME = 'report_time'

