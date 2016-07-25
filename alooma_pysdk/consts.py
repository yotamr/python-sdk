# Log messages
LOG_MSG_BUFFER_FULL = 'The buffer is full. Events will be discarded until ' \
                      'buffer space is freed'
LOG_MSG_BUFFER_FREED = 'The buffer is not full anymore, events will be ' \
                       'queued for reporting'
LOG_MSG_REPORT_AFTER_TERMINATION = 'Can\'t report events after termination'
LOG_MSG_BAD_EVENT = 'Received an invalid event of type "%s", the event was ' \
                    'discarded. Original event  = "%s"'
LOG_MSG_BAD_TOKEN = 'Token denied by remote server, please check that an ' \
                    'input exists with the supplied token and that the ' \
                    'supplied endpoint is correct'
LOG_MSG_NEW_SERVER = 'Selected new server: "%s"'
LOG_MSG_CONNECTION_FAILED = 'Failed to connect to the Alooma server: %s'
LOG_MSG_CALLBACK_FAILURE = 'Failure in callback function call: %s'
LOG_MSG_BATCH_TOO_BIG = 'Remote server closed the socket mid-send, batch size' \
                        'will be re-adjusted. Original Exception: %s'

LOG_MSG_BAD_PARAM_TOKEN = 'Invalid token (%s). Must be a string'
LOG_MSG_BAD_PARAM_BUFFER_SIZE = 'Invalid buffer size: %s'
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
LOG_MSG_BAD_PARAM_USE_SSL = 'Invalid use_ssl value (%s), must be True/False'

# Debug messages
LOG_MSG_SENDING_BATCH = 'Sending a batch of %d events (bytesize: %d) to URL: %s'
LOG_MSG_VERIFYING_CONNECTION = 'Sent connection verification to URL: %s. ' \
                               'Result: %s'
LOG_MSG_BATCH_SENT_RESULT = 'send_batch result: %d, %s'
LOG_MSG_ENQUEUED_FAILED_BATCH = 'Re-enqueued batch of %d events due to failure'
LOG_MSG_NEW_BATCH_SIZE = 'Batch size updated to %d'
LOG_MSG_OMITTED_OVERSIZED_EVENT = 'Omitted an over-sized event of %d bytes'

# General constants
EMPTY_BATCH_SLEEP_TIME = 2  # Seconds
NO_CONNECTION_SLEEP_TIME = 2  # Seconds
REST_URL_TEMPLATE = 'http{secure}://{host}/pysdk/{token}'
TOKEN_VERIFICATION_URL_TEMPLATE = 'http{secure}://{host}/verify/{token}'
CONN_VALIDATION_URL_TEMPLATE = 'http{secure}://{host}/'
CONTENT_TYPE_JSON = {'Content-Type': 'application/json'}
MAX_REQUEST_SIZE_FIELD = 'maxRequestSize'
BATCH_SIZE_MARGIN = 0.06  # Percent

# Defaults
DEFAULT_BATCH_INTERVAL = 5  # Seconds
DEFAULT_BATCH_SIZE = 5000000  # Bytes
DEFAULT_ALOOMA_ENDPOINT = 'inputs.alooma.com'
DEFAULT_BUFFER_SIZE = 100000  # Events

# Wrapper field name consts
WRAPPER_CALLING_FILE = 'calling_file'
WRAPPER_CALLING_LINE = 'calling_line'
WRAPPER_UUID = '@uuid'
WRAPPER_EVENT_TYPE = 'event_type'
WRAPPER_MESSAGE = 'message'
WRAPPER_REPORT_TIME = 'report_time'

