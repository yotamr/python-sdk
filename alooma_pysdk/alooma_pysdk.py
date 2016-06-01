# -*- coding: utf8 -*-
# This module contains the Alooma Python SDK, used to report events to the
# Alooma server
import Queue
import datetime
import decimal
import errno
import inspect
import json
import logging
import os
import random
import socket
import ssl
import threading
import time
import uuid

#####################################################
# We should refrain for adding more dependencies to #
# this file to keep it easy for users to install it #
# on their machines. Especially avoid adding pkgs   #
# which aren't Python built-ins.                    #
#####################################################

# Explicit enum for logging message types
LOG_SSL_WRAP = LOG_CONNECTING = LOG_EXIT_FLUSH = logging.DEBUG
LOG_CONNECTED = LOG_BUFFER_FREED = logging.INFO
LOG_DISCONNECTED = LOG_FAILED_SEND = LOG_BUFFER_FULL = logging.ERROR
LOG_INIT_FAILED = logging.CRITICAL

_sender_instances_lock = threading.Lock()
_sender_instances = {}


def __get_logger():
    """
    If the logger wasn't configured by the calling script, configures the
    default logger - logs all messages to STDOUT and doesn't propagate to root
    :return: The 'alooma_pysdk' logger from the built-in library `logging`
    """
    logger = logging.getLogger(__name__)
    if logger.level == logging.NOTSET:
        logger.addHandler(logging.StreamHandler())
        logger.handlers[0].level = logger.level = logging.DEBUG
        logger.handlers[0].formatter = logging.Formatter(
                '%(asctime)s [%(levelname)s] {}: %(message)s'.format(__name__),
                '%Y-%m-%dT%H:%M:%S')
        logger.propagate = 0
        logger.warn('Using the default logger configuration')
    return logger
_logger = __get_logger()


def terminate():
    """
    Stops all the active Senders by flushing the buffers and closing the
    underlying sockets
    """
    with _sender_instances_lock:
        for sender_key, sender in _sender_instances.iteritems():
            sender.close()
        _sender_instances.clear()


# Declare and instantiate an AloomaEncoder
# This encoder allows JSON encoding of additional types
class AloomaEncoder(json.JSONEncoder):
    """
    Support datetime and decimal encoding when serializing JSONs
    """

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, datetime.date):
            return obj.isoformat()
        elif isinstance(obj, datetime.timedelta):
            return (datetime.datetime.min + obj).time().isoformat()
        elif isinstance(obj, decimal.Decimal):
            return str(obj)
        else:
            return super(AloomaEncoder, self).default(obj)


_json_enc = AloomaEncoder()
_default_ca = os.path.dirname(os.path.realpath(__file__)) + '/alooma_ca'


class PythonSDK:
    """
    The Alooma Python SDK is used to report events to the Alooma platform.
    It is comprised of two elements - the PythonSDK class, which stores all
    events in a buffer, and the Sender class, which periodically sends all the
    events in the buffer.

    There are two ways to get logs and respond to events in this SDK:
    1. The 'alooma.python_sdk' logger - adding this logger to your
       `logging.conf` file allows to configure it with whichever settings the
       user desires.
    2. The `callback` parameter given to the `init` function - the callback
       function is called whenever a log-worthy event occurs, and is passed the
       event code and the proper message (see more details in the `callback`
       function documentation).
    """

    def __init__(self, token, servers='inputs.alooma.com', port=5001,
                 input_label='Python SDK', event_type=None, ssl_ca=_default_ca,
                 callback=None, buffer_size=100000, blocking=True,
                 batch_mode=True, batch_size=4096):
        """
        Initializes the Alooma Python SDK, creating a connection to
        the Alooma server
        :param token:       Your unique Alooma token for this input. If you
                            don't have one, please contact support@alooma.com
        :param servers:     (Optional) A string representing your Alooma server
                            DNS or IP address, or a list of such strings.
                            Usually unnecessary.
        :param port:        (Optional) The destination port (default is 5001)
        :param ssl_ca:      (Optional) The path to a CA file validating the
                            server certificate. Default is a provided CA file
                            for Alooma. If None is passed, a plaintext 
                            connection will be created.
        :param callback:    (Optional) a custom callback function to be called
                            whenever a logged event occurs
        :param buffer_size: Optionally specify the buffer size to store before
                            flushing the buffer and sending all of its contents
        :param batch_mode:  (Optional) If set to false, forces the SDK to flush
                            immediately for every event
        :param batch_size:  (Optional) If batch mode is on, determines the batch
                            size in bytes. Default is 4096
        :param blocking:    (Optional)
        :param input_label: (Optional) The name that will be assigned to this
                            input in the Alooma UI (default is 'Python SDK')
        :param event_type:  (Optional) The event type to be shown in the UI for
                            events originating from this PySDK. Can also be a
                            callable that receives the event as a parameter and
                            calculates the event type based on the event itself,
                            e.g. getting it from a field in the event. Default
                            is the <input_label>.
        """
        _logger.debug('init. locals=%s' % locals())

        # _callback is called for every info \ error event. The user can
        # set it via the param callback to get the event messages and send
        # them elsewhere. If not set, they are logged via the self_logger.
        if callback is None:
            self._callback = self._default_callback
        else:
            self._callback = callback

        # Check inputs.
        errors = []
        if token is not None and not isinstance(token, basestring):
            errors.append("Invalid token. Must be a string")
        if ssl_ca and (not isinstance(ssl_ca, basestring) or not os.access(
                ssl_ca, os.R_OK | os.F_OK)):
            errors.append("Invalid CA file: %s" % ssl_ca)
        if not isinstance(port, int):
            errors.append("Invalid port number: %s" % port)
        if not isinstance(buffer_size, int) or buffer_size < 0:
            errors.append("Invalid buffer size: %s" % buffer_size)
        if not callable(self._callback):
            errors.append("Invalid callback: %s is not callable" %
                          str(type(self._callback)))
        if not isinstance(servers, (str, unicode)) and not isinstance(servers,
                                                                      list):
            errors.append('Invalid server list: must be a list of servers or a '
                          'str or unicode type representing one server')
        if not isinstance(input_label, basestring):
            errors.append('Invalid input_label. Must be a string')
        if event_type and not isinstance(event_type, basestring) \
                and not callable(event_type):
            errors.append('Invalid event_type. Must be either a string or a '
                          'callable. Instead given: %s' % type(event_type))
        if not isinstance(batch_mode, bool):
            errors.append("Invalid Batch_mode flag. "
                          "Must be a boolean value")
        if not isinstance(batch_size, int):
            errors.append("Invalid batch size, must be an int (in bytes)")
        if not isinstance(blocking, bool):
            errors.append('Invalid blocking parameter, must be a boolean')
        else:
            self.blocking = blocking
        if errors:
            errors.append('The PySDK will now terminate.')
            error_message = "\n".join(errors)
            self._notify(LOG_INIT_FAILED, error_message)
            raise ValueError(error_message)

        # Get a Sender to get events from the queue and send them.
        # Sender is a Singleton
        with _sender_instances_lock:
            existing_sender = _sender_instances.get(servers, None)
            if existing_sender:
                self._sender = existing_sender
            else:
                self._sender = _Sender(servers, port, self._notify, buffer_size,
                                       ssl_ca, batch_mode, batch_size)
                _sender_instances[servers] = self._sender

        self.input_label = input_label
        self.token = token

        if callable(event_type):
            # Use the given callable
            self._get_event_type = event_type
        else:
            # Use a function that returns the given string
            if not event_type:
                event_type = input_label
            self._get_event_type = lambda x: event_type

    def report(self, event, metadata=None):
        """
        Reports an event to Alooma by formatting it properly and placing it in
        the buffer to be sent by the Sender instance
        :param event:    A dict / string representing an event
        :param metadata: A dict with metadata to be attached to the event
        :return:         True if the event was successfully enqueued, else False
        """
        # Don't allow reporting if the underlying sender is terminated
        if self._sender.is_terminated:
            self._notify(LOG_FAILED_SEND,
                         'Can\'t report events after termination')
            return False

        # Send the event to the queue if it is a dict or a string.
        if isinstance(event, (dict, basestring)):
            formatted_event = self._format_event(event, metadata)
            if self.blocking:
                while (self._sender.buffer_max_size * 0.9) < \
                        self._sender.buffer_current_size:
                    time.sleep(0.5)

            self._sender.enqueue_event(formatted_event)
            return True

        else:  # Event is not a dict nor a string. Deny it.
            error_message = ('Received an invalid event of type "%s", the event'
                             ' was discarded. Original event  = "%s"' %
                             (type(event), event))
            self._notify(LOG_FAILED_SEND, error_message)
            return False

    def report_many(self, event_list, metadata=None):
        """
        Reports all the given events to Alooma by formatting them properly and
        placing them in the buffer to be sent by the Sender instance
        :param event_list: A list of dicts / strings representing events
        :param metadata:   A dict with metadata to be attached to every event
        """
        for event in event_list:
            self.report(event, metadata)

    def _format_event(self, orig_event, external_metadata):
        """
        Format the event to the expected Alooma format, packing it into a
        message field and adding metadata
        :param orig_event:         The original event that was sent, should be
                                   dict, str or unicode.
        :param external_metadata:  a dict containing metadata to add to the
                                   event
        :return:                   a dict with the original event in a 'message'
                                   field and all the supplied metadata
        """
        event_wrapper = {}

        # Add ISO6801 timestamp and frame info
        timestamp = datetime.datetime.utcnow().isoformat()
        event_wrapper['report_time'] = timestamp

        # Add the enclosing frame
        frame = inspect.currentframe().f_back.f_back
        filename = frame.f_code.co_filename
        linenum = frame.f_lineno
        event_wrapper['calling_file'] = str(filename)
        event_wrapper['calling_line'] = str(linenum)
        event_wrapper['input_type'] = 'Python SDK'
        event_wrapper['input_label'] = self.input_label
        event_wrapper['token'] = self.token
        event_wrapper['@uuid'] = str(uuid.uuid4())

        # Try to set event type. If it throws, put the input label
        try:
            event_wrapper['event_type'] = self._get_event_type(orig_event)
        except Exception:
            event_wrapper['event_type'] = self.input_label

        # Optionally add external metadata
        if external_metadata and isinstance(external_metadata, dict):
            event_wrapper.update(external_metadata)

        # JSON encoding and wrapping
        event = _json_enc.encode(orig_event)
        event_wrapper['message'] = event
        event_wrapper = _json_enc.encode(event_wrapper)

        # Ensure JSON is newline-terminated
        if event_wrapper[-1] != '\n':
            event_wrapper += '\n'

        return event_wrapper

    def _notify(self, msg_type, message):
        """
        Calls the callback function and logs messages using the PySDK logger
        """
        timestamp = datetime.datetime.utcnow()
        _logger.log(msg_type, str(message))
        self._callback(msg_type, message, timestamp)

    @staticmethod
    def _default_callback(msg_type, message, timestamp):
        """
        This default callback function does nothing. It can be replaced via
        the `callback` parameter to the constructor
        :param msg_type:  The type of message passed to the callback function.
                          It's always one of the types in the enum at the top
                          of this file
        :param message:   A str representing a log message emitted from the SDK
                          regarding an event that has occurred
        :param timestamp: A datetime object representing the time when the event
                          occurred

        """
        return

    @property
    def is_connected(self):
        if not hasattr(self, '_sender'):
            return False
        return self._sender.is_connected

    @property
    def servers(self):
        if not hasattr(self, '_sender'):
            return []
        return self._sender.servers


class _Sender:
    """
    This class is launched on a new thread as a service.
    It scans the event queue repeatedly and sends events
    to the server via an SSL-Secure socket.
    """

    def __init__(self, hosts, port, notify, buffer_size, ssl_ca, batch_mode,
                 batch_size):

        # This is a concurrent FIFO queue
        self._event_queue = Queue.Queue(buffer_size)

        # Set connection vars
        self._is_connected = False
        if isinstance(hosts, str) or isinstance(hosts, unicode):
            hosts = hosts.strip().split(',')
        self._hosts = hosts
        self._tcp_host = None
        # Set all socket vars except host, which is selected when
        # connecting
        self._set_socket_vars(port, ssl_ca)
        self._notified_buffer_full = False

        # Set vars
        self._notify = notify
        self._batch_mode = batch_mode
        self.is_terminated = False

        # Only for batch mode
        self._batch_max_size = batch_size

        # Start the sender thread
        self._start_sender_thread()

    def _choose_host(self):
        """
        This method randomly chooses a server from the server list given as
        a parameter to the parent PythonSDK
        :return: The selected host to which the Sender will attempt to
                 connect
        """
        if len(self._hosts) == 1:
            self._tcp_host = self._hosts[0]
        else:
            if self._tcp_host is None:
                self._tcp_host = random.choice(self._hosts)
            else:
                hosts = list(self._hosts)
                hosts.remove(self._tcp_host)
                self._tcp_host = random.choice(hosts)
                self._notify(LOG_CONNECTING,
                             "Selected new server: '%s'" % self._tcp_host)

    def _set_socket_vars(self, port, ssl_ca):
        """
        Sets all the network variables for the Sender except the host
        """
        self._sock = None
        self._tcp_port = port
        self._notified_buffer_full = False

        if ssl_ca:
            self._tcp_ssl_req_cert = ssl.CERT_REQUIRED
            self._tcp_ssl_ca = ssl_ca
        else:
            self._tcp_ssl_req_cert = ssl.CERT_NONE
            self._tcp_ssl_ca = None

    def _connect(self):
        """
        Connects the Sender to the chosen Alooma server
        :return: Once connected, returns True
        """
        num_of_tries = -1
        while not self.is_terminated:
            if num_of_tries == 20:
                return False

            if num_of_tries == 0:
                self._notify(LOG_CONNECTING, "Connecting to the Alooma server")
            elif num_of_tries > 0:
                self._notify(
                        LOG_CONNECTING,
                        "Retrying connection, attempt %d" % (num_of_tries + 1))

            if self._sock is not None:
                self._sock.close()
                self._sock = None
            try:
                self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._choose_host()

                # Check if need SSL, if so - set up SSL.
                if self._tcp_ssl_req_cert:
                    self._notify(LOG_SSL_WRAP, "SSL wrapping the socket")
                    self._sock = ssl.wrap_socket(
                        self._sock,
                        cert_reqs=self._tcp_ssl_req_cert,
                        ca_certs=self._tcp_ssl_ca)

                self._sock.connect((self._tcp_host, int(self._tcp_port)))

            # Trap socket connection Exceptions.
            except Exception as e:
                if self.is_terminated:  # Socket closed on purpose, terminate
                    raise
                error_message = ("Exception caught in socket connection: "
                                 "%s" % str(e))
                self._notify(LOG_DISCONNECTED, error_message)
                num_of_tries += 1
                time.sleep(num_of_tries)

            else:  # Successfully established a connection.
                message = "Established connection to the server"
                self._notify(LOG_CONNECTED, message)
                self._is_connected = True
                return True  # The connection was successful

    def _start_sender_thread(self):
        """Start the sender thread to initiate messaging."""
        send_cycle_routine = self._start_send_cycle_batch if \
            self._batch_mode else self._start_send_cycle
        self._sender_thread = threading.Thread(None,
                                               send_cycle_routine)
        self._sender_thread.daemon = True
        self._sender_thread.setName("pysdk_sender_worker_thread")
        self._sender_thread.start()

    def _start_send_cycle(self):
        """
        Runs on a pysdk_sender_worker_thread and handles sending
        events to the Alooma server.
        """
        event = former_event = None
        while not self.is_terminated:
            while not self._is_connected:
                self._connect()
                time.sleep(1)
            try:
                former_event = event
                event = self.__dequeue_event()
                self._sock.send(event)
            except socket.error, e:  # Socket DCed or is faulted
                if self.is_terminated:  # Socket closed on purpose, terminate
                    return
                self.enqueue_event(former_event)
                self.enqueue_event(event)
                if isinstance(e.args, tuple):
                    if e[0] == errno.EPIPE:
                        error_message = ("The connection to the server"
                                         " was lost")
                        self._notify(LOG_DISCONNECTED, error_message)
                else:
                    self._notify(LOG_FAILED_SEND, e)
                self._is_connected = False
            except Exception, ex:  # Trap non-socket-error Exceptions.
                self.enqueue_event(former_event)
                self.enqueue_event(event)
                self._notify(LOG_FAILED_SEND, ex)

    def _enqueue_batches(self, batches):
        """
        Enqueues several batches, putting all events in the Sender buffer. Only
        when a prior batch failed and needs to be resent along with the current
        batch
        :param batches: a list of batches, each of which is a list of events
        """
        for batch in batches:
            for event in batch:
                self.enqueue_event(event)

    def _send_batch(self, batch):
        """
        Sends a batch to the destination server via the socket
        :param batch:
        """
        batch_string = "\n".join(batch)
        self._sock.send(batch_string)

    def _start_send_cycle_batch(self):
        """
        Runs on a pysdk_sender_worker_thread and handles sending
        events to the Alooma server. Events are sent every
        <self._batch_interval> seconds or whenever batch size reaches
        <self._batch_size>
        """
        batch = []
        batch_size = 0
        while not (self.is_terminated and self._event_queue.empty()):
            while not self._is_connected:
                self._connect()
                if self._is_connected:
                    break
                time.sleep(1)
            try:
                while batch_size < self._batch_max_size and not \
                        self._event_queue.empty():
                    event = self.__dequeue_event(False)
                    batch.append(event)
                    batch_size += len(event)
                if batch:
                    self._send_batch(batch)
                    former_batch = batch[:]
                    batch = []
                    batch_size = 0
                else:
                    time.sleep(1)
            except socket.error, e:  # Socket DCed or is faulted
                if self.is_terminated:  # Socket closed on purpose, terminate
                    return
                self._enqueue_batches([batch, former_batch])
                if isinstance(e.args, tuple):
                    if e[0] == errno.EPIPE:
                        error_message = "The connection to the server was lost"
                        self._notify(LOG_DISCONNECTED, error_message)
                    else:
                        self._notify(LOG_FAILED_SEND, e)
                self._is_connected = False
            except Exception, ex:  # Trap non-socket-error Exceptions.
                self._enqueue_batches([batch, former_batch])
                self._notify(LOG_FAILED_SEND, ex)

    def enqueue_event(self, event):
        """
        Enqueues an event in the buffer to be sent to the Alooma server
        :param event: A dict representing a formatted event to be sent by the
                      sender
        """
        if self._event_queue.qsize() < self._event_queue.maxsize:
            if self._notified_buffer_full:
                self._notified_buffer_full = False
                self._notify(LOG_BUFFER_FREED,
                             "The buffer is not full anymore, events will"
                             " be queued for reporting")
            self._event_queue.put(event)
        else:
            if not self._notified_buffer_full:
                self._notify(LOG_BUFFER_FULL,
                             "The buffer is full. Events will be discarded"
                             " until there is buffer space")
                self._notified_buffer_full = True

    def close(self):
        """
        Closes the socket used to send data to Alooma
        """
        self.is_terminated = True
        flushed = self.__flush()
        if self._sock:
            self._sock.close()
            self._is_connected = False
        self._notify(LOG_DISCONNECTED,
                     'Terminated the connection to %s after flushing %d '
                     'events' % (self._hosts, flushed))

    def __dequeue_event(self, block=True):
        """
        Dequeues an event from the queue to be sent to the Alooma server.
        Used only by the Sender instance
        """
        event = self._event_queue.get(block)
        return event

    def __flush(self):
        q_size = self._event_queue.qsize()
        while not self._event_queue.empty():
            time.sleep(0.5)
        return q_size

    @property
    def servers(self):
        return self._hosts

    @property
    def event_buffer(self):
        return self._event_queue

    @property
    def buffer_max_size(self):
        return self.event_buffer.maxsize

    @property
    def buffer_current_size(self):
        return self.event_buffer.qsize()

    @property
    def is_connected(self):
        return self._is_connected
