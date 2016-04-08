# -*- coding: utf8 -*-
import logging
import os
import time
import datetime
import socket
import Queue
import ssl
import threading
import errno
import json
import inspect
import random
import decimal

# Explicit enum for logging message types
SSL_WRAP = 10  # Wrapping socket in SSL
CONNECTING = 19  # Connection attempts to the LogStash server
CONNECTED = 20  # Successful connection
DISCONNECTED = 30  # Disconnections
SEND_FAILED = 40  # Message sending failure
BUFFER_FULL = 48  # Buffer Full
BUFFER_FREED = 49  # Buffer Freed
CONFIG_FAILED = 50  # Init failure


class Reporter:
    """The py_logstash_reporter is used to report events to a Logstash server.
    It stores events on a buffer and instantiates a subclass
    named Sender that scans the buffer and sends any events in it.
    The callback param gets a function to which the Reporter
    passes errors and info messages
    """

    def __init__(self, servers="localhost", port=5001,
                 ssl_key=None, ssl_crt=None, cert_dir=None, ssl_ca=None,
                 callback=None, buffer_size=10000, batch_mode=True,
                 batch_size=4096, input_label="py_logstash_reporter"):
        """
        input_label: Added as a field to the every json event. Can be used
                     in the server-side as an identification of the data
                     source.
                     Can be either a string or a callable, that receives an
                     event and returns a string to be used as a label.
        """
        # By configuring this logger via the logging conf file,
        # Log messages can be sent to wherever the user likes.
        self._logger = logging.getLogger("pylsr.reporter")
        self._logger.debug("init. locals=%s" % locals())

        # _callback is called for every info \ error event. The user can
        # set it via the param callback to get the event messages and send
        # them elsewhere. If not set, they are logged via the self_logger.
        if callback is None:
            self._callback = self._default_callback
        else:
            self._callback = callback

        # Check inputs.
        errors = []
        if cert_dir or ssl_crt or ssl_key:
            if cert_dir:
                if not isinstance(cert_dir, (str, unicode)):
                    errors.append("Invalid certificate dir: %s" % cert_dir)
                else:
                    ssl_key = os.path.join(cert_dir, ssl_key)
                    ssl_crt = os.path.join(cert_dir, ssl_crt)
            for a_file in [ssl_key, ssl_crt]:
                if not os.access(a_file, os.R_OK | os.F_OK):
                    file_type = "crt" if a_file is ssl_crt else "key"
                    errors.append("Invalid %s file: '%s' - file does not "
                                  "exist or is inaccessible"
                                  % (file_type, a_file))
        if ssl_ca:
            if not isinstance(ssl_ca, (str, unicode)):
                errors.append("Invalid CA file path: %s" % ssl_ca)
            elif not os.access(ssl_ca, os.R_OK | os.F_OK):
                errors.append("Invalid CA file: '%s' - file does not "
                              "exist or is inaccessible" % ssl_ca)
        if not isinstance(port, int):
            errors.append("Invalid port number: %s" % port)
        if not isinstance(buffer_size, int) or buffer_size < 0:
            errors.append("Invalid buffer size: %s" % buffer_size)
        if not callable(self._callback):
            errors.append("Invalid callback: %s is not callable" %
                          str(type(self._callback)))
        if not isinstance(servers, (str, unicode)) and not isinstance(servers,
                                                                      list):
            errors.append(
                'Invalid server list: must be a list of servers or a str or '
                'unicode type representing one server')
        if (not isinstance(input_label, (str, unicode)) and
                not callable(input_label)):
            errors.append("Invalid input_label. "
                          "Must be either a string or a callable")
        if not isinstance(batch_mode, bool):
            errors.append("Invalid Batch_mode flag. "
                          "Must be a boolean value")
        if not isinstance(batch_size, int):
            errors.append("Invalid batch size, must be an int (in bytes)")
        if errors:
            errors.append("py_logstash_forwarder will now terminate.")
            error_message = ". ".join(errors)
            self._notify(CONFIG_FAILED, error_message)
            raise ValueError(error_message)

        # Instantiate a JSON Encoder for event formatting.
        self._json_enc = BetterEncoder()

        # Instantiate a Sender to get events from the queue and send them.
        self._sender = self.Sender(servers, port, self._notify, buffer_size,
                                   ssl_key, ssl_crt, ssl_ca, batch_mode,
                                   batch_size)

        if callable(input_label):
            # use the given callable
            self._get_input_label = input_label
        else:
            # use a function that returns the given string
            self._get_input_label = lambda x: input_label

    def report(self, event, external_metadata=None):
        """Passes a message on to the Sender. A valid event is
        a python dict. Other types aren't supported.
        """

        # Send the event to the queue if it is a dict.
        if isinstance(event, dict):
            formatted_event = self._apply_logstash_format(
                event, external_metadata)
            self._sender.enqueue_event(formatted_event)

        else:  # Event is not a dict. Deny it.
            error_message = ("Received non-Dict event that was not reported to"
                             " logstash.")
            self._notify(SEND_FAILED, error_message)

    def _apply_logstash_format(self, event_dict, external_metadata):
        """Adds an ISO8601 timestamp and stack info to the event
        and wraps it in a JSON
        """
        event_wrapper = {}

        # Add ISO6801 timestamp and frame info
        timestamp = datetime.datetime.utcnow().isoformat()
        event_wrapper["report_time"] = timestamp

        # Add the enclosing frame
        frame = inspect.currentframe().f_back.f_back
        filename = frame.f_code.co_filename
        linenum = frame.f_lineno
        event_wrapper["calling_file"] = str(filename)
        event_wrapper["calling_line"] = str(linenum)
        event_wrapper["input_label"] = self._get_input_label(event_dict)

        # Optionally add external metadata
        if external_metadata and isinstance(external_metadata, dict):
            event_wrapper.update(external_metadata)

        # JSON encoding and wrapping
        event = self._json_enc.encode(event_dict)
        event_wrapper["message"] = event
        event_wrapper = self._json_enc.encode(event_wrapper)

        # Ensure JSON is newline-terminated
        if event_wrapper[-1] != '\n':
            event_wrapper += '\n'

        return event_wrapper

    @staticmethod
    def _default_callback(msg_type, message, timestamp):
        """This default callback func does nothing.
        It can be replaced via the function call (param callback).
        """
        return

    def is_connected(self):
        return self._sender.is_connected

    def _notify(self, msg_type, message):
        """Calls the callback method and logs messages to the
        py_logstash_reporter logger.
        """
        timestamp = time.strftime("[%Y-%m-%d %H:%M:%S]", time.localtime())
        self._logger.info("%s %s" % (msg_type, str(message)))
        self._callback(msg_type, message, timestamp)

    class Sender:
        """This class is launched on a new thread as a service.
        It scans the event queue repeatedly and sends events
        to the server via an SSL-Secure socket.
        """

        def __init__(self, hosts, port, notify, buffer_size, ssl_key,
                     ssl_crt, ssl_ca, batch_mode, batch_size):

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
            self._set_socket_vars(port, ssl_key, ssl_crt, ssl_ca)
            self._notified_buffer_full = False

            # Set vars
            self._logger = logging.getLogger("pylsr.sender")
            self._notify = notify
            self._batch_mode = batch_mode
            # Only for batch mode
            self._batch_max_size = batch_size

            # Start the sender thread
            self._start_sender_thread()

        def _choose_host(self):
            """This method randomly chooses a logstash server from the server
            list given as a parameter to the Reporter
            :return:The elected host to which the py-logstash-reporter will
            attempt to connect
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
                    self._notify(CONNECTING,
                                 "Selected new server: '%s'" % self._tcp_host)

        def _set_socket_vars(self, port, key_path, cert_path, ca_path):
            """Sets all the network variables for the Sender except the host"""
            self._sock = None
            self._tcp_port = port
            if key_path or cert_path:
                self._tcp_ssl_key = key_path
                self._tcp_ssl_cert = cert_path
            else:
                self._tcp_ssl_key = None
                self._tcp_ssl_cert = None
            if ca_path:
                self._tcp_ssl_req_cert = ssl.CERT_REQUIRED
                self._tcp_ssl_ca = ca_path
            else:
                self._tcp_ssl_req_cert = ssl.CERT_NONE
                self._tcp_ssl_ca = None
            self._use_ssl = ca_path or cert_path or key_path
            self._notified_buffer_full = False

        def _connect(self):
            """This method connects the Sender to the Logstash
            server. Returns True once connected.
            """
            wait = -1
            while True:
                wait += 1
                time.sleep(wait)

                if wait == 20:
                    return False

                if wait > 0:
                    self._notify(
                        CONNECTING,
                        "Retrying connection, attempt %d." % (wait + 1))
                else:
                    self._notify(CONNECTING, "Connecting to the Logstash "
                                             "server")

                if self._sock is not None:
                    self._sock.close()
                    self._sock = None
                try:
                    self._sock = socket.socket(socket.AF_INET,
                                               socket.SOCK_STREAM)  # TCP
                    self._choose_host()
                    # Check if need SSL, if so - set up SSL.
                    if self._use_ssl:
                        self._notify(SSL_WRAP, "SSL wrapping the socket")
                        self._sock = ssl.wrap_socket(
                            self._sock,
                            keyfile=self._tcp_ssl_key,
                            certfile=self._tcp_ssl_cert,
                            cert_reqs=self._tcp_ssl_req_cert,
                            ca_certs=self._tcp_ssl_ca)
                    self._sock.connect((self._tcp_host, int(self._tcp_port)))

                # Trap socket connection Exceptions.
                except Exception as e:
                    error_message = ("Exception caught in socket connection: "
                                     "%s" % str(e))
                    self._notify(DISCONNECTED, error_message)

                else:  # Successfully established a connection.
                    message = "Established connection to the server"
                    self._notify(CONNECTED, message)
                    self._is_connected = True
                    return True  # The connection was successful

        def _start_sender_thread(self):
            """Start the sender thread to initiate messaging."""
            send_cycle_routine = self._start_send_cycle_batch if \
                self._batch_mode else self._start_send_cycle
            self._sender_thread = threading.Thread(None,
                                                   send_cycle_routine)
            self._sender_thread.daemon = True
            self._sender_thread.setName("pylsr_sender_worker_thread")
            self._sender_thread.start()

        def _start_send_cycle(self):
            """Runs on a pylsr_sender_worker_thread and handles sending
            events to the LogStash server.
            """
            event = former_event = None
            while True:
                while not self._is_connected:
                    self._connect()
                    time.sleep(1)
                try:
                    former_event = event
                    event = self._dequeue_event()
                    self._sock.send(event)
                except socket.error, e:  # Socket DCed or is faulted
                    self.enqueue_event(former_event)
                    self.enqueue_event(event)
                    if isinstance(e.args, tuple):
                        if e[0] == errno.EPIPE:
                            error_message = ("The connection to the server"
                                             " was lost")
                            self._notify(DISCONNECTED, error_message)
                    else:
                        self._notify(SEND_FAILED, e)
                    self._is_connected = False
                except Exception, ex:  # Trap non-socket-error Exceptions.
                    self.enqueue_event(former_event)
                    self.enqueue_event(event)
                    self._notify(SEND_FAILED, ex)

        def _enqueue_batches(self, batches):
            """

            :param batches:
            """
            for batch in batches:
                for event in batch:
                    self.enqueue_event(event)

        def _send_batch(self, batch):
            """

            :param batch:
            """
            batch_string = "\n".join(batch)
            self._sock.send(batch_string)

        def _start_send_cycle_batch(self):
            """Runs on a pylsr_sender_worker_thread and handles sending
            events to the LogStash server. Events are sent every
            <self._batch_interval> seconds or whenever batch size reaches
            <self._batch_size>
            """
            batch = []
            batch_size = 0
            while True:
                while not self._is_connected:
                    self._connect()
                    if self._is_connected:
                        break
                    time.sleep(1)
                try:
                    while batch_size < self._batch_max_size and not \
                            self._event_queue.empty():
                        event = self._dequeue_event(False)
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
                    self._enqueue_batches([batch, former_batch])
                    if isinstance(e.args, tuple):
                        if e[0] == errno.EPIPE:
                            error_message = ("The connection to the server"
                                             " was lost: %s" % e)
                            self._notify(DISCONNECTED, error_message)
                        else:
                            self._notify(SEND_FAILED, e)
                    self._is_connected = False
                except Exception, ex:  # Trap non-socket-error Exceptions.
                    self._enqueue_batches([batch, former_batch])
                    self._notify(SEND_FAILED, ex)

        def enqueue_event(self, event):
            """Enqueues an event to be sent to the LogStash server."""
            if self._event_queue.qsize() < self._event_queue.maxsize:
                if self._notified_buffer_full:
                    self._notified_buffer_full = False
                    self._notify(BUFFER_FREED,
                                 "The buffer is not full any more, events will"
                                 " be queued for reporting")
                self._event_queue.put(event)
            else:
                if not self._notified_buffer_full:
                    self._notify(BUFFER_FULL,
                                 "The buffer is full. Events will be discarded"
                                 " until there is buffer space")
                    self._notified_buffer_full = True

        def _dequeue_event(self, block=True):
            """Dequeues an event from the queue.
            Used only by the Sender instance.
            """
            event = self._event_queue.get(block)
            return event

        @property
        def event_buffer(self):
            return self._event_queue

        @property
        def is_connected(self):
            return self._is_connected

    @property
    def sender(self):
        return self._sender


class BetterEncoder(json.JSONEncoder):
    """"Support datetime and decimal encoding when serializing jsons
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
            return super(BetterEncoder, self).default(obj)
