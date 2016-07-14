# -*- coding: utf8 -*-
# This module contains the Alooma Python SDK, used to report events to the
# Alooma server
import Queue
import datetime
import decimal
import inspect
import json
import logging
import random
import threading
import time
import uuid

import requests

import consts
import pysdk_exceptions as exceptions


#####################################################
# We should refrain for adding more dependencies to #
# this file to keep it easy for users to install it #
# on their machines. Especially avoid adding pkgs   #
# which aren't Python built-ins.                    #
#####################################################


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


class PythonSDK:
    """
    The Alooma Python SDK is used to report events to the Alooma platform.
    It is comprised of two elements - the PythonSDK class, which stores all
    events in a buffer, and the Sender class, which periodically sends all the
    events in the buffer.

    There are two ways to get logs and respond to events in this SDK:
    1. The 'alooma.python_sdk' logger - adding this logger to your
       `logging.conf` file allows you to configure it however you wish.
    2. The `callback` parameter given to the `init` function - the callback
       function is called whenever a log-worthy event occurs, and is passed the
       event code and the proper message (see more details in the `callback`
       function documentation).
    """

    def __init__(self, token, servers=consts.DEFAULT_ALOOMA_ENDPOINT,
                 event_type=None, callback=None,
                 buffer_size=consts.DEFAULT_BUFFER_SIZE, blocking=False,
                 batch_interval=consts.DEFAULT_BATCH_INTERVAL,
                 batch_size=consts.DEFAULT_BATCH_SIZE, *args, **kwargs):
        """
        Initializes the Alooma Python SDK, creating a connection to
        the Alooma server
        :param token:          Your unique Alooma token for this input. If you
                               don't have one, please contact support@alooma.com
        :param servers:        (Optional) A string representing your Alooma
                               server DNS or IP address, or a list of such
                               strings. Usually unnecessary.
        :param event_type:     (Optional) The event type to be shown in the
                               UI for events originating from this PySDK. Can
                               also be a callable that receives the event as a
                               parameter and calculates the event type based on
                               the event itself, e.g. getting it from a field
                               in the event. Default is the <input_label>.
        :param callback:       (Optional) a custom callback function to be
                               called whenever a logged event occurs
        :param buffer_size:    Optionally specify the buffer size to store
                               before flushing the buffer and sending all of
                               its
                               contents
        :param blocking:       (Optional) If False, never blocks the main thread
                               and instead discards events when the buffer is
                               full. Default is True - blocks until buffer space
                               frees up.
        :param batch_interval: (Optional) Determines the batch interval in
                               seconds. Default is 5 seconds
        :param batch_size:     (Optional) Determines the batch size in bytes.
                               Default is 4096 bytes
        """
        _logger.debug('init. locals=%s' % locals())

        if args or kwargs:
            _logger.warning('The SDK received unrecognized arguments which '
                            'will be ignored. args: %s, kwargs: %s', args,
                            kwargs)

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
            errors.append(consts.LOG_MSG_BAD_PARAM_TOKEN % token)
        if not isinstance(buffer_size, int) or buffer_size < 0:
            errors.append(consts.LOG_MSG_BAD_PARAM_BUFFER_SIZE % buffer_size)
        if not callable(self._callback):
            errors.append(consts.LOG_MSG_BAD_PARAM_CALLBACK %
                          str(self._callback))
        if not isinstance(servers, (str, unicode)) and not isinstance(servers,
                                                                      list):
            errors.append(consts.LOG_MSG_BAD_PARAM_SERVERS % servers)
        if event_type and not isinstance(event_type, basestring) \
                and not callable(event_type):
            et_type = type(event_type)
            errors.append(consts.LOG_MSG_BAD_PARAM_EVENT_TYPE % (event_type,
                                                                 et_type))
        if not isinstance(batch_size, int):
            errors.append(consts.LOG_MSG_BAD_PARAM_BATCH_SIZE % batch_size)
        if not isinstance(batch_interval, (int, float)):
            errors.append(
                consts.LOG_MSG_BAD_PARAM_BATCH_INTERVAL % batch_interval)
        if not isinstance(blocking, bool):
            errors.append(consts.LOG_MSG_BAD_PARAM_BLOCKING % blocking)
        else:
            self.is_blocking = blocking
        if errors:
            errors.append('The PySDK will now terminate.')
            error_message = "\n".join(['Bad parameters given:'] + errors)
            self._notify(consts.LOG_INIT_FAILED, error_message)
            raise ValueError(error_message)

        # Get a Sender to get events from the queue and send them.
        # Sender is a Singleton per parameter group
        sender_params = (servers, token, buffer_size, batch_interval,
                         batch_size)
        self._sender = _get_sender(*sender_params, notify_func=self._notify)

        self.token = token

        if callable(event_type):
            # Use the given callable
            self._get_event_type = event_type
        else:
            # Use a function that returns the given string
            self._get_event_type = lambda x: event_type

    def _format_event(self, orig_event, external_metadata=None):
        """
        Format the event to the expected Alooma format, packing it into a
        message field and adding metadata
        :param orig_event:         The original event that was sent, should be
                                   dict, str or unicode.
        :param external_metadata:  (Optional) a dict containing metadata to add
                                   to the event
        :return:                   a dict with the original event in a 'message'
                                   field and all the supplied metadata
        """
        event_wrapper = {}

        # Add ISO6801 timestamp and frame info
        timestamp = datetime.datetime.utcnow().isoformat()
        event_wrapper[consts.WRAPPER_REPORT_TIME] = timestamp

        # Add the enclosing frame
        frame = inspect.currentframe().f_back.f_back
        filename = frame.f_code.co_filename
        line_number = frame.f_lineno
        event_wrapper[consts.WRAPPER_CALLING_FILE] = str(filename)
        event_wrapper[consts.WRAPPER_CALLING_LINE] = str(line_number)

        # Add the UUID to the event
        event_wrapper[consts.WRAPPER_UUID] = str(uuid.uuid4())

        # Try to set event type. If it throws, put the input label
        try:
            event_wrapper[consts.WRAPPER_EVENT_TYPE] = \
                self._get_event_type(orig_event)
        except Exception:
            pass  # The event type will be the input name, added by Alooma

        # Optionally add external metadata
        if external_metadata and isinstance(external_metadata, dict):
            event_wrapper.update(external_metadata)

        # Wrap the event with metadata
        event_wrapper[consts.WRAPPER_MESSAGE] = orig_event

        return event_wrapper

    def report(self, event, metadata=None, block=None):
        """
        Reports an event to Alooma by formatting it properly and placing it in
        the buffer to be sent by the Sender instance
        :param event:    A dict / string representing an event
        :param metadata: (Optional) A dict with extra metadata to be attached to
                         the event
        :param block:    (Optional) If True, the function will block the thread
                         until the event buffer has space for the event.
                         If False, reported events are discarded if the queue is
                         full. Defaults to None, which uses the global `block`
                         parameter given in the `init`.
        :return:         True if the event was successfully enqueued, else False
        """
        # Don't allow reporting if the underlying sender is terminated
        if self._sender.is_terminated:
            self._notify(consts.LOG_FAILED_SEND,
                         consts.LOG_MSG_REPORT_AFTER_TERMINATION)
            return False

        # Send the event to the queue if it is a dict or a string.
        if isinstance(event, (dict, basestring)):
            formatted_event = self._format_event(event, metadata)

            should_block = block if block is not None else self.is_blocking
            return self._sender.enqueue_event(formatted_event, should_block)

        else:  # Event is not a dict nor a string. Deny it.
            error_message = (consts.LOG_MSG_BAD_EVENT % (type(event), event))
            self._notify(consts.LOG_FAILED_SEND, error_message)
            return False

    def report_many(self, event_list, metadata=None, block=None):
        """
        Reports all the given events to Alooma by formatting them properly and
        placing them in the buffer to be sent by the Sender instance
        :param event_list: A list of dicts / strings representing events
        :param metadata: (Optional) A dict with extra metadata to be attached to
                         the event
        :param block:    (Optional) If True, the function will block the thread
                         until the event buffer has space for the event.
                         If False, reported events are discarded if the queue is
                         full. Defaults to None, which uses the global `block`
                         parameter given in the `init`.
        :return:         A list with tuples, each containing a failed event
                         and its original index. An empty list means success
        """
        failed_list = []
        for index, event in enumerate(event_list):
            queued_successfully = self.report(event, metadata, block)
            if not queued_successfully:
                failed_list.append((index, event))
        return failed_list

    def _notify(self, msg_type, message):
        """
        Calls the callback function and logs messages using the PySDK logger
        """
        timestamp = datetime.datetime.utcnow()
        _logger.log(msg_type, str(message))
        try:
            self._callback(msg_type, message, timestamp)
        except Exception as ex:
            _logger.warning(consts.LOG_MSG_BAD_PARAM_CALLBACK % str(ex))

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

    def __init__(self, hosts, token, buffer_size, batch_interval, batch_size,
                 notify):

        # This is a concurrent FIFO queue
        self._event_queue = Queue.Queue(buffer_size)

        # The session on which requests will be sent
        self._session = requests.Session()

        # Set connection vars
        if isinstance(hosts, str) or isinstance(hosts, unicode):
            hosts = hosts.strip().split(',')
        self._hosts = hosts
        self._http_host = None
        self._token = token
        self._rest_url = None
        # Set all socket vars except host, which is selected when
        # connecting
        self._notified_buffer_full = False

        # Set vars
        self._notify = notify
        self._is_connected = threading.Event()
        self._is_terminated = threading.Event()
        self._batch_max_size = batch_size
        self._batch_max_interval = batch_interval

        # Start the sender thread
        self._start_sender_thread()

    def _choose_host(self):
        """
        This method randomly chooses a server from the server list given as
        a parameter to the parent PythonSDK
        :return: The selected host to which the Sender will attempt to
                 connect
        """
        # If a host hasn't been chosen yet or there is only one host
        if len(self._hosts) == 1 or self._http_host is None:
            self._http_host = self._hosts[0]
        else:  # There is a list of hosts to choose from, pick a random one
            choice = self._http_host
            while choice == self._http_host:
                choice = random.choice(self._hosts)
            self._http_host = choice
            self._notify(consts.LOG_CONNECTING,
                         consts.LOG_MSG_NEW_SERVER % self._http_host)

        # Set the validation and the REST URLs
        self._connection_validation_url = \
            consts.CONN_VALIDATION_URL_TEMPLATE.format(host=self._http_host)
        self._rest_url = consts.REST_URL_TEMPLATE.format(host=self._http_host,
                                                         token=self._token)

    def _verify_connection(self):
        """
        Checks availability of the Alooma server
        :return: If the server is reachable, returns True
        :raises: If connection fails, raises exceptions.ConnectionFailed
        """
        try:
            res = self._session.get(self._connection_validation_url, json={})
            if not res.ok:
                raise requests.exceptions.RequestException(res.content)
            self._is_connected.set()
            return True

        except requests.exceptions.RequestException as ex:
            msg = consts.LOG_MSG_CONNECTION_FAILED % str(ex)
            self._notify(consts.LOG_CANT_CONNECT, msg)
            raise exceptions.ConnectionFailed(msg)

    def _start_sender_thread(self):
        """Start the sender thread to initiate messaging."""
        self._sender_thread = threading.Thread(name='pysdk_sender_thread',
                                               target=self._sender_main)
        self._sender_thread.daemon = True
        self._sender_thread.start()

    def _enqueue_batch(self, batch):
        """
        Enqueues an entire batch, putting all events in the Sender buffer. Used
        to re-enqueue a batch when we fail to send it, to make sure no events
        are lost.
        :param batch: a list of events
        """
        for event in batch:
            self.enqueue_event(event, False)
            # TODO: Handle this, it shouldn't fail when buffer is full

    def _send_batch(self, batch):
        """
        Sends a batch to the destination server via HTTP REST API
        """
        try:
            json_batch = _json_enc.encode(batch)
            res = self._session.post(self._rest_url, data=json_batch,
                                     headers=consts.CONTENT_TYPE_JSON)
            if not res.ok:
                raise exceptions.SendFailed("Got bad response code - %s: %s" % (
                    res.status_code, res.content if res.content else 'No info'))
        except requests.exceptions.RequestException as ex:
            raise exceptions.SendFailed(str(ex))

    def _is_batch_time_over(self, last_batch_time):
        batch_time = (datetime.datetime.utcnow() - last_batch_time)
        return batch_time.total_seconds() > self._batch_max_interval

    def _is_batch_full(self, batch_len):
        return batch_len > self._batch_max_size

    def _get_batch(self, last_batch_time):
        batch = []
        batch_len = 0
        try:
            while not self._is_batch_time_over(last_batch_time) \
                    and not self._is_batch_full(batch_len):
                event = self.__dequeue_event()
                batch.append(event)
                batch_len += len(event)

        except Queue.Empty:  # No more events to fetch
            pass

        if batch:
            return batch
        else:
            raise exceptions.EmptyBatch

    def _sender_main(self):
        """
        Runs on a pysdk_sender_thread and handles sending events to the Alooma
        server. Events are sent every <self._batch_interval> seconds or whenever
        batch size reaches <self._batch_size>
        """
        self._choose_host()
        last_batch_time = datetime.datetime.utcnow()
        batch = None

        while not (self._is_terminated.isSet() and self._event_queue.empty()):
            try:
                if not self._is_connected.isSet():
                    self._verify_connection()

                batch = self._get_batch(last_batch_time)
                self._send_batch(batch)

            except exceptions.ConnectionFailed:  # Failed to connect to server
                time.sleep(consts.NO_CONNECTION_SLEEP_TIME)
                self._is_connected.clear()

            except exceptions.EmptyBatch:  # No events in queue, go to sleep
                time.sleep(consts.EMPTY_BATCH_SLEEP_TIME)

            except exceptions.SendFailed as ex:  # Failed to send an event batch
                self._notify(consts.LOG_FAILED_SEND, ex.message)
                self._is_connected.clear()

                if batch:  # Failed after pulling a batch from the queue
                    self._enqueue_batch(batch)

            else:  # We sent a batch successfully, server is reachable
                self._is_connected.set()

            finally:  # Advance last batch time
                last_batch_time = datetime.datetime.utcnow()

    def enqueue_event(self, event, block):
        """
        Enqueues an event in the buffer to be sent to the Alooma server
        :param event: A dict representing a formatted event to be sent by the
                      sender
        :param block: Whether or not we should block if the event buffer is full
        :return: True if the event was enqueued successfully, else False
        """
        try:
            self._event_queue.put_nowait(event)

            if self._notified_buffer_full:  # Non-blocking and buffer was full
                self._notify(consts.LOG_BUFFER_FREED,
                             consts.LOG_MSG_BUFFER_FREED)
                self._notified_buffer_full = False

        except Queue.Full:
            if block:  # Blocking - should block until space is freed
                self._event_queue.put(event)

            elif not self._notified_buffer_full:  # Don't block, msg not emitted
                self._notify(consts.LOG_BUFFER_FULL, consts.LOG_MSG_BUFFER_FULL)
                self._notified_buffer_full = True
                return False

        return True

    def close(self):
        """
        Marks Sender as terminated, flushes the queue and closes the session
        """
        self._is_terminated.set()
        self.__flush_and_close_session()

    def __dequeue_event(self, block=True, timeout=1):
        """
        Dequeues an event from the queue to be sent to the Alooma server.
        Used only by the Sender instance
        """
        event = self._event_queue.get(block, timeout)
        return event

    def __flush_and_close_session(self):
        queue_size_before_flush = self._event_queue.qsize()
        while not self._event_queue.empty():
            time.sleep(0.5)
        self._session.close()
        self._notify(consts.LOG_TERMINATED,
                     'Terminated the connection to %s after flushing %d '
                     'events' % (self._hosts, queue_size_before_flush))

    @property
    def servers(self):
        return self._hosts

    @property
    def is_terminated(self):
        return self._is_terminated.isSet()

    @property
    def is_connected(self):
        return self._is_connected.isSet()


# Sender instance management
_sender_instances_lock = threading.Lock()
_sender_instances = {}


def _get_sender(*sender_params, **kwargs):
    """
    Utility function acting as a Sender factory - ensures senders don't get
    created twice of more for the same target server
    """
    notify_func = kwargs['notify_func']
    with _sender_instances_lock:
        existing_sender = _sender_instances.get(sender_params, None)
        if existing_sender:
            sender = existing_sender
            sender._notify = notify_func
        else:
            sender = _Sender(*sender_params, notify=notify_func)
        _sender_instances[sender_params] = sender
        return sender


def terminate():
    """
    Stops all the active Senders by flushing the buffers and closing the
    underlying sockets
    """
    with _sender_instances_lock:
        for sender_key, sender in _sender_instances.iteritems():
            sender.close()
        _sender_instances.clear()
