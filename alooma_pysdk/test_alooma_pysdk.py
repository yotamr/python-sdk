import datetime
import json
import logging
from unittest import TestCase

import decimal
import requests
from mock import patch, Mock
from nose.plugins.attrib import attr
from nose.tools import assert_equal, assert_true, assert_false, \
    assert_is_none, assert_is_not_none, assert_not_equal, assert_in, raises, \
    assert_raises

import alooma_pysdk as apysdk
import consts
import pysdk_exceptions as exceptions

STRING_EVENT_REPORTED = 'reported_event'
ALL_WRAPPER_FIELDS = [getattr(consts, f) for f in consts.__dict__
                      if f.startswith('WRAPPER_')]


class TestPythonSDK(TestCase):
    def setUp(self):
        self.sender_mock = Mock()
        self.get_sender_mock = Mock(return_value=self.sender_mock)
        self.__old_get_sender = apysdk._get_sender
        apysdk._get_sender = self.get_sender_mock

    @staticmethod
    def _get_python_sdk(*args, **kwargs):
        if not args:
            args = list(args)
            args.append('some-token')
        sdk = apysdk.PythonSDK(*args, **kwargs)
        return sdk

    def test_bad_param_types_raise(self):
        args = (321, 231, 643, 140, '235', '346', '467', '588', '13')
        raised = False
        try:
            self._get_python_sdk(*args)
        except ValueError as ex:
            raised = True
            for arg in args:
                assert_in(str(arg), str(ex))

        assert_true(raised)

    @patch.object(apysdk, '_logger')
    def test_exrga_args_log_warning(self, logger_mock):
        extra_kwargs = {'some_kwarg': 'hello'}
        self._get_python_sdk(**extra_kwargs)

        # Assert the logger was called and that the extra kwarg was mentioned
        assert_equal(logger_mock.warning.call_count, 1)
        assert_in(extra_kwargs, logger_mock.warning.call_args[0])

    def test_init(self):
        # Test token and parameter defaults are used, event type = input label
        test_token = 'some-token'
        sdk = self._get_python_sdk(test_token)
        passed_to_sender = self.get_sender_mock.call_args
        assert_equal(test_token, sdk.token)
        assert_equal((sdk._notify, (consts.DEFAULT_ALOOMA_ENDPOINT,
                                    sdk.token,
                                    consts.DEFAULT_BUFFER_SIZE,
                                    consts.DEFAULT_BATCH_INTERVAL,
                                    consts.DEFAULT_BATCH_SIZE, True)),
                     (passed_to_sender[1].values()[0], passed_to_sender[0]))

        # Test and ensure event type setting works
        custom_et = 'custom'
        sdk = self._get_python_sdk(event_type=custom_et)
        assert_equal(custom_et, sdk._get_event_type('blah'))

        # Test bad token raises
        expected_exception = exceptions.BadToken
        self.get_sender_mock.side_effect = expected_exception
        assert_raises(expected_exception, self._get_python_sdk, test_token)
        self.get_sender_mock.side_effect = None

        # Test failed connection raises
        expected_exception = exceptions.ConnectionFailed
        self.get_sender_mock.side_effect = expected_exception
        assert_raises(expected_exception, self._get_python_sdk, test_token)
        self.get_sender_mock.side_effect = None

    @patch.object(apysdk.PythonSDK, '_format_event')
    def test_report(self, format_event_mock):
        # Test returns False when terminated
        sdk = self._get_python_sdk()
        self.sender_mock.is_terminated = True
        sdk._format_event.assert_not_called()
        assert_equal(False, sdk.report('some-event'))

        # Test string and dict are accepted but nothing else
        self.sender_mock.is_terminated = False
        format_called_count = format_event_mock.call_count
        enqueue_event_call_count = self.sender_mock.enqueue_event.call_count
        for i in [{'dict': 'event'}, 'str_event', u'unicode_event']:
            assert_true(sdk.report(i))
            assert_equal(format_event_mock.call_count, format_called_count + 1)
            format_called_count = format_event_mock.call_count
            assert_equal(self.sender_mock.enqueue_event.call_count,
                         enqueue_event_call_count + 1)
            enqueue_event_call_count = self.sender_mock.enqueue_event.call_count
        for i in [1, 1.1, True]:
            assert_false(sdk.report(i))
        assert_equal(format_event_mock.call_count, format_called_count)
        assert_equal(enqueue_event_call_count,
                     self.sender_mock.enqueue_event.call_count)

    @patch.object(apysdk.PythonSDK, 'report')
    def test_report_many(self, report_mock):
        # Assert report_many returns the proper amount of events when failing
        report_results = [True, True, False, True]
        bad_indexes = [i for i in range(len(report_results))
                       if report_results[i] is False]
        report_mock.side_effect = report_results
        sdk = self._get_python_sdk()
        ret = sdk.report_many({'index': i} for i in range(len(report_results)))
        returned_bad_indexes = [x[0] for x in ret]
        assert_equal(len(bad_indexes), len(ret))
        assert_true(all([i in returned_bad_indexes for i in bad_indexes]))

    def test_format_event(self):
        # Dict event
        event_type = 'testType'
        event = {'test-message': 'howdy', 'type': event_type}
        sdk = self._get_python_sdk(event_type=lambda e: e['type'])
        formatted = sdk._format_event(event)
        assert_true(isinstance(formatted, dict))  # Assert appended "\n"
        for field in ALL_WRAPPER_FIELDS:
            assert_true(field in formatted, field)  # Are all fields in wrapper?
        # Assert message is correctly inserted to wrapper
        assert_equal(formatted[consts.WRAPPER_MESSAGE], event)
        # Assert event type is properly assigned - et field exists in event
        assert_equal(event_type, formatted[consts.WRAPPER_EVENT_TYPE])
        # Assert event type is properly assigned - et field isn't in the event
        formatted = sdk._format_event({'howdy': 'partner'})
        assert_false(consts.WRAPPER_EVENT_TYPE in formatted)

        # String event
        event = 'someStringEvent'
        custom_metadata_field, custom_metadata_value = 'key', 'val'
        formatted = sdk._format_event(
            event, {custom_metadata_field: custom_metadata_value})
        # Assert message inserted properly
        assert_equal(event, formatted[consts.WRAPPER_MESSAGE])
        # Assert custom metadata was inserted
        assert_equal(custom_metadata_value, formatted[custom_metadata_field])

    def tearDown(self):
        apysdk._get_sender = self.__old_get_sender


class TestSender(TestCase):
    @patch.object(apysdk._Sender, '_start_sender_thread')
    @patch.object(apysdk._Sender, '_verify_connection')
    @patch.object(apysdk._Sender, '_verify_token')
    def test_enqueue_event(self, start_thread_mock, verify_token_mock,
                           verify_connection_mock):
        # Test that event is enqueued properly
        notify_mock = Mock()
        sender = apysdk._Sender('mockHost', 1234, 1, 10, 10,
                                True, notify_mock)
        some_event = {'event': 1}
        ret = sender.enqueue_event(some_event, False)
        assert_true(ret)
        assert_equal(sender._event_queue.get_nowait(), some_event)

        # Test failing with notification when buffer is full
        sender.enqueue_event(some_event, False)
        assert_false(sender.enqueue_event(some_event, False))
        assert_equal(notify_mock.call_args[0], (logging.WARNING,
                                                consts.LOG_MSG_BUFFER_FULL))
        assert_true(sender._notified_buffer_full)

        # Test recovering when buffer frees up
        sender._event_queue.get_nowait()
        assert_true(sender.enqueue_event(some_event, False))
        assert_equal(notify_mock.call_args[0], (logging.WARNING,
                                                consts.LOG_MSG_BUFFER_FREED))

    @patch.object(apysdk._Sender, '_verify_connection')
    @patch.object(apysdk._Sender, '_verify_token')
    @patch.object(apysdk._Sender, '_start_sender_thread')
    def test_get_event(self, start_thread_mock, verify_token_mock,
                           verify_connection_mock):
        # Assert events are properly dequeued
        notify_mock = Mock()
        batch_size = 25
        sender = apysdk._Sender('mockHost', 1234, 2, 10, batch_size, True,
                                notify_mock)
        event = {'rofl': 'lol'}
        event_str = apysdk._json_enc.encode(event)
        sender._event_queue.put(event)
        sender._exceeding_event = None
        assert_equal(event_str, sender._Sender__get_event())

        # Assert exceeding event is dequeued first and events aren't lost
        exceeding_event = {'lmao': 'trololol'}
        sender._event_queue.put(event)
        sender._exceeding_event = exceeding_event
        exceeding_event_str = apysdk._json_enc.encode(exceeding_event)
        assert_equal(exceeding_event_str, sender._Sender__get_event())
        assert_equal(event_str, sender._Sender__get_event())

        # Assert oversized event is omitted
        oversized_event = {'key': ('a' * batch_size)}
        oversized_event_size = len(apysdk._json_enc.encode(oversized_event))
        sender._event_queue.put(oversized_event)
        sender._event_queue.put(event)

        pulled_event = sender._Sender__get_event()
        assert_equal(event_str, pulled_event)
        assert_equal(sender._notify.call_args[0],
                     (logging.WARNING,
                      consts.LOG_MSG_OMITTED_OVERSIZED_EVENT %
                      oversized_event_size))


    @patch.object(apysdk._Sender, '_start_sender_thread')
    @patch.object(apysdk._Sender, '_verify_connection')
    @patch.object(apysdk._Sender, '_verify_token')
    def test_choose_host(self, verify_token_mock, verify_connection_mock,
                         start_thread_mock):
        notify_mock = Mock()
        buffer_size = 1000

        # Test when only one host exists and it's the first time
        host = 'mockHost'
        sender = apysdk._Sender(host, 1234, buffer_size, 100, 100,
                                True, notify_mock)

        sender._choose_host()
        assert_equal(host, sender._http_host)

        # Make sure the host is picked again when called and not using random
        sender._choose_host()
        assert_equal(host, sender._http_host)

        # Test when there are multiple hosts
        hosts = ['1', '2', '3', '4', '5', '6']
        sender = apysdk._Sender(hosts, 1234, buffer_size, 100, 100,
                                True, notify_mock)
        sender._choose_host()
        assert_is_not_none(sender._http_host)

        # Make sure the host is not rechosen upon a `_choose_host` call
        for i in range(100):
            before = sender._http_host
            sender._choose_host()
            after = sender._http_host
            assert_not_equal(before, after)

            # Make sure the new host is properly inserted into the URLs
            assert_in(after, sender._rest_url)
            assert_in(after, sender._connection_validation_url)

    @patch.object(apysdk._Sender, '_start_sender_thread')
    @patch.object(requests, 'Session')
    @raises(exceptions.ConnectionFailed)
    def test_verify_connection(self, session_mock, start_sender_mock):
        # Assert the function throws the right exception when it fails
        sender = apysdk._Sender('12', 1234, 10, 100, 100, 'asd', True)
        sender._notify = Mock()
        sender._connection_validation_url = 'asd'
        sender._session.get.return_value = Mock(ok=False)
        sender._verify_connection()


    @patch.object(apysdk._Sender, '_start_sender_thread')
    @patch.object(requests, 'Session')
    @raises(exceptions.BadToken)
    def test_verify_token(self, session_mock, start_sender_mock):
        # Assert the function throws the right exception when it fails
        sender = apysdk._Sender('12', 1234, 10, 100, 100, 'asd', True)
        sender._notify = Mock()
        sender._connection_validation_url = 'asd'
        sender._session.get.return_value = Mock(ok=False)
        sender._verify_token()


    @attr('slow')
    @patch.object(apysdk._Sender, '_start_sender_thread')
    @patch.object(apysdk._Sender, '_verify_connection')
    @patch.object(apysdk._Sender, '_verify_token')
    def test_get_batch_empty_queue_raises(self, verify_token_mock,
                                          verify_connection_mock,
                                          start_thread_mock):
        notify_mock = Mock()
        buffer_size = 1000
        sender = apysdk._Sender('mockHost', 1234, buffer_size, 100, 100,
                                True, notify_mock)

        # Assert empty queue raises EmptyBatch
        last_batch_time = datetime.datetime.utcnow()
        raised = False
        try:
            sender._get_batch(last_batch_time)
        except exceptions.EmptyBatch:
            raised = True
        assert_true(raised)

    @patch.object(apysdk._Sender, '_start_sender_thread')
    @patch.object(apysdk._Sender, '_verify_connection')
    @patch.object(apysdk._Sender, '_verify_token')
    def test_get_batch(self, verify_token_mock, verify_connection_mock,
                       start_thread_mock):
        notify_mock = Mock()
        buffer_size = 1000
        sender = apysdk._Sender('mockHost', 1234, buffer_size, 100, 100,
                                True, notify_mock)

        # Populate the queue
        for i in range(buffer_size):
            sender._event_queue.put_nowait(
                json.dumps({'num': i, 'some_field': 'some_val'}))

        # Assert we comply with the max batch size (ignore last event)
        last_batch_time = datetime.datetime.utcnow()
        batch = sender._get_batch(last_batch_time)
        assert_true(len(''.join(batch)) < sender._batch_max_size)

        # Assert we comply with the max batch interval
        last_batch_time = datetime.datetime.utcnow() - datetime.timedelta(
            seconds=sender._batch_max_interval)
        try:
            raised_empty_batch = False
            sender._get_batch(last_batch_time)
        except exceptions.EmptyBatch:
            raised_empty_batch = True
        assert_true(raised_empty_batch)

    @patch.object(requests, 'Session')
    def test_send_batch(self, session_mock):
        notify_mock = Mock()
        sender = apysdk._Sender('mockHost', 1234, 10, 100, 100, True,
                                notify_mock)
        encode = apysdk._json_enc.encode
        batch = [encode({"event": 1}), encode({"event": 2})]

        # Assert send batch sends a stringified batch
        sender._send_batch(batch)
        call_args = sender._session.post.call_args
        assert_equal(call_args[0][0], sender._rest_url)
        assert_equal(call_args[1]['data'], '[' + ','.join(batch) + ']')
        assert_equal(call_args[1]['headers'], consts.CONTENT_TYPE_JSON)

        # Assert send fails with proper exception
        sender._session.post.return_value = Mock(ok=False, status_code=500)
        assert_raises(exceptions.SendFailed, sender._send_batch, batch)

        # Assert bad token raises proper error
        sender._session.post.return_value = Mock(status_code=400)
        assert_raises(exceptions.BadToken, sender._send_batch, batch)

        # Assert batch too big raises proper exception
        sender._session.post.side_effect = requests.exceptions.ConnectionError
        assert_raises(exceptions.BatchTooBig, sender._send_batch, batch)


class TestAloomaEncoder(TestCase):
    def test_convert_types(self):
        # This test ensures all the types don't throw when converted
        to_jsonify = {'datetime': datetime.datetime.utcnow(),
                      'date': datetime.date.today(),
                      'timedelta': datetime.timedelta(days=1),
                      'decimal': decimal.Decimal(10),
                      'int': 1, 'string': 'hello'}
        encoder = apysdk.AloomaEncoder()
        encoder.encode(to_jsonify)


@patch.object(apysdk, '_Sender')
def test_get_sender(sender_mock):
    sender_params = ('param', 'another_param')
    sender_params2 = ('param', 'a_different_param')
    notify_kwargs = {'notify_func': 'notify'}

    # Make sure same params retrieve the same sender
    apysdk._get_sender(*sender_params, **notify_kwargs)
    apysdk._get_sender(*sender_params, **notify_kwargs)
    assert_equal(1, len(apysdk._sender_instances))

    # Make sure different params retrieve a new sender
    sender3 = apysdk._get_sender(*sender_params2, **notify_kwargs)
    assert_equal(2, len(apysdk._sender_instances))


def test_terminate():
    targets = {'a': Mock(), 'b': Mock(), 'c': Mock()}
    apysdk._sender_instances.update(targets)
    apysdk.terminate()

    for sender_mock in targets.values():
        assert_equal(1, sender_mock.close.call_count)

    assert_equal(0, len(apysdk._sender_instances))
