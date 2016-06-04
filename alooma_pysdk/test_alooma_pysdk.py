from mock import patch, Mock
from nose.tools import assert_equals, assert_true, assert_false
import consts
import alooma_pysdk as apysdk

STRING_EVENT_REPORTED = 'reported_event'
ALL_WRAPPER_FIELDS = [getattr(consts, f) for f in consts.__dict__
                      if f.startswith('WRAPPER_')]


class TestPythonSDK(object):

    def _get_python_sdk(self, *args, **kwargs):
        self.sender_mock = Mock()
        self.get_sender_mock = Mock(return_value=self.sender_mock)
        apysdk._get_sender = self.get_sender_mock
        if not args:
            args = list(args)
            args.append('some-token')
        sdk = apysdk.PythonSDK(*args, **kwargs)
        return sdk

    def test_init(self):
        # Test token and parameter defaults are used
        test_token = 'some-token'
        sdk = self._get_python_sdk(test_token)
        passed_to_sender = self.get_sender_mock.call_args
        assert_equals(test_token, sdk.token)
        assert_equals((sdk._notify, (consts.DEFAULT_ALOOMA_ENDPOINT,
                                     consts.DEFAULT_ALOOMA_PORT,
                                     consts.DEFAULT_BUFFER_SIZE,
                                     consts.DEFAULT_CA,
                                     consts.DEFAULT_BATCH_INTERVAL,
                                     consts.DEFAULT_BATCH_SIZE)),
                      (passed_to_sender[1].values()[0], passed_to_sender[0]))

    @patch.object(apysdk.PythonSDK, '_format_event')
    def test_report(self, format_event_mock):
        # Test returns False when terminated
        sdk = self._get_python_sdk()
        self.sender_mock.is_terminated = True
        sdk._format_event.assert_not_called()
        assert_equals(False, sdk.report('some-event'))

        # Test string and dict are accepted but nothing else
        self.sender_mock.is_terminated = False
        format_called_count = format_event_mock.call_count
        enqueue_event_call_count = self.sender_mock.enqueue_event.call_count
        for i in [{'dict': 'event'}, 'str_event', u'unicode_event']:
            assert_true(sdk.report(i))
            assert_equals(format_event_mock.call_count, format_called_count + 1)
            format_called_count = format_event_mock.call_count
            assert_equals(self.sender_mock.enqueue_event.call_count,
                          enqueue_event_call_count + 1)
            enqueue_event_call_count = self.sender_mock.enqueue_event.call_count
        for i in [1, 1.1, True]:
            assert_false(sdk.report(i))
        assert_equals(format_event_mock.call_count, format_called_count)
        assert_equals(enqueue_event_call_count,
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
        assert_equals(len(bad_indexes), len(ret))
        assert_true(all([i in returned_bad_indexes for i in bad_indexes]))




