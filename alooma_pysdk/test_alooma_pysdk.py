from mock import patch
from nose.tools import assert_equals
import consts
import alooma_pysdk as apysdk

STRING_EVENT_REPORTED = 'reported_event'


class TestPythonSDK(object):

    @patch.object(apysdk, '_get_sender')
    def test_init(self, get_sender_mock):
        # Test token and parameter defaults are used
        test_token = 'some-token'
        sdk = apysdk.PythonSDK(test_token)
        passed_to_sender = get_sender_mock.call_args
        assert_equals(test_token, sdk.token)
        assert_equals((sdk._notify, (consts.DEFAULT_ALOOMA_ENDPOINT,
                                     consts.DEFAULT_ALOOMA_PORT,
                                     consts.DEFAULT_BUFFER_SIZE,
                                     consts.DEFAULT_CA,
                                     consts.DEFAULT_BATCH_INTERVAL,
                                     consts.DEFAULT_BATCH_SIZE)),
                      (passed_to_sender[1].values()[0], passed_to_sender[0]))
