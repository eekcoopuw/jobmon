from unittest import mock

import pytest
from tenacity import stop_after_attempt

from jobmon.requests import requester


def test_server_502(client_env):
    """
    GBDSCI-1553

    We should be able to automatically retry if server returns 5XX
    status code. If we exceed retry budget, we should raise informative error
    """
    from jobmon.client import shared_requester

    err_response = (
        502,
        b'<html>\r\n<head><title>502 Bad Gateway</title></head>\r\n<body '
        b'bgcolor="white">\r\n<center><h1>502 Bad Gateway</h1></center>\r\n'
        b'<hr><center>nginx/1.13.12</center>\r\n</body>\r\n</html>\r\n'
    )
    good_response = (
        200,
        {'time': '2019-02-21 17:40:07'}
    )

    test_requester = requester.Requester(shared_requester.url)

    # mock requester.get_content to return 2 502s then 200
    with mock.patch('jobmon.requests.requester.get_content') as m:
        # Docs: If side_effect is an iterable then each call to the mock
        # will return the next value from the iterable
        m.side_effect = [err_response] * 2 + \
            [good_response] + [err_response] * 2

        test_requester.send_request("/time", {}, "get")  # fails at first

        # should have retried twice + one success
        retrier = test_requester.send_request.retry
        assert retrier.statistics['attempt_number'] == 3

        # if we end up stopping we should get an error
        with pytest.raises(RuntimeError, match='Status code was 502'):
            retrier.stop = stop_after_attempt(1)
            test_requester.send_request("/time", {}, "get")
