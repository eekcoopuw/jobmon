import pytest
from threading import Thread

from jobmon.connection_config import ConnectionConfig
from jobmon.exceptions import ReturnCodes, NoResponseReceived
from jobmon.reply_server import ReplyServer
from jobmon.requester import Requester


RS_PORT = 5559
RS_PORT_TO_STOP = 5560


def resp_not_tuple():
    return "not_a_tuple"


def resp_no_rc():
    return ("not_an_int_rc", 12)


def act_sums_args(x, y):
    return (0, x + y)


@pytest.fixture(scope='module')
def reply_server():
    rs = ReplyServer(RS_PORT)
    rs.register_action("resp_not_tuple", resp_not_tuple)
    rs.register_action("resp_no_rc", resp_no_rc)
    rs.register_action("act_sums_args", act_sums_args)
    thr = Thread(target=rs.listen)
    thr.daemon = True
    thr.start()
    yield
    rs.stop_listening()


@pytest.fixture(scope='module')
def reply_server_to_stop():
    rs = ReplyServer(RS_PORT_TO_STOP)
    thr = Thread(target=rs.listen)
    thr.daemon = True
    thr.start()
    yield


@pytest.fixture(scope='module')
def requester():
    cc = ConnectionConfig('localhost', RS_PORT, request_timeout=500)
    req = Requester(cc)
    yield req
    req.disconnect()


@pytest.fixture(scope='module')
def requester_sends_stop():
    cc = ConnectionConfig('localhost', RS_PORT_TO_STOP, request_timeout=500)
    req = Requester(cc)
    yield req
    req.disconnect()


def test_valid_request(reply_server, requester):
    rc, msg = requester.send_request({'action': 'alive'})
    assert rc == 0
    assert msg == "Yes, I am alive"


def test_valid_request_with_args(reply_server, requester):
    rc, msg = requester.send_request({'action': 'act_sums_args',
                                      'args': [1, 2]})
    assert rc == 0
    assert msg == 3


def test_invalid_action(reply_server, requester):
    rc, _ = requester.send_request({'action': 'this_is_not_an_action'})
    assert rc == ReturnCodes.INVALID_ACTION


def test_invalid_action_args(reply_server, requester):
    rc, msg = requester.send_request({
        'action': 'alive', 'kwargs': {'nota': 'llowed'}
    })
    assert rc == ReturnCodes.GENERIC_ERROR
    assert "unexpected keyword" in msg


def test_invalid_request(reply_server, requester):
    rc, msg = requester.send_request(['not a dictionary'])
    assert rc == ReturnCodes.INVALID_REQUEST_FORMAT
    rc, msg = requester.send_request({'args': 'no_action', 'kwargs':
                                      'no_action'})
    assert rc == ReturnCodes.INVALID_REQUEST_FORMAT
    rc, msg = requester.send_request({'action': 'some_action',
                                      'args': 'no_action',
                                      'kwargs': 'no_action',
                                      'unrecognized_key': 1234})
    assert rc == ReturnCodes.INVALID_REQUEST_FORMAT


def test_invalid_responses(reply_server, requester):
    rc, _ = requester.send_request({'action': 'resp_not_tuple'})
    assert rc == ReturnCodes.INVALID_RESPONSE_FORMAT
    rc, _ = requester.send_request({'action': 'resp_no_rc'})
    assert rc == ReturnCodes.INVALID_RESPONSE_FORMAT


def test_stop(reply_server_to_stop, requester_sends_stop):
    rc, msg = requester_sends_stop.send_request("stop")
    assert rc == 0
    assert msg == "ReplyServer stopping"


def test_request_timeout():
    cc = ConnectionConfig('localhost', 1234567, request_timeout=100)
    req = Requester(cc)
    with pytest.raises(NoResponseReceived):
        req.send_request({'action': 'alive'}, verbose=True)
    req.disconnect()
