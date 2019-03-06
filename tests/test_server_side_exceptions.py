import logging

import jobmon.server.server_side_exception as sse

logger = logging.getLogger(__file__)


def raiser(s: str):
    raise ValueError(f"Bad Things {s}")


def test_wrapped():
    try:
        try:
            raiser("dog")
            # should not succeed
            assert False
        except Exception:
            sse.log_and_raise("cat", logger)
    except sse.ServerSideException as e:
        # Should land here, must contain both messages
        assert "dog" in e.msg
        assert "cat" in e.msg
