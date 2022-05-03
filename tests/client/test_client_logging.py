import logging
from datetime import date

import pytest


@pytest.mark.skip()
def test_client_logging_default_format(client_env, capsys):
    from jobmon.client.client_logging import ClientLogging

    ClientLogging().attach(logger_name="test.test")
    logger = logging.getLogger("test.test")
    logger.info("This is a test")
    captured = capsys.readouterr()
    logs = captured.out.split("\n")
    # should only contain two lines, one empty, one above log
    for log in logs:
        if log:
            # check format and message
            assert date.today().strftime("%Y-%m-%d") in log
            assert "test.test" in log
            assert "INFO" in log
            assert "This is a test" in log


@pytest.mark.skip()
def test_client_logging_customized_handler(client_env, capsys):
    from jobmon.client.client_logging import ClientLogging

    h = logging.StreamHandler()  # stderr
    # This formatter logs nothing but the fixed msg.
    # Nobody would create a log like this; thus, it proves it's using my logger.
    h.setFormatter("I log this instead of your message")
    h.setLevel(logging.INFO)
    ClientLogging().attach(logger_name="test.test", handler=h)
    logger = logging.getLogger("test.test")
    logger.info("This is a test")
    captured = capsys.readouterr()
    logs = captured.out.split("\n")
    # should only contain two lines, one empty, one formatter text
    for log in logs:
        if log:
            assert "I log this instead of your message" in log
            assert "This is a test" not in log
