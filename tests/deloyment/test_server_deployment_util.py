import getpass
import os
import os.path as path
import pytest

from jobmon.server.deployment.util import validate_slack_token, find_release


def test_validate_slack_token():
    r = validate_slack_token("12345")
    assert r is False
    r = validate_slack_token("xoxb-349025779811-pPm1nr1BMyd28dciIc0FILCW")
    assert r is True


def test_find_release():
    tag = ["somethingdoesnotexist"]
    assert find_release(tag) is None
    tag = ["release-0.9.9"]
    assert find_release(tag) == "0.9.9"


