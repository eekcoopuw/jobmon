import pytest

from jobmon.server.workflow_reaper.notifiers import SlackNotifier


def test_no_raise():
    notifier = SlackNotifier(token='fake_token',
                             default_channel='jobmon-alerts',
                             slack_api_url='https://slack.com/apis/chat.postMessage')
    try:
        notifier.send(msg="This should fail because of fake link")
    except Exception as e:
        print(str(e))
        pytest.fail("There should be no exception")
