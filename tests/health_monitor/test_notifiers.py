import pytest

from jobmon.server.health_monitor.notifiers import SlackNotifier


def test_no_raise():
    notifier = SlackNotifier(token='fake_token',
                             default_channel='jobmon-alerts')
    notifier.slack_api_url = 'https://slack.com/apis/chat.postMessage'
    try:
        notifier.send(msg="This should fail because of fake link")
    except Exception as e:
        print(str(e))
        pytest.fail("There should be no exception")
