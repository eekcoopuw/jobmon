import pytest

from jobmon.server.services.health_monitor.notifiers import SlackNotifier


def test_raises_correctly():
    notifier = SlackNotifier(token='fake_token',
                             default_channel='jobmon-alerts')
    notifier.slack_api_url = 'https://slack.com/apis/chat.postMessage'
    with pytest.raises(RuntimeError):
        notifier.send(msg="This should fail because of fake link")
