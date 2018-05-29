import requests


class SlackNotifier(object):

    def __init__(self, token, default_channel):

        self._token = token
        self.default_channel = default_channel

    def send(self, msg, channel=None):
        if not channel:
            channel = self.default_channel
        resp = requests.post(
            'https://slack.com/api/chat.postMessage',
            headers={'Authorization': 'Bearer {}'.format(self._token)},
            json={'channel': channel, 'text': msg}
        )
        if resp.status_code != requests.codes.OK:
            raise RuntimeError(
                "Could not send Slack message. {}".format(resp.content))
