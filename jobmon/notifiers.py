import requests
import logging


logger = logging.getLogger(__name__)


class SlackNotifier(object):

    def __init__(self, token, default_channel):

        self._token = token
        self.default_channel = default_channel
        self.slack_api_url = 'https://slack.com/api/chat.postMessage'

    def send(self, msg, channel=None):
        if not channel:
            channel = self.default_channel
        try:
            resp = requests.post(
                self.slack_api_url,
                headers={'Authorization': 'Bearer {}'.format(self._token)},
                json={'channel': channel, 'text': msg})
            if resp.status_code != requests.codes.OK:
                raise RuntimeError(
                    "Could not send Slack message. {}".format(resp.content))
        except RuntimeError as e:
            if "Server Error" in str(e):  # catch Slack server outage error
                logger.error(str(e))
            else:
                raise
