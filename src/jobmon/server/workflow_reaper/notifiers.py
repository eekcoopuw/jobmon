"""Places to notify upon certain events (ex. slack to notify of unhealthy workflow)."""
import logging
from typing import Optional

import requests

logger = logging.getLogger(__name__)


class SlackNotifier(object):
    """Send notifications via slack."""

    def __init__(self, slack_api_url: str, token: str, default_channel: str):
        """Container for connection with Slack.
        Args:
            token (str): token gotten from your app in api.slack.com
            default_channel (str): name of channel to which you want to post
        """
        self._token = token
        self.default_channel = default_channel
        self.slack_api_url = slack_api_url

    def send(self, msg: str, channel: Optional[str] = None):
        """Send message to Slack using requests.post."""
        if channel is not None:
            channel = self.default_channel
        resp = requests.post(
            self.slack_api_url,
            headers={'Authorization': 'Bearer {}'.format(self._token)},
            json={'channel': channel, 'text': msg})
        logger.debug(resp)
        if resp.status_code != requests.codes.OK:
            error = "Could not send Slack message. {}".format(resp.content)
            # To raise an exception here causes the docker container stop, and
            # becomes hard to restart.
            # Log the error instead. So we can enter the container to fix
            # issues when necessary.
            # Log the status code so that it's easier to identify the cause.
            logger.error(resp.status_code)
            logger.error(error)
