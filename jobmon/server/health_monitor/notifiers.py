import requests
from jobmon.server.jobmonLogging import jobmonLogging as logging
from jobmon.models.attributes import constants

logger = logging.getLogger(__name__)


class SlackNotifier(object):

    def __init__(self, token, default_channel):
        """Container for connection with Slack
        Args:
            token (str): token gotten from your app in api.slack.com
            default_channel (str): name of channel to which you want to post
        """
        self._token = token
        self.default_channel = default_channel
        self.slack_api_url = constants.deploy_attribute['SLACK_API_URL']

    def send(self, msg, channel=None):
        logger.debug(logging.myself())
        """Send message to Slack using requests.post"""
        if not channel:
            channel = self.default_channel
        resp = requests.post(
            self.slack_api_url,
            headers={'Authorization': 'Bearer {}'.format(self._token)},
            json={'channel': channel, 'text': msg})
        logger.debug(resp)
        if resp.status_code != requests.codes.OK:
            error = "Could not send Slack message. {}".format(resp.content)
            if "Server Error" in error:  # catch Slack server outage error
                logger.error(error)
            else:
                raise RuntimeError(error)
