import requests

from jobmon import config


def validate_slack_token(slack_token: str) -> bool:
    """
    Checks whether a given slack token is valid

    :param slack_token: A Slack Bot User OAuth Access Token
    :return: True if the token validates, False otherwise
    """
    resp = requests.post(
        config.slack_api_url,
        headers={'Authorization': 'Bearer {}'.format(slack_token)})
    if resp.status_code != 200:
        print(f"Response returned a bad status code: {resp.status_code} "
              f"(Expected 200) with content {resp.json()}. "
              f"Retry with token or skip")
        return False
    return resp.json()['error'] != 'invalid_auth'
