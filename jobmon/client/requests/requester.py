import requests
from typing import Optional, Tuple, Any

from tenacity import (retry, wait_exponential, retry_if_result,
                      stop_after_delay, RetryCallState)

from jobmon.client import ClientLogging as logging


logger = logging.getLogger(__name__)


def is_5XX(result: Tuple[int, dict]) -> bool:
    '''
    return True if get_content result has 5XX status '''
    logger.info("is_5XX")
    status = result[0]
    logger.info("status: {}".format(status))
    is_bad = status > 499 and status < 600
    logger.debug("is_bad: {}".format(is_bad))
    return is_bad


def raise_if_exceed_retry(retry_state: RetryCallState):
    '''
    if we trigger retry error, raise informative RuntimeError
    '''
    logger.info("raise_if_exceed_retry")
    status, content = retry_state.outcome.result()
    raise RuntimeError(
        f'Exceeded HTTP request retry budget. '
        f'Status code was {status} and content was {content}')


class Requester(object):
    """Sends an HTTP messages via the Requests library to one of the running
    services, either the JQS or the JSM, and returns the response from the
    server. A common use case is where the swarm of application jobs send
    status messages via a Requester
    to the JobStateManager or requests job status from the JobQueryServer

    """

    def __init__(self, url: str):
        """set class defaults. attempt to connect with server."""
        logger.info("Requester __init__")
        logger.info("url: {}".format(url))
        self.url = url

    @retry(
        wait=wait_exponential(max=10),
        stop=stop_after_delay(120),
        retry=retry_if_result(is_5XX),
        retry_error_callback=raise_if_exceed_retry)
    def send_request(self, app_route: str, message: dict, request_type: str,
                     verbose: Optional[bool] = True) -> Tuple[int, Any]:
        """
        Send request to server.

        If we get a 5XX status code, we will retry for up to 2 minutes using
        exponential backoff.

        Args:
            app_route: The specific end point with which you want to
            interact. The app_route must always start with a slash ('/') and
            must match one of the function decorations of @jsm.route or
            @jqs.route on the server side.

            message: The message dict to be sent to the server.
            Must contain any arguments the JSM/JQS route needs to operate.
            If the request is a 'GET', the value of the message dict will
            likely be parsed into the url. If the request is a 'POST' or 'PUT',
            the message dict will get stored in a dictionary that is parsed on
            the server side and passed into the work done by that route.
                For example, a valid message for a request to add a task_dag
                to the JSM might be:

                    {'name': 'my_name',
                     'user': 'my_user',
                     'dag_hash': 'my_dag_hash'}

            request_type: The type of request desired, either 'get',
            'post, or 'put'

            verbose: Whether to print the servers reply to
                stdout as well as return it. Defaults to False.

        Returns:
            Server reply message

        Raises:
            RuntimeError if 500 errors occur for > 2 minutes
        """
        route = self.build_full_url(app_route)
        logger.info("send_request route: {}".format(route))
        if request_type not in ['get', 'post', 'put']:
            logger.error("Invalid request_type: {}".format(request_type))
            raise ValueError("request_type must be one of 'get', 'post', or "
                             "'put'. Got {}".format(request_type))
        logger.debug("Request message: {}".format(message))
        if request_type == 'post':
            r = requests.post(route, json=message,
                              headers={'Content-Type': 'application/json'})
        elif request_type == 'get':
            r = requests.get(route, params=message,
                             headers={'Content-Type': 'application/json'})
        else:
            r = requests.put(route, json=message,
                             headers={'Content-Type': 'application/json'})
        status_code, content = get_content(r)
        if content:
            if verbose is True:
                logger.debug(f"Received: {content}")
        logger.debug("Response content: {}".format(content))
        return status_code, content

    def build_full_url(self, app_route: str) -> str:
        logger.info(self.url + app_route)
        return self.url + app_route


def get_content(response: requests.Response) -> Tuple[int, dict]:
    if 'application/json' in response.headers.get('Content-Type'):
        try:
            content = response.json()
        except TypeError:  # for test_client, response.json is a dict not fn
            content = response.json
    else:
        content = response.content
    logger.debug(f"response status:{response.status_code}; content:{content}")
    return response.status_code, content
