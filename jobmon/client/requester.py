import logging
import requests
from tenacity import retry, wait_exponential, retry_if_result, stop_after_delay


logger = logging.getLogger(__name__)


def is_5XX(result):
    '''
    return True if get_content result has 5XX status '''
    status = result[0]
    is_bad = status > 499 and status < 600
    return is_bad


def raise_if_exceed_retry(retry_state):
    '''
    if we trigger retry error, raise informative RuntimeError
    '''
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

    def __init__(self, url):
        """set class defaults. attempt to connect with server."""
        self.url = url

    @retry(
        wait=wait_exponential(max=10),
        stop=stop_after_delay(120),
        retry=retry_if_result(is_5XX),
        retry_error_callback=raise_if_exceed_retry)
    def send_request(self, app_route, message, request_type, verbose=True):
        """
        Send request to server.

        If we get a 5XX status code, we will retry for up to 2 minutes using
        exponential backoff.

        Args:
            app_route (str): The specific end point with which you want to
            interact. The app_route must always start with a slash ('/') and
            must match one of the function decorations of @jsm.route or
            @jqs.route on the server side.

            message (dict): The message dict to be sent to the server.
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

            request_type (str): The type of request desired, either 'get',
            'post, or 'put'

            verbose (bool, optional): Whether to print the servers reply to
                stdout as well as return it. Defaults to False.

        Returns:
            Server reply message

        Raises:
            RuntimeError if 500 errors occur for > 2 minutes
        """
        route = self.build_full_url(app_route)
        if request_type not in ['get', 'post', 'put']:
            raise ValueError("request_type must be one of 'get', 'post', or "
                             "'put'. Got {}".format(request_type))
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
        return status_code, content

    def build_full_url(self, app_route):
        return self.url + app_route


def get_content(response):
    if 'application/json' in response.headers.get('Content-Type'):
        try:
            content = response.json()
        except TypeError:  # for test_client, response.json is a dict not fn
            content = response.json
    else:
        content = response.content
    return response.status_code, content
