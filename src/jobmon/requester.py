import json
import logging
import requests
from typing import Tuple, Any, Dict

import tenacity

from jobmon import log_config


default_logger = log_config.configure_logger(__name__)


def http_request_ok(status_code: int) -> bool:
    return status_code in (200, 302, 307)


class Requester(object):
    """Sends an HTTP messages via the Requests library to one of the running
    services, either the JQS or the JSM, and returns the response from the
    server. A common use case is where the swarm of application jobs send
    status messages via a Requester
    to the JobStateManager or requests job status from the JobQueryServer
    """

    def __init__(self, url: str, max_retries: int = 10, stop_after_delay: int = 120):
        self.url = url
        self.max_retries = max_retries
        self.stop_after_delay = stop_after_delay
        self.server_structlog_context: Dict[str, str] = {}

    def add_server_structlog_context(self, **kwargs):
        for key, value in kwargs.items():
            self.server_structlog_context[key] = value

    def send_request(self, app_route: str, message: dict, request_type: str,
                     logger: logging.Logger = default_logger,
                     tenacious: bool = True) -> Tuple[int, Any]:
        """
        Send request to server.

        If we get a 5XX status code, we will retry for up to 2 minutes using
        exponential backoff.

        Args:
            app_route:
                The specific end point with which you want to
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

            request_type: The type of request desired, either 'get', 'post, or 'put'

            logger: which logging context to use for message and response logging

            tenacious: use tenacity for retries


        Returns:
            Server reply message

        Raises:
            RuntimeError if 500 errors occur for > 2 minutes

        """
        if tenacious:
            res = self._tenacious_send_request(app_route, message, request_type, logger)
        else:
            res = self._send_request(app_route, message, request_type, logger)
        return res

    def _tenacious_send_request(self, app_route: str, message: dict, request_type: str,
                                logger: logging.Logger = default_logger) -> Tuple[int, Any]:

        def is_5XX(result: Tuple[int, dict]) -> bool:
            '''return True if get_content result has 5XX status '''
            status = result[0]
            is_bad = status > 499 and status < 600
            if is_bad:
                logger.warning(f"is_5XX? status: {status}")
            else:
                logger.debug(f"is_5XX? status: {status}")
            return is_bad

        def raise_if_exceed_retry(retry_state: tenacity.RetryCallState):
            '''if we trigger retry error, raise informative RuntimeError'''
            logger.exception(f"Retry exceeded. {retry_state}")
            status, content = retry_state.outcome.result()
            raise RuntimeError(f'Exceeded HTTP request retry budget. Status code was {status} '
                               f'and content was {content}')

        # so we can access it in tests
        self._retry = tenacity.Retrying(
            stop=tenacity.stop_after_delay(self.stop_after_delay),
            wait=tenacity.wait_exponential(self.max_retries),
            retry=tenacity.retry_if_result(is_5XX),
            retry_error_callback=raise_if_exceed_retry
        )

        return self._retry.__call__(self._send_request, app_route, message, request_type,
                                    logger)

    def _send_request(self, app_route: str, message: dict, request_type: str,
                      logger: logging.Logger = default_logger) -> Tuple[int, Any]:
        # construct url
        route = self.url + app_route
        logger.debug(f"Route: {route}, message: {message}")

        if request_type in ['post', 'put']:
            message["server_structlog_context"] = self.server_structlog_context
        else:
            {}

        # send request to server
        if request_type == 'post':
            response = requests.post(route, json=message,
                                     headers={'Content-Type': 'application/json'})
        elif request_type == 'get':
            response = requests.get(route, params=message,
                                    data=json.dumps(self.server_structlog_context),
                                    headers={'Content-Type': 'application/json'})
        elif request_type == 'put':
            response = requests.put(route, json=message,
                                    headers={'Content-Type': 'application/json'})
        else:
            raise ValueError(
                f"request_type must be one of 'get', 'post', or 'put'. Got {request_type}"
            )

        status_code, content = get_content(response)
        logger.debug(f"Route: {route}; status: {status_code}; content: {content}")
        return status_code, content


def get_content(response) -> Tuple[int, Any]:
    # parse reponse
    if 'application/json' in response.headers.get('Content-Type', ''):
        try:
            content = response.json()
        except TypeError:  # for test_client, response.json is a dict not fn
            content = response.json
    else:
        content = response.content
    return response.status_code, content
