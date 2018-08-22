import logging
import requests


logger = logging.getLogger(__name__)


class Requester(object):
    """Sends an HTTP messages via the Requests library to one of the running
    services, either the JQS or the JSM, and returns the response from the
    server. A common use case is where the swarm of application jobs send
    status messages via a Requester
    to the JobStateManager or requests job status from the JobQueryServer

    """

    def __init__(self, config, service):
        """set class defaults. attempt to connect with server."""
        if service == 'jsm':
            port = config.jsm_port
        elif service == 'jqs':
            port = config.jqs_port
        else:
            raise ValueError("Service can only be jqs or jsm. Got {}"
                             .format(service))
        self.url = "http://" + config.host + ":{}".format(port)

    def send_request(self, app_route, message, request_type, verbose=True):
        """Send request to server.

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
        content = get_content(r)
        if content:
            if verbose is True:
                logger.debug(content)
        return r.status_code, content

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
    return content
