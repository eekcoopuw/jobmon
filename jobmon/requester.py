import logging
import requests

from jobmon.config import config


logger = logging.getLogger(__name__)


class Requester(object):
    """Sends messages to a Responder node through zmq. sends messages to a
    Responder via request dictionaries which the Responder node consumes and
    responds to. A common use case is where the swarm of application jobs send
    status messages to a Responder in the CentralJobStateMonitor

    Args
        connection_config (ConnectionConfig): host and port info for a remote
            jobmon instance
    """

    def __init__(self, port):
        """set class defaults. attempt to connect with server."""

        self.url = config.host + ":{}".format(port)

    def send_request(self, app_route, message, request_type, verbose=False):
        """send request to server. Need to document what form this message
        takes.

        Args:
            message (dict): The message dict must at minimum have an 'action'
                keyword. For example, a valid message might be:

                    {'action': 'write_msg_todb',
                     'msg': 'some message to be written to the db'}

                Valid actions and further key-value pairs are to be defined
                in sub-class Requester=Responder pairs.

            verbose (bool, optional): Whether to print the servers reply to
                stdout as well as return it. Defaults to False.

        Returns:
            Server reply message
        """
        route = self.build_full_url(app_route)
        if request_type not in ['get', 'post']:
            raise ValueError("request_type must be one of 'get' or 'post'. "
                             "Got {}".format(request_type))
        if request_type == 'post':
            reply = requests.post(route, data=message)
        else:
            reply = requests.get(route, params=message)
        if verbose is True:
            logger.debug(reply)
        return reply

    def build_full_url(self, app_route):
        return self.url + app_route
