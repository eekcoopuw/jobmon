import logging
import os
import socket
from flask import Flask, jsonify

from jobmon.exceptions import ReturnCodes
from jobmon.config import config

try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError


logger = logging.getLogger(__name__)


class ReplyServer(object):
    """This really is a server, in that there is one of these, it listens on a
    Receiver object (a zmq channel) and does stuff as a result of those
    commands.  A singleton in the directory. Runs as a separate process or
    thread.

    Error Codes
    INVALID_RESPONSE_FORMAT
    INVALID_ACTION
    GENERIC_ERROR
    """

    app = Flask(__name__)

    def __init__(self):
        self._is_alive
        port = self.port()
        logger.info("{} is running on {}:{} "
                    .format(__name__, config.host, port))
        self.app.run(host="0.0.0.0", port=port, debug=True, threaded=True)

    @property
    def port(self):
        if self.__class__.__name__ == 'JobStateManager':
            port = config.jsm_port
        elif self.__class__.__name__ == 'JobQueryServer':
            port = config.jqs_port
        return port

    @property
    def node_name(self):
        """name of node that server is running on"""
        return socket.gethostname()

    @app.route('/', method=['GET'])
    def _is_alive(self):
        """A simple 'action' that sends a response to the requester indicating
        that this responder is in fact listening"""
        logmsg = "{}: Responder received is_alive?".format(os.getpid())
        logger.debug(logmsg)
        return jsonify(return_code=ReturnCodes.OK, msg="Yes, I am alive")
