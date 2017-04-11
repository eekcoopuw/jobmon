import os
import json
from socket import gethostname

import zmq

from jobmon.setup_logger import setup_logger


class PublisherAlreadyRunning(Exception):
    def __init__(self, monfile):
        super(PublisherAlreadyRunning, self).__init__(
            "A publisher already exists. To safely create a new publisher, "
            "terminate the process listed in '{}' and delete the "
            "file".format(monfile))


class Publisher(object):
    """docstring for Publisher"""
    logger = None

    def __init__(self, out_dir):
        """set class defaults. make out_dir if it doesn't exist. write config
        for client nodes to read

        Args:
            out_dir (str): The directory where the connection settings are
                to be stored so Requesters know which endpoint to communicate
                with
        """
        if not Publisher.logger:
            Publisher.logger = setup_logger('central_monitor',
                                            'central_monitor_logging.yaml')
        self.out_dir = os.path.abspath(os.path.expanduser(out_dir))
        self.out_dir = os.path.realpath(self.out_dir)
        try:
            os.makedirs(self.out_dir)
        except OSError:  # It throws if the directory already exists!
            pass
        self.port = None
        self.socket = None

    @property
    def node_name(self):
        """name of node that server is running on"""
        return gethostname()

    def write_connection_info(self, host, port):
        """dump server connection configuration to network filesystem for
        client nodes to read using json.dump

        Args:
            host (string): node name that server is running on
            port (int): port that server is listening at
        """
        monfn = '{}/publisher_info.json'.format(self.out_dir)
        Publisher.logger.debug(
            '{}: Writing connection info to {}'.format(os.getpid(), monfn))
        if os.path.exists(monfn):
            raise PublisherAlreadyRunning(monfn)
        with open(monfn, 'w') as f:
            json.dump({'host': host, 'port': port, 'pid': os.getpid()}, f)

    def _open_socket(self):
        context = zmq.Context()
        self.socket = context.socket(zmq.PUB)  # server blocks on receive
        self.port = self.socket.bind_to_random_port('tcp://*')
        self.write_connection_info(self.node_name, self.port)  # dump config

    def _close_socket(self):
        """stops listening at network socket/port."""
        Publisher.logger.info('Stopping publisher...')
        os.remove('{}/publisher_info.json'.format(self.out_dir))
        self.socket.close()
        Publisher.logger.info('Publisher stopped.')
        return True

    def start_publisher(self):
        Publisher.logger.info('Starting publisher')
        self._open_socket()

    def restart_publisher(self):
        Publisher.logger.info('Restarting publisher')
        self._close_socket()
        self._open_socket()

    def publish_info(self, topic, msg_data):
        self.socket.send(topic, json.dumps(msg_data))
