import hashlib

from http import HTTPStatus as StatusCodes
from typing import Dict

from jobmon.client import shared_requester
from jobmon.client.requester import Requester
from jobmon.client.client_logging import ClientLogging as logging


logger = logging.getLogger(__name__)


class Node(object):

    def __init__(self, task_template_version_id: int,
                 node_args: Dict, requester: Requester = shared_requester):
        """
        Args:
            task_template_version_id: The associated task_template_version_id.
            node_args: key-value pairs of arg_id and a value
        """
        self.node_id = None  # node_id of None implies that it is unbound
        self.task_template_version_id = task_template_version_id
        self.node_args = node_args
        self.node_args_hash = self._hash_node_args()
        self.requester = requester

    def bind(self) -> int:
        """Retrieve an id for a matching node_arg_hash in the database. If it
        doesn't exist, first create one."""
        node_id = self._get_node_id()
        if node_id is None:
            logger.info(f'node_id for node: {self} not found, creating a new'
                        f'entry in the database and binding node.')
            node_id = self._insert_node_and_node_args()
        else:
            logger.info(f'Found node_id: {node_id} for node: {self}, binding '
                        f'node.')

        self.node_id = node_id
        return node_id

    def _hash_node_args(self) -> int:
        """a node_arg_hash is a hash of the encoded result of the args and
        values concatenated together"""
        arg_ids = list(self.node_args.keys())
        arg_ids.sort()

        arg_values = [str(self.node_args[key]) for key in arg_ids]
        arg_ids = [str(arg) for arg in arg_ids]

        hash_value = int(hashlib.sha1(''.join(arg_ids + arg_values).encode(
            'utf-8')).hexdigest(), 16)
        return hash_value

    def _get_node_id(self) -> int:
        logger.info(f'Querying for node {self}')
        return_code, response = self.requester.send_request(
            app_route='/node',
            message={
                'task_template_version_id': self.task_template_version_id,
                'node_args_hash': self.node_args_hash
            },
            request_type='get'
        )
        if return_code == StatusCodes.OK:
            return response['node_id']
        else:
            raise ValueError(f'Unexpected status code {return_code} from GET '
                             f'request through route /node')

    def _insert_node_and_node_args(self) -> int:
        logger.info(f'Insert node: {self}')
        return_code, response = self.requester.send_request(
            app_route='/node',
            message={
                'task_template_version_id': self.task_template_version_id,
                'node_args_hash': self.node_args_hash,
                'node_args': self.node_args
            },
            request_type='post'
        )
        if return_code == StatusCodes.OK:
            return response['node_id']
        else:
            raise ValueError(f'Unexpected status code {return_code} from POST '
                             f'request through route /node')

    def __str__(self) -> str:
        return (f'node_id: {self.node_id}, '
                f'task_template_version_id: {self.task_template_version_id}, '
                f'node_args: {self.node_args}, '
                f'node_args_hash: {self.node_args_hash}')
