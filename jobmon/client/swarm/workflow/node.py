import hashlib

from http import HTTPStatus as StatusCodes
from typing import Dict

from jobmon.client import shared_requester
from jobmon.client.requester import Requester
from jobmon.client.client_logging import ClientLogging as logging


logger = logging.getLogger(__name__)


class Node(object):
    """

    """

    def __init__(self, task_template_version_id: int,
                 node_args: Dict, requester: Requester = shared_requester):
        """
        Args:
            task_template_version_id:
            node_args:
        """
        self.task_template_version_id = task_template_version_id
        self.node_args = node_args
        self._node_args_hash = None
        self.requester = requester

    @property
    def node_args_hash(self) -> int:
        """a node_arg_hash is a hash of the encoded result of
        concatenating the args and values together"""
        if self._node_args_hash is None:
            arg_ids = list(self.node_args.keys())
            arg_ids.sort()

            arg_values = [str(self.node_args[key]) for key in arg_ids]
            arg_ids = [str(arg) for arg in arg_ids]

            hash_value = int(hashlib.sha1(''.join(arg_ids + arg_values).encode(
                'utf-8')).hexdigest(), 16)
            self._node_args_hash = hash_value
        return self._node_args_hash

    def bind(self) -> int:
        """Retrieve an id for a matching node_arg_hash in the database. If it
        doesn't exist, first create one.

        """
        node_id = self._get_node_id()
        if node_id is None:
            node_id = self._insert_node()
        return node_id

    def _get_node_id(self) -> int:
        return_code, response = self.requester.send_request(
            app_route='/node',
            message={
                'task_template_version_id': str(self.task_template_version_id),
                'node_arg_hash': str(self.node_args_hash)
            },
            request_type='get'
        )
        if return_code == StatusCodes.OK:
            return response['node_id']
        else:
            raise ValueError(f'Unexpected status code {return_code} from GET '
                             f'request through route /node')

    def _insert_node(self) -> int:
        return_code, response = self.requester.send_request(
            app_route='/node',
            message={
                'task_template_version_id': str(self.task_template_version_id),
                'node_arg_hash': str(self.node_args_hash)
            },
            request_type='post'
        )
        if return_code == StatusCodes.OK:
            return response['node_id']
        else:
            raise ValueError(f'Unexpected status code {return_code} from POST '
                             f'request through route /node')
