import hashlib
import json

from http import HTTPStatus as StatusCodes
from typing import Dict, List, Optional

from jobmon.client import shared_requester
from jobmon.client.requester import Requester
from jobmon.client.client_logging import ClientLogging as Logging


logger = Logging.getLogger(__name__)


class Node(object):

    def __init__(self, task_template_version_id: int,
                 node_args: Dict,
                 requester: Optional[Requester] = shared_requester,
                 upstream_nodes: Optional[List["Node"]] = None):
        """A node represents an individual node for a Dag.
        A node stores node arguments and can register itself with the database
        via the Jobmon Query Service and the Jobmon State Manager.

        Args:
            task_template_version_id: The associated task_template_version_id.
            node_args: key-value pairs of arg_id and a value.
            upstream_nodes: list of nodes that this node is dependent on.
        """
        self.node_id = None  # node_id of None implies that it is unbound
        self.task_template_version_id = task_template_version_id
        self.node_args = node_args
        self.node_args_hash = self._hash_node_args()
        self.requester = requester
        self.upstream_nodes = set()
        self.downstream_nodes = set()

        if upstream_nodes is not None:
            [self.upstream_nodes.add(node) for node in upstream_nodes]
            # Add this node to the upstream nodes' downstream
            [node.downstream_nodes.add(self) for node in upstream_nodes]

    def bind(self) -> int:
        """Retrieve an id for a matching node in the database. If it doesn't
        exist, first create one."""
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
                'node_args': json.dumps(self.node_args)
            },
            request_type='post'
        )
        if return_code == StatusCodes.OK:
            return response['node_id']
        else:
            raise ValueError(f'Unexpected status code {return_code} from POST '
                             f'request through route /node')

    def add_upstream_node(self, upstream_node: "Node"):
        """Add a node to this one's upstream Nodes."""
        self.upstream_nodes.add(upstream_node)
        # Add this node to the upstream nodes' downstream
        upstream_node.downstream_nodes.add(self)

    def add_upstream_nodes(self, upstream_nodes: List["Node"]):
        """Add many nodes to this one's upstream Nodes."""
        [self.add_upstream_node(node) for node in upstream_nodes]

    def __str__(self) -> str:
        return (f'node_id: {self.node_id}, '
                f'task_template_version_id: {self.task_template_version_id}, '
                f'node_args: {self.node_args}, '
                f'node_args_hash: {self.node_args_hash}')

    def __eq__(self, other: "Node") -> bool:
        return hash(self) == hash(other)

    def __lt__(self, other: "Node") -> bool:
        return hash(self) < hash(other)
