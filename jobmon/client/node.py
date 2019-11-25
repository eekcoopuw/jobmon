from __future__ import annotations

import hashlib
import json
from http import HTTPStatus as StatusCodes
from typing import Dict, List, Set, TYPE_CHECKING

from jobmon.client import shared_requester
from jobmon.client._logging import ClientLogging as logging
from jobmon.client.requests.requester import Requester

if TYPE_CHECKING:
    from jobmon.client.internals.task import Task


logger = logging.getLogger(__name__)


class Node:

    def __init__(self,
                 task_template_version_id: int,
                 node_args: Dict,
                 task: Task,
                 requester: Requester = shared_requester):
        """A node represents an individual node for a Dag.
        A node stores node arguments and can register itself with the database
        via the Jobmon Query Service and the Jobmon State Manager.

        Args:
            task_template_version_id: The associated task_template_version_id.
            node_args: key-value pairs of arg_id and a value.
            upstream_nodes: list of nodes that this node is dependent on.
        """
        self.task_template_version_id = task_template_version_id
        self.node_args = node_args
        self.node_args_hash = self._hash_node_args()
        self.requester = requester
        self.upstream_nodes: Set[Node] = set()
        self.downstream_nodes: Set[Node] = set()

    @property
    def node_id(self) -> int:
        if not hasattr(self, "_node_id"):
            raise AttributeError(
                "node_id cannot be accessed before node is bound")
        return self._node_id

    def bind(self) -> int:
        """Retrieve an id for a matching node from the server. If it doesn't
        exist, first create one."""
        node_id = self._get_node_id()
        if node_id is None:
            logger.info(f'node_id for node: {self} not found, creating a new'
                        f'entry and binding node.')
            node_id = self._insert_node_and_node_args()
        else:
            logger.info(f'Found node_id: {node_id} for node: {self}, binding '
                        f'node.')
        self._node_id = node_id
        return self.node_id

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
                             f'request through route /node. Expected code 200.'
                             f' Response content:'
                             f' {response}')

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
                             f'request through route /node. Expected code 200.'
                             f' Response content: {response}')

    def add_upstream_node(self, upstream_node: Node) -> None:
        """Add a node to this one's upstream Nodes."""
        self.upstream_nodes.add(upstream_node)
        # Add this node to the upstream nodes' downstream
        upstream_node.downstream_nodes.add(self)

    def add_upstream_nodes(self, upstream_nodes: List[Node]) -> None:
        """Add many nodes to this one's upstream Nodes."""
        for node in upstream_nodes:
            self.add_upstream_node(node)

    def add_downstream_node(self, downstream_node: Node) -> None:
        """Add a node to this one's downstream Nodes."""
        self.downstream_nodes.add(downstream_node)
        # avoid endless recursion, set directly
        downstream_node.downstream_nodes.add(self)

    def add_downstream_nodes(self, downstream_nodes: List[Node]) -> None:
        for node in downstream_nodes:
            self.add_downstream_node(node)

    def __str__(self) -> str:
        return (f'task_template_version_id: {self.task_template_version_id}, '
                f'node_args: {self.node_args}, '
                f'node_args_hash: {self.node_args_hash}')

    def __eq__(self, other: Node) -> bool:
        return hash(self) == hash(other)

    def __lt__(self, other: Node) -> bool:
        return hash(self) < hash(other)

    def __hash__(self) -> int:
        return self.node_args_hash
