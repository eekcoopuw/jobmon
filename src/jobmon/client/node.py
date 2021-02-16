from __future__ import annotations

import hashlib
import json
from http import HTTPStatus as StatusCodes
from typing import Dict, List, Optional, Set

from jobmon.client.client_config import ClientConfig
from jobmon.requester import Requester

import structlog as logging


logger = logging.getLogger(__name__)


class Node:

    def __init__(self, task_template_version_id: int, node_args: Dict,
                 requester: Optional[Requester] = None):
        """A node represents an individual task within a Dag.

        This includes its relationship to other nodes that it is dependent upon or nodes that
        depend upon it. A node stores node arguments (arguments relating to the
        actual shape of the dag and number of tasks created for a given
        stage/task template) and can register itself with the database via the
        Jobmon Query Service and the Jobmon State Manager.

        Args:
            task_template_version_id: The associated task_template_version_id.
            node_args: key-value pairs of arg_id and a value.
            requester_url (str): url to communicate with the flask services.
        """
        self.task_template_version_id = task_template_version_id
        self.node_args = node_args
        self.node_args_hash = self._hash_node()
        self.upstream_nodes: Set[Node] = set()
        self.downstream_nodes: Set[Node] = set()

        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester

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

    def _hash_node(self) -> int:
        """a node_hash is a hash of the encoded result of the args and values concatenated
        together and concatenated with the task_template_version_id"""
        arg_ids = list(self.node_args.keys())
        arg_ids.sort()

        arg_values = [str(self.node_args[key]) for key in arg_ids]
        arg_ids = [str(arg) for arg in arg_ids]

        hash_value = int(hashlib.sha1(''.join(arg_ids + arg_values +
                                              [str(self.task_template_version_id)]).encode(
            'utf-8')).hexdigest(), 16)
        return hash_value

    def _get_node_id(self) -> int:
        logger.info(f'Querying for node {self}')
        return_code, response = self.requester.send_request(
            app_route='/client/node',
            message={
                'task_template_version_id': self.task_template_version_id,
                'node_args_hash': self.node_args_hash
            },
            request_type='get',
            logger=logger
        )
        if return_code == StatusCodes.OK:
            return response['node_id']
        else:
            raise ValueError(f'Unexpected status code {return_code} from GET '
                             f'request through route /client/node. Expected code 200.'
                             f' Response content:'
                             f' {response}')

    def _insert_node_and_node_args(self) -> int:
        logger.info(f'Insert node: {self}')
        return_code, response = self.requester.send_request(
            app_route='/client/node',
            message={
                'task_template_version_id': self.task_template_version_id,
                'node_args_hash': self.node_args_hash,
                'node_args': json.dumps(self.node_args)
            },
            request_type='post',
            logger=logger
        )
        if return_code == StatusCodes.OK:
            return response['node_id']
        else:
            raise ValueError(f'Unexpected status code {return_code} from POST '
                             f'request through route /client/node. Expected code 200.'
                             f' Response content: {response}')

    def add_upstream_node(self, upstream_node: 'Node') -> None:
        """Add a single node to this one's upstream Nodes.

        Args:
            upstream_node: node to add a dependency on
        """
        self.upstream_nodes.add(upstream_node)
        # Add this node to the upstream nodes' downstream
        upstream_node.downstream_nodes.add(self)

    def add_upstream_nodes(self, upstream_nodes: List['Node']) -> None:
        """Add many nodes to this one's upstream Nodes.

        Args:
            upstream_nodes: list of nodes to add dependencies on
        """
        for node in upstream_nodes:
            self.add_upstream_node(node)

    def add_downstream_node(self, downstream_node: 'Node') -> None:
        """Add a node to this one's downstream Nodes.

        Args:
            downstream_node: Node that will be dependent on this node
        """
        self.downstream_nodes.add(downstream_node)
        # avoid endless recursion, set directly
        downstream_node.upstream_nodes.add(self)

    def add_downstream_nodes(self, downstream_nodes: List['Node']) -> None:
        """Add a list of nodes as this node's downstream nodes

        Args:
            downstream_nodes: Nodes that will be dependent on this node.

        """

        for node in downstream_nodes:
            self.add_downstream_node(node)

    def __str__(self) -> str:
        return (f'task_template_version_id: {self.task_template_version_id}, '
                f'node_args: {self.node_args}, '
                f'node_args_hash: {self.node_args_hash}')

    def __eq__(self, other: 'Node') -> bool:
        return hash(self) == hash(other)

    def __lt__(self, other: 'Node') -> bool:
        return hash(self) < hash(other)

    def __hash__(self) -> int:
        hash_value = hashlib.sha1()
        hash_value.update(bytes(str(self.node_args_hash).encode('utf-8')))
        hash_value.update(bytes(str(self.task_template_version_id).encode('utf-8')))
        return int(hash_value.hexdigest(), 16)
