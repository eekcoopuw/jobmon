import hashlib

from http import HTTPStatus as StatusCodes
from typing import Optional, Set

from jobmon.client import shared_requester
from jobmon.client._logging import ClientLogging as logging
from jobmon.client.node import Node
from jobmon.client.requests.requester import Requester

logger = logging.getLogger(__name__)


class Dag(object):
    """Stores Nodes, talks to the server in regard to itself"""

    def __init__(self, requester: Requester = shared_requester):
        self.nodes: Set[Node] = set()
        self.requester = requester

    @property
    def dag_id(self) -> int:
        if not hasattr(self, "_dag_id"):
            raise AttributeError(
                "_dag_id cannot be accessed before dag is bound")
        return self._dag_id

    def add_node(self, node: Node) -> None:
        """Add a node to this dag."""
        # wf.add_task should call ClientNode.add_node() + pass the tasks' node
        self.nodes.add(node)

    def bind(self) -> int:
        """Retrieve an id for a matching dag from the server. If it doesn't
        exist, first create one, including its edges."""

        if len(self.nodes) == 0:
            raise RuntimeError('No nodes were found in the dag. An empty dag '
                               'cannot be bound.')

        dag_id = self._get_dag_id()
        dag_hash = hash(self)
        if dag_id is None:
            logger.info(f'dag_id for dag with hash: {dag_hash} not found, '
                        f'creating a new entry and binding the dag.')
            self._dag_id = self._insert_dag()
            logger.info(f'Inserting edges for dag with hash: {dag_hash}.')
            # insert edges
            self._insert_edges()
        else:
            self._dag_id = dag_id
            logger.info(f'Found dag_id: {self.dag_id} for dag with hash: '
                        f'{dag_hash}')
        logger.debug(f'dag_id is: {self.dag_id}')
        return self.dag_id

    def _get_dag_id(self) -> Optional[int]:
        dag_hash = hash(self)
        logger.info(f'Querying for dag with hash: {dag_hash}')
        return_code, response = self.requester.send_request(
            app_route=f'/dag',
            message={"dag_hash": dag_hash},
            request_type='get'
        )
        if return_code == StatusCodes.OK:
            return response['dag_id']
        else:
            raise ValueError(f'Unexpected status code {return_code} from GET '
                             f'request through route /dag/{dag_hash} . '
                             f'Expected code 200. Response content: '
                             f'{response}')

    def _insert_dag(self) -> int:
        dag_hash = hash(self)

        return_code, response = self.requester.send_request(
            app_route='/dag',
            message={"dag_hash": dag_hash},
            request_type='post'
        )
        if return_code == StatusCodes.OK:
            return response['dag_id']
        else:
            raise ValueError(f'Unexpected status code {return_code} from POST '
                             f'request through route /dag/{dag_hash} . '
                             f'Expected code 200. Response content: '
                             f'{response}')

    def _insert_edges(self) -> None:
        logger.info(f'Inserting edges into dag with id {self.dag_id}')

        # convert the set into a dictionary that can be dumped and sent over
        # as json
        nodes_and_edges = {}

        for node in self.nodes:
            # get the node ids for all upstream and downstream nodes
            upstream_nodes = [upstream_node.node_id
                              for upstream_node in node.upstream_nodes]
            downstream_nodes = [downstream_node.node_id
                                for downstream_node in node.downstream_nodes]

            nodes_and_edges[node.node_id] = {
                'upstream_nodes': upstream_nodes,
                'downstream_nodes': downstream_nodes
            }

        logger.debug(f'message included in edge post request: '
                     f'{nodes_and_edges}')
        return_code, response = self.requester.send_request(
            app_route=f'/edge/{self.dag_id}',
            message=nodes_and_edges,
            request_type='post'
        )
        if return_code != StatusCodes.OK:
            raise ValueError(f'Unexpected status code {return_code} from POST '
                             f'request through route /dag/{self.dag_id}'
                             f' . Expected code 200. Response content: '
                             f'{response}')

    def __hash__(self) -> int:
        """Determined by hashing all sorted node hashes and their downstream"""
        hash_value = hashlib.sha1()
        if len(self.nodes) > 0:  # if the dag is empty, we want to skip this
            for node in sorted(self.nodes):
                hash_value.update(str(hash(node)).encode('utf-8'))
                for downstream_node in sorted(node.downstream_nodes):
                    hash_value.update(
                        str(hash(downstream_node)).encode('utf-8'))
        return int(hash_value.hexdigest(), 16)
