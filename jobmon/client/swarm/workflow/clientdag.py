import hashlib
import json

from http import HTTPStatus as StatusCodes
from typing import Optional

from jobmon.client.swarm.workflow.clientnode import ClientNode
from jobmon.client import shared_requester
from jobmon.client.requester import Requester
from jobmon.client.client_logging import ClientLogging as Logging

# what do we do with edges? What is in charge of them?
#   node gets edge info from tasks,
#     dag inserts edges from nodes into edge table

logger = Logging.getLogger(__name__)


class ClientDag(object):
    """Stores Nodes, talks to the server in regard to itself"""

    def __init__(self, requester: Optional[Requester] = shared_requester):
        self.dag_id = None  # None implies that it is unbound
        self.nodes = set()
        self.requester = requester

    def add_node(self, node: ClientNode):
        """Add a node to this dag."""
        # wf.add_task should call Node.add_node() and pass the tasks' node
        self.nodes.add(node)

    def bind(self):
        """Retrieve an id for a matching dag from the server. If it doesn't
        exist, first create one, including its edges."""
        self.validate()

        if len(self.nodes) == 0:
            raise RuntimeError('No nodes were found in the dag. An empty dag '
                               'cannot be bound.')

        dag_id = self._get_dag_id()
        dag_hash = hash(self)
        if dag_id is None:
            logger.info(f'dag_id for dag with hash: {dag_hash} not found, '
                        f'creating a new entry and binding the dag.')
            dag_id = self._insert_dag()
            logger.info(f'Inserting edges for dag with hash: {dag_hash}.')
            # insert edges
            self._insert_edges()
        else:
            logger.info(f'Found dag_id: {dag_id} for dag with hash: '
                        f'{dag_hash}')
        self.dag_id = dag_id
        return self.dag_id

    def _get_dag_id(self):
        dag_hash = hash(self)
        logger.info(f'Querying for dag with hash: {dag_hash}')
        return_code, response = self.requester.send_request(
            app_route=f'/client_dag/{dag_hash}',
            request_type='get'
        )
        if return_code == StatusCodes.OK:
            return response['dag_id']
        else:
            raise ValueError(f'Unexpected status code {return_code} from GET '
                             f'request through route /dag/{dag_hash} . '
                             f'Expected code 200. Response content: '
                             f'{response}')

    def _insert_dag(self):
        dag_hash = hash(self)
        logger.info(f'Inserting dag with hash: {dag_hash}')
        return_code, response = self.requester.send_request(
            app_route=f'/client_dag/{dag_hash}',
            request_type='post'
        )
        if return_code == StatusCodes.OK:
            return response['dag_id']
        else:
            raise ValueError(f'Unexpected status code {return_code} from POST '
                             f'request through route /dag/{dag_hash} . '
                             f'Expected code 200. Response content: '
                             f'{response}')

    def _insert_edges(self):
        logger.info(f'Inserting edges into dag with id {self.dag_id}')

        # convert the set into a dictionary that can be dumped and sent over
        # the wire as json
        nodes_and_edges = {}
        for node in self.nodes:
            nodes_and_edges[node.node_id] = {
                'upstream_nodes': node.upstream_nodes,
                'downstream_nodes': node.downstream_nodes
            }
        return_code, response = self.requester.send_request(
            app_route=f'/edge/{self.dag_id}',
            message={
                'nodes_and_edges': json.dumps(nodes_and_edges)
            },
            request_type='post'
        )
        if return_code != StatusCodes.OK:
            raise ValueError(f'Unexpected status code {return_code} from POST '
                             f'request through route /dag/{self.dag_id} . '
                             f'Expected code 200. Response content: '
                             f'{response}')

    def validate(self) -> bool:
        """Is this actually a directed acyclical graph?
        Uses Depth First Search on each node to detect a back edge.

        Immediately raises an error if it finds a cycle"""

        for node in self.nodes:
            node_list

    def __hash__(self) -> str:
        """Determined by hashing all sorted node hashes and their downstream"""
        hash_value = hashlib.sha1()
        if len(self.nodes) > 0:  # if the dag is empty, we want to skip this
            for node in sorted(self.nodes):
                hash_value.update(
                    bytes("{:x}".format(node.node_args_hash).encode('utf-8'))
                )
                for downstream_node in sorted(node.downstream_nodes):
                    hash_value.update(
                        bytes("{:x}".format(
                            downstream_node.node_args_hash).encode('utf-8'))
                    )
        return hash_value.hexdigest()
