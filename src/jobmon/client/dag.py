import hashlib
from http import HTTPStatus as StatusCodes
from typing import Optional, Set, Dict, List, Tuple

import structlog as logging

from jobmon.client.client_config import ClientConfig
from jobmon.client.node import Node
from jobmon.requester import Requester, http_request_ok
from jobmon.exceptions import (NodeDependencyNotExistError, DuplicateNodeArgsError,
                               InvalidResponse)


logger = logging.getLogger(__name__)


class Dag(object):

    def __init__(self, requester: Optional[Requester] = None):
        """The DAG (Directed Acyclic Graph) captures the tasks (nodes) as they are
        related to each other in their dependency structure. The Dag is traversed in
        the order of node dependencies so a workflow run is a single instance of
        traversing through a dag. This object stores the nodes and communicates
        with the server with regard to itself.

        Args:
            requester_url (str): url to communicate with the flask services.
        """

        self.nodes: Set[Node] = set()

        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester

    @property
    def dag_id(self) -> int:
        """Database unique ID of this DAG"""
        if not hasattr(self, "_dag_id"):
            raise AttributeError("_dag_id cannot be accessed before dag is bound")
        return self._dag_id

    def add_node(self, node: Node) -> None:
        """Add a node to this dag.
        Args:
            node (Node): Node to add to the dag
        """
        # validate node has unique node args within this task template version
        if node in self.nodes:
            raise DuplicateNodeArgsError(
                "A duplicate node was found for task_template_version_id="
                f"{node.task_template_version_id}. Node args were {node.node_args}"
            )
        # wf.add_task should call ClientNode.add_node() + pass the tasks' node
        self.nodes.add(node)

    def bind(self, chunk_size: int = 500) -> int:
        """Retrieve an id for a matching dag from the server. If it doesn't
        exist, first create one, including its edges."""

        if len(self.nodes) == 0:
            raise RuntimeError('No nodes were found in the dag. An empty dag '
                               'cannot be bound.')

        self._bulk_bind_nodes(chunk_size)

        dag_id = self._get_dag_id()
        dag_hash = hash(self)
        if dag_id is None:
            logger.info(f'dag_id for dag with hash: {dag_hash} not found, '
                        f'creating a new entry and binding the dag.')
            self._dag_id = self._insert_dag()
        else:
            self._dag_id = dag_id
            logger.info(f'Found dag_id: {self.dag_id} for dag with hash: '
                        f'{dag_hash}')
        logger.debug(f'dag_id is: {self.dag_id}')
        return self.dag_id

    def validate(self):
        nodes_in_dag = self.nodes
        for node in nodes_in_dag:
            # Make sure no task contains up/down stream tasks that are not in the workflow
            for n in node.upstream_nodes:
                if n not in nodes_in_dag:
                    raise NodeDependencyNotExistError(
                        f"Upstream node, {hash(n)}, for node, {hash(node)},"
                        "does not exist in the dag."
                    )
            for n in node.downstream_nodes:
                if n not in nodes_in_dag:
                    raise NodeDependencyNotExistError(
                        f"Downstream node, {hash(n)}, for node, {hash(node)},"
                        "does not exist in the dag."
                    )

    def _bulk_bind_nodes(self, chunk_size: int) -> None:

        def get_chunk(total_nodes: int, chunk_number: int) -> Optional[Tuple[int, int]]:
            # This function is created for unit testing
            if (chunk_number - 1) * chunk_size >= total_nodes:
                return None
            return ((chunk_number - 1) * chunk_size,
                    min(total_nodes - 1, chunk_number * chunk_size - 1))

        nodes_in_dag = list(self.nodes)
        nodes_received = {}
        total_nodes = len(self.nodes)
        chunk_number = 1
        chunk_boarder = get_chunk(total_nodes, chunk_number)
        while chunk_boarder is not None:
            # do something to bind
            nodes_to_send = []
            for i in range(chunk_boarder[0], chunk_boarder[1] + 1):
                node = nodes_in_dag[i]
                n = {"task_template_version_id": node.task_template_version_id,
                     "node_args_hash": node.node_args_hash,
                     "node_args": node.node_args}
                nodes_to_send.append(n)
            rc, response = self.requester.send_request(
                app_route='/client/nodes',
                message={'nodes': nodes_to_send},
                request_type='post',
                logger=logger
            )
            if http_request_ok(rc) is False:
                raise InvalidResponse(
                    f'Unexpected status code {rc} from GET '
                    f'request through route /client/workflow. Expected code '
                    f'200. Response content: {response}')
            else:
                nodes_received.update(response['nodes'])
            chunk_number += 1
            chunk_boarder = get_chunk(total_nodes, chunk_number)

        for node in nodes_in_dag:
            k = f"{node.task_template_version_id}:{node.node_args_hash}"
            if k in nodes_received.keys():
                node._node_id = int(nodes_received[k])
            else:
                raise InvalidResponse(
                    f"Fail to find node_id in HTTP response for node_args_hash "
                    f"{node.node_args_hash} and task_template_version_id "
                    f"{node.task_template_version_id} HTTP Response:\n {response}")

    def _get_dag_id(self) -> Optional[int]:
        dag_hash = hash(self)
        logger.info(f'Querying for dag with hash: {dag_hash}')
        return_code, response = self.requester.send_request(
            app_route='/client/dag',
            message={"dag_hash": dag_hash},
            request_type='get',
            logger=logger
        )
        if return_code == StatusCodes.OK:
            return response['dag_id']
        else:
            raise ValueError(f'Unexpected status code {return_code} from GET '
                             f'request through route /client/dag/{dag_hash} . '
                             f'Expected code 200. Response content: '
                             f'{response}')

    def _insert_dag(self) -> int:

        # convert the set into a dictionary that can be dumped and sent over
        # as json
        dag_hash = hash(self)
        nodes_and_edges: Dict[int, Dict[str, List]] = {}

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

        logger.debug(f'message included in edge post request: {nodes_and_edges}')

        return_code, response = self.requester.send_request(
            app_route='/client/dag',
            message={"dag_hash": hash(self),
                     "nodes_and_edges": nodes_and_edges},
            request_type='post',
            logger=logger
        )
        if return_code == StatusCodes.OK:
            return response['dag_id']
        else:
            raise ValueError(f'Unexpected status code {return_code} from POST request through '
                             f'route /client/dag/{dag_hash}. Expected code 200. Response content: '
                             f'{response}')

    def __hash__(self) -> int:
        """Determined by hashing all sorted node hashes and their downstream"""
        hash_value = hashlib.sha1()
        if len(self.nodes) > 0:  # if the dag is empty, we want to skip this
            for node in sorted(self.nodes):
                hash_value.update(str(hash(node)).encode('utf-8'))
                for downstream_node in sorted(node.downstream_nodes):
                    hash_value.update(str(hash(downstream_node)).encode('utf-8'))
        return int(hash_value.hexdigest(), 16)
