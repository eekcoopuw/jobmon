import hashlib

from jobmon.client.swarm.workflow.node import Node
from jobmon.client.client_logging import ClientLogging as Logging

# what do we do with edges? What is in charge of them?
#   node gets edge info from tasks,
#     dag inserts edges from nodes into edge table

logger = Logging.getLogger(__name__)


class Dag(object):
    """Stores Nodes, talks to the database in regard to itself"""

    def __init__(self):
        self.nodes = []
        # set or list or ordereddict with set abstraction like task_dag?

    def add_node(self, node: Node):
        # wf.add_task should call Node.add_node() and pass the tasks' node
        self.nodes.append(node)

    def bind(self):
        # get dag id
        #   needs a GET route
        # if no dag id, insert
        #   needs a POST route
        #   add nodes with upstream and downstreams to edge table
        pass

    def __hash__(self) -> str:
        """Determined by hashing all sorted node hashes and their downstream"""
        hash_value = hashlib.sha1()
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
