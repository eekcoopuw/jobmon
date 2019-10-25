from jobmon.client.swarm.workflow.node import Node

# what do we do with edges? What is in charge of them?


class Dag(object):
    """Stores Nodes, talks to the database about itself"""

    def __init__(self):
        self.nodes = []
        # do we use the same TaskDagMeta model as before?
        # if not, are there other attributes aside from id and hash?

    def add_node(self, node: Node):
        # wf.add_task should call Node.add_node() and pass the tasks' node
        self.nodes.append(node)

    def bind(self):
        # get dag id
        #   needs a GET route
        # if no dag id, insert
        #   needs a POST route
        pass

    def __hash__(self):
        # hash of all nodes
        # nodes don't store their edges - how to hash uniquely?
        # Unlikely but...
        #   nodes could be the same but their
        #   upstream/downstream different right?
        pass
