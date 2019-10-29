from jobmon.client.swarm.workflow.node import Node

# what do we do with edges? What is in charge of them?
#   node gets edge info from tasks,
#     dag inserts edges from nodes into edge table


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
        #   add nodes with upstream and downstreams to edge table
        pass

    def __hash__(self):
        # hash of all nodes
        # nodes don't store their edges - how to hash uniquely?
        # Unlikely but...
        #   nodes could be the same but their
        #   upstream/downstream different right?
        pass

    # old stuff to reference
    # @property
    # def hash(self):
    #     hashval = hashlib.sha1()
    #     for task_hash in sorted(self.tasks):
    #         hashval.update(bytes("{:x}".format(task_hash).encode('utf-8')))
    #         task = self.tasks[task_hash]
    #         for dtask in sorted(task.downstream_tasks):
    #             hashval.update(
    #                 bytes("{:x}".format(dtask.hash).encode('utf-8')))
    #     return hashval.hexdigest()