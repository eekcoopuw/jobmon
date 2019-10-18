import hashlib


class NodeArgs(object):
    """

    """

    def __init__(self, node_id: int, arg_id: int, val: str):
        self.node_id = node_id
        self.arg_id = arg_id
        self.val = val
    
    def __hash__(self) -> int:
        return int(hashlib.sha1(command.encode('utf-8')).hexdigest(), 16)