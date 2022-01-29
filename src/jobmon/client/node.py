"""A node represents an individual task within a DAG."""
from __future__ import annotations

import hashlib
from http import HTTPStatus as StatusCodes
import json
import logging
from typing import Any, Dict, List, Optional, Set

from jobmon.client.client_config import ClientConfig
from jobmon.client.task_template_version import TaskTemplateVersion
from jobmon.constants import SpecialChars
from jobmon.requester import Requester


logger = logging.getLogger(__name__)


class Node:
    """A node represents an individual task within a Dag."""

    def __init__(
        self,
        task_template_version: TaskTemplateVersion,
        node_args: Dict[str, Any],
        requester: Optional[Requester] = None,
    ) -> None:
        """A node represents an individual task within a Dag.

        This includes its relationship to other nodes that it is dependent upon or nodes that
        depend upon it. A node stores node arguments (arguments relating to the
        actual shape of the dag and number of tasks created for a given
        stage/task template) and can register itself with the database via the
        Jobmon Query Service and the Jobmon State Manager.

        Args:
            task_template_version: The associated TaskTemplateVersion.
            node_args: key-value pairs of arg_name and a value.
            requester: Requester object to communicate with the flask services.
        """
        self.task_template_version = task_template_version
        self.node_args = node_args
        self.mapped_node_args = self.task_template_version.convert_arg_names_to_ids(
            **self.node_args
        )
        self.node_args_hash = self._hash_node_args()
        self.upstream_nodes: Set[Node] = set()
        self.downstream_nodes: Set[Node] = set()

        if requester is None:
            requester_url = ClientConfig.from_defaults().url
            requester = Requester(requester_url)
        self.requester = requester

    @property
    def node_id(self) -> int:
        """Unique id for each node."""
        if not hasattr(self, "_node_id"):
            raise AttributeError("node_id cannot be accessed before node is bound")
        return self._node_id

    @property
    def task_template_version_id(self) -> int:
        return self.task_template_version.id

    @property
    def default_name(self) -> str:
        """The default name of this node in the array."""
        name = (
            self.task_template_version.task_template.template_name
            + "_"
            + "_".join([str(k) + "-" + str(self.node_args[k]) for k in self.node_args.keys()])
        )

        # special char protection
        name = "".join(
            [
                c if c not in SpecialChars.ILLEGAL_SPECIAL_CHARACTERS else "_"
                for c in name
            ]
        )

        # long name protection
        name = name if len(name) < 250 else name[0:249]
        return name

    def bind(self) -> int:
        """Retrieve an id for a matching node from the server.

        If it doesn't exist, first create one.
        """
        node_id = self._get_node_id()
        if node_id is None:
            logger.debug(
                f"node_id for node: {self} not found, creating a new"
                f"entry and binding node."
            )
            node_id = self._insert_node_and_node_args()
        else:
            logger.debug(f"Found node_id: {node_id} for node: {self}, binding node.")
        self._node_id = node_id
        return self.node_id

    def _hash_node_args(self) -> int:
        """A hash of the node.

        The hash is the encoded result of the args and values concatenated together.
        """
        arg_ids = list(self.mapped_node_args.keys())
        arg_ids.sort()

        arg_values = [str(self.mapped_node_args[key]) for key in arg_ids]
        str_arg_ids = [str(arg) for arg in arg_ids]

        hash_value = int(
            hashlib.sha1("".join(str_arg_ids + arg_values).encode("utf-8")).hexdigest(), 16
        )
        return hash_value

    def _get_node_id(self) -> int:
        logger.debug(f"Querying for node {self}")
        return_code, response = self.requester.send_request(
            app_route="/node",
            message={
                "task_template_version_id": self.task_template_version_id,
                "node_args_hash": self.node_args_hash,
            },
            request_type="get",
            logger=logger,
        )
        if return_code == StatusCodes.OK:
            return response["node_id"]
        else:
            raise ValueError(
                f"Unexpected status code {return_code} from GET "
                f"request through route /node. Expected code 200."
                f" Response content:"
                f" {response}"
            )

    def _insert_node_and_node_args(self) -> int:
        logger.debug(f"Insert node: {self}")
        return_code, response = self.requester.send_request(
            app_route="/node",
            message={
                "task_template_version_id": self.task_template_version_id,
                "node_args_hash": self.node_args_hash,
                "node_args": json.dumps(self.mapped_node_args),
            },
            request_type="post",
            logger=logger,
        )
        if return_code == StatusCodes.OK:
            return response["node_id"]
        else:
            raise ValueError(
                f"Unexpected status code {return_code} from POST "
                f"request through route /node. Expected code 200."
                f" Response content: {response}"
            )

    def add_upstream_node(self, upstream_node: Node) -> None:
        """Add a single node to this one's upstream Nodes.

        Args:
            upstream_node: node to add a dependency on
        """
        self.upstream_nodes.add(upstream_node)
        # Add this node to the upstream nodes' downstream
        upstream_node.downstream_nodes.add(self)

    def add_upstream_nodes(self, upstream_nodes: List[Node]) -> None:
        """Add many nodes to this one's upstream Nodes.

        Args:
            upstream_nodes: list of nodes to add dependencies on
        """
        for node in upstream_nodes:
            self.add_upstream_node(node)

    def add_downstream_node(self, downstream_node: Node) -> None:
        """Add a node to this one's downstream Nodes.

        Args:
            downstream_node: Node that will be dependent on this node
        """
        self.downstream_nodes.add(downstream_node)
        # avoid endless recursion, set directly
        downstream_node.upstream_nodes.add(self)

    def add_downstream_nodes(self, downstream_nodes: List[Node]) -> None:
        """Add a list of nodes as this node's downstream nodes.

        Args:
            downstream_nodes: Nodes that will be dependent on this node.
        """
        for node in downstream_nodes:
            self.add_downstream_node(node)

    def __repr__(self) -> str:
        """Repr the node attributes."""
        return (
            "Node("
            f"task_template_version_id={self.task_template_version_id}, "
            f"node_args={self.node_args}, "
            f"node_args_hash={self.node_args_hash})"
        )

    def __eq__(self, other: object) -> bool:
        """Check if two nodes have equal hashes."""
        if not isinstance(other, Node):
            return False
        else:
            return hash(self) == hash(other)

    def __lt__(self, other: Node) -> bool:
        """Check if this hash is less than anothers."""
        return hash(self) < hash(other)

    def __hash__(self) -> int:
        """Create a hash that will be a unique identifier for the node."""
        hash_value = hashlib.sha1()
        hash_value.update(bytes(str(self.node_args_hash).encode("utf-8")))
        hash_value.update(bytes(str(self.task_template_version_id).encode("utf-8")))
        return int(hash_value.hexdigest(), 16)
