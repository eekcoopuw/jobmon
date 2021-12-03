from __future__ import annotations

import logging
from typing import Dict, List, Optional, Set, Tuple

from jobmon.client.client_config import ClientConfig
from jobmon.client.cluster import Cluster
from jobmon.client.task_resources import TaskResources
from jobmon.cluster_type.base import ClusterQueue
from jobmon.constants import TaskResourcesType, TaskStatus
from jobmon.exceptions import InvalidResponse
from jobmon.requester import http_request_ok, Requester
from jobmon.serializers import SerializeSwarmTask


logger = logging.getLogger(__name__)


class Array:
    """Swarm Array object."""

    def __init__(self, array_id: int):
        pass

    def from_wire(self, wire_tuple: Tuple, requester: Requester):

