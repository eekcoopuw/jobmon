"""Routes for Task Resources."""
import json
import ast
from typing import Dict, Tuple

from sqlalchemy import select

from jobmon.server.web.models import DB
from jobmon.server.web.models.task_resources import TaskResources
from jobmon.server.web.models.cluster import Cluster
from jobmon.server.web.models.cluster_type import ClusterType
from jobmon.server.web.models.queue import Queue

from jobmon.server.web.server_side_exception import InvalidUsage


class IgnoreFormatter(dict):

    def __missing__(self, key):
        return "{" + key + "}"


def _get_logfile_template(task_resources_id: int, template_type: str) -> Tuple[Dict, str]:

    valid_template_types = ["array", "job", "both"]
    if template_type not in valid_template_types:
        raise InvalidUsage(
            f"Valid Template Types are {valid_template_types}. Got {template_type}."
        )
    elif template_type == "both":
        template_types = ["array", "job"]
    else:
        template_types = [template_type]

    # Check if the array is already bound, if so return it
    select_stmt = select(
        ClusterType.logfile_templates, TaskResources.requested_resources, Queue.name
    ).join_from(
        TaskResources, Queue, TaskResources.queue_id == Queue.id
    ).join(
        Cluster, Queue.cluster_id == Cluster.id
    ).join(
        ClusterType, Cluster.cluster_type_id == ClusterType.id
    ).where(
        TaskResources.id == task_resources_id
    )

    log_templates, requested_resources, queue_name = DB.session.execute(select_stmt).fetchone()
    log_templates = json.loads(log_templates)
    requested_resources = ast.literal_eval(requested_resources)

    logpaths: Dict = {}
    for log_type in ["stderr", "stdout"]:
        for template_type in template_types:
            if template_type in log_templates and log_type in requested_resources:
                if log_type in log_templates[template_type]:
                    log_type_dict = logpaths.get(log_type, {})
                    ignore_missing_mapper = IgnoreFormatter(root=requested_resources[log_type])
                    log_format = log_templates[template_type][log_type]
                    log_type_dict[template_type] = log_format.format_map(ignore_missing_mapper)
                    logpaths[log_type] = log_type_dict

    requested_resources.update(logpaths)
    return requested_resources, queue_name
