from __future__ import annotations

import logging
from typing import Callable, Dict, List, Optional, Set, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from jobmon.client.distributor.distributor_task_instance import DistributorTaskInstance
    from jobmon.client.distributor.distributor_workflow_run import DistributorWorkflowRun

logger = logging.getLogger(__name__)


class DistributorWorkflow:

    def __init__(self, workflow_id: int):
        self.workflow_id = workflow_id
        self.task_instances: Set[DistributorTaskInstance] = set()

    def add_task_instance(self, task_instance: DistributorTaskInstance):
        if task_instance.workflow_id != self.workflow_id:
            raise ValueError(
                f"workflow_id mismatch. TaskInstance={task_instance.workflow_id}. "
                f"Workflow={self.workflow_id}."
            )
        self.task_instances[task_instance.task_instance_id] = task_instance
        task_instance.workflow = self

    def get_metadata(self):
        # app_route = f"/array/{array_id}"
        # return_code, response = self.requester.send_request(
        #     app_route=app_route, message={}, request_type="get", logger=logger
        # )
        # if http_request_ok(return_code) is False:
        #     raise InvalidResponse(
        #         f"Unexpected status code {return_code} from POST "
        #         f"request through route {app_route}. Expected "
        #         f"code 200. Response content: {response}"
        #     )
        self.max_concurrently_running = 100
