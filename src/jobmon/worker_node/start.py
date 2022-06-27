"""Start up distributing process."""
from pathlib import Path
from typing import Dict, Optional

from jobmon.cluster import Cluster
from jobmon.exceptions import InvalidResponse
from jobmon.requester import http_request_ok, Requester
from jobmon.worker_node.worker_node_config import WorkerNodeConfig
from jobmon.worker_node.worker_node_task_instance import WorkerNodeTaskInstance


class WorkerNodeFactory:

    def __init__(
        self,
        cluster_name: str,
        worker_node_config: Optional[WorkerNodeConfig] = None
    ) -> None:
        self._cluster_name = cluster_name
        if worker_node_config is None:
            worker_node_config = WorkerNodeConfig.from_defaults()
        self._worker_node_config = worker_node_config

        cluster = Cluster.get_cluster(cluster_name, Requester(self._worker_node_config.url))
        self._worker_node_interface = cluster.get_worker_node()

    def _initialize_logfiles(
        self,
        task_instance_id: int,
        template_type: str,
        job_name: str
    ) -> Dict[str, Path]:
        requester = Requester(self._worker_node_config.url)
        app_route = f"/task_instance/{task_instance_id}/logfile_template/{template_type}"
        return_code, response = requester.send_request(
            app_route=app_route,
            message={},
            request_type="post",
        )
        if http_request_ok(return_code) is False:
            raise InvalidResponse(
                f"Unexpected status code {return_code} from POST "
                f"request through route {app_route}. Expected "
                f"code 200. Response content: {response}"
            )

        logfiles: Dict[str, Path] = {}
        task_name = response["task_name"]
        for log_type, template in response["logpaths"].items():
            initial_logfile = Path(
                template.format(
                    name=job_name,
                    distributor_id=self._worker_node_interface.distributor_id
                )
            )
            final_logfile = initial_logfile.parent / f"{task_name}{initial_logfile.suffix}"
            if initial_logfile.exists():
                initial_logfile.rename(final_logfile)
            else:
                final_logfile.touch()
            logfiles[log_type] = final_logfile

        return logfiles

    def get_job_task_instance(
        self,
        task_instance_id: int,
    ) -> WorkerNodeTaskInstance:
        """Set up and return WorkerNodeTaskInstance object."""

        logfiles = self._initialize_logfiles(
            task_instance_id=task_instance_id,
            template_type="job",
            job_name=str(task_instance_id)
        )

        worker_node_task_instance = WorkerNodeTaskInstance(
            cluster_interface=self._worker_node_interface,
            task_instance_id=task_instance_id,
            stdout=logfiles.get("stdout", None),
            stderr=logfiles.get("stdout", None),
            heartbeat_interval=self._worker_node_config.task_instance_heartbeat_interval,
            report_by_buffer=self._worker_node_config.heartbeat_report_by_buffer,
            requester=Requester(self._worker_node_config.url),
        )
        return worker_node_task_instance

    def get_array_task_instance(
        self,
        array_id: int,
        batch_number: int,
    ) -> WorkerNodeTaskInstance:
        """Set up and return WorkerNodeTaskInstance object."""
        requester = Requester(self._worker_node_config.url)

        # Always assumed to be a value in the range [1, len(array)]
        array_step_id = self._worker_node_interface.array_step_id

        # Fetch from the database
        app_route = f"/get_array_task_instance_id/{array_id}/{batch_number}/{array_step_id}"
        rc, resp = requester.send_request(
            app_route=app_route, message={}, request_type="get"
        )
        if http_request_ok(rc) is False:
            raise InvalidResponse(
                f"Unexpected status code {rc} from POST "
                f"request through route {app_route}. Expected code "
                f"200. Response content: {rc}"
            )
        task_instance_id = resp["task_instance_id"]

        logfiles = self._initialize_logfiles(
            task_instance_id=task_instance_id,
            template_type="array",
            job_name=f"{array_id}-{batch_number}"
        )

        worker_node_task_instance = WorkerNodeTaskInstance(
            cluster_interface=self._worker_node_interface,
            task_instance_id=task_instance_id,
            stdout=logfiles.get("stdout", None),
            stderr=logfiles.get("stdout", None),
            heartbeat_interval=self._worker_node_config.task_instance_heartbeat_interval,
            report_by_buffer=self._worker_node_config.heartbeat_report_by_buffer,
            requester=requester,
        )
        return worker_node_task_instance
