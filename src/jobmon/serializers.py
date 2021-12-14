"""Serializing data when going to and from the database."""
import ast
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union


class SerializeDistributorTask:
    """Serialize the data to and from the database for an DistributorTask object."""

    @staticmethod
    def to_wire(
        task_id: int,
        array_id: int,
        name: str,
        command: str,
        requested_resources: dict,
    ) -> tuple:
        """Submitting the above args to the database for an DistributorTask object."""
        return (
            task_id,
            array_id,
            name,
            command,
            requested_resources,
        )

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Coerce types for all nullables that are cast using ast.literal_eval.

        It is a potential security issue but was the only solution I could find to turning
        the data into json twice.
        """
        return {
            "task_id": int(wire_tuple[0]),
            "array_id": int(wire_tuple[1]) if wire_tuple[1] is not None else None,
            "name": wire_tuple[2],
            "command": wire_tuple[3],
            "requested_resources": {}
            if wire_tuple[4] is None
            else ast.literal_eval(wire_tuple[4]),
        }


class SerializeSwarmTask:
    """Serialize the data to and from the db for a Swarm Task."""

    @staticmethod
    def to_wire(task_id: int, status: str) -> tuple:
        """Submit task id and status to the database from a SwarmTask object."""
        return task_id, status

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Get task id and status for a SwarmTask."""
        return {"task_id": int(wire_tuple[0]), "status": wire_tuple[1]}


class SerializeTaskInstance:
    """Serialize the data to and from the database for an DistributorTaskInstance."""

    @staticmethod
    def to_wire(
        task_instance_id: int, workflow_run_id: int, distributor_id: Union[int, None],
            array_id: Union[int, None]
    ) -> tuple:
        """Submit the above args for an DistributorTaskInstance object to the database."""
        return task_instance_id, workflow_run_id, distributor_id, array_id

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Retrieve the DistributorTaskInstance information from the database."""
        distributor_id = int(wire_tuple[2]) if wire_tuple[2] else None
        array_id = int(wire_tuple[3]) if wire_tuple[3] else None
        return {
            "task_instance_id": int(wire_tuple[0]),
            "workflow_run_id": int(wire_tuple[1]),
            "distributor_id": distributor_id,
            "array_id": array_id
        }


class SerializeTaskInstanceErrorLog:
    """Serialize the data to and from the database for an TaskInstanceErrorLog."""

    @staticmethod
    def to_wire(
        task_instance_error_log_id: int, error_time: datetime, description: str
    ) -> tuple:
        """Submit the args for an SerializeTaskInstanceErrorLog object to the database."""
        return task_instance_error_log_id, error_time, description

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Retrieve the SerializeTaskInstanceErrorLog information from the database."""
        return {
            "task_instance_error_log_id": int(wire_tuple[0]),
            "error_time": str(wire_tuple[1]),
            "description": str(wire_tuple[2]),
        }


class SerializeExecutorTaskInstanceErrorLog:
    """Serialize the data to and from the database for an ExecutorTaskInstanceErrorLog."""

    @staticmethod
    def to_wire(
        task_instance_error_log_id: int, error_time: datetime, description: str
    ) -> tuple:
        """A to_wire method.

        Submit the above args for an SerializeExecutorTaskInstanceErrorLog
        object to the database.
        """
        return task_instance_error_log_id, error_time, description

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Retrieve the SerializeExecutorTaskInstanceErrorLog information from the database."""
        return {
            "task_instance_error_log_id": int(wire_tuple[0]),
            "error_time": str(wire_tuple[1]),
            "description": str(wire_tuple[2]),
        }


class SerializeClientTool:
    """Serialize the data to and from the database for a Tool object."""

    @staticmethod
    def to_wire(id: int, name: str) -> tuple:
        """Submit the id and name of a Tool to the database."""
        return (id, name)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Get a Tool's information from the database."""
        return {"id": int(wire_tuple[0]), "name": wire_tuple[1]}


class SerializeClientToolVersion:
    """Serialize the data to and from the database for a ToolVersion."""

    @staticmethod
    def to_wire(id: int, tool_id: int) -> tuple:
        """Submit the id and tool_id for a Tool Version to the database."""
        return (id, tool_id)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Get the Tool Version information from thee database."""
        return {"id": int(wire_tuple[0]), "tool_id": int(wire_tuple[1])}


class SerializeClientTaskTemplate:
    """Serialize the data to and from the database for a TaskTemplate."""

    @staticmethod
    def to_wire(id: int, tool_version_id: int, template_name: str) -> tuple:
        """Submit the TaskTemplate information for transfer over http."""
        return (id, tool_version_id, template_name)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Convert packed wire format to kwargs for use in services."""
        return {
            "id": int(wire_tuple[0]),
            "tool_version_id": int(wire_tuple[1]),
            "template_name": wire_tuple[2],
        }


class SerializeClientTaskTemplateVersion:
    """Serialize the data to and from the database for a TaskTemplateVersion."""

    @staticmethod
    def to_wire(
        task_template_version_id: int,
        command_template: str,
        node_args: List[str],
        task_args: List[str],
        op_args: List[str],
        id_name_map: dict,
    ) -> Tuple[int, str, List[str], List[str], List[str], dict]:
        """Submit the TaskTemplateVersion information to the database."""
        return (
            task_template_version_id,
            command_template,
            node_args,
            task_args,
            op_args,
            id_name_map,
        )

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> Dict:
        """Get the TaskTemplateVersion info from the database."""
        return {
            "task_template_version_id": int(wire_tuple[0]),
            "command_template": wire_tuple[1],
            "node_args": wire_tuple[2],
            "task_args": wire_tuple[3],
            "op_args": wire_tuple[4],
            "id_name_map": wire_tuple[5],
        }


class SerializeWorkflowRun:
    """Serialize the data to and from the database for a WorkflowRun."""

    @staticmethod
    def to_wire(id: int, workflow_id: int) -> tuple:
        """Submit the WorkflowRun information to the database."""
        return (id, workflow_id)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Get the WorkflowRun information from the database."""
        return {"id": int(wire_tuple[0]), "workflow_id": int(wire_tuple[1])}


class SerializeClusterType:
    """Serialize the data to and from the database for a ClusterType."""

    @staticmethod
    def to_wire(id: int, name: str, package_location: str) -> tuple:
        """Submit the ClusterType information to the database."""
        return (id, name, package_location)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Get the Cluster information from the database."""
        return {
            "id": int(wire_tuple[0]),
            "name": str(wire_tuple[1]),
            "package_location": str(wire_tuple[2]),
        }


class SerializeCluster:
    """Serialize the data to and from the database for a Cluster."""

    @staticmethod
    def to_wire(
        id: int, name: str, cluster_type_name: str, package_location: str
    ) -> tuple:
        """Submit the Cluster information to the database."""
        return (id, name, cluster_type_name, package_location)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Get the Cluster information from the database."""
        return {
            "id": int(wire_tuple[0]),
            "name": str(wire_tuple[1]),
            "cluster_type_name": str(wire_tuple[2]),
            "package_location": str(wire_tuple[3]),
        }


class SerializeQueue:
    """Serialize the data to and from the database for a Queue."""

    @staticmethod
    def to_wire(id: int, name: str, parameters: str) -> tuple:
        """Submit the Queue information to the database."""
        return (id, name, parameters)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Get the Queue information from the database."""
        return {
            "queue_id": int(wire_tuple[0]),
            "queue_name": str(wire_tuple[1]),
            "parameters": {}
            if wire_tuple[2] is None
            else ast.literal_eval(wire_tuple[2]),
        }


class SerializeTaskResources:
    """Serialize the data to and from the db for a TaskResources."""

    @staticmethod
    def to_wire(
        task_id: int,
        queue_id: int,
        task_resources_type_id: str,
        resource_scales: str,
        requested_resources: str,
    ) -> tuple:
        """Submit the TaskResources info to the database."""
        return (
            task_id,
            queue_id,
            task_resources_type_id,
            resource_scales,
            requested_resources,
        )

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Get the whole  for a TaskResources."""
        return {
            "id": int(wire_tuple[0]),
            "task_id": int(wire_tuple[1]),
            "queue_id": int(wire_tuple[2]),
            "task_resources_type_id": str(wire_tuple[3]),
            "resource_scales": str(wire_tuple[4]),
            "requested_resources": str(wire_tuple[5]),
        }


class SerializeTaskResourceUsage:
    """Serialize the data to and from the database for Task resource usage."""

    @staticmethod
    def to_wire(
        num_attempts: Optional[int] = None,
        nodename: Optional[str] = None,
        runtime: Optional[int] = None,
        memory: Optional[int] = None,
    ) -> tuple:
        """Submit the Task resource usage information to the database."""
        return num_attempts, nodename, runtime, memory

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Get the Task resource usage information from the database."""
        return {
            "num_attempts": wire_tuple[0],
            "nodename": wire_tuple[1],
            "runtime": wire_tuple[2],
            "memory": wire_tuple[3],
        }


class SerializeTaskTemplateResourceUsage:
    """Serialize the data to and from the database for TaskTemplate resource usage."""

    @staticmethod
    def to_wire(
        num_tasks: Optional[int] = None,
        min_mem: Optional[int] = None,
        max_mem: Optional[int] = None,
        mean_mem: Optional[float] = None,
        min_runtime: Optional[int] = None,
        max_runtime: Optional[int] = None,
        mean_runtime: Optional[float] = None,
        median_mem: Optional[float] = None,
        median_runtime: Optional[float] = None,
        ci_mem: Optional[List[float]] = None,
        ci_runtime: Optional[List[float]] = None,
    ) -> tuple:
        """Submit the TaskTemplate resource usage information to the database."""
        return (
            num_tasks,
            min_mem,
            max_mem,
            mean_mem,
            min_runtime,
            max_runtime,
            mean_runtime,
            median_mem,
            median_runtime,
            ci_mem,
            ci_runtime,
        )

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Get the TaskTemplate resource usage information from the database."""
        return {
            "num_tasks": wire_tuple[0],
            "min_mem": wire_tuple[1],
            "max_mem": wire_tuple[2],
            "mean_mem": wire_tuple[3],
            "min_runtime": wire_tuple[4],
            "max_runtime": wire_tuple[5],
            "mean_runtime": wire_tuple[6],
            "median_mem": wire_tuple[7],
            "median_runtime": wire_tuple[8],
            "ci_mem": wire_tuple[9],
            "ci_runtime": wire_tuple[10],
        }


class SerializeDistributorArray:
    """"""
    @staticmethod
    def to_wire(
        array_id: int,
        task_resources_id: int,
        requested_resources: Dict,
    ) -> tuple:
        """Submit the TaskTemplate resource usage information to the database."""
        return (
            array_id,
            task_resources_id,
            requested_resources,
        )

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Get the TaskTemplate resource usage information from the database."""
        return {
            "array_id": wire_tuple[0],
            "task_resources_id": wire_tuple[1],
            "requested_resources": {}
            if wire_tuple[2] is None
            else ast.literal_eval(wire_tuple[2])
        }
