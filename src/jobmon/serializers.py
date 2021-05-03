"""Serializing data when going to and from the database."""
import ast
from datetime import datetime
from typing import Union


class SerializeExecutorTask:
    """Serialize the data to and from the database for an ExecutorTask object."""

    @staticmethod
    def to_wire(task_id: int, workflow_id: int, node_id: int,
                task_args_hash: int, name: str, command: str, status: str,
                max_runtime_seconds: int, context_args: str,
                resource_scales: str, queue: str, num_cores: int,
                m_mem_free: str, j_resource: str, hard_limits: str) -> tuple:
        """Submitting the above args to the database for an ExecutorTask object."""
        return (task_id, workflow_id, node_id, task_args_hash, name, command,
                status, max_runtime_seconds, context_args, resource_scales,
                queue, num_cores, m_mem_free, j_resource, hard_limits)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Coerce types for all nullables that are cast using ast.literal_eval is a potential
        security issue but was the only solution I could find to turning the data into json
        twice.
        """
        max_runtime_seconds = int(wire_tuple[7]) if wire_tuple[7] else None
        context_args = ast.literal_eval(
            wire_tuple[8]) if wire_tuple[8] else None
        resource_scales = ast.literal_eval(
            wire_tuple[9]) if wire_tuple[9] else None
        num_cores = int(wire_tuple[11]) if wire_tuple[11] else None
        m_mem_free = float(wire_tuple[12]) if wire_tuple[12] else None

        return {"task_id": int(wire_tuple[0]),
                "workflow_id": int(wire_tuple[1]),
                "node_id": int(wire_tuple[2]),
                "task_args_hash": int(wire_tuple[3]),
                "name": wire_tuple[4],
                "command": wire_tuple[5],
                "status": wire_tuple[6],
                "max_runtime_seconds": max_runtime_seconds,
                "context_args": context_args,
                "resource_scales": resource_scales,
                "queue": wire_tuple[10],
                "num_cores": num_cores,
                "m_mem_free": m_mem_free,
                "j_resource": wire_tuple[13],
                "hard_limits": wire_tuple[14]
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


class SerializeExecutorTaskInstance:
    """Serialize the data to and from the database for an ExecutorTaskInstance."""

    @staticmethod
    def to_wire(task_instance_id: int, workflow_run_id: int,
                executor_id: Union[int, None]) -> tuple:
        """Submit the above args for an ExecutorTaskInstance object to the database."""
        return task_instance_id, workflow_run_id, executor_id

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Retrieve the ExecutorTaskInstance information from the database."""
        executor_id = int(wire_tuple[2]) if wire_tuple[2] else None
        return {"task_instance_id": int(wire_tuple[0]),
                "workflow_run_id": int(wire_tuple[1]),
                "executor_id": executor_id}


class SerializeExecutorTaskInstanceErrorLog:
    """Serialize the data to and from the database for an ExecutorTaskInstanceErrorLog."""

    @staticmethod
    def to_wire(task_instance_error_log_id: int, error_time: datetime,
                description: str) -> tuple:
        """Submit the above args for an SerializeExecutorTaskInstanceErrorLog
        object to the database."""
        return task_instance_error_log_id, error_time, description

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Retrieve the SerializeExecutorTaskInstanceErrorLog information from the database."""
        return {"task_instance_error_log_id": int(wire_tuple[0]),
                "error_time": str(wire_tuple[1]),
                "description": str(wire_tuple[2])}


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


class SerializeClientTaskTemplateVersion:
    """Serialize the data to and from the database for a TaskTemplateVersion."""

    @staticmethod
    def to_wire(task_template_version_id: int, id_name_map: dict) -> tuple:
        """Submit the TaskTemplateVersion information to the database."""
        return (task_template_version_id, id_name_map)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Get the TaskTemplateVersion info from the database."""
        return {"task_template_version_id": int(wire_tuple[0]),
                "id_name_map": wire_tuple[1]}


class SerializeWorkflowRun:
    """Serialize the data to and from the database for a WorkflowRun."""

    @staticmethod
    def to_wire(id: int, workflow_id: int) -> tuple:
        """Submit the WorkflowRun information to the database."""
        return (id, workflow_id)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        """Get the WorkflowRun information from the database."""
        return {"id": int(wire_tuple[0]),
                "workflow_id": int(wire_tuple[1])}
