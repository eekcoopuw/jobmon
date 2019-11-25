from typing import Union
import ast


class SerializeExecutorTask:

    @staticmethod
    def to_wire(task_id: int, workflow_id: int, node_id: int,
                task_args_hash: int, name: str, command: str, status: str,
                max_runtime_seconds: int, context_args: str,
                resource_scales: str, queue: str, num_cores: int,
                m_mem_free: str, j_resource: str, hard_limits: str,
                last_nodename: str, last_process_group_id: int) -> tuple:
        return (task_id, workflow_id, node_id, task_args_hash, name, command,
                status, max_runtime_seconds, context_args, resource_scales,
                queue, num_cores, m_mem_free, j_resource, hard_limits,
                last_nodename, last_process_group_id)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        # coerce types for all nullables that are cast
        # using ast.literal_eval is a potential security issue but was the only
        # solution I could find to turning the data into json twice
        context_args = ast.literal_eval(
            wire_tuple[8]) if wire_tuple[8] else None
        num_cores = int(wire_tuple[11]) if wire_tuple[11] else None
        m_mem_free = float(wire_tuple[12]) if wire_tuple[12] else None
        last_process_group_id = int(wire_tuple[16]) if wire_tuple[16] else None

        return {"task_id": int(wire_tuple[0]),
                "workflow_id": int(wire_tuple[1]),
                "node_id": int(wire_tuple[2]),
                "task_args_hash": int(wire_tuple[3]),
                "name": wire_tuple[4],
                "command": wire_tuple[5],
                "status": wire_tuple[6],
                "max_runtime_seconds": int(wire_tuple[7]),
                "context_args": context_args,
                "resource_scales": ast.literal_eval(wire_tuple[9]),
                "queue": wire_tuple[10],
                "num_cores": num_cores,
                "m_mem_free": m_mem_free,
                "j_resource": wire_tuple[13],
                "hard_limits": wire_tuple[14],
                "last_nodename": wire_tuple[15],
                "last_process_group_id": last_process_group_id
                }


class SerializeSwarmTask:

    @staticmethod
    def to_wire(task_id: int, task_args_hash: int, status: str) -> tuple:
        return task_id, task_args_hash, status

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        return {"task_id": int(wire_tuple[0]),
                "task_args_hash": int(wire_tuple[1]),
                "status": wire_tuple[2]}


class SerializeExecutorTaskInstance:

    @staticmethod
    def to_wire(task_instance_id: int, executor_id: Union[int, None]) -> tuple:
        return task_instance_id, executor_id

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        executor_id = int(wire_tuple[1]) if wire_tuple[1] else None
        return {"task_instance_id": int(wire_tuple[0]),
                "executor_id": executor_id}


class SerializeClientTool:

    @staticmethod
    def to_wire(id: int, name: str) -> tuple:
        return (id, name)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        return {"id": int(wire_tuple[0]), "name": wire_tuple[1]}


class SerializeClientToolVersion:

    @staticmethod
    def to_wire(id: int, tool_id: int) -> tuple:
        return (id, tool_id)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        return {"id": int(wire_tuple[0]), "tool_id": int(wire_tuple[1])}


class SerializeClientTaskTemplateVersion:

    @staticmethod
    def to_wire(task_template_version_id: int, id_name_map: dict) -> tuple:
        return (task_template_version_id, id_name_map)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple) -> dict:
        return {"task_template_version_id": int(wire_tuple[0]),
                "id_name_map": wire_tuple[1]}
