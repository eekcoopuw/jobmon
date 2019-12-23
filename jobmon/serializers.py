from typing import Union
import ast


class SerializeExecutorJob:

    @staticmethod
    def to_wire(dag_id: int, job_id: int, name: str, job_hash: int,
                command: str, status: str, max_runtime_seconds: int,
                context_args: str, resource_scales: str, queue: str,
                num_cores: int, m_mem_free: str, j_resource: str,
                last_nodename: str, last_process_group_id: int,
                hard_limits: str) -> tuple:
        return (dag_id, job_id, name, job_hash, command, status,
                max_runtime_seconds, context_args, resource_scales, queue,
                num_cores, m_mem_free, j_resource, last_nodename,
                last_process_group_id, hard_limits)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple):
        # coerce types for all nullables that are cast
        # using ast.literal_eval is a potential security issue but was the only
        # solution I could find to turning the data into json twice
        context_args = ast.literal_eval(
            wire_tuple[7]) if wire_tuple[7] else None
        resource_scales = ast.literal_eval(
            wire_tuple[8]) if wire_tuple[8] else None
        num_cores = int(wire_tuple[10]) if wire_tuple[10] else None
        m_mem_free = float(wire_tuple[11]) if wire_tuple[11] else None
        last_process_group_id = int(wire_tuple[14]) if wire_tuple[14] else None

        return {"dag_id": int(wire_tuple[0]),
                "job_id": int(wire_tuple[1]),
                "name": wire_tuple[2],
                "job_hash": int(wire_tuple[3]),
                "command": wire_tuple[4],
                "status": wire_tuple[5],
                "max_runtime_seconds": int(wire_tuple[6]),
                "context_args": context_args,
                "resource_scales": resource_scales,
                "queue": wire_tuple[9],
                "num_cores": num_cores,
                "m_mem_free": m_mem_free,
                "j_resource": wire_tuple[12],
                "last_nodename": wire_tuple[13],
                "last_process_group_id": last_process_group_id,
                "hard_limits": wire_tuple[15]}


class SerializeSwarmJob:

    @staticmethod
    def to_wire(job_id: int, job_hash: int, status: str) -> tuple:
        return (job_id, job_hash, status)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple):
        return {"job_id": int(wire_tuple[0]),
                "job_hash": int(wire_tuple[1]),
                "status": wire_tuple[2]}


class SerializeExecutorJobInstance:

    @staticmethod
    def to_wire(job_instance_id: int, executor_id: Union[int, None]) -> tuple:
        return (job_instance_id, executor_id)

    @staticmethod
    def kwargs_from_wire(wire_tuple: tuple):
        executor_id = int(wire_tuple[1]) if wire_tuple[1] else None
        return {"job_instance_id": int(wire_tuple[0]),
                "executor_id": executor_id}
