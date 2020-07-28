from __future__ import annotations

import hashlib
from http import HTTPStatus as StatusCodes
from string import Formatter
from typing import Optional, Dict

from jobmon.client import shared_requester
from jobmon.client import ClientLogging as logging
from jobmon.client.requests.requester import Requester
from jobmon.exceptions import InvalidResponse
from jobmon.serializers import SerializeClientTaskTemplateVersion


logger = logging.getLogger(__name__)


class TaskTemplateVersion:

    def __init__(self, task_template_id: int, command_template: str, node_args: list,
                 task_args: list, op_args: list, requester: Requester = shared_requester):
        # id vars
        self.task_template_id = task_template_id
        self.command_template = command_template

        # hash args
        self._node_args: set
        self.node_args = set(node_args)
        self._task_args: set
        self.task_args = set(task_args)
        self._op_args: set
        self.op_args = set(op_args)

        self.requester = requester

    @property
    def template_args(self) -> set:
        """The argument names in the command template"""
        return set([i[1] for i in Formatter().parse(self.command_template)
                    if i[1] is not None])

    @property
    def node_args(self) -> set:
        """any named arguments in command_template that make the command unique
        within this template for a given workflow run. Generally these are
        arguments that can be parallelized over."""
        return self._node_args

    @node_args.setter
    def node_args(self, val: set):
        if self.is_bound:
            raise AttributeError("Cannot set node_args. node_args must be declared during "
                                 "instantiation")
        if not self.template_args.issuperset(val):
            raise ValueError("The format keys declared in command_template must be a "
                             "superset of the keys declared in node_args. Values recieved "
                             f"were --- \ncommand_template is: {self.command_template}. "
                             f"\ncommand_template format keys are {self.template_args}. "
                             f"\nnode_args is: {val}. \nmissing format keys in "
                             f"command_template are {set(val) - self.template_args}.")
        self._node_args = val

    @property
    def task_args(self) -> set:
        """any named arguments in command_template that make the command unique
        across workflows if the node args are the same as a previous workflow.
        Generally these are arguments about data moving though the task."""
        return self._task_args

    @task_args.setter
    def task_args(self, val: set):
        if self.is_bound:
            raise AttributeError("Cannot set task_args. task_args must be declared during "
                                 "instantiation")
        if not self.template_args.issuperset(val):
            raise ValueError("The format keys declared in command_template must bes a "
                             "superset of the keys declared in task_args. Values recieved "
                             f"were --- \ncommand_template is: {self.command_template}. "
                             f"\ncommand_template format keys are {self.template_args}. "
                             f"\nnode_args is: {val}. \nmissing format keys in "
                             f"command_template are {set(val) - self.template_args}.")
        self._task_args = val

    @property
    def op_args(self) -> set:
        """any named arguments in command_template that can change without
        changing the identity of the task. Generally these are things like the
        task executable location or the verbosity of the script."""
        return self._op_args

    @op_args.setter
    def op_args(self, val: set):
        if self.is_bound:
            raise AttributeError("Cannot set op_args. op_args must be declared during "
                                 "instantiation")
        if not self.template_args.issuperset(val):
            raise ValueError("The format keys declared in command_template must be a "
                             "superset of the keys declared in op_args. Values received "
                             f"were --- \ncommand_template is: {self.command_template}. "
                             f"\ncommand_template format keys are {self.template_args}. "
                             f"\nnode_args is: {val}. \nmissing format keys in "
                             f"command_template are {set(val) - self.template_args}.")
        self._op_args = val

    @property
    def arg_mapping_hash(self) -> int:
        hashable = "".join(sorted(self.node_args) + sorted(self.task_args) +
                           sorted(self.op_args))
        return int(hashlib.sha1(hashable.encode('utf-8')).hexdigest(), 16)

    @property
    def is_bound(self) -> bool:
        return hasattr(self, "_task_template_version_id")

    @property
    def id(self) -> int:
        if not self.is_bound:
            raise AttributeError("id cannot be accessed before workflow is bound")
        return self._task_template_version_id

    @property
    def id_name_map(self) -> Dict[str, int]:
        if not self.is_bound:
            raise AttributeError("arg_id_name_map cannot be accessed before workflow is bound")
        return self._id_name_map

    def bind(self) -> None:
        response = self._get_task_template_version_info()
        if response is not None:
            response_dict = SerializeClientTaskTemplateVersion.kwargs_from_wire(response)
        else:
            response = self._insert_task_template_version()
            response_dict = SerializeClientTaskTemplateVersion.kwargs_from_wire(response)

        self._task_template_version_id = response_dict["task_template_version_id"]
        self._id_name_map = response_dict["id_name_map"]

    def _get_task_template_version_info(self) -> Optional[tuple]:
        app_route = f"/task_template/{self.task_template_id}/version"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"arg_mapping_hash": self.arg_mapping_hash,
                     "command_template": self.command_template},
            request_type="get"
        )

        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST request through route '
                f'{app_route}. Expected code 200. Response content: {response}'
            )

        return response["task_template_version"]

    def _insert_task_template_version(self) -> tuple:
        app_route = f"/task_template/{self.task_template_id}/add_version"
        return_code, response = self.requester.send_request(
            app_route=app_route,
            message={"command_template": self.command_template,
                     "arg_mapping_hash": self.arg_mapping_hash,
                     "node_args": list(self.node_args),
                     "task_args": list(self.task_args),
                     "op_args": list(self.op_args)},
            request_type='post'
        )

        if return_code != StatusCodes.OK:
            raise InvalidResponse(
                f'Unexpected status code {return_code} from POST request through route '
                f'{app_route}. Expected code 200. Response content: {response}'
            )

        return response["task_template_version"]
