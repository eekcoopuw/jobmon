"""Database Table for Task Template Versions."""
from typing import Dict, List

from sqlalchemy import Column, ForeignKey, Integer, Text
from sqlalchemy.orm import relationship

from jobmon.serializers import SerializeClientTaskTemplateVersion
from jobmon.server.web.models import Base
from jobmon.server.web.models.arg_type import ArgType


class TaskTemplateVersion(Base):
    """Database Table for Task Template Versions."""

    __tablename__ = "task_template_version"

    def to_wire_as_client_task_template_version(self) -> tuple:
        """Serialized Task Template Version objects."""
        # serialized = SerializeClientTool.to_wire(id=self.id, name=self.name)
        # return serialized
        id_name_map = {}
        args_by_type: Dict[str, List[str]] = {
            "node_args": [],
            "task_args": [],
            "op_args": [],
        }
        for arg_mapping in self.template_arg_map:
            id_name_map[arg_mapping.argument.name] = arg_mapping.argument.id

            if arg_mapping.arg_type_id == ArgType.NODE_ARG:
                args_by_type["node_args"].append(arg_mapping.argument.name)
            if arg_mapping.arg_type_id == ArgType.TASK_ARG:
                args_by_type["task_args"].append(arg_mapping.argument.name)
            if arg_mapping.arg_type_id == ArgType.OP_ARG:
                args_by_type["op_args"].append(arg_mapping.argument.name)

        return SerializeClientTaskTemplateVersion.to_wire(
            task_template_version_id=self.id,
            command_template=self.command_template,
            node_args=args_by_type["node_args"],
            task_args=args_by_type["task_args"],
            op_args=args_by_type["op_args"],
            id_name_map=id_name_map,
            task_template_id=self.task_template.id,
        )

    id = Column(Integer, primary_key=True)
    task_template_id = Column(Integer, ForeignKey("task_template.id"))
    command_template = Column(Text)
    arg_mapping_hash = Column(Integer)

    # orm relationship
    task_template = relationship(
        "TaskTemplate", back_populates="task_template_versions"
    )
    template_arg_map = relationship(
        "TemplateArgMap", back_populates="task_template_version"
    )
