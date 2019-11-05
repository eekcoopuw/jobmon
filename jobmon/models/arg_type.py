from jobmon.models import DB


class ArgType(DB.Model):

    __tablename__ = 'arg_type'

    NODE_ARG = 1
    TASK_ARG = 2
    OP_ARG = 3

    def to_wire_as_client_tool(self) -> tuple:
        pass

    id = DB.Column(DB.Integer, primary_key=True)
    name = DB.Column(DB.String(255), nullable=False)

    command_template_arg_type_mappings = DB.relationship(
        "CommandTemplateArgTypeMapping", back_populates="argument_type")
