from jobmon.models import DB


class Arg(DB.Model):

    __tablename__ = 'arg'

    def to_wire_as_client_tool(self) -> tuple:
        pass

    id = DB.Column(DB.Integer, primary_key=True)
    name = DB.Column(DB.String(255), nullable=False)

    command_template_arg_type_mappings = DB.relationship(
        "CommandTemplateArgTypeMapping", back_populates="argument")
