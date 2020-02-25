from jobmon.models import DB


class Arg(DB.Model):

    __tablename__ = 'arg'

    id = DB.Column(DB.Integer, primary_key=True)
    name = DB.Column(DB.String(255))

    command_template_arg_type_mappings = DB.relationship(
        "CommandTemplateArgTypeMapping", back_populates="argument")
