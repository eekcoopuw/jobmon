from jobmon.models import DB


class Arg(DB.Model):

    __tablename__ = 'arg'

    id = DB.Column(DB.Integer, primary_key=True)
    name = DB.Column(DB.String(255))

    template_arg_map = DB.relationship("TemplateArgMap", back_populates="argument")
