from jobmon.models import DB


class Node(DB.Model):

    __tablename__ = 'node'

    id = DB.Column(DB.Integer, primary_key=True)
    task_template_version_id = DB.Column(
        DB.Integer,
        DB.ForeignKey('task_template_version.id')
    )
    node_args_hash = DB.Column(DB.Integer)
