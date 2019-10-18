from jobmon.models import DB


class Node(DB.model):

    __tablename__ = 'node'

    id = DB.Column(DB.Integer, primary_key=True)
    dag_id = DB.Column(DB.Integer, DB.ForeignKey('dag.id'))
    task_template_version_id = DB.Column(
        DB.Integer,
        DB.ForeignKey('task_template_version.id')
    )
    node_arg_hash = DB.Column(DB.Integer)
