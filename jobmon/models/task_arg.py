from jobmon.models import DB


class TaskArg(DB.Model):

    __tablename__ = 'task_arg'

    task_id = DB.Column(DB.Integer, primary_key=True)
    arg_id = DB.Column(DB.Integer, primary_key=True)
    val = DB.Column(DB.String(255))
