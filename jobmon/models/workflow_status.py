from jobmon.models import DB


class WorkflowStatus(DB.Model):
    __tablename__ = 'workflow_status'

    CREATED = 'C'
    RUNNING = 'R'
    STOPPED = 'S'
    ERROR = 'E'
    DONE = 'D'

    id = DB.Column(DB.String(1), primary_key=True)
    label = DB.Column(DB.String(150), nullable=False)
