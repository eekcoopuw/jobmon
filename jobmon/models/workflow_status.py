from jobmon.models import DB


class WorkflowStatus(DB.Model):
    __tablename__ = 'workflow_status'

    REGISTERED = 'G'
    BOUND = 'B'
    ABORTED = 'A'
    CREATED = 'C'
    RUNNING = 'R'
    FAILED = 'F'
    DONE = 'D'

    id = DB.Column(DB.String(1), primary_key=True)
    label = DB.Column(DB.String(150), nullable=False)
