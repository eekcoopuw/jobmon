"""Task Resources Database Table."""
from jobmon.server.web.models import DB
from jobmon.server.web.models import task_resources_type  # noqa F401


class TaskResources(DB.Model):
    """
    The table in the database that holds all task specific resources:
        queue_id - designated queue
        requested_resources
    """

    __tablename__ = 'task_resources'

    id = DB.Column(DB.Integer, primary_key=True)
    task_id = DB.Column(DB.Integer, DB.ForeignKey('task.id'))
    queue_id = DB.Column(DB.Integer, DB.ForeignKey('queue.id'))
    task_resources_type_id = DB.Column(
        DB.String(1), DB.ForeignKey('task_resources_type.id'))

    resource_scales = DB.Column(DB.String(1000), default=None)

    requested_resources = DB.Column(DB.Text, default=None)

    # ORM relationships
    task = DB.relationship("Task", foreign_keys=[task_id])
    queue = DB.relationship("Queue", foreign_keys=[queue_id])
    task_resources_type = DB.relationship("TaskResourcesType", # noqa F811
                                          foreign_keys=[task_resources_type_id])

    def activate(self):
        """Activate Task Resources on Task object."""
        self.task.task_resources_id = self.id
