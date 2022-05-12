"""Task Resources Database Table."""
from jobmon.serializers import SerializeTaskResources
from jobmon.server.web.models import DB
from jobmon.server.web.models import task_resources_type  # noqa F401


class TaskResources(DB.Model):
    """The table in the database that holds all task specific resources.

    Task specific resources:
        queue_id - designated queue
        requested_resources
    """

    __tablename__ = "task_resources"

    def to_wire_as_task_resources(self) -> tuple:
        """Serialize executor task object."""
        serialized = SerializeTaskResources.to_wire(
            task_resources_id=self.id,
            queue_name=self.queue.name,
            task_resources_type_id=self.task_resources_type_id,
            requested_resources=self.requested_resources,
        )
        return serialized

    id = DB.Column(DB.Integer, primary_key=True)
    queue_id = DB.Column(DB.Integer, DB.ForeignKey("queue.id"))
    task_resources_type_id = DB.Column(
        DB.String(1), DB.ForeignKey("task_resources_type.id")
    )

    requested_resources = DB.Column(DB.Text, default=None)

    # ORM relationships
    queue = DB.relationship("Queue", foreign_keys=[queue_id])
    task_resources_type = DB.relationship(
        "TaskResourcesType", foreign_keys=[task_resources_type_id]
    )
