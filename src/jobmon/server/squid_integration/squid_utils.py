class QueuedTI:
    """This is a class to hold the task instance.

    So far we only need actual distributor_id and cluster id.
    TODO: in 3.1, the actual distributor_id will be subtask_id in DB.
    """
    def __init__(self):
        self.task_instance_id = None
        self.distributor_id = None
        self.cluster_type_name = None
        self.package_location = None
        self.cluster_id = None

    def tostr(self) -> str:
        return f"taks_instance_id: {self.task_instance_id}, " \
               f"distributor_id: {self.distributor_id}, " \
               f"cluster_name: {self.cluster_type_name}, " \
               f"cluster_id: {self.cluster_id}, " \
               f"package_location: {self.package_location}"

    @staticmethod
    def create_instance_from_db(session, task_instance_id):  # type: ignore
        """Get the ti related info that the integrator needs."""

        # This complicate join will eventually hunt us down.
        # Will be better to store cluster_id instead of cluster_type_id in task_instance.
        # TODO: use subtask_id instead of distributor_id in 3.1
        sql = f"""
            SELECT ti.id as id, distributor_id as distributor_id, 
                ct.name as name, package_location, c.id as cluster_id   
            FROM task t, task_instance ti, task_resources tr, queue q, cluster_type ct, cluster c
            WHERE ti.id = {task_instance_id}
            AND ti.task_id = t.id
            AND t.task_resources_id = tr.id
            AND tr.queue_id = q.id
            AND q.cluster_id = c.id
            AND ti.cluster_type_id = ct.id;
        """
        row = session.execute(sql).fetchone()
        session.commit()
        if row:
            instance = QueuedTI()
            instance.task_instance_id = row["id"]
            instance.distributor_id = row["distributor_id"]
            instance.cluster_type_name = row["name"]
            instance.package_location = row["package_location"]
            instance.cluster_id = row["cluster_id"]
            return instance
        else:
            return None


