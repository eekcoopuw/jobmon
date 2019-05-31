

class SwarmJob:
    """
    This is a simplified Job object used on the RESTful API client side
    when only job_id, job_hash, and status are needed.
    """

    def __init__(self, job_id: int, status: str, job_hash: int):
        # Takes one row of the SQL query return
        self.job_id = job_id
        self.status = status
        self.job_hash = job_hash

    @classmethod
    def from_wire(cls, dct):
        return cls(dct["job_id"], dct["status"], int(dct["job_hash"]))
