from jobmon.models.job import Job


class StubJob:
    """
    This is a simplified Job object used on the RESTful API client side
    when only job_id, job_hash, and status are needed.
    """

    def __init__(self, job: list):
        # Takes one row of the SQL query return
        self.job_id = job[0]
        self.status = job[1]
        self.job_hash = int(job[2])

    @staticmethod
    def job_to_dict(job: Job):
        return {'job_id': job.job_id,
                'job_hash': job.job_hash,
                'status': job.status}

    @classmethod
    def job_to_stub(cls, job: Job):
        return cls([job.job_id, job.status, job.job_hash])

    @classmethod
    def from_wire(cls, listOfDict):
        return_list: list = []
        for dict in listOfDict:
            a: list = [dict["job_id"], dict["status"], dict["job_hash"]]
            return_list.append(cls(a))
        return return_list

    def to_wire(self):
        return {'job_id': self.job_id,
                'job_hash': self.job_hash,
                'status': self.status,
                }
