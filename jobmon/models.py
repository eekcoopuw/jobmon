from sqlalchemy.sql import func
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class Batch(Base):
    __tablename__ = 'batch'

    batch_id = Column(Integer, primary_key=True)
    name = Column(String(150))
    user = Column(String(150))
    created_date = Column(DateTime, default=func.now())


class Job(Base):
    __tablename__ = 'job'

    jid = Column(Integer, primary_key=True)
    batch_id = Column(
        Integer,
        ForeignKey('batch.batch_id'))
    name = Column(String(150))
    runfile = Column(String(150))
    args = Column(String(1000))
    submitted_date = Column(DateTime, default=func.now())

    def to_wire_format_dict(self):
        """Just the fields that we want to return over the wire and that can be
        serialized to JSON"""
        return \
            {
                "jid": self.jid,
                "name": self.name
            }


class JobInstance(Base):
    __tablename__ = 'job_instance'

    job_instance_id = Column(Integer, primary_key=True)
    jid = Column(
        Integer,
        ForeignKey('job.jid'),
        nullable=False)
    job_instance_type = Column(String(50))
    retry_number = Column(Integer)
    usage_str = Column(String(250))
    wallclock = Column(String(50))
    maxvmem = Column(String(50))
    cpu = Column(String(50))
    io = Column(String(50))
    current_status = Column(
        Integer,
        ForeignKey('status.id'),
        nullable=False)
    submitted_date = Column(DateTime, default=func.now())

    def to_wire_format_dict(self):

        return \
            {
                "job_instance_id": self.job_instance_id,
                "jid": self.jid,
                "current_status": self.current_status
            }


class JobInstanceError(Base):
    __tablename__ = 'job_instance_error'

    id = Column(Integer, primary_key=True)
    job_instance_id = Column(
        Integer,
        ForeignKey('job_instance.job_instance_id'),
        nullable=False)
    error_time = Column(DateTime, default=func.now())
    description = Column(String(1000), nullable=False)


class JobInstanceStatus(Base):
    __tablename__ = 'job_instance_status'

    id = Column(Integer, primary_key=True)
    job_instance_id = Column(
        Integer,
        ForeignKey('job_instance.job_instance_id'),
        nullable=False)
    status = Column(
        Integer,
        ForeignKey('status.id'),
        nullable=False)
    status_time = Column(DateTime, default=func.now())


class Status(Base):
    __tablename__ = 'status'

    SUBMITTED = 1
    RUNNING = 2
    FAILED = 3
    COMPLETE = 4
    UNREGISTERED_STATE = 5

    id = Column(Integer, primary_key=True)
    label = Column(String(150), nullable=False)


def load_default_statuses(session):
    statuses = []
    for status in ['SUBMITTED', 'RUNNING', 'FAILED', 'COMPLETE',
                   'UNREGISTERED_STATE']:
        statuses.append(Status(id=getattr(Status, status), label=status))
    session.add_all(statuses)
    session.commit()
