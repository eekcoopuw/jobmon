from sqlalchemy.sql import func
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class Job(Base):
    __tablename__ = 'job'

    jid = Column(Integer, primary_key=True)
    sge_jid = Column(Integer)
    name = Column(String(150))
    runfile = Column(String(150))
    args = Column(String(1000))
    usage_str = Column(String(250))
    wallclock = Column(String(50))
    maxvmem = Column(String(50))
    cpu = Column(String(50))
    io = Column(String(50))
    current_status = Column(Integer, nullable=False)
    submitted_date = Column(DateTime, default=func.now())


class JobStatus(Base):
    __tablename__ = 'job_status'

    id = Column(Integer, primary_key=True)
    jid = Column(
            Integer,
            ForeignKey('job.jid'),
            nullable=False)
    status = Column(
            Integer,
            ForeignKey('status.id'),
            nullable=False)
    status_time = Column(DateTime, default=func.now())


class Status(Base):
    __tablename__ = 'status'

    id = Column(Integer, primary_key=True)
    label = Column(String(150), nullable=False)


class JobError(Base):
    __tablename__ = 'error'

    id = Column(Integer, primary_key=True)
    jid = Column(
            Integer,
            ForeignKey('job.jid'),
            nullable=False)
    error_time = Column(DateTime, default=func.now())
    description = Column(String(1000), nullable=False)


def default_statuses(session):
    statuses = []
    for i, s in enumerate(['submitted', 'running', 'failed', 'complete']):
        statuses.append(Status(id=i+1, label=s))
    session.add_all(statuses)
    session.commit()
