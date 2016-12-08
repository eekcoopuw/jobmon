from sqlalchemy.sql import func
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class Job(Base):
    __tablename__ = 'job'

    monitored_jid = Column(Integer, primary_key=True)
    sge_id = Column(Integer)
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

    def to_wire_format_dict(self):
        """Just the fields that we want to reutnr over the wire and that can be serialized to JSON"""
        return \
            {
                "monitored_jid": self.monitored_jid,
                "sge_id": self.sge_id,
                "name": self.name,
                "current_status": self.current_status
            }
        # return \
        #     '{' + \
        #         ' "monitored_jid": {}, "sge_id": {}, "name": "{}", "current_status": {}'.format(
        #             self.monitored_jid, self.sge_id, self.name, self.current_status
        #         ) + \
        #     '}'


class JobStatus(Base):
    __tablename__ = 'job_status'

    id = Column(Integer, primary_key=True)
    monitored_jid = Column(
            Integer,
            ForeignKey('job.monitored_jid'),
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

    id = Column(Integer, primary_key=True)
    label = Column(String(150), nullable=False)


class JobError(Base):
    __tablename__ = 'error'

    id = Column(Integer, primary_key=True)
    monitored_jid = Column(
            Integer,
            ForeignKey('job.monitored_jid'),
            nullable=False)
    error_time = Column(DateTime, default=func.now())
    description = Column(String(1000), nullable=False)


def load_default_statuses(session):
    statuses = []
    for status in ['SUBMITTED', 'RUNNING', 'FAILED', 'COMPLETE']:
        statuses.append(Status(id=getattr(Status, status), label=status))
    session.add_all(statuses)
    session.commit()
