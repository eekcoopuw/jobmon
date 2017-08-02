from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import func

db = SQLAlchemy()


class Batch(db.Model):
    __tablename__ = 'batch'

    batch_id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(150))
    user = db.Column(db.String(150))
    created_date = db.Column(db.DateTime, default=func.now())


class Job(db.Model):
    __tablename__ = 'job'

    jid = db.Column(db.Integer, primary_key=True)
    batch_id = db.Column(
        db.Integer,
        db.ForeignKey('batch.batch_id'))
    name = db.Column(db.String(150))
    runfile = db.Column(db.String(150))
    args = db.Column(db.String(1000))
    submitted_date = db.Column(db.DateTime, default=func.now())

    def to_wire_format_dict(self):
        """Just the fields that we want to return over the wire and that can be
        serialized to JSON"""
        return self.__dict__



class JobInstance(db.Model):
    __tablename__ = 'job_instance'

    job_instance_id = db.Column(db.Integer, primary_key=True)
    jid = db.Column(
        db.Integer,
        db.ForeignKey('job.jid'),
        nullable=False,
        primary_key=True)
    job_instance_type = db.Column(db.String(50))
    retry_number = db.Column(db.Integer)
    usage_str = db.Column(db.String(250))
    wallclock = db.Column(db.String(50))
    maxvmem = db.Column(db.String(50))
    cpu = db.Column(db.String(50))
    io = db.Column(db.String(50))
    current_status = db.Column(
        db.Integer,
        db.ForeignKey('status.id'),
        nullable=False)
    submitted_date = db.Column(db.DateTime, default=func.now())

    def to_wire_format_dict(self):

        return \
            {
                "job_instance_id": self.job_instance_id,
                "jid": self.jid,
                "current_status": self.current_status
            }

class JobInstanceError(db.Model):
    __tablename__ = 'job_instance_error'

    id = db.Column(db.Integer, primary_key=True)
    job_instance_id = db.Column(
        db.Integer,
        db.ForeignKey('job_instance.job_instance_id'),
        nullable=False)
    error_time = db.Column(db.DateTime, default=func.now())
    description = db.Column(db.String(1000), nullable=False)


class JobInstanceStatus(db.Model):
    __tablename__ = 'job_instance_status'

    id = db.Column(db.Integer, primary_key=True)
    job_instance_id = db.Column(
        db.Integer,
        db.ForeignKey('job_instance.job_instance_id'),
        nullable=False)
    status = db.Column(
        db.Integer,
        db.ForeignKey('status.id'),
        nullable=False)
    status_time = db.Column(db.DateTime, default=func.now())


class Status(db.Model):
    __tablename__ = 'status'

    SUBMITTED = 1
    RUNNING = 2
    FAILED = 3
    COMPLETE = 4
    UNREGISTERED_STATE = 5

    id = db.Column(db.Integer, primary_key=True)
    label = db.Column(db.String(150), nullable=False)

status_names = ['INVALID_ZERO_INDEX', 'SUBMITTED', 'RUNNING', 'FAILED', 'COMPLETE',
                   'UNREGISTERED_STATE']


def load_default_statuses():
    statuses = []
    for status in ['SUBMITTED', 'RUNNING', 'FAILED', 'COMPLETE',
                   'UNREGISTERED_STATE']:
        statuses.append(Status(id=getattr(Status, status), label=status))
    db.session.add_all(statuses)
    db.session.commit()
