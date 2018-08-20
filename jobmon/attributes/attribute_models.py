from sqlalchemy import Column, ForeignKey, Integer, String
from jobmon.models.sql_base import Base


class WorkflowAttributeType(Base):
    __tablename__ = 'workflow_attribute_type'

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    type = Column(String(255))


class WorkflowAttribute(Base):
    __tablename__ = 'workflow_attribute'

    id = Column(Integer, primary_key=True)
    workflow_id = Column(Integer,
                         ForeignKey('workflow.id'))
    attribute_type = Column(Integer,
                            ForeignKey('workflow_attribute_type.id'))
    value = Column(String(255))


class WorkflowRunAttributeType(Base):
    __tablename__ = 'workflow_run_attribute_type'

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    type = Column(String(255))


class WorkflowRunAttribute(Base):
    __tablename__ = 'workflow_run_attribute'

    id = Column(Integer, primary_key=True)
    workflow_run_id = Column(Integer,
                             ForeignKey('workflow_run.id'))
    attribute_type = Column(Integer,
                            ForeignKey('workflow_run_attribute_type.id'))
    value = Column(String(255))


class JobAttributeType(Base):
    __tablename__ = 'job_attribute_type'

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    type = Column(String(255))


class JobAttribute(Base):
    __tablename__ = 'job_attribute'

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer,
                    ForeignKey('job.job_id'))
    attribute_type = Column(Integer,
                            ForeignKey('job_attribute_type.id'))
    value = Column(String(255))
