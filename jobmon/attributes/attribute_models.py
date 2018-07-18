from sqlalchemy import Column, ForeignKey, Integer, String
from jobmon.sql_base import Base


class WorkflowAttributeTypes(Base):
    __tablename__ = 'workflow_attribute_types'

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    type = Column(String(255))


class WorkflowAttributes(Base):
    __tablename__ = 'workflow_attributes'

    id = Column(Integer, primary_key=True)
    workflow_id = Column(Integer, ForeignKey('workflow.id'))
    attribute_type = Column(Integer)
    value = Column(String(255))


class WorkflowRunAttributeTypes(Base):
    __tablename__ = 'workflow_run_attribute_types'

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    type = Column(String(255))


class WorkflowRunAttributes(Base):
    __tablename__ = 'workflow_run_attributes'

    id = Column(Integer, primary_key=True)
    workflow_run_id = Column(Integer, ForeignKey('workflow_run.id'))
    attribute_type = Column(Integer)
    value = Column(String(255))


class JobAttributeTypes(Base):
    __tablename__ = 'job_attribute_types'

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    type = Column(String(255))


class JobAttributes(Base):
    __tablename__ = 'job_attributes'

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey('job.job_id'))
    attribute_type = Column(Integer)
    value = Column(String(255))

