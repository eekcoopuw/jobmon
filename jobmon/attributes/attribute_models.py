from sqlalchemy import Column, ForeignKey, Integer, String
from jobmon.models.sql_base import Base
from jobmon.models.workflow import Workflow
from jobmon.models.workflow_run import WorkflowRun


class WorkflowAttributeType(Base):
    __tablename__ = 'workflow_attribute_type'

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    type = Column(String(255))

    @classmethod
    def from_wire(cls, dct):
        return cls(
            id=dct['workflow_attribute_type_id'],
            name=dct['name'],
            type=dct['type']
        )

    def to_wire(self):
        return {
            'workflow_attribute_type_id': self.id,
            'name': self.name,
            'type': self.type
        }


class WorkflowAttribute(Base):
    __tablename__ = 'workflow_attribute'

    id = Column(Integer, primary_key=True)
    workflow_id = Column(Integer,
                         ForeignKey('workflow.id'))
    attribute_type = Column(Integer,
                            ForeignKey('workflow_attribute_type.id'))
    value = Column(String(255))

    @classmethod
    def from_wire(cls, dct):
        return cls(
            id=dct['workflow_attribute_id'],
            workflow_id=dct['workflow_id'],
            attribute_type=dct['attribute_type'],
            value=dct['value']
        )

    def to_wire(self):
        return {
            'workflow_attribute_id': self.id,
            'workflow_id': self.workflow_id,
            'attribute_type': self.attribute_type,
            'value': self.value
        }


class WorkflowRunAttributeType(Base):
    __tablename__ = 'workflow_run_attribute_type'

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    type = Column(String(255))

    @classmethod
    def from_wire(cls, dct):
        return cls(
            id=dct['workflow_run_attribute_type_id'],
            name=dct['name'],
            type=dct['type']
        )

    def to_wire(self):
        return {
            'workflow_run_attribute_type_id': self.id,
            'name': self.name,
            'type': self.type
        }


class WorkflowRunAttribute(Base):
    __tablename__ = 'workflow_run_attribute'

    id = Column(Integer, primary_key=True)
    workflow_run_id = Column(Integer,
                             ForeignKey('workflow_run.id'))
    attribute_type = Column(Integer,
                            ForeignKey('workflow_run_attribute_type.id'))
    value = Column(String(255))

    @classmethod
    def from_wire(cls, dct):
        return cls(
            id=dct['workflow_run_attribute_id'],
            workflow_run_id=dct['workflow_run_id'],
            attribute_type=dct['attribute_type'],
            value=dct['value']
        )

    def to_wire(self):
        return {
            'workflow_run_attribute_id': self.id,
            'workflow_run_id': self.workflow_id,
            'attribute_type': self.attribute_type,
            'value': self.value
        }


class JobAttributeType(Base):
    __tablename__ = 'job_attribute_type'

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    type = Column(String(255))

    @classmethod
    def from_wire(cls, dct):
        return cls(
            id=dct['job_attribute_type_id'],
            name=dct['name'],
            type=dct['type']
        )

    def to_wire(self):
        return {
            'job_attribute_type_id': self.id,
            'name': self.name,
            'type': self.type
        }


class JobAttribute(Base):
    __tablename__ = 'job_attribute'

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer,
                    ForeignKey('job.job_id'))
    attribute_type = Column(Integer,
                            ForeignKey('job_attribute_type.id'))
    value = Column(String(255))

    @classmethod
    def from_wire(cls, dct):
        return cls(
            id=dct['job_attribute_id'],
            job_id=dct['job_id'],
            attribute_type=dct['attribute_type'],
            value=dct['value']
        )

    def to_wire(self):
        return {
            'job_attribute_id': self.id,
            'job_id': self.job_id,
            'attribute_type': self.attribute_type,
            'value': self.value
        }
