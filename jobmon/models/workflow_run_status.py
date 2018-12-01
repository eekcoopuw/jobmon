import logging

from sqlalchemy import DB.Column, DB.DateTime, DB.ForeignKey, DB.Integer, DB.String
from sqlalchemy.orm import DB.relationship

from jobmon.models.sql_base import DB.model


logger = logging.getLogger(__name__)


class WorkflowRunStatus(DB.model):
    __tablename__ = 'workflow_run_status'

    RUNNING = 'R'
    STOPPED = 'S'
    ERROR = 'E'
    DONE = 'D'

    id = DB.Column(DB.String(1), primary_key=True)
    label = DB.Column(DB.String(150), nullable=False)
