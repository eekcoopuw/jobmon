"""Array Table for the Database."""
from functools import partial

from sqlalchemy.sql import func
from werkzeug.local import LocalProxy

from jobmon.server.web.log_config import get_logger
from jobmon.server.web.models import DB


# new structlog logger per flask request context. internally stored as flask.g.logger
logger = LocalProxy(partial(get_logger, __name__))


class Array(DB.Model):
    """Array Database object."""

    __tablename__ = "array"

    id = DB.Column(DB.Integer, primary_key=True)
    task_template_version_id = DB.Column(DB.Integer)
    workflow_id = DB.Column(DB.Integer)
    task_resources_id = DB.Column(DB.Integer)
    max_concurrently_running = DB.Column(DB.Integer)
    threshold_to_submit = DB.Column(DB.Integer)
    num_completed = DB.Column(DB.Integer, default=None)
    cluster_id = DB.Column(DB.Integer)
    created_date = DB.Column(DB.DateTime, default=func.now())
