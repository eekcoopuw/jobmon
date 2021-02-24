"""SQLAlchemy database objects."""
from flask_sqlalchemy import SQLAlchemy
from jobmon.server.web.models import *  # noqa F403, F401

DB = SQLAlchemy()
