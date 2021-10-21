"""SQLAlchemy database objects."""
from flask_sqlalchemy import SQLAlchemy
from jobmon.server.web.models import *  # noqa F403, F401

# Expire_on_commit set to false, so accessing model attributes post-commit
# does not reload from the database
DB = SQLAlchemy(session_options={"expire_on_commit": False})
