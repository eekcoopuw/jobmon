from flask import Blueprint

jobmon_client = Blueprint("jobmon_client", __name__)

from . import jobmon_client_2