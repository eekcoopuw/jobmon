"""Routes used by task instances on worker nodes."""
from datetime import datetime, timedelta
from http import HTTPStatus as StatusCodes
import os
from typing import Any

from flask import current_app, jsonify
from sqlalchemy import func, select
from sqlalchemy import orm
from structlog import get_logger

from jobmon.server.web import session_factory

# scoped session associated with the current thread
SessionLocal = orm.scoped_session(session_factory)


logger = get_logger(__name__)


# ############################ SHARED LANDING ROUTES ##########################################
def is_alive() -> Any:
    """Action that sends a response to the requester indicating that responder is listening."""
    logger.info(f"{os.getpid()}: {current_app.__class__.__name__} received is_alive?")
    resp = jsonify(msg="Yes, I am alive")
    resp.status_code = StatusCodes.OK
    return resp


def _get_time() -> str:
    with SessionLocal() as session:
        db_time = session.execute(select(func.now())).scalar()
        str_time = db_time.strftime("%Y-%m-%d %H:%M:%S")
    return str_time


def get_pst_now() -> Any:
    """Get the time from the database."""
    time = _get_time()
    resp = jsonify(time=time)
    resp.status_code = StatusCodes.OK
    return resp


def health() -> Any:
    """Test connectivity to the database.

    Return 200 if everything is OK. Defined in each module with a different route, so it can
    be checked individually.
    """
    _get_time()
    resp = jsonify(status="OK")
    resp.status_code = StatusCodes.OK
    return resp


__CONNECTION_POOL_RESET__: datetime = None
__RESET_SKIP_SECONDS = 5


def reset_connection_pool() -> Any:
    """Reset the engine's connection pool (primarily for db hot cutover)."""
    global __CONNECTION_POOL_RESET__
    if (
        __CONNECTION_POOL_RESET__ is None
        or datetime.now()
        > __CONNECTION_POOL_RESET__ + timedelta(seconds=__RESET_SKIP_SECONDS)
    ):
        engine = SessionLocal().get_bind()
        # A new connection pool is created immediately after the old one has been disposed
        engine.dispose()
        __CONNECTION_POOL_RESET__ = datetime.now()
        logger.info(
            f"{os.getpid()}: {current_app.__class__.__name__} "
            f"reset the engine's connection pool at {__CONNECTION_POOL_RESET__}"
        )
        resp = jsonify(
            msg=f"Engine's connection pool has been reset "
            f"at {__CONNECTION_POOL_RESET__}"
        )
    else:
        resp = jsonify(
            msg=f"Engine's connection pool reset skipped " f"at {datetime.now()}"
        )
    resp.status_code = StatusCodes.OK
    return resp


# ############################ TESTING ROUTES ################################################
def test_route() -> None:
    """Test route to force a 500 error."""
    session = SessionLocal()
    with session.begin():
        session.execute("SELECT * FROM blip_bloop_table").all()
        session.commit()
