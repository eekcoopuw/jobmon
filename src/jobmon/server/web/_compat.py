from sqlalchemy.sql import func
from sqlalchemy.sql.dml import Insert

from jobmon.server.web import session_factory
from jobmon.server.web.server_side_exception import ServerError


def add_ignore(insert_stmt: Insert) -> Insert:
    if session_factory().bind.dialect.name == "mysql":
        insert_stmt = insert_stmt.prefix_with("IGNORE")
    elif session_factory().bind.dialect.name == "sqlite":
        insert_stmt = insert_stmt.prefix_with("OR IGNORE")
    else:
        raise ServerError(
            "invalid sql dialect. Only (mysql, sqlite) are supported. Got"
            + session_factory.bind.dialect.name
        )
    return insert_stmt


def add_time(next_report_increment: float) -> func:
    if session_factory().bind.dialect.name == "mysql":
        add_time_func = func.ADDTIME(
            func.now(), func.SEC_TO_TIME(next_report_increment)
        )
    elif session_factory().bind.dialect.name == "sqlite":
        add_time_func = func.datetime(func.now(), f"+{next_report_increment} seconds")

    else:
        raise ServerError(
            "invalid sql dialect. Only (mysql, sqlite) are supported. Got"
            + session_factory.bind.dialect.name
        )
    return add_time_func
