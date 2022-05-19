from sqlalchemy.sql.dml import Insert

from jobmon.server.web.routes import SessionLocal
from jobmon.server.web.server_side_exception import ServerError


def add_ignore(insert_stmt: Insert) -> Insert:
    if SessionLocal.bind.dialect.name == "mysql":
        insert_stmt = insert_stmt.prefix_with("IGNORE")
    elif SessionLocal.bind.dialect.name == "sqlite":
        insert_stmt = insert_stmt.prefix_with("OR IGNORE")
    else:
        raise ServerError(
            "invalid sql dialect. Only (mysql, sqlite) are supported. Got"
            + SessionLocal.bind.dialect.name
        )
    return insert_stmt
