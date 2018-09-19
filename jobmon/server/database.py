from contextlib import contextmanager
import sqlalchemy as sql
from sqlalchemy.orm import sessionmaker, scoped_session
import logging

from jobmon.server.the_server_config import get_the_server_config


logger = logging.getLogger(__name__)
engine = sql.create_engine(get_the_server_config().conn_str, pool_recycle=300,
                           pool_size=3, max_overflow=100, pool_timeout=120)
logger.info("conn_str is {}".format(engine.url))
Session = sessionmaker(bind=engine)
ScopedSession = scoped_session(Session)


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()

    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def recreate_engine():
    """Recreate the engine with any changes to the Session or ScopedSession"""
    global engine, Session, ScopedSession
    engine = sql.create_engine(get_the_server_config().conn_str,
                               pool_recycle=300, pool_size=3,
                               max_overflow=100, pool_timeout=120)
    Session = sessionmaker(bind=engine)
    ScopedSession = scoped_session(Session)
