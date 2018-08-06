from contextlib import contextmanager
import sqlalchemy as sql
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import StaticPool

from jobmon.config import config


if 'sqlite' in config.conn_str:

    # TODO: I've intermittently seen transaction errors when using a
    # sqlite backend. If those continue, investigate these sections of the
    # sqlalchemy docs:
    #
    # http://docs.sqlalchemy.org/en/latest/dialects/sqlite.html#sqlite-isolation-level
    # http://docs.sqlalchemy.org/en/latest/dialects/sqlite.html#pysqlite-serializable
    #
    # There seem to be some known issues with the pysqlite driver...
    engine = sql.create_engine(config.conn_str,
                               connect_args={'check_same_thread': False},
                               poolclass=StaticPool)
else:
    engine = sql.create_engine(config.conn_str, pool_recycle=300,
                               pool_size=3, max_overflow=100, pool_timeout=120)
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
    global engine, Session, ScopedSession
    engine = sql.create_engine(config.conn_str, pool_recycle=300,
                               pool_size=3, max_overflow=100, pool_timeout=120)
    Session = sessionmaker(bind=engine)
    ScopedSession = scoped_session(Session)
