from contextlib import contextmanager
import sqlalchemy as sql
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from jobmon.config import config


def ephemera_session():
    engine = sql.create_engine('sqlite://',
                               connect_args={'check_same_thread': False},
                               poolclass=StaticPool)
    Session = sessionmaker(bind=engine)
    return Session


def db_session():
    engine = sql.create_engine(config.conn_str, pool_recycle=300,
                               pool_size=3, max_overflow=100, pool_timeout=120)
    Session = sessionmaker(bind=engine)
    return Session


@contextmanager
def session_scope(ephemera=False):
    """Provide a transactional scope around a series of operations."""
    if ephemera:
        Session = ephemera_session()
    else:
        Session = db_session()
    session = Session()

    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
