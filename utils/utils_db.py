from contextlib import contextmanager
from urllib.parse import urlparse

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

_dbsession = None
_engine = None
_sqlalchemy_url = None


def set_session(sqlalchemy_url=None, engine=None):
    global _dbsession, _engine, _sqlalchemy_url

    _sqlalchemy_url = _sqlalchemy_url or sqlalchemy_url
    if not _sqlalchemy_url:
        return

    _engine = _engine or engine or create_engine(_sqlalchemy_url)
    _dbsession = scoped_session(sessionmaker(bind=_engine))
    return _dbsession


def commit_session(exception=None):
    if exception:
        _dbsession.rollback()
    else:
        _dbsession.commit()
    _dbsession.remove()
    if hasattr(_engine, "dispose"):
        _engine.dispose()


def rollback_session():
    _dbsession.rollback()
    _dbsession.remove()
    if hasattr(_engine, "dispose"):
        _engine.dispose()


def session():
    return _dbsession


@contextmanager
def connect_postgres(uri, env=None):
    connection = "test"
    if uri:
        result = urlparse(uri)
        connection = psycopg2.connect(
            database=result.path[1:],
            user=result.username,
            password=result.password,
            host=result.hostname,
            port=result.port
        )
    try:
        yield connection
    finally:
        if uri:
            connection.close()
