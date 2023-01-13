from datetime import datetime
import os
import traceback
import pytz
import psycopg2
from urllib.parse import urlparse
from contextlib import contextmanager
import requests

from sqlalchemy.sql import text
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from streamlit import session_state as cache

_dbsession = None
_engine = None
_sqlalchemy_url = None


def set_session(sqlalchemy_url=None, engine=None):
    global _dbsession, _engine, _sqlalchemy_url
    _engine = create_engine(sqlalchemy_url)
    _dbsession = scoped_session(sessionmaker(bind=_engine, autoflush=True))
    return _dbsession

def commit_session(exception=None):
    if exception:
        _dbsession.rollback()
    else:
        _dbsession.commit()

    _dbsession.close()
    _dbsession.remove()
    _dbsession.flush()

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


def sql_query(env = 'env', consult = True, r_insert = False, SQL='SELECT * FROM credito_historico_analises'):
    
    try:
        if env == 'prod':
            uri = os.getenv("DB_PROD")
        else:
            uri = os.getenv("DB_URI")

        set_session(uri)
        
        query = _dbsession.execute(text(SQL))
        
        if consult:
            data = query.fetchall()
        else:
            data = None

            if r_insert:
                data = query.fetchone()
                data = data[0]
        
        commit_session()
        
    except Exception:
        data = None
        e = str(traceback.format_exc())
        wrn_wrn(local = 'Conexão DB', aviso=e)
        commit_session(exception=e)
        
    finally:
        return data

def wrn_wrn(self, local, aviso):
    now = datetime.now()
    API_SLACK = "https://hooks.slack.com/services/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"
    data = {
        "channel": "CBR2V3XEX",
        "blocks": [{
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "Aviso Cockpit crédito"}}, ],
        "attachments": [
            {"color": "#ffff00",
                "blocks": [{
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f'{local} - Hora: {now.strftime("%d/%m/%Y %H:%M:%S")}'
                    }},
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f'Nome analista: {cache.dados_analista["nome"]}'
                        }},
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": str(aviso)
                        }}
                ]}]}
    requests.post(url=API_SLACK, json=data)
