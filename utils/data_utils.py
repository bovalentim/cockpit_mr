import os

import pandas as pd
import streamlit as st
import datetime
import pytz

from utils.utils_db import connect_postgres


def get_current_local_time():
    return datetime.now(pytz.timezone("America/Sao_Paulo")).replace(tzinfo=None)

@st.experimental_singleton
def get_boletos_prod():
    # bH_PROD
    URI = os.getenv("DB_BH")
    query_formalizados = """
        select *,
	case when status is not null and (current_date::date - expire_on) > 90 then 'nok'
	else 'ok' end as pmt_situation
from db_boletos
        """
    with connect_postgres(URI) as conn:
        df_boletos = pd.read_sql(query_formalizados, conn)

    return df_boletos


@st.experimental_singleton
def get_resps_gcs():
    # DB_SOLFACIL
    URI = os.getenv("DB_RISK")
    query_formalizados = """
select fin.financiamento_id,
    p.id_parceiro,
    fin.parceiro_id as parcerio_id,
    p.cnpj,
    p.nome_fantasia,
    p.nome_completo,
    p.email, fin.dias_de_atraso
    from financiamentos fin
    left join partners p on fin.parceiro_id = p.id_parceiro 
    """
    with connect_postgres(URI) as conn:
        df_responsaveis = pd.read_sql(query_formalizados, conn)

    return df_responsaveis

def get_nfe_assessment(financiamento_id):
    # DB_SOLFACIL
    URI = os.getenv("DB_RISK")
    query_formalizados = f"""
select * from (
select row_number() over (partition by nfe_key  order by created_at desc) as assessment_index,
na.* 
from nfe_assessment na
) tb 
where assessment_index = 1
--and id_financiamento = '497614'
and id_financiamento = {financiamento_id}
order by created_at desc
--group by 1 order by 2 desc
    """
    with connect_postgres(URI) as conn:
        df_responsaveis = pd.read_sql(query_formalizados, conn)

    return df_responsaveis

