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

def get_nfe_assessment():
    URI = os.getenv("DB_RISK")
    query = f"""select fin.financiamento_criado_em,
fin.financiamento_id,
nfa.has_file,
nfa.read_nfe,
nfa.parsed_nfe,
nfa.rs_watt_nfe,
fin.valor_do_projeto,
fin.valor_financiado,
--fin.etapa,
--fin.status,
--fin.num_etapa,
nfa.nfe_key as nfkey,
nfp.cpf_cnpj_nfe,
nfp.numero_nfe,
nfp.cfop_nfe,
nfp.valor_nfe,
nfp.potencia_nfe,
nfp.city_nfe,
nfp.uf_nfe,
nfp.financiamento_id as fin_id_nfp,
nfp.status_nfe,
nfa.match_cpf,
nfa.match_cep,
nfa.match_numero,
nfa.match_rua,
nfa.match_valor_financiado,
nfa.match_valor_equipamento,
nfa.match_cidade,
nfa.match_uf,
nfa.match_potencia,
nfa.list_files,
nfa.has_file,
nfa.match_fornecedor,
nfa.has_boleto,
nfa.has_multiple_files,
nfa.has_comission,
nfa.has_insurance

from financiamentos fin
left join (select *,
row_number() over (partition by financiamento_id order by created_at desc) as index 
from nfe_parsed) nfp on fin.financiamento_id::varchar = nfp.financiamento_id::varchar and nfp.index = 1
left join (select *,
row_number() over (partition by id_financiamento order by created_at desc) as index 
from nfe_assessment) nfa on fin.financiamento_id::varchar = nfa.id_financiamento::varchar and nfa.index = 1
where fin.formalizado = 'Formalizado'
and num_etapa > 10 and num_etapa < 14
order by formalizado_em desc
    """
    with connect_postgres(URI) as conn:
        df = pd.read_sql(query, conn)

    return df

