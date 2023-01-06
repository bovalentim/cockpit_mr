import pandas as pd
import streamlit as st

from modules.authenticate import login_block
from utils.data_utils import get_resps_gcs, get_boletos_prod, get_nfe_assessment
from utils.utils import show_df_aggrid, get_current_local_time,set_logo,apply_formats,format_bool
from metabase_api.metabase import Metabase

st.set_page_config(layout="wide", page_title="Cockpit Análise MR", page_icon="favicon_solfacil.png")
set_logo()
st.markdown("<h2 style='text-align: center; color: #212121;'>Cockpit MR</h2>", unsafe_allow_html=True)

if "email" not in st.session_state:
    st.session_state.email = None

login_block()
show_cols = ['financiamento_id',
             'nome_cliente', 'numero_parcela', 'status',
             'total_amount', 'expire_on', 'description', 'pdf_link',
             'typable_line', 'nome_fantasia']
rename_cols = {"id": 'ID', "identifier": 'Identificador Boleto',
               "installments_number": 'Total de Parcelas',
               "nome_cliente": 'Nome Cliente',
               "numero_parcela": 'Parcela Nº:',
               "status": 'Status Boleto',
               "status_fin": 'Status Financiamento',
               "type": 'Tipo Boleto',
               "total_amount": 'Total Boleto',
               "expire_on": 'Data Vencimento',
               "description": 'Descrição',
               "pdf_link": 'Link Boleto PDF',
               "typable_line": 'Linha Numérica - Boleto', "financiamento_id": 'ID Financiamento', "numero_ccb": 'CCB',
               "parcerio_id": 'ID Parceiro',
               "cnpj_x": 'CNPJ',
               "nome_fantasia": 'Nome Fantasia', 'nome_completo': "GC Responsável", 'email': "E-mail GC",
               "pkey": "Pkey Parceiro", "etapa": 'Etapa Financiamento', "dias_de_atraso": 'Dias de Atraso - Boleto',
               "pmt_situation": 'Situação Boleto'
               }




# Permissionamento:

map_permissions = {"superuser": "Gerencial",
                   "admin": "Gerencial",
                   "user": "Comercial",
                   }

view_details = map_permissions.get(st.session_state.access, None)

if not view_details:
    st.error("Você não tem permissão para visualizar")
    st.stop()

gc_detail = None
########Cockpit_MR:
first_input = None
first_input = str(st.text_input("Financiamento ID: ", help='ID para início da análise')).replace('.','')
clear_analysis = None
if first_input:
    m=Metabase()
    start_analysis = get_current_local_time()
    mr_ids = pd.DataFrame(m.get_question('803'))
    nfe_info = get_nfe_assessment(first_input)
    this_analysis = mr_ids.query(f"id == {first_input}").copy()
    cols = st.columns(2)
    cols[0].subheader('Dados do Financiamento')
    cols[0].markdown(f'''<span style="color:green">**ID Financiamento:**</span> {this_analysis["etapa"].values[0]} <br>
            <span style="color:green">**Valor Projeto:**</span> R$ {this_analysis["valor_do_projeto"].values[0]} <br>
            <span style="color:green">**Parceiro:**</span> {this_analysis['cnpj_parceiro'].values[0]} <br>
            <span style="color:green">**Score Prioridade:**</span> {int(this_analysis['score_final'].values[0])} <br>
'''
                     , True)
    cols[1].subheader('Dados do NFe')
    cols[1].markdown(f'''<span style="color:green">**Match CPF:**</span> {format_bool(nfe_info["match_cpf"].values[0])} <br>
                <span style="color:green">**Match Valor Financiado:**</span> R$ {format_bool(nfe_info["match_valor_financiado"].values[0])} <br>
                <span style="color:green">**Match Valor Equipamento:**</span> {format_bool(nfe_info['match_valor_equipamento'].values[0])} <br>
                <span style="color:green">**Match Cidade:**</span> {format_bool(int(nfe_info['match_cidade'].values[0]))} <br>
                <span style="color:green">**Match Número:**</span> {format_bool(int(nfe_info['match_numero'].values[0]))} <br>
                <span style="color:green">**Match Estado:**</span> {format_bool(int(nfe_info['match_uf'].values[0]))} <br>
                
    '''
                    , True)
    import ipdb;ipdb.set_trace()