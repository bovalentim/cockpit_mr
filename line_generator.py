from utils.data_utils import get_nfe_assessment
from utilities.metabase.metabase import metabase
from utils.utils import show_df_aggrid, set_logo,get_current_local_time,format_bool
import streamlit as st



def generate_lines():
    st.set_page_config(layout="wide", page_title="Cockpit Análise MR", page_icon="favicon_solfacil.png")
    set_logo()
    st.markdown("<h2 style='text-align: center; color: #212121;'>Cockpit MR</h2>", unsafe_allow_html=True)
    m=metabase()
    df_mr_meta = m.get_question('2635')
    df_nfes = get_nfe_assessment()
    df_filas = df_mr_meta.merge(df_nfes,on='financiamento_id')
##Fila 1:
    # df_filas.loc[(('read_nfe' == 1)&('has_multiple_files' == 0)&(df_filas['valor_nfe'].astype(float)<=df_filas['valor_financiado'].astype(float))&(df
    # _filas['rs_watt_nfe'].astype(float) <= 3.5)),'fila_mr'] = 1

##Fila 2:
    # df_filas.loc[(('read_nfe' == 1) & (
    #             df_filas['valor_nfe'].astype(float) <= df_filas['valor_financiado'].astype(float)) & (
    #                           df_filas['rs_watt_nfe'].astype
    #                           (float) <= 3.5)), 'fila_mr'] = 2

##Fila 3:
    # df_filas.loc[(('read_nfe' == 1) & (
    #             df_filas['valor_nfe'].astype(float) >= df_filas['valor_financiado'].astype(float)) & (
    #                           df_filas['rs_watt_nfe'].astype
    #                           (float) <= 3.5)), 'fila_mr'] = 3
##Fila 4:
    show_df_aggrid()


    import ipdb;ipdb.set_trace()

    return df_filas


generate_lines()

