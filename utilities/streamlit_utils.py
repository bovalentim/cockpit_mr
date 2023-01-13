import streamlit as st
import pandas as pd

import requests
import pytz
import ast

from streamlit import session_state as cache
from datetime import datetime
from utilities.metabase.metabase import metabase
from utilities.database.db_utils import sql_query

class cockpit_utils():
    def __init__(self):
        pass

    def init_cache(self):
        self.grupos_cache = ['control', 'dados_analista', 'dados_analise', 'adm_view', 'analise_view', 'inputs']
        for grupo in self.grupos_cache:
            if grupo not in cache: cache[f'{grupo}'] = {}
    
    # Salvando mensagens de erro no DB
    def save_errors(self, error, json):
        sql_query(consult = False, SQL = f"""INSERT INTO error_log_credito (date, dados_analista, error) VALUES (TIMESTAMP '{datetime.now(pytz.timezone('America/Sao_Paulo'))}', '{json}' , '{error}');""")
    
    def highlight(self, s):
        
        if 'análise' in s.name:
            background = ['background-color: #FFF47310']*len(s) 
        elif 'parceiro' in s.name:
            background = ['background-color: #FFF47330']*len(s) 
        elif 'projeto' in s.name:
            background = ['background-color: #FFF47390']*len(s) 
        elif s.name in ['CPF', 'Nome completo', 'Data de nascimento', 'RG', 'Nome da mãe'] or 'representante' in s.name or 'Representante' in s.name or 'End. do rep.' in s.name:
            background = ['background-color: #FFF47350']*len(s)
        elif s.name in ['CNPJ do cliente', 'Razão social do cliente', 'Nome fantasia do cliente']:
            background = ['background-color: #FFF47370']*len(s)                  
        else: 
            background = ['background-color: transparent']*len(s)       
        return background
        
    def bttn_up(self):
       
        return f""" 
            <div class='buttons-div2'>
                <a class='css-1ubr51b' href='#inicio' "target="_blank">Subir para o início</a>
            </div>
        """   
    
    @st.cache(suppress_st_warning=True)
    def construir_dossie(self, dossie):
         # 6. Verificação e ordenação do Dossiê
         
        # check_anterior = {}
        hist_dossie = {}
        
        if dossie:
            hist_dossie = []
        
            for value in  dossie:
                if value is not None:
                    _ck = {}
                    _ck['Analista'] = value[0] if value[0] is not None else ''
                    _ck['Data'] = value[3].strftime('%d/%m/%y %H:%M') if value[3] is not None else 'Análise não finalizada'
                    _ck['Comentário'] = value[1] if value[1] is not None else ''
                    hist_dossie.append(_ck)  
        
        return hist_dossie

    @st.cache(suppress_st_warning=True)
    def construir_cmntr(self, df_dossie):
        _comentario = ''
        for index, value  in df_dossie[['Analista', 'Data', 'Comentário']].iterrows():
            _comentario += 'Analista: ' + value[0] + '\n'
            _comentario += 'Data: ' + value[1] + '\n'
            _comentario += value[2] + '\n'
            _comentario += '--------------------------------' + '\n'
        return _comentario

class send_slack():
    def __init__(self):
        pass

    def wrn_erro(self, local_erro, erro):
        now = datetime.now(pytz.timezone('America/Sao_Paulo'))
        API_SLACK = "https://hooks.slack.com/services/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"
        data = {
            "channel": "CBR2V3XEX",
            "blocks": [{
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "Erro Cockpit crédito"}}, ],
            "attachments": [
                {"color": "#ff0000",
                 "blocks": [{
                     "type": "section",
                     "text": {
                         "type": "mrkdwn",
                         "text": f'Ocorreu um erro na funcionalidade **{local_erro}** - Hora: {now.strftime("%d/%m/%Y %H:%M:%S")} - ID: {cache.dados_analise["id_financiamento"]}'
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
                             "text": str(erro)
                         }}
                 ]}]}
        requests.post(url=API_SLACK, json=data)

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

def header(analista,id_financiamento = None):
    
    st.markdown(
                '<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">',
                unsafe_allow_html=True)

    st.markdown(
                '<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css">',
                unsafe_allow_html=True)
    if id_financiamento is not None:
        st.markdown(f"""
        <nav class="navbar fixed-top navbar-expand-sm navbar-dark" style="background-color: white;">
            
        <button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarNav" aria-controls="navbarNav" aria-expanded="false" aria-label="Toggle navigation">
            <span class="navbar-toggler-icon"></span>
        </button>
        <div class="collapse navbar-collapse" id="navbarNav">
            <a href="https://solfacil.com.br/financiamentos" style="padding: 0px 0px 0px 50px;">
            <img class="png-icon d-inline-block align-right" src="https://cdn.solfacil.com.br/assets/img/logo/solfacil-logo.svg" height="25">
            </a>
            <a style="color:#616161;padding: 0px 100px 0px 50px; font-size: 25px; font-weight: 500">
            {analista}
            </a>
            <ul class="navbar-nav">
            <li class="nav-item">
                    <div class="dropdown">
                        <button class="dropbtn">Etapas</button>
                        <div class="dropdown-content">
                            <a class="nav-link" style="color:#616161;" href="https://solfacil.com.br/financiamento/simulacao/{id_financiamento} " target="_blank">Simulação</a>
                            <a class="nav-link" style="color:#616161;" href=https://solfacil.com.br/financiamento/dados_do_cliente/{id_financiamento} " target="_blank">Dados do cliente</a>
                            <a class="nav-link" style="color:#616161;" href=https://solfacil.com.br/financiamento/analise_do_cliente/{id_financiamento} " target="_blank">Análise do cliente</a>
                            <a class="nav-link" style="color:#616161;" href=https://solfacil.com.br/financiamento/resumo/{id_financiamento} "target="_blank">Resumo</a>
                            <a class="nav-link" style="color:#616161;" href=https://solfacil.com.br/financiamento/retorno_nogord/{id_financiamento} "target="_blank">Retorno NG</a>
                        </div>
                    </div>
                </li>            
            <ul class="navbar-nav">
            <li class="nav-item">
                    <div class="dropdown">
                        <button class="dropbtn">Consultas</button>
                        <div class="dropdown-content">
                            <a class="nav-link" style="color:#616161;" href="https://servicos.receita.fazenda.gov.br/servicos/cnpjreva/Cnpjreva_Solicitacao.asp?cnpj="target="_blank">Cartão CNPJ</a>
                            <a class="nav-link" style="color:#616161;" href="https://sitenet.serasa.com.br/experian-concentre-web/concentre/consulta.xhtml" target="_blank">Serasa</a>
                            <a class="nav-link" style="color:#616161;" href="https://auth.idwall.co/v2/?callbackUrl=https%3A%2F%2Fdashboard.idwall.co%2Freports%2F" target="_blank">Idwall</a>
                            <a class="nav-link" style="color:#616161;" href="https://servicos.ibama.gov.br/sicafiext/" target="_blank">Ibama</a>
                            <a class="nav-link" style="color:#616161;" href="http://3.16.136.7:8502/" target="_blank">Consultas bureaux</a>
                        </div>
                    </div>
                </li>
            <li class="nav-item">
                <a class="nav-link" style="color:#616161;" href='#conclusao' "target="_blank">Conclusão</a>
            </li>
            </li>
            <li class="nav-item">
                <a class="nav-link" style="color:#616161;" href='#inicio' "target="_blank">Início</a>
            </li>
            </ul>
        </div>
        </nav>
        """, unsafe_allow_html=True)
    
    else:
        st.markdown(f"""
        <nav class="navbar fixed-top navbar-expand-sm navbar-dark" style="background-color: white;">
            
        <button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarNav" aria-controls="navbarNav" aria-expanded="false" aria-label="Toggle navigation">
            <span class="navbar-toggler-icon"></span>
        </button>
        <div class="collapse navbar-collapse" id="navbarNav">
            <a href="https://solfacil.com.br/financiamentos" style="padding: 0px 0px 0px 50px;">
            <img class="png-icon d-inline-block align-right" src="https://cdn.solfacil.com.br/assets/img/logo/solfacil-logo.svg" height="25">
            </a>
            <a style="color:#616161;padding: 0px 100px 0px 50px; font-size: 25px; font-weight: 500">
            {analista}
            </a>
            
        </div>
        </nav>
        """, unsafe_allow_html=True)