import os
import time
import traceback
import pandas as pd
import pytz
import streamlit as st
from datetime import datetime
from streamlit import session_state as cache
from utilities.database.db_utils import sql_query
from utilities.id_management.line_mng import manage_line
from utilities.metabase.metabase import metabase
from utilities.streamlit_utils import cockpit_utils
from utilities.streamlit_utils import send_slack
from utilities.database.rds_data import fazer_query


def data_id(id, scr = False):

    # final = metabase().id_data_asdict(question = "1727", id= id)
    final = metabase().id_data_asdict(question = "1607", id= id)
    if final['tf_tipo'] == 'PF':
        return fazer_query(final = final, id = id).NG_pf(scr)
        
    elif final['tf_tipo'] == 'PJ':

        if 'Analisar como PF' in final['motivo_analise_manual_string']:
            return fazer_query(final = final, id = id).NG_pf(scr)
        else:
            return fazer_query(final = final, id = id).NG_pj(scr)

    elif final['tf_tipo'] == 'PR':
        return fazer_query(final = final, id = id).NG_pr(scr)

class manage_ids():
    def __init__(self):
        pass

    def get_id(self, id_analista = 1, perfil = "D"):
        self.m = manage_line()

        self.m.get_in_line(id_analista)
       
        try:
            vez = False
            while not vez:
                vez = self.m.check_position(id_analista)
                if not vez:
                    time.sleep(5)
                else:
                    retorno = self.choose_id(id_analista = id_analista, perfil = perfil)
                    if 'id_analise' in retorno:
                        st.session_state.id_analise = retorno['id_analise']

                    
        
        except :
            self.m.get_out_of_line(id_analista)
            
            e = str(traceback.format_exc())
            send_slack().wrn_wrn(local = "Pegar ID", aviso=e)
            retorno = {'Retorno': 'Instabilidade no banco de dados'}
            
        finally:
            return retorno

    def choose_id(self, id_analista = 1, perfil = "D"):
        # Perfil
        dic_perfil = {"A": 4,
         "B": 3,
         "C": 2,
         "D": 1}

        perfil = dic_perfil[perfil]

        # Pega a tabela histórico de análises 
        tbl_hist = pd.DataFrame(sql_query(SQL= f"""SELECT id_analise, id_financiamento, id_analista, double_check, pendencia, data_submissao, inicio_analise, perfil, tipo_dc, linha, quem_destinou FROM credito_historico_analises 
                                                            WHERE fim_analise is null or pendencia is True  ORDER BY inicio_analise ASC, data_dc ASC""")
                                                            , columns=['id_analise', 'financiamento_id', 'id_analista', 'double_check', 'pendencia', 'ultima_sub_cliente', 'inicio_analise', 'perfil', 'tipo_dc', 'linha', 'id_quem_destinou'])

        # Verifica se há ids pendentes desse analista
        df_em_analise = tbl_hist.loc[(tbl_hist['id_analista'] == id_analista) & (tbl_hist['pendencia'] != True)]

        if cache.dados_analista['tipo_dc'] != '' or cache.dados_analista['tipo_dc'] is not None:
            df_em_analise_dc = tbl_hist.loc[ (tbl_hist['id_analista']== 0)
                                        & (tbl_hist['id_quem_destinou'] != id_analista)
                                        & (tbl_hist['pendencia'] != True) 
                                        & (tbl_hist['tipo_dc'].isin([cache.dados_analista['tipo_dc'], None]))] 

            df_em_analise = tbl_hist.loc[(tbl_hist['id_analista'] == id_analista) & (tbl_hist['pendencia'] != True)]

            df_em_analise = pd.concat([df_em_analise, df_em_analise_dc]).drop_duplicates()

        mb = metabase()
        # Question: de acordo com cada linha (1618 PF e 1928 PJ) - Fila
        tbl_mb = mb.get_question(question = f"""{ os.getenv("question_fila", 1928)}""")
        #tbl_mb = mb.get_question(question =  1928)
        # Sem IDs em análise:
        if df_em_analise.shape[0] == 0:

            try:
                df_fila = pd.merge(tbl_mb, tbl_hist, how='left', left_on= 'financiamento_id',right_on='financiamento_id', indicator=True).query('_merge=="left_only"')
            except:
                if tbl_mb.empty:
                    self.m.get_out_of_line(id_analista)
                    return {'Retorno': 'Sem IDs pendentes'}
            
            df_fila['perfil_x'] = df_fila['perfil_x'].astype(int)
            id_return = df_fila.loc[df_fila['perfil_x']<=perfil]
            
            if id_return.empty:     
                self.m.get_out_of_line(id_analista)  
                return {'Retorno': 'Sem IDs pendentes'}

            data = (id_return.reindex()).iloc[0]

            SQL = f"""INSERT INTO credito_historico_analises (id_financiamento, linha, id_analista, inicio_analise,pendencia, quem_destinou, data_submissao, perfil) 
            VALUES ({data['financiamento_id']}, '{data['linha_x']}', {id_analista}, TIMESTAMP '{datetime.now(pytz.timezone('America/Sao_Paulo'))}', False, {id_analista}, TIMESTAMP '{data['ultima_submissao_cliente']}', '{data['perfil_x']}') RETURNING id_analise;"""

            id_analise = sql_query(consult = False, r_insert = True, SQL = SQL)
            
            # Fix error - ID duplicado 
            analista_atribuido = self.check_analista(id_analise)

            if not analista_atribuido[0] == id_analista: 
                send_slack().wrn_wrn(local = "Pegar dados da plataforma (Conflito análises)", aviso='Conflito análises')
                return {'Retorno': 'Conflito entre analises.'}          
            #---------

            self.m.get_out_of_line(id_analista)
            
            if data['motivo_analise_manual_string'] == "":
                data_project = None
            else:
                try:    data_project = data_id(str(data['financiamento_id']))
                except Exception: 
                    e = str(traceback.format_exc())
                    print(e)
                    data_project = None 
            
            id_perfil = data['perfil_x']
            linha = data['linha_x']


            dossie = sql_query(SQL = f"""SELECT f.nome, h.dossie, h.check_list, h.fim_analise
                                                FROM credito_historico_analises h
                                                LEFT JOIN credito_analistas f ON h.id_analista = f.id
                                                WHERE h.id_financiamento = {data['financiamento_id']} and id_analise <> {id_analise}
                                                ORDER BY h.inicio_analise DESC""")
            
            
            double_ck = False
            
            fila_motivo = data['motivo_analise_manual_string']

        # Se houverem análises pendentes no histórico e esse ID ainda permanecer em análise:
        else:
            
            data = (df_em_analise.reindex()).iloc[0]

            id_analise = data['id_analise']

            if data['id_analista'] == 0:
                sql_query(consult = False, SQL = f"""UPDATE credito_historico_analises SET id_analista = {id_analista} WHERE id_analise = {id_analise};""")

                # Fix error - ID duplicado 
                analista_atribuido = self.check_analista(id_analise)

                if not analista_atribuido[0] == id_analista: 
                    send_slack().wrn_wrn(local = "Pegar dados da plataforma (Conflito análises)", aviso='Conflito análises')
                    return {'Retorno': 'Conflito entre analises.'}          
                #---------
            
            double_ck = data['double_check']
            inicio = data['inicio_analise']
            linha = data['linha']
            id_perfil = data['perfil']

            
            if (double_ck == True or str(double_ck) == 'True') and (str(inicio) == 'NaT' or inicio is None or pd.isnull(inicio)):
                sql_query(consult = False, SQL = f"""UPDATE credito_historico_analises SET inicio_analise = TIMESTAMP '{datetime.now(pytz.timezone('America/Sao_Paulo'))}' WHERE id_analise = {id_analise};""")

            self.m.get_out_of_line(id_analista) 

            try:    data_project = data_id(str(data['financiamento_id']))
            except Exception: 
                e = str(traceback.format_exc())
                print(e)
                data_project = None 
            
            fila_motivo = ''
            
   
            dossie = sql_query(SQL = f"""SELECT f.nome, h.dossie, h.check_list, h.fim_analise
                                                FROM credito_historico_analises h
                                                LEFT JOIN credito_analistas f ON h.id_analista = f.id
                                                WHERE h.id_financiamento = {data['financiamento_id']} and id_analise <> {id_analise}
                                                ORDER BY h.inicio_analise DESC""")

        
        # Estrutura de um dicionário para o retorno
        relatorio = {
            'id_analise':id_analise,
            'id_analista': id_analista,
            'id_financiamento': data['financiamento_id'],
            'fila_motivo' : fila_motivo,
            'dados_analise': data_project, 
            'double_check?': double_ck,    
            'dossiê': dossie,
            'id_perfil': id_perfil,
            'linha': linha
        }
        

        return relatorio
     
    
    def update_hist(self, id_analise, checklist, comentario, id_analista ='', pendencia = False, tipo_pendencia = None, double_check = False, dados_dck = None, desc_pend = ''):
        if id_analista != '':
            sql_query(consult = False, SQL = f"""UPDATE credito_historico_analises SET 
                                                        fim_analise = TIMESTAMP '{datetime.now(pytz.timezone('America/Sao_Paulo'))}', 
                                                        dossie = '{comentario}',
                                                        pendencia = '{pendencia}',
                                                        tipo_pendencia = '{tipo_pendencia}', 
                                                        check_list = '{checklist}', 
                                                        descricao_pendencia = '{desc_pend}' 
                                                        WHERE id_analise = {id_analise} and id_analista = {id_analista};""")                 
        else:   
            sql_query(consult = False, SQL = f"""UPDATE credito_historico_analises SET fim_analise = TIMESTAMP '{datetime.now(pytz.timezone('America/Sao_Paulo'))}', dossie = '{comentario}',pendencia = '{pendencia}', tipo_pendencia = '{tipo_pendencia}', check_list = '{checklist}', descricao_pendencia = '{desc_pend}' WHERE id_analise = {id_analise};""")      

    def update_analista(self, id_analise, id_analista):
        sql_query(consult = False, SQL = f"""UPDATE credito_historico_analises SET id_analista = '{id_analista}' WHERE id_analise = {id_analise};""")
            
    def send_id(self, dados_id):

        if dados_id['double_check'] is not True: dados_id['double_check'] = False

        if (dados_id['data_sub'] is not None) and (not pd.isnull(dados_id['data_sub'])):
            sql_query(consult = False, SQL = f"""INSERT INTO credito_historico_analises (id_financiamento, linha, id_analista, inicio_analise, quem_destinou, data_submissao, perfil,double_check) 
                                VALUES ({dados_id['id_financiamento']}, '{dados_id['linha']}', {dados_id['id_analista']}, TIMESTAMP '{datetime.now(pytz.timezone('America/Sao_Paulo'))}', {dados_id['id_destinou']}, TIMESTAMP '{dados_id['data_sub']}',{dados_id['perfil']}, {dados_id['double_check']});""")
        else:
            sql_query(consult = False, SQL = f"""INSERT INTO credito_historico_analises (id_financiamento, linha, id_analista, inicio_analise, quem_destinou,perfil, double_check) 
                                VALUES ({dados_id['id_financiamento']}, '{dados_id['linha']}', {dados_id['id_analista']}, TIMESTAMP '{datetime.now(pytz.timezone('America/Sao_Paulo'))}', {dados_id['id_destinou']},{dados_id['perfil']}, {dados_id['double_check']});""")
       
    def double_ck(self, dados_dck):
        
        if (dados_dck['data_sub'] is not None) and (not pd.isnull(dados_dck['data_sub'])):
            sql_query(consult = False, SQL = f"""INSERT INTO credito_historico_analises (id_financiamento, linha, id_analista, data_dc, quem_destinou, double_check, data_submissao, tipo_dc, perfil) 
                                VALUES ({dados_dck['id_financiamento']}, '{dados_dck['linha']}', {dados_dck['id_analista']}, TIMESTAMP '{datetime.now(pytz.timezone('America/Sao_Paulo'))}', {dados_dck['id_destinou']}, True, TIMESTAMP '{dados_dck['data_sub']}', '{dados_dck['tipo_dc']}', '{dados_dck['perfil_projeto']}');""")
        else:
            sql_query(consult = False, SQL = f"""INSERT INTO credito_historico_analises (id_financiamento, linha, id_analista, data_dc, quem_destinou, double_check, tipo_dc, perfil) 
                                VALUES ({dados_dck['id_financiamento']}, '{dados_dck['linha']}', {dados_dck['id_analista']}, TIMESTAMP '{datetime.now(pytz.timezone('America/Sao_Paulo'))}', {dados_dck['id_destinou']}, True, '{dados_dck['tipo_dc']}', '{dados_dck['perfil_projeto']}');""")
       
    def get_pendencias(self, id_analista = 1):
        pend = pd.DataFrame(sql_query(SQL= f"""SELECT f.nome, h.quem_destinou, h.id_analise, h.id_financiamento, h.linha, h.perfil, h.tipo_pendencia, h.descricao_pendencia, h.fim_analise, h.data_submissao, h.double_check FROM credito_historico_analises h
                                                        LEFT JOIN credito_analistas f ON f.id = h.id_analista
                                                        WHERE pendencia = True and h.id_analista = {id_analista} 
                                                        ORDER BY inicio_analise ASC""")
                                                        , columns=['Analista', 'Quem destinou','ID Análise','ID Financiamento', 'Linha', 'Perfil', 'Pendência', 'Descrição','Data', 'Data de submissão', 'double_check'])

        return pend
    
    def get_historico(self):
        historico = pd.DataFrame(sql_query(SQL= f"""SELECT f.nome, h.id_analise, h.id_financiamento, h.linha, h.inicio_analise, h.fim_analise, h.dossie, h.pendencia, h.tipo_pendencia, h.descricao_pendencia, h.double_check, h.data_dc, h.perfil, h.tipo_dc FROM credito_historico_analises h
                                                            LEFT JOIN credito_analistas f ON f.id = h.id_analista
                                                            WHERE fim_analise is null or pendencia is True ORDER BY inicio_analise ASC""")
                                                            , columns=['Analista','ID Análise','ID Financiamento', 'Linha','Inicio', 'Fim', 'Dossiê', 'Pendência', 'Tipo Pendência','Descrição Pendência', 'Double Check', 'Envio', 'Perfil', 'Tipo de DC'])

        return historico

    def atualizar_cadastro(self, id_analista, dados_analista):
        # dados_analista = {nome: '', ativo: '' , pf: '', pj: '', pr: '', supervisor: '', email: '', dc: '', tipo: ''}
        sql_query(consult = False, SQL = f"""UPDATE credito_analistas SET ativo = '{dados_analista['ativo']}',double_check = '{dados_analista['dc']}', perfil = '{dados_analista['perfil']}', tipo_dc = '{dados_analista['tipo']}'
                                                            WHERE id = {id_analista};""")

    def inserir_cadastro(self, dados_analista):
        # dados_analista = {nome: '', ativo: '' , pf: '', pj: '', pr: '', supervisor: '', email: '', double_check: ''}
        sql_query(consult = False, SQL = f"""INSERT INTO credito_analista (nome, ativo, admin, email, double_check, perfil) 
                            VALUES ('{dados_analista['nome']}', '{dados_analista['ativo']}', 
                                    '{dados_analista['admin']}', '{dados_analista['email']}', '{dados_analista['double_check']}', '{dados_analista['perfil']}');""")

    def get_cadastro(self):
        cadastro = pd.DataFrame(sql_query(SQL= f"""SELECT id, nome, ativo, admin, email, double_check, perfil, tipo_dc FROM credito_analistas ORDER BY ativo DESC, nome""")
                                                            , columns=['id', 'Nome', 'Ativo?','Admin', 'E-mail', 'Double-check', 'Perfil', 'Tipo DC'])

        return cadastro

    def fim_pend(self, id_analise, adm = False):
        if adm == False:
            sql_query(consult = False, SQL = f"""UPDATE credito_historico_analises SET dossie = CONCAT(dossie, CONCAT('\nFinalizado em: ', TIMESTAMP '{datetime.now(pytz.timezone('America/Sao_Paulo'))}')), pendencia = '{False}' WHERE id_analise = {id_analise};""")
        else:
            sql_query(consult = False, SQL = f"""UPDATE credito_historico_analises SET dossie = CONCAT(dossie, CONCAT('{adm}', TIMESTAMP '{datetime.now(pytz.timezone('America/Sao_Paulo'))}')), pendencia = '{False}' WHERE id_analise = {id_analise};""")

    def ids_na_fila(self, id_analista = 1):
        
        fila = pd.DataFrame(sql_query(SQL= f"""SELECT h.id_analise, h.id_financiamento, h.linha, f.nome, h.inicio_analise, h.double_check FROM credito_historico_analises h
                                                        LEFT JOIN credito_analistas f ON f.id = h.quem_destinou
                                                        WHERE h.fim_analise is null and h.id_analista = {id_analista} 
                                                        ORDER BY inicio_analise ASC""")
                                                        , columns=['ID Análise','ID Financiamento', 'Linha', 'Quem destinou', 'Início', 'DC'])

        return fila

    
    def get_analistas_dc(self):
        return pd.DataFrame(sql_query(SQL = f"""SELECT 
                                            CASE WHEN tipo_dc is not null and tipo_dc <> '' Then
                                                CONCAT(nome, ' - ', tipo_dc) 
                                            ELSE
                                                nome END analista, 
                                                id,
                                                tipo_dc
                                            FROM credito_analistas
                                            WHERE double_check is True
                                            ORDER BY tipo_dc DESC, nome ASC"""), columns=['Nome', 'id', 'tipo_double_check'])

    def check_analista(self, id_analise):
        return pd.DataFrame(sql_query(SQL = f"""SELECT f.id
                                                FROM credito_historico_analises h
                                                LEFT JOIN credito_analistas f ON h.id_analista = f.id 
                                                WHERE h.id_analise = {id_analise}"""), columns=['id'])['id'].to_list()

    def send_id_adm(self, data):
        self.m = manage_line()
        #data = {'id_financiamento':id_envio,'linha':id_linha, 'dossie':dossie, 'id_analista': id_analista_envio, 'perfil': perfil}
        self.m.get_in_line(cache.dados_analista['id'])

        try:
            vez = False
            while not vez:
                vez = self.m.check_position(cache.dados_analista['id'])
                if not vez:
                    time.sleep(7)
                else:
                    if data['data_sub'] is None:
                        #Criar análise inicio=fim e insere dossie
                        
                        sql_query(consult = False, SQL = f"""INSERT INTO credito_historico_analises (id_financiamento, linha, id_analista,dossie, inicio_analise, fim_analise, quem_destinou, perfil) 
                        VALUES ({data['id_financiamento']}, '{data['linha']}', {cache.dados_analista['id']},'{data['dossie']}', TIMESTAMP '{datetime.now(pytz.timezone('America/Sao_Paulo'))}', TIMESTAMP '{datetime.now(pytz.timezone('America/Sao_Paulo'))}', {cache.dados_analista['id']}, {data['perfil']});""")
                        
                        #Enviar ID para analista.
                        sql_query(consult = False, SQL = f"""INSERT INTO credito_historico_analises (id_financiamento, linha, id_analista, inicio_analise, quem_destinou, perfil) 
                        VALUES ({data['id_financiamento']}, '{data['linha']}', {data['id_analista']}, TIMESTAMP '{datetime.now(pytz.timezone('America/Sao_Paulo'))}', {cache.dados_analista['id']}, {data['perfil']});""")
                    
                    else:
                        #Criar análise inicio=fim e insere dossie
                        sql_query(consult = False, SQL = f"""INSERT INTO credito_historico_analises (id_financiamento, linha, id_analista,dossie, inicio_analise, fim_analise, quem_destinou, data_submissao, perfil)  
                        VALUES ({data['id_financiamento']}, '{data['linha']}', {cache.dados_analista['id']},'{data['dossie']}', TIMESTAMP '{datetime.now(pytz.timezone('America/Sao_Paulo'))}', TIMESTAMP '{datetime.now(pytz.timezone('America/Sao_Paulo'))}', {cache.dados_analista['id']}, TIMESTAMP '{data['data_sub']}', {data['perfil']});""")
                        
                        #Enviar ID para analista.
                        sql_query(consult = False, SQL = f"""INSERT INTO credito_historico_analises (id_financiamento, linha, id_analista, inicio_analise, quem_destinou, data_submissao, perfil) 
                        VALUES ({data['id_financiamento']}, '{data['linha']}', {data['id_analista']}, TIMESTAMP '{datetime.now(pytz.timezone('America/Sao_Paulo'))}', {cache.dados_analista['id']}, TIMESTAMP '{data['data_sub']}', {data['perfil']});""")
                    
            retorno = True
                    

        except Exception as e:
            print(e)
            retorno = False
            
        finally:
            self.m.get_out_of_line(cache.dados_analista['id'])
            return retorno

