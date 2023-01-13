import gc
import os
import sys
import json
import traceback
import requests
import streamlit as st

import pandas as pd
from datetime import datetime
from utilities.database.db_utils import sql_query
from utilities.streamlit_utils import send_slack



#%% NGPF
class fazer_query():
    def __init__(self, final, id):
        self.final = final
        self.scrcpf_query=None
        self.scrcnpj_query=None
        self.id=id
        #if final['tf_tipo'][0]=='PF':
        if final['tf_tipo']=='PF' or (final['tf_tipo']=='PJ' and ('Analisar como PF' in final['motivo_analise_manual_string'])):
            print('PF')
            self.comeco=f"""WITH RECURSIVE 
                        db AS (SELECT fk AS db_fk,
                            bv_nome                                 AS bvcpf_nome,
                            bv_nome_mae                             AS bvcpf_nome_mae,
                            bv_data_nascimento                      AS bvcpf_data_nascimento,
                            bv_score                                AS bvcpf_score,
                            plataforma_id_financiamento 			AS db_plataforma_id_financiamento,
                            politica_data_consulta 					AS db_politica_data_consulta,
                            politica_linha 							AS db_politica_linha,
                            politica_motivo_entrada 				AS db_politica_motivo_entrada,
                            politica_motivo_manual 					AS db_politica_motivo_manual,
                            politica_motivo_avalista 				AS db_politica_motivo_avalista,
                            politica_motivo_entrada_mais_avalista 	AS db_politica_motivo_entrada_mais_avalista,
                            plataforma_is_amazonia_solar 			AS db_plataforma_is_amazonia_solar,
                            politica_renda_estimada 				AS db_politica_renda_estimada,
                            politica_renda_min 						AS db_politica_renda_min,
                            politica_is_renda_aprovada 				AS db_politica_is_renda_aprovada,
                            politica_score_solfacil 				AS db_politica_score_solfacil,
                            plataforma_cpf 							AS db_plataforma_cpf,
                            plataforma_is_avalista 					AS db_plataforma_is_avalista,
                            politica_classe                         AS db_politica_classe,
                            politica_score_solfacil_sem_up_dec      AS db_score_solfacil_decimal,
                            politica_prinad_ponderada               AS db_politica_prinad_ponderada,
                            serasa_score 							AS db_serasa_score,
                            teste_ab                                AS db_teste_ab,
                            serasa_lista_participacao_societaria_cnpj 			AS db_serasa_lista_participacao_societaria_cnpj,
                            serasa_lista_participacao_societaria_data_atual 	AS db_serasa_lista_participacao_societaria_data_atual,
                            serasa_lista_participacao_societaria_data_desde 	AS db_serasa_lista_participacao_societaria_data_desde,
                            serasa_lista_participacao_societaria_nome 			AS db_serasa_lista_participacao_societaria_nome,
                            serasa_lista_participacao_societaria_restricao 		AS db_serasa_lista_participacao_societaria_restricao,
                            serasa_lista_participacao_societaria_situacao 		AS db_serasa_lista_participacao_societaria_situacao,
                            serasa_lista_participacao_societaria_uf 			AS db_serasa_lista_participacao_societaria_uf,
                            serasa_lista_participacao_societaria 				AS db_serasa_lista_participacao_societaria,
                            plataforma_economia_maior_prazo 					AS db_plataforma_economia_maior_prazo
                            FROM db_ngpf
      					    WHERE plataforma_id_financiamento = '{self.id}' AND plataforma_is_avalista=FALSE
                            AND plataforma_cpf = '{final['cli_cpf'].replace('-','').replace('.','')}'
                            AND (politica_data_consulta 
                            BETWEEN '{datetime.strptime(final['ultima_validacao_analise_manual'], '%Y-%m-%dT%H:%M:%S.%f-03:00').replace(microsecond=0)}'::timestamp - INTERVAL '2 minute'
                                AND '{datetime.strptime(final['ultima_validacao_analise_manual'], '%Y-%m-%dT%H:%M:%S.%f-03:00')}'::timestamp)
                            ORDER BY politica_data_consulta DESC LIMIT 1),
        		        lixo AS (SELECT db_plataforma_cpf
                            FROM db),
                          """
            
        elif final['tf_tipo']=='PJ':
            print('PJ')
            self.comeco=f"""WITH RECURSIVE
               db AS (SELECT fk AS db_fk,
                    bv_cnpj_atividade                           AS bvcnpj_atividade,
                    bv_cnpj_data_fundacao                       AS bvcnpj_data_fundacao,
                    bv_cnpj_score                               AS bvcnpj_score,
                    bv_cpf_score                                AS bvcoob_score,
                    serasa_cnpj_data_fundacao                   AS serasa_data_fundacao,
                    serasa_cnpj_atividade                       AS serasa_atividade,
                    serasa_cnpj_tipo_empresa                    AS serasa_tipo_empresa,
                    serasa_cnpj_score                           AS serasa_score_cnpj,
                    politica_classe                             AS db_politica_classe,
                    politica_score_solfacil_sem_up_dec          AS db_score_solfacil_decimal,
                    politica_prinad_ponderada                   AS db_politica_prinad_ponderada,
                    plataforma_id_financiamento                 AS db_plataforma_id_financiamento,
					politica_data_consulta                      AS db_politica_data_consulta,
					politica_motivo_manual                      AS db_politica_motivo_manual,
					politica_decisao_pj                         AS db_politica_decisao_pj,
					politica_decisao                            AS db_politica_decisao,
					politica_tipo_de_analise                    AS db_politica_tipo_de_analise,
					plataforma_is_amazonia_solar                AS db_plataforma_is_amazonia_solar,
					politica_faturamento_estimado               AS db_politica_faturamento_estimado,
					politica_faturamento_min                    AS db_politica_faturamento_min,
					politica_is_faturamento_aprovado            AS db_politica_is_faturamento_aprovado,
					politica_score_solfacil                     AS db_politica_score_solfacil,
					plataforma_coobrigado_cpf                   AS db_plataforma_coobrigado_cpf,
					plataforma_cnpj                             AS db_plataforma_cnpj,
                    plataforma_economia_maior_prazo             AS db_plataforma_economia_maior_prazo,
                    teste_ab                                    AS db_teste_ab,
                    (1- plataforma_vlr_parcela_escolhida/(plataforma_parcela_maior_prazo/(1-plataforma_economia_maior_prazo))) AS db_economia_escolhida
					FROM db_ngpj
					WHERE plataforma_id_financiamento = '{self.id}' 
                    AND plataforma_cnpj = '{final['emp_cnpj'].replace('-','').replace('.','').replace('/','')}'
                    AND (politica_data_consulta 
                            BETWEEN '{datetime.strptime(final['ultima_validacao_analise_manual'], '%Y-%m-%dT%H:%M:%S.%f-03:00').replace(microsecond=0)}'::timestamp - INTERVAL '2 minute'
                                AND '{datetime.strptime(final['ultima_validacao_analise_manual'], '%Y-%m-%dT%H:%M:%S.%f-03:00')}'::timestamp)
                    ORDER BY politica_data_consulta DESC LIMIT 1),
		       lixo AS (SELECT db_plataforma_cnpj
  		   		    FROM db),
               lixoa AS (SELECT db_plataforma_coobrigado_cpf
  		   		     FROM db),
                            """
        elif final['tf_tipo'] == 'PR':
            print('PR')
            self.comeco=f"""WITH RECURSIVE 
                            db AS (SELECT fk AS db_fk,
                                bv_nome                                 AS bvcpf_nome,
                                bv_nome_mae                             AS bvcpf_nome_mae,
                                bv_data_nascimento                      AS bvcpf_data_nascimento,
                                
                                bv_score                                AS bvcpf_score,
                                serasa_score 							AS db_serasa_score,
                                politica_score_solfacil 				AS db_politica_score_solfacil,
                                politica_score_solfacil_decimal         AS db_politica_score_solfacil_decimal,
                                
                                plataforma_id_financiamento 			AS db_plataforma_id_financiamento,
                                politica_data_consulta 					AS db_politica_data_consulta,
                                politica_motivo_entrada 				AS db_politica_motivo_entrada,
                                politica_motivo_manual 					AS db_politica_motivo_manual,
                                politica_renda_estimada 				AS db_politica_renda_estimada,
                                politica_is_renda_aprovada 				AS db_politica_is_renda_aprovada,
                                politica_renda_min_formalizacao 		AS db_politica_renda_min_formalizacao,
                                plataforma_cpf 							AS db_plataforma_cpf,
                                politica_prinad_ponderada               AS db_politica_prinad_ponderada,
                                plataforma_economia_maior_prazo 		AS db_plataforma_economia_maior_prazo,
                                serasa_lista_participacao_societaria_cnpj 			AS db_serasa_lista_participacao_societaria_cnpj,
                                serasa_lista_participacao_societaria_data_atual 	AS db_serasa_lista_participacao_societaria_data_atual,
                                serasa_lista_participacao_societaria_data_desde 	AS db_serasa_lista_participacao_societaria_data_desde,
                                serasa_lista_participacao_societaria_nome 			AS db_serasa_lista_participacao_societaria_nome,
                                serasa_lista_participacao_societaria_restricao 		AS db_serasa_lista_participacao_societaria_restricao,
                                serasa_lista_participacao_societaria_situacao 		AS db_serasa_lista_participacao_societaria_situacao,
                                serasa_lista_participacao_societaria_uf 			AS db_serasa_lista_participacao_societaria_uf,
                                serasa_lista_participacao_societaria 				AS db_serasa_lista_participacao_societaria,
                                exato_cnd_ibama_is_emitida				            AS cnd_ibama_is_emitida
                                FROM db_ngpr
                                WHERE plataforma_id_financiamento = '{self.id}'
                                AND plataforma_cpf = '{final['cli_cpf'].replace('-','').replace('.','')}'
                                AND (to_timestamp(politica_data_consulta, 'DD/MM/YYYY HH24:MI:SS') 
                                BETWEEN '{datetime.strptime(final['ultima_validacao_analise_manual'], '%Y-%m-%dT%H:%M:%S.%f-03:00').replace(microsecond=0)}'::timestamp - INTERVAL '2 minute'
                                    AND '{datetime.strptime(final['ultima_validacao_analise_manual'], '%Y-%m-%dT%H:%M:%S.%f-03:00')}'::timestamp)
                                ORDER BY politica_data_consulta DESC LIMIT 1),
                            lixo AS (SELECT db_plataforma_cpf
                                FROM db),
                          """
        else:
            print('erro - tf_tipo comeco')
       
    def db(self):
        query = sql_query(env = 'prod', SQL =self.comeco + """
                                    bla AS (SELECT 'bla')
                                    SELECT * FROM db
                                    """)
        return dict(query[0]._mapping) #(pd.DataFrame(query), query)
    
    def db_pjpf(self):
        _SQL = F"""WITH RECURSIVE
               db AS (SELECT fk AS db_fk,
					politica_data_consulta                      AS db_politica_data_consulta,
                    politica_motivo_manual                      AS db_politica_motivo_manual
					FROM db_ngpj
					WHERE plataforma_id_financiamento = '{self.id}' 
                    AND plataforma_cnpj = '{self.final['emp_cnpj'].replace('-','').replace('.','').replace('/','')}'
                    AND (politica_data_consulta 
                            BETWEEN '{datetime.strptime(self.final['ultima_validacao_analise_manual'], '%Y-%m-%dT%H:%M:%S.%f-03:00').replace(microsecond=0)}'::timestamp - INTERVAL '2 minute'
                                AND '{datetime.strptime(self.final['ultima_validacao_analise_manual'], '%Y-%m-%dT%H:%M:%S.%f-03:00')}'::timestamp)
                    ORDER BY politica_data_consulta DESC LIMIT 1)
               SELECT * FROM db"""
        
        query = sql_query(env = 'prod', SQL = _SQL)
        return dict(query[0]._mapping)

    def bvcpf(self):
        query=sql_query(env = 'prod', SQL =self.comeco + """
                                bvcpf AS (SELECT fk AS bvcpf_fk,
                                        cpf AS bvcpf_cpf,
                                        created_at AS bvcpf_created_at,
                                        --id,
                                        json AS bvcpf_json
                                        --error_message,
                                        --CACHE,
                                        FROM bv_cpf_acerta_essencial INNER JOIN lixoa ON bv_cpf_acerta_essencial.cpf = lixoa.db_plataforma_coobrigado_cpf ORDER BY bvcpf_created_at DESC LIMIT 1)
                                    SELECT * FROM bvcpf""")
        return(pd.DataFrame(query)['bvcpf_json'][0]['ns29:essencial'],query)

    def bvcnpj(self):
        query=sql_query(env = 'prod', SQL =self.comeco + """
                   	  		    bvcnpj AS (SELECT fk AS bvcnpj_fk,
                	  		          DOCUMENT AS bvcnpj_document,
                	  		          created_at AS bvcnpj_created_at,
                	  		          json AS bvcnpj_json
                	  		          FROM bv_cnpj_define_risco_positivo INNER JOIN lixo ON bv_cnpj_define_risco_positivo."document" = lixo.db_plataforma_cnpj
                                      ORDER BY bvcnpj_created_at DESC LIMIT 1)
                                      SELECT * FROM bvcnpj
                                        """)
        return(pd.DataFrame(query)['bvcnpj_json'][0]['definePositivoRisco'],query)
    
    def scrcnpj(self):
        query=sql_query(env = 'prod', SQL =self.comeco + """
                        scr AS (SELECT *, ROW_NUMBER() OVER(PARTITION BY scr_blablasddsfsd.scr_data_base_consultada ORDER BY scr_blablasddsfsd.scr_data_base_consultada DESC, scr_blablasddsfsd.scr_created_at DESC) AS numero 
                                FROM
                                    (SELECT fk AS scr_fk,
                                        documento AS scr_documento,
                                        created_at AS scr_created_at,
                                        data_base_consultada AS scr_data_base_consultada,
                                        a_vencer,
                                        a_vencer_ate30,
                                        vencidos,
                                        limite_total,
                                        limite_nao_utilizado,
                                        qtd_ifs,
                                        prejuizo,
                                        used_modality
                                    FROM bmp_scr_simples INNER JOIN lixo ON bmp_scr_simples.documento = lixo.db_plataforma_cnpj
                                    WHERE error_message IS NULL AND is_disponivel = 1) AS scr_blablasddsfsd
                                    )
                            SELECT * FROM scr
                            WHERE numero<2        			        
                        """)
        
        if query == []: return pd.DataFrame(['Sem dados para resumo traduzido cnpj']), pd.DataFrame(['Sem dados para resumo modalidade cnpj'])

        return(self.trata_scr(query))
    
    def scr_cpf(self):
        if self.final['tf_tipo'] in ['PF','PR'] or (self.final['tf_tipo']=='PJ' and ('Analisar como PF' in self.final['motivo_analise_manual_string'])):
            query=sql_query(env = 'prod', SQL =self.comeco + """
                            scr AS (SELECT *, ROW_NUMBER() OVER(PARTITION BY scr_blablasddsfsd.scr_data_base_consultada ORDER BY scr_blablasddsfsd.scr_data_base_consultada DESC, scr_blablasddsfsd.scr_created_at DESC) AS numero FROM
         			        (SELECT fk AS scr_fk,
         			        documento AS scr_documento,
         				    created_at AS scr_created_at,
         				    data_base_consultada AS scr_data_base_consultada,
                           a_vencer,
                           a_vencer_ate30,
                           vencidos,
                           limite_total,
                           limite_nao_utilizado,
                           qtd_ifs,
                           prejuizo,
                           used_modality
         			        FROM bmp_scr_simples INNER JOIN lixo ON bmp_scr_simples.documento = lixo.db_plataforma_cpf
         			        WHERE error_message IS NULL AND is_disponivel = 1) AS scr_blablasddsfsd
         			        )
                        SELECT * FROM scr
                        WHERE numero<2
                             """)
            
            if query == []: return pd.DataFrame(['Sem dados para resumo traduzido cliente']), pd.DataFrame(['Sem dados para resumo modalidade cliente'])            

        elif self.final['tf_tipo']=='PJ':
            query=sql_query(env = 'prod', SQL =self.comeco + """
                            scr AS (SELECT *, ROW_NUMBER() OVER(PARTITION BY scr_blablasddsfsd.scr_data_base_consultada ORDER BY scr_blablasddsfsd.scr_data_base_consultada DESC, scr_blablasddsfsd.scr_created_at DESC) AS numero 
                                FROM
                                    (SELECT fk AS scr_fk,
                                        documento AS scr_documento,
                                        created_at AS scr_created_at,
                                        data_base_consultada AS scr_data_base_consultada,
                                        a_vencer,
                                        a_vencer_ate30,
                                        vencidos,
                                        limite_total,
                                        limite_nao_utilizado,
                                        qtd_ifs,
                                        prejuizo,
                                        used_modality
                                    FROM bmp_scr_simples INNER JOIN lixoa ON bmp_scr_simples.documento = lixoa.db_plataforma_coobrigado_cpf
                                    WHERE error_message IS NULL AND is_disponivel = 1) AS scr_blablasddsfsd
                                    )
                            SELECT * FROM scr
                            WHERE numero<2
                            """)
            
            if query == []: return pd.DataFrame(['Sem dados para resumo traduzido coobrigado']), pd.DataFrame(['Sem dados para resumo modalidade coobrigado'])    

        return (self.trata_scr(query))
 
    def scr_cpf_avalista(self):
        query=sql_query(env = 'prod', SQL=f"""
                        WITH RECURSIVE 
                           db AS (SELECT fk AS db_fk,
           					plataforma_id_financiamento                 AS db_plataforma_id_financiamento,
           					politica_data_consulta                      AS db_politica_data_consulta,
           					politica_linha                              AS db_politica_linha,
           					politica_motivo_entrada                     AS db_politica_motivo_entrada,
           					politica_motivo_manual                      AS db_politica_motivo_manual,
                            politica_motivo_avalista                    AS db_politica_motivo_avalista,
           					politica_motivo_entrada_mais_avalista       AS db_politica_motivo_entrada_mais_avalista,
           					plataforma_is_amazonia_solar                AS db_plataforma_is_amazonia_solar,
           					politica_renda_estimada                     AS db_politica_renda_estimada,
           					politica_renda_min                          AS db_politica_renda_min,
           					politica_is_renda_aprovada                  AS db_politica_is_renda_aprovada,
           					politica_score_solfacil                     AS db_politica_score_solfacil,
           					plataforma_cpf                              AS db_plataforma_cpf,
                            plataforma_is_avalista                      AS db_plataforma_is_avalista
                            FROM db_ngpf
   					        WHERE plataforma_id_financiamento = '{self.id}' AND plataforma_is_avalista=TRUE
                            ORDER BY politica_data_consulta DESC LIMIT 1),
           		        lixo AS (SELECT db_plataforma_cpf
                               FROM db),
                        scr AS (SELECT *, ROW_NUMBER() OVER(PARTITION BY scr_blablasddsfsd.scr_data_base_consultada ORDER BY scr_blablasddsfsd.scr_data_base_consultada DESC, scr_blablasddsfsd.scr_created_at DESC) AS numero FROM
         			        (SELECT fk AS scr_fk,
         			        documento AS scr_documento,
         				    created_at AS scr_created_at,
         				    data_base_consultada AS scr_data_base_consultada,
                           a_vencer,
                           a_vencer_ate30,
                           vencidos,
                           limite_total,
                           limite_nao_utilizado,
                           qtd_ifs,
                           prejuizo,
                           used_modality
         			        FROM bmp_scr_simples INNER JOIN lixo ON bmp_scr_simples.documento = lixo.db_plataforma_cpf
         			        WHERE error_message IS NULL AND is_disponivel = 1) AS scr_blablasddsfsd
         			        )
                        SELECT * FROM scr
                        WHERE numero<2
                        """)

        if query == []: return pd.DataFrame(['Sem dados para resumo traduzido avalista']), pd.DataFrame(['Sem dados para resumo modalidade avalista'])

        return(self.trata_scr(query))

    def dados_avalista(self):
        query = sql_query(env = 'prod', SQL=f"""
                        WITH RECURSIVE 
                           db AS (SELECT 
                                fk                                      AS db_fk,
                                plataforma_id_financiamento             AS db_plataforma_id_financiamento,
                                politica_data_consulta                  AS db_politica_data_consulta,
                                politica_linha                          AS db_politica_linha,
                                politica_motivo_entrada                 AS db_politica_motivo_entrada,
                                politica_motivo_manual                  AS db_politica_motivo_manual,
                                politica_motivo_avalista                AS db_politica_motivo_avalista,
                                politica_classe                         AS db_politica_classe,
                                politica_motivo_entrada_mais_avalista   AS db_politica_motivo_entrada_mais_avalista,
                                plataforma_is_amazonia_solar            AS db_plataforma_is_amazonia_solar,
                                politica_renda_estimada                 AS db_politica_renda_estimada,
                                politica_renda_min                      AS db_politica_renda_min,
                                politica_is_renda_aprovada              AS db_politica_is_renda_aprovada,
                                politica_score_solfacil                 AS db_politica_score_solfacil,
                                politica_prinad_ponderada               AS db_politica_prinad_ponderada,
                                plataforma_cpf                          AS db_plataforma_cpf,
                                plataforma_is_avalista                  AS db_plataforma_is_avalista,
                                plataforma_economia_maior_prazo         AS db_plataforma_economia_maior_prazo
                            FROM db_ngpf
   					        WHERE plataforma_id_financiamento = '{self.id}' AND plataforma_is_avalista = TRUE
                            AND plataforma_cpf = '{self.final['clia_cpf_avalista'].replace('-','').replace('.','')}'
                            AND (politica_data_consulta 
                            BETWEEN '{datetime.strptime(self.final['ultima_validacao_analise_manual'], '%Y-%m-%dT%H:%M:%S.%f-03:00').replace(microsecond=0)}'::timestamp - INTERVAL '2 minute'
                                AND '{datetime.strptime(self.final['ultima_validacao_analise_manual'], '%Y-%m-%dT%H:%M:%S.%f-03:00')}'::timestamp)
                            ORDER BY politica_data_consulta DESC LIMIT 1)
                        SELECT * FROM db
                        """)
        return dict(query[0]._mapping) #(pd.DataFrame(query))
  
    def trata_scr(self,x):
        print(x)
        scr = pd.DataFrame(x)
        print(scr)
        doc = scr['scr_documento'][0]
        
        resumo_trad = scr.pivot(index='scr_documento',columns='scr_data_base_consultada',values='a_vencer').rename(index={doc:'a_vencer'})
        for i in ['a_vencer', 'a_vencer_ate30', 'vencidos', 'limite_total' ,'limite_nao_utilizado', 'qtd_ifs', 'prejuizo']:
            resumo_trad = pd.concat([resumo_trad, scr.pivot(index='scr_documento',columns='scr_data_base_consultada',values=i).rename(index={doc:i})])
        
        resumo_trad = resumo_trad.tail(resumo_trad.shape[0] -1)
        x=0
        for i in scr['used_modality']: # tem que fazer esse loop depois do ultimo pq o ultimo coleta a lista de resumomodalidade
            if i == []: i = [{"tipo": "asdf1", "modalidade": "9999", "dominio": "asdf2", "subdominio": "asdf3", "valorVencimento": -999.0}]
            if x == 0:
                resumo_mod = pd.DataFrame(i)
                resumo_mod.rename(index={0:(resumo_mod['tipo'][0]+resumo_mod['modalidade'][0])},columns={'valorVencimento':scr['scr_data_base_consultada'][x]},inplace=True)
                x+=1
            else:
                lixo = pd.DataFrame(i)
                lixo.rename(index={0:(lixo['tipo'][0]+lixo['modalidade'][0])},columns={'valorVencimento':scr['scr_data_base_consultada'][x]},inplace=True)
                resumo_mod = pd.concat([resumo_mod,lixo]).reset_index(drop=True)
                resumo_mod[resumo_mod.columns[4:999]] = resumo_mod.groupby(['tipo','modalidade'], sort=False)[resumo_mod.columns[4:999]].apply(lambda x: x.ffill().bfill())
                resumo_mod = resumo_mod.drop_duplicates()
                x += 1

        resumo_mod = resumo_mod.fillna(0)

        return resumo_trad, resumo_mod

    def serasa_avalista(self):
        query = sql_query(env = 'prod', SQL=f"""
                        WITH RECURSIVE 
                           db AS (SELECT 
                                  serasa_score                                      AS db_serasa_score_avalista,
                                  serasa_lista_participacao_societaria_cnpj         AS avalista_db_serasa_lista_participacao_societaria_cnpj,
                                  serasa_lista_participacao_societaria_data_atual   AS avalista_db_serasa_lista_participacao_societaria_data_atual,
                                  serasa_lista_participacao_societaria_data_desde   AS avalista_db_serasa_lista_participacao_societaria_data_desde,
                                  serasa_lista_participacao_societaria_nome         AS avalista_db_serasa_lista_participacao_societaria_nome,
                                  serasa_lista_participacao_societaria_restricao    AS avalista_db_serasa_lista_participacao_societaria_restricao,
                                  serasa_lista_participacao_societaria_situacao     AS avalista_db_serasa_lista_participacao_societaria_situacao,
                                  serasa_lista_participacao_societaria_uf           AS avalista_db_serasa_lista_participacao_societaria_uf,
                                  serasa_lista_participacao_societaria              AS avalista_db_serasa_lista_participacao_societaria
                                FROM db_ngpf
       					        WHERE plataforma_id_financiamento = '{self.id}' AND plataforma_is_avalista = TRUE
                                ORDER BY politica_data_consulta DESC LIMIT 1)
                          SELECT * FROM db
                           """)
        return dict(query[0]._mapping)
    
    def NG_pf(self, only_update_scr = False):
        data = self.final
        tem_avalista = False if (data['fin_avalista_id'] is None) or (data['decisao_avalista'] == 'Negado') else True 

        # SCR - CPF Cliente
        try:
            scr = self.scr_cpf()
            resumo_traduzido = scr[0]
            resumo_modalidade = scr[1]      
        except Exception:
            e = str(traceback.format_exc())
            send_slack().wrn_wrn(local = 'Erro ao buscar info SCR', aviso=e)
            print('Erro SCR - Cliente')
            resumo_traduzido = pd.DataFrame(['Erro ao buscar resumo traduzido cliente'])
            resumo_modalidade = pd.DataFrame(['Erro ao buscar resumo modalidade cliente'])

        
        # SCR - CPF Avalista
        try:
            if tem_avalista:
                try:
                    dados_avalista = self.dados_avalista()
                except Exception:
                    e = str(traceback.format_exc())
                    send_slack().wrn_wrn(local = 'Erro ao buscar info avalista', aviso=e)
                    dados_avalista = None
                
                scr_avalista = self.scr_cpf_avalista()
                resumo_traduzido_avalista = scr_avalista[0]
                resumo_modalidade_avalista = scr_avalista[1]
            else:
                print('SCR - Não tem avalista')
                resumo_traduzido_avalista = None
                resumo_modalidade_avalista = None
                dados_avalista = None
        except Exception:
            print('Erro SCR - Avalista')
            resumo_traduzido_avalista = pd.DataFrame(['Erro ao buscar resumo traduzido avalista'])
            resumo_modalidade_avalista = pd.DataFrame(['Erro ao buscar resumo modalidade avalista'])

        if only_update_scr: return [resumo_traduzido, resumo_modalidade, resumo_traduzido_avalista, resumo_modalidade_avalista]    

        data.update(self.db())

        try:
            if data['db_teste_ab'] is None:
                data['teste_ab_score'] = "Não tem"
                
            else:
                if 'score_ks1_a' in data['db_teste_ab']:
                    data['teste_ab_score'] = 'Tratamento'
                elif 'score_ks1_b' in data['db_teste_ab']:
                    data['teste_ab_score'] = 'Controle'
                else: data['teste_ab_score'] = "Erro"
        
        except Exception:
            e = str(traceback.format_exc())
            data['teste_ab_score'] = "Erro"
            send_slack().wrn_wrn(local = 'Erro - Valor teste AB score', aviso=e)
            print('Erro valor teste ab score')

        # Informações societárias do cliente
        try:
            if float(data['cota_valor_do_projeto']) >= 60000:
                info_societaria_serasa = pd.DataFrame(list(zip( data["db_serasa_lista_participacao_societaria_nome"], 
                                                                data["db_serasa_lista_participacao_societaria_cnpj"], 
                                                                data["db_serasa_lista_participacao_societaria"], 
                                                                data["db_serasa_lista_participacao_societaria_data_desde"], 
                                                                data["db_serasa_lista_participacao_societaria_data_atual"], 
                                                                data["db_serasa_lista_participacao_societaria_uf"], 
                                                                data["db_serasa_lista_participacao_societaria_restricao"], 
                                                                data["db_serasa_lista_participacao_societaria_situacao"])),
                                                    columns =['Nome', 'Cnpj','Participação','Data Desde','Data Atual','UF','Restrição','Situação'])
                if len(info_societaria_serasa) == 0:
                    info_societaria_serasa = pd.DataFrame.from_dict({'Nome':['Não há CNPJs associados a esse CPF']})
            else:
                info_societaria_serasa = pd.DataFrame.from_dict({'Nome':['O NG não consultou CNPJs para esse CPF']})
        except Exception:
            info_societaria_serasa = pd.DataFrame.from_dict({'Nome':['Erro ao pegar informações de participação societária cliente']})

        # Informações societárias do avalista e score serasa
        if tem_avalista:
            try:
                data.update(self.serasa_avalista())
            except Exception:
                data['db_serasa_score_avalista'] = 'Erro ao pegar informações do avalista'
                info_societaria_serasa_avalista = pd.DataFrame.from_dict({'Nome':['Erro ao pegar informações de participação societária avalista']})
                print('Erro - score_serasa_avalista')
                data['db_serasa_score_avalista'] = 'Erro'

            
            try:
                if float(data['cota_valor_do_projeto']) >= 60000:
                    info_societaria_serasa_avalista = pd.DataFrame(list(zip(data["avalista_db_serasa_lista_participacao_societaria_nome"],
                                                                            data["avalista_db_serasa_lista_participacao_societaria_cnpj"], 
                                                                            data["avalista_db_serasa_lista_participacao_societaria"], 
                                                                            data["avalista_db_serasa_lista_participacao_societaria_data_desde"], 
                                                                            data["avalista_db_serasa_lista_participacao_societaria_data_atual"], 
                                                                            data["avalista_db_serasa_lista_participacao_societaria_uf"], 
                                                                            data["avalista_db_serasa_lista_participacao_societaria_restricao"], 
                                                                            data["avalista_db_serasa_lista_participacao_societaria_situacao"])),
                                                        columns = ['Nome', 'Cnpj','Participação','Data Desde','Data Atual','UF','Restrição','Situação'])
                    if len(info_societaria_serasa_avalista) == 0:
                        info_societaria_serasa_avalista = pd.DataFrame.from_dict({'Nome':['Não há CNPJs associados a esse CPF (avalista)']})
                else:
                    info_societaria_serasa_avalista = pd.DataFrame.from_dict({'Nome':['O NG não consultou CNPJs para esse CPF (avalista)']})

            except Exception:
                print('Erro - info_societaria_serasa_avalista')
                info_societaria_serasa_avalista = pd.DataFrame.from_dict({'Nome':['Erro ao pegar informações de participação societária avalista']})
        else:
            data['db_serasa_score_avalista'] = 'Não tem avalista'
            info_societaria_serasa_avalista = pd.DataFrame.from_dict({'Nome':['Não tem avalista']})
        
        try:
            if 'Analisar como PF' in data['motivo_analise_manual_string']:
                dados_pj = self.db_pjpf()
            else:
                dados_pj = None
        except:
            dados_pj = None
            e = str(traceback.format_exc())
            send_slack().wrn_wrn(local = 'Erro - Dados PJ-PF', aviso=e)
        
        return [data,   
                resumo_traduzido, resumo_modalidade, 
                resumo_traduzido_avalista, resumo_modalidade_avalista, 
                info_societaria_serasa, info_societaria_serasa_avalista,
                dados_avalista, dados_pj]        
        
    def NG_pj(self, only_update_scr = False):
        data = self.final

        #SCR - CNPJ
        try:
            scr_cnpj = self.scrcnpj()
            resumo_traduzido_cnpj = scr_cnpj[0]
            resumo_modalidade_cnpj = scr_cnpj[1]
        except Exception:
            e = str(traceback.format_exc())
            print(e)
            send_slack().wrn_wrn(local = 'Erro ao buscar info SCR - CNPJ', aviso=e)
            print('Erro SCR - CNPJ')
            resumo_traduzido_cnpj = pd.DataFrame(['Erro ao buscar resumo traduzido cnpj'])
            resumo_modalidade_cnpj = pd.DataFrame(['Erro ao buscar resumo modalidade cnpj'])
        
        #SCR - Coobrigado
        try:
            scr_coob = self.scr_cpf()
            resumo_traduzido_coob = scr_coob[0]
            resumo_modalidade_coob = scr_coob[1]
        except Exception:
            e = str(traceback.format_exc())
            print(e)
            send_slack().wrn_wrn(local = 'Erro ao buscar info SCR - Coobrigado', aviso=e)
            print('Erro SCR - Coobrigado')
            resumo_traduzido_coob = pd.DataFrame(['Erro ao buscar resumo traduzido coobrigado'])
            resumo_modalidade_coob = pd.DataFrame(['Erro ao buscar resumo modalidade coobrigado'])

        if only_update_scr: return [resumo_traduzido_cnpj, resumo_modalidade_cnpj, resumo_traduzido_coob, resumo_modalidade_coob]  

        try:
            data.update(self.db())
        except Exception:
            e = str(traceback.format_exc())
            send_slack().wrn_wrn(local = 'Erro pegar dados db_ngpj', aviso=e)
            print('Erro BV/Serasa Lista de sócios')

        try:
            if data['db_teste_ab'] is None:
                data['teste_ab_score'] = "Não tem"
                data['teste_ab_faturamento'] = "Não tem"
            else:
                if 'faturamento_a' in data['db_teste_ab']:
                    data['teste_ab_faturamento'] = 'Sim'
                elif 'faturamento_b' in data['db_teste_ab']:
                    data['teste_ab_faturamento'] = 'Não'
                else: data['teste_ab_faturamento'] = "Erro"

                if 'score_a' in data['db_teste_ab']:
                    data['teste_ab_score'] = 'Tratamento'
                elif 'score_b' in data['db_teste_ab']:
                    data['teste_ab_score'] = 'Controle'
                else: data['teste_ab_score'] = "Erro"
        except Exception:
            e = str(traceback.format_exc())
            send_slack().wrn_wrn(local = 'Erro - Valor teste ab', aviso=e)
            print('Erro valor teste ab')

        try:
            serasa = json.loads(data['con_cnpj_resultado_api_serasa'])

            info_societaria_serasa=pd.DataFrame(list(zip( serasa["socios_nome"], 
                                                                serasa["socios_documento"], 
                                                                serasa["socios_data_entrada"], 
                                                                serasa["socios_restricao"], 
                                                                serasa["socios_nivel"],  
                                                                serasa["socios_situacao"])),
                                                    columns =['Nome', 'Documento','Data de entrada','Pendência','Participação','Situação'])
        except Exception:
            e = str(traceback.format_exc())
            send_slack().wrn_wrn(local = 'Erro Serasa Lista de sócios', aviso=e)
            print('Erro Serasa Lista de sócios')
            info_societaria_serasa = None

        try:
            info_societaria_bv = pd.DataFrame(json.loads(data['con_cnpj_bv_define_risco_socios']))
        except Exception:
            e = str(traceback.format_exc())
            send_slack().wrn_wrn(local = 'Erro BV Lista de sócios', aviso=e)
            print('Erro BV Lista de sócios')
            info_societaria_bv = None

 
        
        return [data,resumo_traduzido_cnpj,resumo_modalidade_cnpj,resumo_traduzido_coob,resumo_modalidade_coob,info_societaria_serasa,info_societaria_bv]

    def NG_pr(self, only_update_scr = False):
        data = self.final

        # SCR - CPF Cliente
        try:
            scr = self.scr_cpf()
            resumo_traduzido = scr[0]
            resumo_modalidade = scr[1]      
        except Exception:
            e = str(traceback.format_exc())
            send_slack().wrn_wrn(local = 'Erro ao buscar info SCR', aviso=e)
            print('Erro SCR - Cliente')
            resumo_traduzido = pd.DataFrame(['Erro ao buscar resumo traduzido cliente'])
            resumo_modalidade = pd.DataFrame(['Erro ao buscar resumo modalidade cliente'])

        if only_update_scr: return [resumo_traduzido, resumo_modalidade]    

        data.update(self.db())

        # Informações societárias do cliente
        try:
            if float(data['cota_valor_do_projeto']) >= 60000:
                info_societaria_serasa = pd.DataFrame(list(zip( data["db_serasa_lista_participacao_societaria_nome"], 
                                                                data["db_serasa_lista_participacao_societaria_cnpj"], 
                                                                data["db_serasa_lista_participacao_societaria"], 
                                                                data["db_serasa_lista_participacao_societaria_data_desde"], 
                                                                data["db_serasa_lista_participacao_societaria_data_atual"], 
                                                                data["db_serasa_lista_participacao_societaria_uf"], 
                                                                data["db_serasa_lista_participacao_societaria_restricao"], 
                                                                data["db_serasa_lista_participacao_societaria_situacao"])),
                                                    columns =['Nome', 'Cnpj','Participação','Data Desde','Data Atual','UF','Restrição','Situação'])
                if len(info_societaria_serasa) == 0:
                    info_societaria_serasa = pd.DataFrame.from_dict({'Nome':['Não há CNPJs associados a esse CPF']})
            else:
                info_societaria_serasa = pd.DataFrame.from_dict({'Nome':['O NG não consultou CNPJs para esse CPF']})
        except Exception:
            info_societaria_serasa = pd.DataFrame.from_dict({'Nome':['Erro ao pegar informações de participação societária cliente']})
        
        return [data,   
                resumo_traduzido, resumo_modalidade, 
                info_societaria_serasa]        


        
            
