import ast
import traceback
import requests
import pandas as pd
from io import StringIO
import boto3

class metabase():
    def __init__(self):
        self.session = requests.Session()   
        self.token = get_secret()

    def get_question(self, question): 
        url = "https://metabase.solfacil.com.br/api/card/" + question + "/query/json"

        header = {
                'accept': 'application/json',
                "X-Metabase-Session": str(self.token) 
                }
        
        r = self.session.post(url, headers = header)
        tbl = pd.read_json(StringIO(r.text), convert_dates=True)

        return tbl

    def id_data(self, question, id): 
        url = "https://metabase.solfacil.com.br/api/card/" + question + "/query"

        header = {
                'accept': 'application/json',
                "X-Metabase-Session": str(self.token) 
                }
        json_data = {
                'ignore_cache': False,
                'parameters': [
                    {
                        'type': 'category',
                        'value': str(id),
                        'target': [
                            'variable',
                            [
                                'template-tag',
                                'id',
                            ],
                        ],
                    },
                ],
            }
        
        r = self.session.post(url, headers = header, json=json_data)
        tbl = pd.read_json(StringIO(r.text))
        colunas = []
        for i in tbl.iloc[1]['data']:
            colunas.append(i['display_name'])

        info = pd.DataFrame(tbl.iloc[0]['data'], columns=colunas).astype(str)

        return info.loc[0]

    def id_data_asdict(self, question, id): 
        url = "https://metabase.solfacil.com.br/api/card/" + question + "/query"

        header = {
                'accept': 'application/json',
                "X-Metabase-Session": str(self.token) 
                }
        json_data = {
                'ignore_cache': False,
                'parameters': [
                    {
                        'type': 'category',
                        'value': str(id),
                        'target': [
                            'variable',
                            [
                                'template-tag',
                                'id',
                            ],
                        ],
                    },
                ],
            }
        
        r = self.session.post(url, headers=header, json=json_data)
        tbl = pd.read_json(StringIO(r.text))
        colunas = []
        for i in tbl.iloc[1]['data']:
            colunas.append(i['display_name'])

        index = 0
        info_dict = {}
        for i in tbl.iloc[0]['data'][0]:
            info_dict[colunas[index]] = i
            index += 1

        return info_dict

def get_secret():

    secret_name = "metabase.token"
    region_name = "us-east-2"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except Exception:
        return None

    return ast.literal_eval(get_secret_value_response['SecretString'])['token']
    

