import json
import requests
from utils.aws import get_ssm_secret
import os


class Metabase():
    def __init__(self):
        self.token = None
        self.username = os.getenv("USERNAME_METABASE")
        self.pwd = os.getenv("PWD_METABASE")
        # self.username = json.loads(get_ssm_secret('login_apps_sf')).get('username')
        # self.pwd = '23081994'
        self.base_url = "https://metabase.solfacil.com.br/"
        self.get_token()

    def get_token(self):
        url = self.base_url + "api/session"
        headers = {
            "Content-Type": "application/json"
        }
        body = {
            "username": self.username,
            "password": self.pwd
        }

        response = requests.post(url, headers=headers, data=json.dumps(body))
        if response.status_code > 201:
            print("Erro", response.content)
            return False

        response_content = json.loads(response.content)
        self.token = response_content["id"]

        return True

    def get_question(self, num_question): #453 para a Partners
        question_url = self.base_url + "api/card/" + num_question + "/query/json"
        headers = {
            "Content-Type": "application/json",
            "X-Metabase-Session": self.token
        }

        response = requests.post(question_url, headers=headers)
        if response.status_code > 201:
            print("Error", response.content)
            return None
        response_content = json.loads(response.content)

        return response_content