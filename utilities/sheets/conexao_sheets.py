import os
import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build


ROOT_PATH = os.path.abspath(os.path.dirname(__file__))
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = os.path.join(ROOT_PATH, 'keys.json')
SAMPLE_SPREADSHEET_ID = '1ZcCExMqJZNvTvgIXISWpZWFkh14X-YXM-dL_rctQkDQ'


@st.cache(allow_output_mutation=True, show_spinner=False)
def get_data(linha, rerun):
    
    SAMPLE_RANGE_NAME = f'{linha}!A:D'

    service = build_connection()

    sheet = service.spreadsheets()

    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=SAMPLE_RANGE_NAME).execute()
    
    data = pd.DataFrame.from_dict(result['values'])
    data.columns = data.iloc[0]
    data.drop(data.index[0], inplace=True)
    
    del sheet
    del service
    return data

def build_connection():
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build('sheets', 'v4', credentials=creds)