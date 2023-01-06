from datetime import datetime

import numpy as np
import pandas as pd
import pytz
import streamlit as st
from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode


def set_logo():
    st.markdown(
        f"""
        <div>
        <img class="center" width=100% height = 80px src=https://s3.sa-east-1.amazonaws.com/cdn.solfacil.com.br/assets/img/logo/solfacil-logo.svg>
        </div>
        """,
        unsafe_allow_html=True
    )


def show_df_aggrid(df, key_table="table_df"):
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_side_bar()
    from st_aggrid import JsCode
    gb.configure_column("Link Boleto PDF",
                        headerName="Link Boleto PDF",
                        cellRenderer=JsCode(
                            '''function(params) {return `<a href=${params.value} target="_blank">${params.value}</a>`}'''),
                        width=300)
    gb.configure_default_column(groupable=True,
                                value=True,
                                enableRowGroup=True,
                                aggFunc="sum",
                                editable=False)
    gb.configure_pagination(paginationPageSize=50, paginationAutoPageSize=False)
    gridOptions = gb.build()

    AgGrid(df, gridOptions=gridOptions,
           enable_enterprise_modules=True,
           allow_unsafe_jscode=True)


def apply_formats(string, format):
    allowed_formats = ['float', 'int', 'coin', 'percentage', 'cnpj', 'date']
    if format not in allowed_formats:
        raise LookupError(f"format '{format}' not found on allowed formats -> {allowed_formats}")
    if isinstance(string, float) and np.isnan(string):
        return '--'
    if not string:
        return "--"
    if format == "coin":
        mult = ''
        if abs(string) >= 1000000:
            string /= 1000000
            mult = " M"
        elif abs(string) >= 1000:
            string /= 1000
            mult = " K"
        x = "R$ {:,.2f}".format(string)
        return x + mult

    elif format == "int":
        return f'{str(int(round(string, 0)))}'

    elif format == "float":
        return "{:,.2f}".format(float(round(string, 2)))

    elif format == "percentage":
        return f'{str(round(string * 100, 2)).replace(".", ",")} %'

    elif format == "cnpj":
        if len(string) == 14:
            return '{}.{}.{}/{}-{}'.format(string[:2], string[2:5], string[5:8], string[8:12], string[12:14])
        elif len(string) == 8:
            return '{}.{}.{}'.format(string[:2], string[2:5], string[5:8])
        else:
            raise IOError(f"cnpj '{string}' must be 8 or 14 digits")

    elif format == 'date':
        try:
            date = pd.to_datetime(string).strftime("%d/%m/%Y")
        except Exception as e:
            date = string
        return date


def apply_formats_to_df(df, dict_formats):
    """
    Function to apply formats to a pandas dataframe given a column and format
    :param df: df
    :param dict_formats: dict to apply format... must be in shape {'col1':'format1', 'col2':'format2' etc.}
    :return: df with formats applied
    """
    for column, format in dict_formats.items():
        df[column] = df[column].apply(lambda x: apply_formats(x, format))

    return df


def get_current_local_time():
    return datetime.now(pytz.timezone("America/Sao_Paulo")).replace(tzinfo=None)

def format_bool(input) -> str:
    '''Format boolean to check_mark'''
    if str(input).lower() == '1':
        return ':heavy_check_mark:'
    return ':x:'