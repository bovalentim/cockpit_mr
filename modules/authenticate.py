import os
import streamlit as st
import requests
import json

from utils.aws import get_ssm_secret


def generate_login_block():
    block1 = st.sidebar.empty()
    block2 = st.sidebar.empty()
    block3 = st.sidebar.empty()

    return block1, block2, block3


def clean_blocks(blocks):
        blocks.empty()


def set_login(blocks):
    form = blocks.form("Login")
    form.markdown(f'''<h5 style="background-color:#FFFFFF;
        text-align: center;
        color:#666;
        font: Rubik,sans-serif;
        font-size:1.25rem;
        border-radius:2%;
        ">
        <b>Login @ Solfácil
        </b>
        </h5>''', unsafe_allow_html=True)

    return form


def is_authenticated(email, password):
    simulate = False
    if simulate == True:
        email_teste = 'fernando.vanes'
        st.session_state.email = email_teste+"@solfacil.com.br"
        st.session_state.access = 'user'
        return True
    else:
        url = "https://4ntmpjzj43.execute-api.us-east-2.amazonaws.com/prod/"
        headers = {
            "API-TOKEN": json.loads(get_ssm_secret("AUTH_API_TOKEN"))[os.getenv("ENV")],
            "content-type": "application/json"
        }

        body = {
            "username": email,
            "pwd": password,
            "application": "cockpit_mr"
        }

        response = requests.post(url=url, data=json.dumps(body), headers=headers)
        if response.status_code > 201:
            print(response.content)
            return False

        st.session_state.email = email
        st.session_state.access = json.loads(response.content)['permission']['access']
        return True


def login(blocks):
    form = set_login(blocks)
    email = form.text_input("E-mail")
    pwd = form.text_input("Senha", type="password")
    btn = form.form_submit_button("ENTRAR")
    return email, pwd, btn


def login_block():
    # login_blocks = generate_login_block()
    login_blocks = st.empty()
    email, password, btn = login(login_blocks)
    if st.session_state.email or (btn and email and password and is_authenticated(email, password)):
        clean_blocks(login_blocks)
    else:
        st.stop()
