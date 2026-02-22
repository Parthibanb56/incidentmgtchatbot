import streamlit as st

st.set_page_config(page_title="Policy AI Assistant", layout="wide")

st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Chat Assistant", "Dashboard"])