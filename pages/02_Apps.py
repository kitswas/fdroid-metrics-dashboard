"""
App metrics page for F-Droid dashboard
"""

import streamlit as st

from views.apps import show_apps_page

st.set_page_config(
    page_title="App Metrics - F-Droid Dashboard", page_icon="📱", layout="wide"
)

show_apps_page()
