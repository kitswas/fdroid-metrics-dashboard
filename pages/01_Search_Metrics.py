"""
Search metrics page for F-Droid dashboard
"""

import streamlit as st

from views.search import show_search_page

st.set_page_config(
    page_title="Search Metrics - F-Droid Dashboard", page_icon="🔍", layout="wide"
)

show_search_page()
