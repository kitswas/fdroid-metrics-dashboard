"""
Search metrics page for F-Droid dashboard
"""

import streamlit as st

from etl.data_fetcher_ui import show_data_fetcher, show_quick_fetch_buttons
from views.search import show_search_page

st.set_page_config(
    page_title="Search Metrics - F-Droid Dashboard", page_icon="🔍", layout="wide"
)

# If fetcher UI is active, show only fetcher UI and back button
if st.session_state.get("show_search_fetch", False):
    st.title("📥 Fetch Additional Search Data")
    if st.button("⬅️ Back to Search Metrics", key="search_fetch_back"):
        st.session_state["show_search_fetch"] = False
        st.rerun()
    # Show quick fetch buttons first for better UX
    data_fetched = show_quick_fetch_buttons("search", "search_sidebar_")
    # Show detailed data fetching interface if quick fetch wasn't used
    if not data_fetched:
        data_fetched = show_data_fetcher("search", "search_sidebar_")
    if data_fetched:
        st.success("✅ Data fetched successfully! Refresh 🔄 the page to see new data.")
else:
    show_search_page()
