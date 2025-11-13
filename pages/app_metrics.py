"""
App metrics page for F-Droid dashboard
"""

import streamlit as st

from etl.data_fetcher_ui import show_data_fetcher, show_quick_fetch_buttons
from views.apps import show_apps_page

st.set_page_config(
    page_title="App Metrics - F-Droid Dashboard", page_icon="📱", layout="wide"
)


# If fetcher UI is active, show only fetcher UI and back button
if st.session_state.get("show_apps_fetch", False):
    st.title("📥 Fetch Additional App Data")
    if st.button("⬅️ Back to App Metrics", key="apps_fetch_back"):
        st.session_state["show_apps_fetch"] = False
        st.rerun()
    # Show quick fetch buttons first for better UX
    data_fetched = show_quick_fetch_buttons("apps", "apps_sidebar_")
    # Show detailed data fetching interface if quick fetch wasn't used
    if not data_fetched:
        data_fetched = show_data_fetcher("apps", "apps_sidebar_")
    if data_fetched:
        st.success("✅ Data fetched successfully! Refresh 🔄 the page to see new data.")
else:
    show_apps_page()
