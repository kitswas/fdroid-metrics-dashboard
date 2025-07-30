"""
F-Droid Metrics Dashboard - Multipage Application
"""

import streamlit as st

from pages.apps import show_apps_page
from pages.search import show_search_page


def main():
    """Main dashboard application."""
    st.set_page_config(
        page_title="F-Droid Metrics Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Main navigation
    st.sidebar.title("📊 F-Droid Metrics")
    st.sidebar.markdown("---")

    # Page selection
    page = st.sidebar.selectbox(
        "Select Dashboard", ["🔍 Search Metrics", "📱 App Metrics"], index=0
    )

    st.sidebar.markdown("---")
    st.sidebar.markdown("### About")
    st.sidebar.markdown(
        "This dashboard analyzes metrics from the F-Droid app store, "
        "including search patterns and app download statistics."
    )

    st.sidebar.markdown("### Data Sources")
    st.sidebar.markdown("- **Search**: search.f-droid.org")
    st.sidebar.markdown("- **Apps**: http01/02/03.fdroid.net")

    # Route to appropriate page
    if page == "🔍 Search Metrics":
        show_search_page()
    elif page == "📱 App Metrics":
        show_apps_page()


if __name__ == "__main__":
    main()
