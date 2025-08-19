"""
F-Droid Metrics Dashboard - Multipage Application
"""

import streamlit as st

from etl.analyzer_apps import AppMetricsAnalyzer
from pages.apps import show_apps_page
from pages.package_details import (
    show_package_details_page,
    show_package_search_and_select,
)
from pages.search import show_search_page


def main():
    """Main dashboard application."""
    st.set_page_config(
        page_title="F-Droid Metrics Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Check if we're viewing a specific package
    package_id = st.query_params.get("package", None)

    if package_id:
        # Show package details page
        st.sidebar.title("📊 F-Droid Metrics")
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 📦 Package Details")
        st.sidebar.markdown(f"**Viewing:** `{package_id}`")

        if st.sidebar.button("🔙 Back to Main Dashboard"):
            st.query_params.clear()
            st.rerun()

        # Initialize analyzer and get dates
        analyzer = AppMetricsAnalyzer()
        available_dates = analyzer.get_available_dates()

        if not available_dates:
            st.error(
                "No app data found. Please fetch data first from the main Apps page."
            )
            if st.button("Go to Apps Page"):
                st.query_params.clear()
                st.rerun()
            return

        # Date selection for package details
        st.sidebar.subheader("Date Range")
        if len(available_dates) > 1:
            start_date = st.sidebar.date_input(
                "Start Date",
                value=pd.to_datetime(available_dates[0]).date(),
                min_value=pd.to_datetime(available_dates[0]).date(),
                max_value=pd.to_datetime(available_dates[-1]).date(),
                key="package_start_date",
            )
            end_date = st.sidebar.date_input(
                "End Date",
                value=pd.to_datetime(available_dates[-1]).date(),
                min_value=pd.to_datetime(available_dates[0]).date(),
                max_value=pd.to_datetime(available_dates[-1]).date(),
                key="package_end_date",
            )

            # Filter dates
            selected_dates = [
                date
                for date in available_dates
                if start_date <= pd.to_datetime(date).date() <= end_date
            ]
        else:
            selected_dates = available_dates
            st.sidebar.info(f"Only one date available: {available_dates[0]}")

        show_package_details_page(package_id, analyzer, selected_dates)
        return

    # Main navigation for normal dashboard
    st.sidebar.title("📊 F-Droid Metrics")
    st.sidebar.markdown("---")

    # Page selection
    page = st.sidebar.selectbox(
        "Select Dashboard",
        ["🔍 Search Metrics", "📱 App Metrics", "📦 Package Browser"],
        index=0,
    )

    st.sidebar.markdown("---")
    st.sidebar.markdown("### About")
    st.sidebar.markdown(
        "This dashboard analyzes metrics from the F-Droid app store, "
        "including search patterns and app download statistics."
    )

    st.sidebar.markdown("### Data Sources")
    st.sidebar.markdown("- **Search**: search.f-droid.org")
    st.sidebar.markdown("- **Apps**: http01/02/03.fdroid.net, originserver.f-droid.org")

    # Route to appropriate page
    if page == "🔍 Search Metrics":
        show_search_page()
    elif page == "📱 App Metrics":
        show_apps_page()
    elif page == "📦 Package Browser":
        show_package_browser_page()


def show_package_browser_page():
    """Show the package browser page."""
    st.title("📦 F-Droid Package Browser")
    st.markdown(
        "Browse and analyze individual F-Droid packages with detailed download statistics."
    )

    # Initialize analyzer
    analyzer = AppMetricsAnalyzer()

    # Date selection
    available_dates = analyzer.get_available_dates()
    if not available_dates:
        st.warning("No app data files found locally.")
        st.info("💡 **Please fetch data first from the App Metrics page.**")

        if st.button("📱 Go to App Metrics"):
            # This will trigger a rerun and change the page selection
            st.query_params.clear()
            st.session_state["dashboard_page"] = "📱 App Metrics"
            st.rerun()
        return

    st.sidebar.subheader("Date Range")
    if len(available_dates) > 1:
        start_date = st.sidebar.date_input(
            "Start Date",
            value=pd.to_datetime(available_dates[0]).date(),
            min_value=pd.to_datetime(available_dates[0]).date(),
            max_value=pd.to_datetime(available_dates[-1]).date(),
            key="browser_start_date",
        )
        end_date = st.sidebar.date_input(
            "End Date",
            value=pd.to_datetime(available_dates[-1]).date(),
            min_value=pd.to_datetime(available_dates[0]).date(),
            max_value=pd.to_datetime(available_dates[-1]).date(),
            key="browser_end_date",
        )

        # Filter dates
        selected_dates = [
            date
            for date in available_dates
            if start_date <= pd.to_datetime(date).date() <= end_date
        ]
    else:
        selected_dates = available_dates
        st.sidebar.info(f"Only one date available: {available_dates[0]}")

    # Show package search and selection interface
    show_package_search_and_select(analyzer, selected_dates)


if __name__ == "__main__":
    import pandas as pd

    main()
