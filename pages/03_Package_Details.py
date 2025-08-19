"""
Package details page for F-Droid dashboard
"""

import pandas as pd
import streamlit as st

from etl.analyzer_apps import AppMetricsAnalyzer
from views.package_details import (
    show_package_details_page,
    show_package_search_and_select,
)

st.set_page_config(
    page_title="Package Details - F-Droid Dashboard", page_icon="📦", layout="wide"
)

# Check if we're viewing a specific package via query parameters or session state
package_id = st.query_params.get("package", None) or st.session_state.get(
    "selected_package", None
)

if package_id:
    # Clear the session state package since we're now viewing it
    if "selected_package" in st.session_state:
        # Update query params to reflect the current package
        st.query_params["package"] = package_id
        del st.session_state["selected_package"]
    # Show package details page
    st.sidebar.title("F-Droid Metrics")
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📦 Package Details")
    st.sidebar.markdown(f"**Viewing:** `{package_id}`")

    if st.sidebar.button("🔙 Back to Package Browser"):
        st.query_params.clear()
        st.rerun()

    # Initialize analyzer and get dates
    analyzer = AppMetricsAnalyzer()
    available_dates = analyzer.get_available_dates()

    if not available_dates:
        st.error("No app data found. Please fetch data first from the main Apps page.")
        if st.button("Go to Apps Page"):
            st.switch_page("pages/02_Apps.py")
        st.stop()

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

else:
    # Show package browser page
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
            st.switch_page("pages/02_Apps.py")
    else:
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
