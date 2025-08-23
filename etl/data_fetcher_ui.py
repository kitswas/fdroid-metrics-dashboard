"""
Data fetching UI components for F-Droid dashboard
"""

from datetime import datetime, timedelta

import streamlit as st

from etl.data_fetcher import DataFetcher


def show_data_fetcher(data_type: str, key_prefix: str = "") -> bool:
    """Show data fetching interface for search or app data."""
    st.subheader(f"📥 Fetch {data_type.title()} Data")

    # Initialize data fetcher
    if "data_fetcher" not in st.session_state:
        st.session_state.data_fetcher = DataFetcher()

    fetcher = st.session_state.data_fetcher

    # Check current data availability
    with st.expander("📊 Data Availability Status", expanded=False):
        try:
            availability = fetcher.check_data_availability(data_type)

            col1, col2 = st.columns(2)
            with col1:
                st.metric("Local Files", availability["local_count"])
                if availability["local_date_range"][0]:
                    st.write(
                        f"**Local Range:** {availability['local_date_range'][0]} to {availability['local_date_range'][1]}"
                    )
                else:
                    st.write("**No local data found**")

            with col2:
                st.metric("Remote Files", availability["remote_count"])
                if availability["remote_date_range"][0]:
                    st.write(
                        f"**Remote Range:** {availability['remote_date_range'][0]} to {availability['remote_date_range'][1]}"
                    )
                else:
                    st.write("**No remote data available**")

            if availability["missing_dates"]:
                missing_count = len(availability["missing_dates"])
                st.info(
                    f"Found {missing_count} files that can be downloaded. First 10: {', '.join(availability['missing_dates'])}"
                )

        except Exception as e:
            st.error(f"Failed to check data availability: {e}")

    # Date range selection
    st.subheader("📅 Select Date Range to Fetch")

    col1, col2 = st.columns(2)

    with col1:
        # Default to last 7 days
        default_start = (datetime.now() - timedelta(days=7)).date()
        start_date = st.date_input(
            "Start Date", value=default_start, key=f"{key_prefix}fetch_start_date"
        )

    with col2:
        end_date = st.date_input(
            "End Date", value=datetime.now().date(), key=f"{key_prefix}fetch_end_date"
        )

    # Validate date range
    if start_date > end_date:
        st.error("Start date must be before end date")
        return False

    # Calculate number of days
    date_range_days = (end_date - start_date).days + 1

    if data_type == "apps":
        # For apps, we fetch from 3 servers
        total_files = date_range_days * 3
        st.info(
            f"Will attempt to fetch {date_range_days} days × 3 servers = {total_files} files"
        )
    else:
        st.info(f"Will attempt to fetch {date_range_days} files")

    # Check what files would be missing
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    try:
        missing_dates = fetcher.get_missing_dates(data_type, start_str, end_str)
        if missing_dates:
            st.warning(f"Found {len(missing_dates)} missing files in date range")
            with st.expander("Show missing dates"):
                st.write(missing_dates)
        else:
            st.success("All files for this date range are already downloaded!")
    except Exception as e:
        st.error(f"Failed to check missing dates: {e}")

    # Fetch button
    if st.button(
        f"🚀 Fetch {data_type.title()} Data",
        key=f"{key_prefix}fetch_button",
        type="primary",
    ):
        return fetch_data_with_progress(fetcher, data_type, start_str, end_str)

    return False


def fetch_data_with_progress(
    fetcher: DataFetcher, data_type: str, start_date: str, end_date: str
) -> bool:
    """Fetch data with progress feedback."""
    try:
        with st.spinner(f"Fetching {data_type} data..."):
            progress_bar = st.progress(0)
            status_text = st.empty()

            def progress_callback(progress: float) -> None:
                progress_bar.progress(progress)

            def status_callback(msg: str) -> None:
                status_text.text(msg)

            results = fetcher.fetch_date_range(
                data_type,
                start_date,
                end_date,
                progress_callback=progress_callback,
                status_callback=status_callback,
            )

        # Show results
        st.subheader("📋 Fetch Results")

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Files", results["total_files"])
        with col2:
            st.metric("✅ Successful", results["successful"], delta=None)
        with col3:
            st.metric("⏭️ Skipped", results["skipped"], delta=None)
        with col4:
            st.metric("❌ Failed", results["failed"], delta=None)

        # Show success/failure breakdown
        if results["successful"] > 0:
            st.success(f"Successfully downloaded {results['successful']} files!")

        if results["skipped"] > 0:
            st.info(f"Skipped {results['skipped']} files (already exist)")

        if results["failed"] > 0:
            st.error(f"Failed to download {results['failed']} files")
            with st.expander("Show errors"):
                for error in results["errors"]:
                    st.text(error)

        return results["successful"] > 0 or results["skipped"] > 0

    except Exception as e:
        st.error(f"Failed to fetch data: {e}")
        return False


def show_quick_fetch_buttons(data_type: str, key_prefix: str = "") -> bool:
    """Show quick fetch buttons for common time ranges."""
    st.subheader("⚡ Quick Fetch")
    st.markdown("Fetch data for common time ranges:")

    if "data_fetcher" not in st.session_state:
        st.session_state.data_fetcher = DataFetcher()

    fetcher = st.session_state.data_fetcher

    col1, col2, col3, col4 = st.columns(4)

    today = datetime.now().date()

    with col1:
        if st.button("Last week", key=f"{key_prefix}quick_week"):
            start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
            end_date = today.strftime("%Y-%m-%d")
            return fetch_data_with_progress(fetcher, data_type, start_date, end_date)

    with col2:
        if st.button("Last month", key=f"{key_prefix}quick_month"):
            start_date = (today - timedelta(days=30)).strftime("%Y-%m-%d")
            end_date = today.strftime("%Y-%m-%d")
            return fetch_data_with_progress(fetcher, data_type, start_date, end_date)

    with col3:
        if st.button("Last 3 months", key=f"{key_prefix}quick_3months"):
            start_date = (today - timedelta(days=90)).strftime("%Y-%m-%d")
            end_date = today.strftime("%Y-%m-%d")
            return fetch_data_with_progress(fetcher, data_type, start_date, end_date)

    with col4:
        if st.button("Last 6 months", key=f"{key_prefix}quick_6months"):
            start_date = (today - timedelta(days=180)).strftime("%Y-%m-%d")
            end_date = today.strftime("%Y-%m-%d")
            return fetch_data_with_progress(fetcher, data_type, start_date, end_date)

    return False
