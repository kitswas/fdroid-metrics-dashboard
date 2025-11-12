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
                st.metric("Local Snapshots", availability["local_count"])
                if availability["local_date_range"][0]:
                    st.write(
                        f"**Local Range:** {availability['local_date_range'][0]} to {availability['local_date_range'][1]}"
                    )
                else:
                    st.write("**No local data found**")

            with col2:
                st.metric("Remote Snapshots", availability["remote_count"])
                if availability["remote_date_range"][0]:
                    st.write(
                        f"**Remote Range:** {availability['remote_date_range'][0]} to {availability['remote_date_range'][1]}"
                    )
                else:
                    st.write("**No remote data available**")

            if availability["missing_dates"]:
                missing_count = len(availability["missing_dates"])
                st.info(
                    f"ℹ️ Found {missing_count}+ snapshots available to download. "
                    f"First 10 dates: {', '.join(availability['missing_dates'][:10])}"
                )

            st.caption(
                "💡 **Tip:** Data files are weekly snapshots. Each snapshot represents "
                "cumulative metrics since the previous week."
            )

        except Exception as e:
            st.error(f"Failed to check data availability: {e}")

    # Date range selection
    st.subheader("📅 Select Date Range to Fetch")

    st.info(
        "ℹ️ **Note:** Data files are weekly snapshots representing cumulative metrics "
        "since the previous snapshot. Files will be **downloaded and overwritten** to ensure fresh data."
    )

    col1, col2 = st.columns(2)

    with col1:
        # Default to last 30 days for better coverage of weekly snapshots
        default_start = (datetime.now() - timedelta(days=30)).date()
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

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    # Check what files are available in the date range
    try:
        # Get all available remote dates
        remote_dates = fetcher.get_available_remote_dates(data_type)

        # Filter by date range
        available_in_range = [
            date for date in remote_dates if start_str <= date <= end_str
        ]

        if data_type == "apps":
            # For apps, we now intelligently check per-server availability
            num_servers = len(fetcher.servers)

            # Get per-server availability to show realistic numbers
            server_dates = fetcher._get_apps_per_server_dates()
            available_per_server = {
                server: len([d for d in available_in_range if d in dates])
                for server, dates in server_dates.items()
            }
            total_available = sum(available_per_server.values())

            st.info(
                f"📊 Found **{len(available_in_range)}** available snapshots in date range\n\n"
                f"**Files to download:** {total_available} (across {num_servers} servers)\n\n"
                f"**Per-server availability:**\n"
                + "\n".join(
                    f"- {server}: {count} files"
                    for server, count in available_per_server.items()
                )
            )
        else:
            st.info(
                f"📊 Found **{len(available_in_range)}** available snapshots in date range "
                f"(**{len(available_in_range)} files** will be downloaded)"
            )

        if not available_in_range:
            st.warning("⚠️ No data files available on the server for this date range")
        else:
            with st.expander("📋 Show dates to be fetched"):
                st.write(available_in_range)

    except Exception as e:
        st.error(f"Failed to check available dates: {e}")

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
            st.metric("❌ Failed", results["failed"], delta=None)
        with col4:
            st.metric("⏭️ Skipped", results["skipped"], delta=None)

        # Show success/failure breakdown
        if results["successful"] > 0:
            st.success(f"Successfully downloaded {results['successful']} files!")

        if results["skipped"] > 0:
            st.info(
                f"ℹ️ {results['skipped']} files were skipped because they are not available on those servers"
            )

        if results["failed"] > 0:
            st.error(f"Failed to download {results['failed']} files")
            with st.expander("Show errors"):
                for error in results["errors"]:
                    st.text(error)

        return results["successful"] > 0

    except Exception as e:
        st.error(f"Failed to fetch data: {e}")
        return False


def show_quick_fetch_buttons(data_type: str, key_prefix: str = "") -> bool:
    """Show quick fetch buttons for common time ranges."""
    st.subheader("⚡ Quick Fetch")
    st.markdown(
        "Fetch data for common time ranges. The system will download available weekly snapshots "
        "within each period."
    )

    if "data_fetcher" not in st.session_state:
        st.session_state.data_fetcher = DataFetcher()

    fetcher = st.session_state.data_fetcher

    col1, col2, col3, col4 = st.columns(4)

    today = datetime.now().date()

    with col1:
        if st.button("Last 2 weeks", key=f"{key_prefix}quick_2weeks"):
            start_date = (today - timedelta(days=14)).strftime("%Y-%m-%d")
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
