"""
App metrics page for F-Droid dashboard
"""

import logging

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from etl.analyzer_apps import AppMetricsAnalyzer
from etl.data_fetcher_ui import show_data_fetcher, show_quick_fetch_buttons
from etl.fdroid_metadata import FDroidMetadataFetcher
from etl.getdata_apps import SERVERS

logger = logging.getLogger(__name__)


def show_apps_page():
    """Show the app metrics page."""
    st.title("📱 F-Droid App Metrics")
    st.markdown(
        "Analyze app download patterns and usage statistics from F-Droid HTTP servers (http01, http02, http03, originserver)."
    )

    # Initialize analyzer
    analyzer = AppMetricsAnalyzer()

    # Sidebar for filters
    st.sidebar.header("App Metrics Filters")

    # Date selection
    available_dates = analyzer.get_available_dates()
    if not available_dates:
        st.warning("No app data files found locally.")

        # Show data fetching interface
        st.info("💡 **Fetch data directly from F-Droid servers:**")

        # Create tabs for fetching and analysis
        tab_fetch, tab_analysis = st.tabs(["📥 Fetch Data", "📊 Analysis"])

        with tab_fetch:
            # Show data fetching interface
            data_fetched = show_data_fetcher("apps", "apps_")

            # Show quick fetch buttons
            if not data_fetched:
                data_fetched = show_quick_fetch_buttons("apps", "apps_")

            if data_fetched:
                st.success(
                    "✅ Data fetched successfully! Switch to the Analysis tab or refresh the page."
                )
                if st.button("🔄 Refresh Page", key="apps_refresh"):
                    st.rerun()

        with tab_analysis:
            st.info("Please fetch data first using the 'Fetch Data' tab.")

        return

    st.sidebar.subheader("Date Range")
    if len(available_dates) > 1:
        start_date = st.sidebar.date_input(
            "Start Date",
            value=pd.to_datetime(available_dates[0]).date(),
            min_value=pd.to_datetime(available_dates[0]).date(),
            max_value=pd.to_datetime(available_dates[-1]).date(),
            key="apps_start_date",
        )
        end_date = st.sidebar.date_input(
            "End Date",
            value=pd.to_datetime(available_dates[-1]).date(),
            min_value=pd.to_datetime(available_dates[0]).date(),
            max_value=pd.to_datetime(available_dates[-1]).date(),
            key="apps_end_date",
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

    # Add data fetching option in sidebar
    st.sidebar.markdown("---")
    st.sidebar.subheader("📥 Fetch More Data")
    if st.sidebar.button("🔽 Fetch Additional Data", key="apps_sidebar_fetch"):
        st.session_state.show_apps_fetch = True

    # Show data fetching interface if requested
    if st.session_state.get("show_apps_fetch", False):
        with st.expander("📥 Fetch Additional App Data", expanded=True):
            data_fetched = show_data_fetcher("apps", "apps_sidebar_")
            if data_fetched:
                st.success(
                    "✅ Data fetched successfully! Refresh the page to see new data."
                )
                if st.button("🔄 Refresh Page", key="apps_sidebar_refresh"):
                    st.session_state.show_apps_fetch = False
                    st.rerun()

    # Main content tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
        [
            "📈 Overview",
            "📂 Request Paths",
            "📦 Packages",
            "🌍 Geographic",
            "🖥️ Server Comparison",
            "🛠️ Technical",
        ]
    )

    with tab1:
        show_apps_overview(analyzer, selected_dates)

    with tab2:
        show_path_analysis(analyzer, selected_dates)

    with tab3:
        show_package_analysis(analyzer, selected_dates)

    with tab4:
        show_apps_geographic_analysis(analyzer, selected_dates)

    with tab5:
        show_server_comparison(analyzer, selected_dates)

    with tab6:
        show_apps_technical_analysis(analyzer, selected_dates)


def show_apps_overview(analyzer: AppMetricsAnalyzer, dates: list):
    """Show overview metrics."""
    st.header("📈 App Metrics Overview")

    if len(dates) == 1:
        # Single day analysis
        summary = analyzer.get_daily_summary(dates[0])

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Hits", f"{summary['total_hits']:,}")
        with col2:
            st.metric("Active Servers", f"{summary['servers_active']}/{len(SERVERS)}")
        with col3:
            st.metric("Unique Paths", f"{summary['unique_paths']:,}")
        with col4:
            error_rate = (
                (summary["total_errors"] / summary["total_hits"]) * 100
                if summary["total_hits"] > 0
                else 0
            )
            st.metric("Error Rate", f"{error_rate:.1f}%")

        # Top paths and countries charts
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Top Request Paths")
            if summary["top_paths"]:
                paths_df = pd.DataFrame(summary["top_paths"], columns=["Path", "Hits"])
                # Shorten long paths for display
                paths_df["Short Path"] = paths_df["Path"].apply(
                    lambda x: x if len(x) <= 40 else x[:37] + "..."
                )
                fig = px.bar(
                    paths_df.head(10), x="Hits", y="Short Path", orientation="h"
                )
                fig.update_layout(
                    height=400, yaxis={"categoryorder": "total ascending"}
                )
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Top Countries")
            if summary["top_countries"]:
                countries_df = pd.DataFrame(
                    summary["top_countries"], columns=["Country", "Hits"]
                )
                fig = px.pie(countries_df.head(10), values="Hits", names="Country")
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

    else:
        # Multi-day time series
        ts_data = analyzer.get_time_series_data(dates)

        if not ts_data.empty:
            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Hits", f"{ts_data['total_hits'].sum():,}")
            with col2:
                st.metric("Avg Weekly Hits", f"{ts_data['total_hits'].mean():.0f}")
            with col3:
                st.metric("Peak Week Hits", f"{ts_data['total_hits'].max():,}")
            with col4:
                st.metric(
                    "Avg Servers Active",
                    f"{ts_data['servers_active'].mean():.1f}/{len(SERVERS)}",
                )

            # Time series charts
            fig = make_subplots(
                rows=3,
                cols=1,
                subplot_titles=(
                    "Weekly App Hits",
                    "Active Servers",
                    "Unique Request Paths",
                ),
                vertical_spacing=0.1,
            )

            fig.add_trace(
                go.Scatter(
                    x=ts_data["date"], y=ts_data["total_hits"], name="Total Hits"
                ),
                row=1,
                col=1,
            )

            fig.add_trace(
                go.Scatter(
                    x=ts_data["date"],
                    y=ts_data["servers_active"],
                    name="Active Servers",
                ),
                row=2,
                col=1,
            )

            fig.add_trace(
                go.Scatter(
                    x=ts_data["date"],
                    y=ts_data["unique_paths"],
                    name="Unique Paths",
                ),
                row=3,
                col=1,
            )

            fig.update_layout(height=800, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)


def show_path_analysis(analyzer: AppMetricsAnalyzer, dates: list):
    """Show detailed path analysis."""
    st.header("📂 Request Path Analysis")
    st.info(
        "📊 **Data Frequency:** The data is collected weekly. 'Weeks Active' indicates the number of weeks where the path had actual requests (hits > 0)."
    )

    path_df = analyzer.get_path_analysis(dates)

    if path_df.empty:
        st.warning("No path data available for selected dates.")
        return

    # Summary metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Unique Paths", len(path_df))
    with col2:
        if len(path_df) > 0:
            path = path_df.iloc[0]["path"]
            truncated_path = path if len(path) <= 50 else path[:47] + "..."
            st.metric("Most Popular Path", truncated_path)
        else:
            st.metric("Most Popular Path", "N/A")
    with col3:
        st.metric(
            "Max Path Hits",
            f"{path_df['total_hits'].max():,}" if len(path_df) > 0 else "0",
        )

    # Filters
    col1, col2 = st.columns(2)
    with col1:
        min_hits = st.number_input(
            "Minimum total hits", min_value=0, value=100, key="apps_min_hits"
        )
    with col2:
        top_n = st.selectbox(
            "Show top N paths", [10, 20, 50, 100], index=1, key="apps_top_n"
        )

    # Filter data
    filtered_df = path_df[path_df["total_hits"] >= min_hits].head(top_n)

    # Path categories analysis
    if not filtered_df.empty:
        st.subheader("Path Categories")

        # Categorize paths
        def categorize_path(path):
            if path == "/":
                return "Root"
            elif "/repo/" in path and ".jar" in path:
                return "Repository JAR"
            elif "/archive/" in path and ".jar" in path:
                return "Archive JAR"
            elif "/repo/diff/" in path:
                return "Repository Diff"
            elif "/repo/" in path:
                return "Repository"
            elif "/archive/" in path:
                return "Archive"
            else:
                return "Other"

        filtered_df["category"] = filtered_df["path"].apply(categorize_path)
        category_stats = (
            filtered_df.groupby("category")["total_hits"].sum().reset_index()
        )
        category_stats = category_stats.sort_values("total_hits", ascending=False)

        col1, col2 = st.columns(2)
        with col1:
            fig = px.pie(
                category_stats,
                values="total_hits",
                names="category",
                title="Hits by Path Category",
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.bar(
                category_stats,
                x="total_hits",
                y="category",
                orientation="h",
                title="Path Category Hits",
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, use_container_width=True)

        # Top paths chart
        st.subheader(f"Top {min(20, len(filtered_df))} Request Paths")
        display_df = filtered_df.head(20).copy()
        display_df["short_path"] = display_df["path"].apply(
            lambda x: x if len(x) <= 60 else x[:57] + "..."
        )

        fig = px.bar(
            display_df,
            x="total_hits",
            y="short_path",
            title="Most Requested Paths",
            labels={"total_hits": "Total Hits", "short_path": "Request Path"},
        )
        fig.update_layout(height=600, yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)

        # Path statistics table
        st.subheader("Path Statistics")
        display_table_df = filtered_df[
            ["path", "total_hits", "appearances", "avg_hits", "category"]
        ].copy()
        display_table_df["avg_hits"] = display_table_df["avg_hits"].round(1)
        st.dataframe(
            display_table_df,
            use_container_width=True,
            column_config={
                "appearances": st.column_config.NumberColumn("Weeks Active"),
                "avg_hits": st.column_config.NumberColumn("Avg Hits/Week"),
                "total_hits": st.column_config.NumberColumn("Total Hits"),
            },
        )


def show_package_analysis(analyzer: AppMetricsAnalyzer, dates: list):
    """Show F-Droid package API analysis."""
    st.header("📦 F-Droid Package API Analysis")
    st.markdown(
        "Analyze package requests to the F-Droid API (`/api/v1/packages/`) to understand which apps are most accessed."
    )
    st.info(
        "📊 **Data Frequency:** The data is collected weekly. 'Weeks Active' indicates the number of weeks where the package had actual downloads (hits > 0)."
    )

    package_df = analyzer.get_package_analysis(dates)

    if package_df.empty:
        st.warning("No package API data available for selected dates.")
        return

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Packages", len(package_df))
    with col2:
        st.metric("Total Package Hits", f"{package_df['total_hits'].sum():,}")
    with col3:
        if len(package_df) > 0:
            package_name = package_df.iloc[0]["package_name"]
            truncated_name = (
                package_name if len(package_name) <= 30 else package_name[:27] + "..."
            )
            st.metric("Most Popular Package", truncated_name)
        else:
            st.metric("Most Popular Package", "N/A")
    with col4:
        st.metric(
            "Max Package Hits",
            f"{package_df['total_hits'].max():,}" if len(package_df) > 0 else "0",
        )

    # Filters
    col1, col2 = st.columns(2)
    with col1:
        min_hits = st.number_input(
            "Minimum total hits", min_value=0, value=50, key="packages_min_hits"
        )
    with col2:
        top_n = st.selectbox(
            "Show top N packages", [10, 20, 50, 100], index=1, key="packages_top_n"
        )

    # Filter data
    filtered_df = package_df[package_df["total_hits"] >= min_hits].head(top_n)

    if not filtered_df.empty:
        # Package categories analysis
        st.subheader("Package Categories")

        # Initialize metadata fetcher with caching
        @st.cache_resource
        def get_metadata_fetcher():
            """Get a cached instance of the metadata fetcher."""
            return FDroidMetadataFetcher(cache_dir="./cache/metadata")

        metadata_fetcher = get_metadata_fetcher()

        # Show cache statistics
        cache_stats = metadata_fetcher.get_cache_stats()
        if st.checkbox("Show metadata cache info", value=False):
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Cached Packages", cache_stats["cached_packages"])
            with col2:
                st.metric("Memory Cache", cache_stats["memory_cache_size"])
            with col3:
                st.metric("Cache Size (MB)", cache_stats["cache_dir_size_mb"])

            if st.button("Clear Metadata Cache"):
                metadata_fetcher.clear_cache()
                st.success("Cache cleared!")
                st.rerun()

        def categorize_package(package_name):
            """Get the real F-Droid category for a package, with fallback to pattern-based categorization."""
            return metadata_fetcher.get_primary_category(package_name)

        # Show progress while fetching metadata
        progress_text = f"Fetching F-Droid metadata for {len(filtered_df)} packages..."
        progress_bar = st.progress(0, text=progress_text)

        categories = []
        for i, package_name in enumerate(filtered_df["package_name"]):
            category = categorize_package(package_name)
            categories.append(category)

            # Update progress
            progress = (i + 1) / len(filtered_df)
            progress_bar.progress(
                progress, text=f"{progress_text} ({i + 1}/{len(filtered_df)})"
            )

        filtered_df["category"] = categories
        progress_bar.empty()  # Clear progress bar
        category_stats = (
            filtered_df.groupby("category")["total_hits"].sum().reset_index()
        )
        category_stats = category_stats.sort_values("total_hits", ascending=False)

        col1, col2 = st.columns(2)
        with col1:
            fig = px.pie(
                category_stats,
                values="total_hits",
                names="category",
                title="Package Hits by Category",
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.bar(
                category_stats,
                x="total_hits",
                y="category",
                orientation="h",
                title="Package Category Hits",
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, use_container_width=True)

        # Top packages chart
        st.subheader(f"Top {min(20, len(filtered_df))} Most Requested Packages")
        display_df = filtered_df.head(20).copy()
        display_df["short_name"] = display_df["package_name"].apply(
            lambda x: x if len(x) <= 50 else x[:47] + "..."
        )

        fig = px.bar(
            display_df,
            x="total_hits",
            y="short_name",
            title="Most Requested F-Droid Packages",
            labels={"total_hits": "Total Hits", "short_name": "Package Name"},
            color="category",
        )
        fig.update_layout(height=600, yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)

        # Average hits over time analysis
        if len(dates) > 1:
            st.subheader("Package Popularity Trends")
            st.markdown("Shows average hits per week for the most popular packages")

            trend_df = filtered_df.head(10)[
                ["package_name", "avg_hits", "appearances"]
            ].copy()
            fig = px.scatter(
                trend_df,
                x="appearances",
                y="avg_hits",
                size="avg_hits",
                hover_name="package_name",
                title="Package Consistency vs Popularity",
                labels={
                    "appearances": "Weeks Active",
                    "avg_hits": "Average Hits per Week",
                },
            )
            st.plotly_chart(fig, use_container_width=True)

        # Package statistics table
        st.subheader("Package Statistics")
        display_table_df = filtered_df[
            ["package_name", "total_hits", "appearances", "avg_hits", "category"]
        ].copy()
        display_table_df["avg_hits"] = display_table_df["avg_hits"].round(1)
        display_table_df = display_table_df.rename(
            columns={
                "package_name": "Package Name",
                "total_hits": "Total Hits",
                "appearances": "Weeks Active",
                "avg_hits": "Avg Hits/Week",
                "category": "Category",
            }
        )
        st.dataframe(display_table_df, use_container_width=True)

        # Package search functionality
        st.subheader("🔍 Search Packages")
        search_term = st.text_input(
            "Search for specific packages:",
            placeholder="Enter package name or keyword...",
        )

        if search_term:
            search_results = package_df[
                package_df["package_name"].str.contains(
                    search_term, case=False, na=False
                )
            ]

            if not search_results.empty:
                st.write(
                    f"Found {len(search_results)} packages matching '{search_term}':"
                )
                search_display = search_results[
                    ["package_name", "total_hits", "appearances", "avg_hits"]
                ].copy()
                search_display["avg_hits"] = search_display["avg_hits"].round(1)
                st.dataframe(search_display, use_container_width=True)
            else:
                st.info(f"No packages found matching '{search_term}'")


def show_apps_geographic_analysis(analyzer: AppMetricsAnalyzer, dates: list):
    """Show geographic analysis."""
    st.header("🌍 App Downloads Geographic Analysis")

    country_df = analyzer.get_country_analysis(dates)

    if country_df.empty:
        st.warning("No geographic data available for selected dates.")
        return

    # Remove unknown countries (marked as '-')
    known_countries = country_df[country_df["country"] != "-"]
    unknown_hits = country_df[country_df["country"] == "-"]["total_hits"].sum()

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Countries Represented", len(known_countries))
    with col2:
        top_country = known_countries.iloc[0] if len(known_countries) > 0 else None
        st.metric(
            "Top Country", top_country["country"] if top_country is not None else "N/A"
        )
    with col3:
        st.metric(
            "Top Country Hits",
            f"{top_country['total_hits']:,}" if top_country is not None else "0",
        )
    with col4:
        st.metric("Unknown Location Hits", f"{unknown_hits:,}")

    # Top countries chart
    if not known_countries.empty:
        top_countries = known_countries.head(20)

        fig = px.bar(
            top_countries,
            x="total_hits",
            y="country",
            title="Top 20 Countries by App Downloads",
            labels={"total_hits": "Total Hits", "country": "Country"},
        )
        fig.update_layout(height=600, yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)

        # Geographic distribution table
        st.subheader("Country Statistics")
        display_df = known_countries.head(50)[
            ["country", "total_hits", "avg_hits"]
        ].copy()
        display_df["avg_hits"] = display_df["avg_hits"].round(1)
        st.dataframe(display_df, use_container_width=True)


def show_server_comparison(analyzer: AppMetricsAnalyzer, dates: list):
    """Show server comparison analysis."""
    st.header("🖥️ Server Comparison")

    if len(dates) == 1:
        # Single day server comparison
        comparison_df = analyzer.get_server_comparison(dates[0])

        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric(
                "Active Servers",
                f"{len(comparison_df[comparison_df['hits'] > 0])}/{len(SERVERS)}",
            )
        with col2:
            st.metric("Total Combined Hits", f"{comparison_df['hits'].sum():,}")
        with col3:
            top_server = (
                comparison_df.loc[comparison_df["hits"].idxmax()]
                if comparison_df["hits"].sum() > 0
                else None
            )
            st.metric(
                "Top Server",
                top_server["server"].item() if top_server is not None else "N/A",
            )
        with col4:
            st.metric(
                "Top Server Hits",
                f"{top_server['hits']:,}" if top_server is not None else "0",
            )

        # Server comparison charts
        col1, col2 = st.columns(2)

        with col1:
            fig = px.bar(comparison_df, x="server", y="hits", title="Hits by Server")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.bar(
                comparison_df,
                x="server",
                y="unique_paths",
                title="Unique Paths by Server",
            )
            st.plotly_chart(fig, use_container_width=True)

        # Detailed comparison table
        st.subheader("Server Details")
        st.dataframe(comparison_df, use_container_width=True)

    else:
        # Multi-day server analysis
        st.info(
            "Multi-day server comparison analysis - showing aggregated data across selected dates"
        )

        # Get time series data to show server activity over time
        ts_data = analyzer.get_time_series_data(dates)

        if not ts_data.empty:
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=ts_data["date"],
                    y=ts_data["servers_active"],
                    mode="lines+markers",
                    name="Active Servers",
                    line=dict(width=3),
                )
            )
            fig.update_layout(
                title="Server Activity Over Time",
                xaxis_title="Date",
                yaxis_title="Number of Active Servers",
                yaxis=dict(range=[0, 3.5], dtick=1),
            )
            st.plotly_chart(fig, use_container_width=True)


def show_apps_technical_analysis(analyzer: AppMetricsAnalyzer, dates: list):
    """Show technical analysis including errors and detailed metrics."""
    st.header("🛠️ App Metrics Technical Analysis")

    # Load error data for the selected dates
    all_errors = {}
    all_paths = {}
    server_stats = {
        server: {"hits": 0, "errors": 0, "dates": 0} for server in analyzer.servers
    }

    for date in dates:
        try:
            data = analyzer.load_merged_data(date)

            # Track which servers were active
            active_servers = data.get("servers", [])
            for server in active_servers:
                server_stats[server]["dates"] += 1

                try:
                    server_data = analyzer.load_data(date, server)
                    server_stats[server]["hits"] += server_data.get("hits", 0)
                    server_stats[server]["errors"] += sum(
                        error_data.get("hits", 0)
                        for error_data in server_data.get("errors", {}).values()
                    )
                except FileNotFoundError:
                    pass

            # Aggregate errors
            errors = data.get("errors", {})
            for error_code, error_data in errors.items():
                if error_code not in all_errors:
                    all_errors[error_code] = 0
                all_errors[error_code] += error_data.get("hits", 0)

            # Aggregate paths
            paths = data.get("paths", {})
            for path, path_data in paths.items():
                if path not in all_paths:
                    all_paths[path] = 0
                hits = (
                    path_data.get("hits", 0)
                    if isinstance(path_data, dict)
                    else path_data
                )
                all_paths[path] += hits

        except Exception as e:
            # Log unexpected errors but continue processing
            logger.warning(f"Error processing date {date}: {e}")
            continue

    # Server reliability analysis
    st.subheader("Server Reliability")
    server_reliability_df = pd.DataFrame(
        [
            {
                "Server": server,
                "Total Hits": stats["hits"],
                "Total Errors": stats["errors"],
                "Weeks Active": stats["dates"],
                "Error Rate (%)": (stats["errors"] / stats["hits"] * 100)
                if stats["hits"] > 0
                else 0,
                "Availability (%)": (stats["dates"] / len(dates) * 100)
                if len(dates) > 0
                else 0,
            }
            for server, stats in server_stats.items()
        ]
    )

    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(
            server_reliability_df,
            x="Server",
            y="Availability (%)",
            title="Server Availability",
        )
        fig.update_layout(yaxis=dict(range=[0, 105]))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.bar(
            server_reliability_df,
            x="Server",
            y="Error Rate (%)",
            title="Server Error Rate",
        )
        st.plotly_chart(fig, use_container_width=True)

    st.dataframe(server_reliability_df, use_container_width=True)

    # Error analysis
    if all_errors:
        st.subheader("HTTP Error Analysis")

        errors_df = pd.DataFrame(
            list(all_errors.items()), columns=["Error Code", "Total Hits"]
        )
        errors_df = errors_df.sort_values("Total Hits", ascending=False)

        col1, col2 = st.columns(2)

        with col1:
            fig = px.pie(
                errors_df,
                values="Total Hits",
                names="Error Code",
                title="App Download Error Distribution",
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.dataframe(errors_df, use_container_width=True)

    # Top paths by type
    if all_paths:
        st.subheader("Request Analysis by Path Type")

        # Categorize paths for better analysis
        jar_paths = {k: v for k, v in all_paths.items() if ".jar" in k}
        diff_paths = {k: v for k, v in all_paths.items() if "/diff/" in k}
        root_paths = {k: v for k, v in all_paths.items() if k in ["/", ""]}

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("JAR File Requests", f"{sum(jar_paths.values()):,}")
            if jar_paths:
                jar_df = (
                    pd.DataFrame(list(jar_paths.items()), columns=["Path", "Hits"])
                    .sort_values("Hits", ascending=False)
                    .head(5)
                )
                st.dataframe(jar_df, use_container_width=True)

        with col2:
            st.metric("Repository Diff Requests", f"{sum(diff_paths.values()):,}")
            if diff_paths:
                diff_df = (
                    pd.DataFrame(list(diff_paths.items()), columns=["Path", "Hits"])
                    .sort_values("Hits", ascending=False)
                    .head(5)
                )
                st.dataframe(diff_df, use_container_width=True)

        with col3:
            st.metric("Root Path Requests", f"{sum(root_paths.values()):,}")
            if root_paths:
                root_df = pd.DataFrame(
                    list(root_paths.items()), columns=["Path", "Hits"]
                ).sort_values("Hits", ascending=False)
                st.dataframe(root_df, use_container_width=True)
