"""
Search metrics page for F-Droid dashboard
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from etl.analyzer_search import SearchMetricsAnalyzer
from etl.data_fetcher_ui import show_data_fetcher, show_quick_fetch_buttons


def show_search_page():
    """Show the search metrics page."""
    st.title("🔍 F-Droid Search Metrics")
    st.markdown(
        "Analyze search patterns and usage statistics from the F-Droid app store search."
    )

    # Initialize analyzer
    analyzer = SearchMetricsAnalyzer()

    # Sidebar for filters
    st.sidebar.header("Search Metrics Filters")

    # Date selection
    available_dates = analyzer.get_available_dates()
    if not available_dates:
        st.warning("No search data files found locally.")

        # Show data fetching interface
        st.info("💡 **Fetch data directly from F-Droid servers:**")

        # Create tabs for fetching and analysis
        tab_fetch, tab_analysis = st.tabs(["📥 Fetch Data", "📊 Analysis"])

        with tab_fetch:
            # Show data fetching interface
            data_fetched = show_data_fetcher("search", "search_")

            # Show quick fetch buttons
            if not data_fetched:
                data_fetched = show_quick_fetch_buttons("search", "search_")

            if data_fetched:
                st.success(
                    "✅ Data fetched successfully! Switch to the Analysis tab or refresh the page."
                )
                if st.button("🔄 Refresh Page", key="search_refresh"):
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
            key="search_start_date",
        )
        end_date = st.sidebar.date_input(
            "End Date",
            value=pd.to_datetime(available_dates[-1]).date(),
            min_value=pd.to_datetime(available_dates[0]).date(),
            max_value=pd.to_datetime(available_dates[-1]).date(),
            key="search_end_date",
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
    if st.sidebar.button("🔽 Fetch Additional Data", key="search_sidebar_fetch"):
        st.session_state.show_search_fetch = True

    # Show data fetching interface if requested
    if st.session_state.get("show_search_fetch", False):
        with st.expander("📥 Fetch Additional Search Data", expanded=True):
            data_fetched = show_data_fetcher("search", "search_sidebar_")
            if data_fetched:
                st.success(
                    "✅ Data fetched successfully! Refresh the page to see new data."
                )
                if st.button("🔄 Refresh Page", key="search_sidebar_refresh"):
                    st.session_state.show_search_fetch = False
                    st.rerun()

    # Main content tabs
    tab1, tab2, tab3, tab4 = st.tabs(
        ["📈 Overview", "🔍 Search Queries", "🌍 Geographic", "🛠️ Technical"]
    )

    with tab1:
        show_search_overview(analyzer, selected_dates)

    with tab2:
        show_query_analysis(analyzer, selected_dates)

    with tab3:
        show_search_geographic_analysis(analyzer, selected_dates)

    with tab4:
        show_search_technical_analysis(analyzer, selected_dates)


def show_search_overview(analyzer: SearchMetricsAnalyzer, dates: list):
    """Show overview metrics."""
    st.header("📈 Search Overview")

    if len(dates) == 1:
        # Single day analysis
        summary = analyzer.get_daily_summary(dates[0])

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Hits", f"{summary['total_hits']:,}")
        with col2:
            st.metric("Unique Queries", f"{summary['unique_queries']:,}")
        with col3:
            st.metric("Total Errors", f"{summary['total_errors']:,}")
        with col4:
            error_rate = (
                (summary["total_errors"] / summary["total_hits"]) * 100
                if summary["total_hits"] > 0
                else 0
            )
            st.metric("Error Rate", f"{error_rate:.1f}%")

        # Top queries chart
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Top Search Queries")
            if summary["top_queries"]:
                queries_df = pd.DataFrame(
                    summary["top_queries"], columns=["Query", "Hits"]
                )
                fig = px.bar(queries_df.head(10), x="Hits", y="Query", orientation="h")
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
                # Calculate actual total unique queries across all dates
                query_df = analyzer.get_query_analysis(dates)
                total_unique_queries = len(query_df) if not query_df.empty else 0
                st.metric("Total Unique Queries", f"{total_unique_queries:,}")

            # Time series charts
            fig = make_subplots(
                rows=2,
                cols=1,
                subplot_titles=("Weekly Search Hits", "Weekly Unique Search Queries"),
                vertical_spacing=0.15,
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
                    y=ts_data["unique_queries"],
                    name="Unique Queries",
                ),
                row=2,
                col=1,
            )

            fig.update_layout(height=600, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)


def show_query_analysis(analyzer: SearchMetricsAnalyzer, dates: list):
    """Show detailed query analysis."""
    st.header("🔍 Search Query Analysis")
    st.info(
        "📊 **Data Frequency:** The data is collected weekly. 'Weeks Active' indicates the number of weeks where the query had actual searches (hits > 0)."
    )

    query_df = analyzer.get_query_analysis(dates)

    if query_df.empty:
        st.warning("No query data available for selected dates.")
        return

    # Summary metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Unique Queries", len(query_df))
    with col2:
        st.metric(
            "Most Popular Query",
            query_df.iloc[0]["query"] if len(query_df) > 0 else "N/A",
        )
    with col3:
        st.metric(
            "Max Query Hits",
            f"{query_df['total_hits'].max():,}" if len(query_df) > 0 else "0",
        )

    # Filters
    col1, col2 = st.columns(2)
    with col1:
        min_hits = st.number_input(
            "Minimum total hits", min_value=0, value=10, key="search_min_hits"
        )
    with col2:
        top_n = st.selectbox(
            "Show top N queries", [10, 20, 50, 100], index=1, key="search_top_n"
        )

    # Filter data
    filtered_df = query_df[query_df["total_hits"] >= min_hits].head(top_n)

    # Top queries chart
    if not filtered_df.empty:
        fig = px.bar(
            filtered_df.head(20),
            x="total_hits",
            y="query",
            title=f"Top {min(20, len(filtered_df))} Search Queries",
            labels={"total_hits": "Total Hits", "query": "Search Query"},
        )
        fig.update_layout(height=600, yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)

        # Query statistics table
        st.subheader("Query Statistics")
        display_df = filtered_df[
            ["query", "total_hits", "appearances", "avg_hits"]
        ].copy()
        display_df["avg_hits"] = display_df["avg_hits"].round(1)
        st.dataframe(
            display_df,
            use_container_width=True,
            column_config={
                "appearances": st.column_config.NumberColumn("Weeks Active"),
                "avg_hits": st.column_config.NumberColumn("Avg Hits/Week"),
                "total_hits": st.column_config.NumberColumn("Total Hits"),
            },
        )


def show_search_geographic_analysis(analyzer: SearchMetricsAnalyzer, dates: list):
    """Show geographic analysis."""
    st.header("🌍 Search Geographic Analysis")

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
            title="Top 20 Countries by Search Hits",
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


def show_search_technical_analysis(analyzer: SearchMetricsAnalyzer, dates: list):
    """Show technical analysis including errors and paths."""
    st.header("🛠️ Search Technical Analysis")

    # Load error data for the selected dates
    all_errors = {}
    all_paths = {}

    for date in dates:
        try:
            data = analyzer.load_data(date)

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

        except FileNotFoundError:
            continue

    # Error analysis
    if all_errors:
        st.subheader("Search HTTP Error Analysis")

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
                title="Search Error Distribution",
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.dataframe(errors_df, use_container_width=True)

    # Path analysis
    if all_paths:
        st.subheader("Top Search Request Paths")

        paths_df = pd.DataFrame(list(all_paths.items()), columns=["Path", "Total Hits"])
        paths_df = paths_df.sort_values("Total Hits", ascending=False).head(20)

        fig = px.bar(
            paths_df,
            x="Total Hits",
            y="Path",
            title="Top 20 Search Request Paths",
            labels={"Total Hits": "Total Hits", "Path": "Request Path"},
        )
        fig.update_layout(height=500, yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)
