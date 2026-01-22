"""
Package details page for F-Droid dashboard
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from etl.analyzer_apps import AppMetricsAnalyzer
from etl.fdroid_metadata import FDroidMetadataFetcher

# UI Constants
TOTAL_DOWNLOADS = "Total Downloads"
MEASUREMENT_PERIOD = "Measurement Period"
DOWNLOADS_IN_PERIOD = "Downloads in Period"
API_HITS_IN_PERIOD = "API Hits in Period"
CUMULATIVE_DOWNLOADS = "Cumulative Downloads"
CUMULATIVE_API_HITS = "Cumulative API Hits"
PACKAGE_ID = "Package ID"
API_HITS = "API Hits"
ACTIVE_DATES = "Active Dates"


@st.cache_resource
def get_metadata_fetcher() -> FDroidMetadataFetcher:
    """Get a cached instance of the metadata fetcher."""
    return FDroidMetadataFetcher(cache_dir="./cache/metadata")


@st.cache_data
def get_all_packages_with_downloads_cached(
    _analyzer: AppMetricsAnalyzer, dates: list[str]
) -> pd.DataFrame:
    """Get cached all packages with downloads data."""
    return _analyzer.get_all_packages_with_downloads(dates)


def show_package_details_page(
    package_id: str, analyzer: AppMetricsAnalyzer, dates: list
) -> None:
    """Show detailed information for a specific package."""

    st.title(f"📦 Package Details: {package_id}")
    st.markdown(f"**{PACKAGE_ID}:** `{package_id}`")

    # Get package download data
    package_data = analyzer.get_package_downloads(package_id, dates)

    if package_data["total_downloads"] == 0 and package_data["api_hits"] == 0:
        st.warning(
            f"No download or API data found for package '{package_id}' in the selected date range."
        )

        # Show search suggestions
        st.subheader("💡 Search Suggestions")
        st.markdown(
            f"Try searching for packages in the main Apps page or check if the {PACKAGE_ID.lower()} is correct."
        )

        if st.button("🔙 Back to Package Browser"):
            # Clear query params to go back to package browser
            st.query_params.clear()
            st.rerun()
        return

    # Overview metrics
    st.subheader("📊 Overview")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(TOTAL_DOWNLOADS, f"{package_data['total_downloads']:,}")
    with col2:
        st.metric("API Info Requests", f"{package_data['api_hits']:,}")
    with col3:
        st.metric("Versions Available", len(package_data["versions"]))
    with col4:
        st.metric("Countries", len(package_data["countries"]))

    # Try to fetch metadata from F-Droid
    st.subheader("📋 Package Information")
    try:
        fetcher = get_metadata_fetcher()
        metadata = fetcher.get_package_metadata(package_id)

        if metadata:
            col1, col2 = st.columns(2)

            with col1:
                if "Summary" in metadata:
                    st.markdown(f"**Summary:** {metadata['Summary']}")
                if "Categories" in metadata:
                    categories = (
                        metadata["Categories"]
                        if isinstance(metadata["Categories"], list)
                        else [metadata["Categories"]]
                    )
                    st.markdown(f"**Categories:** {', '.join(categories)}")
                if "License" in metadata:
                    st.markdown(f"**License:** {metadata['License']}")
                st.link_button(
                    "View app on F-Droid",
                    icon=":material/open_in_new:",
                    type="primary",
                    url=f"https://f-droid.org/packages/{package_id}",
                )

            with col2:
                if "WebSite" in metadata:
                    st.markdown(f"**Website:** {metadata['WebSite']}")
                if "SourceCode" in metadata:
                    st.markdown(f"**Source Code:** {metadata['SourceCode']}")
                if "IssueTracker" in metadata:
                    st.markdown(f"**Issue Tracker:** {metadata['IssueTracker']}")

            if "Description" in metadata:
                st.markdown("**Description:**")
                st.markdown(metadata["Description"])
        else:
            st.info("Package metadata not available in F-Droid repository.")
    except Exception as e:
        st.warning(f"Could not fetch package metadata: {e}")

    # Version downloads analysis
    if package_data["versions"]:
        st.subheader("📈 Downloads by Version")

        # Create version DataFrame
        version_df = pd.DataFrame(
            [
                {"version": version, "downloads": downloads}
                for version, downloads in package_data["versions"].items()
            ]
        ).sort_values("downloads", ascending=False)

        col1, col2 = st.columns(2)

        with col1:
            # Bar chart of version downloads
            fig = px.bar(
                version_df.head(20),  # Show top 20 versions
                x="downloads",
                y="version",
                orientation="h",
                title="Downloads by Version (Top 20)",
                labels={"downloads": "Downloads", "version": "Version Code"},
            )
            fig.update_layout(height=600, yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, width="stretch")

        with col2:
            # Pie chart for top versions
            top_versions = version_df.head(10)
            if len(version_df) > 10:
                others_downloads = version_df.iloc[10:]["downloads"].sum()
                others_row = pd.DataFrame(
                    [{"version": "Others", "downloads": others_downloads}]
                )
                pie_data = pd.concat([top_versions, others_row], ignore_index=True)
            else:
                pie_data = top_versions

            fig = px.pie(
                pie_data,
                values="downloads",
                names="version",
                title="Download Distribution by Version",
            )
            st.plotly_chart(fig, width="stretch")

        # Version downloads table
        st.subheader("📋 Version Download Statistics")
        st.dataframe(
            version_df,
            width="stretch",
            column_config={
                "version": st.column_config.TextColumn("Version Code"),
                "downloads": st.column_config.NumberColumn("Downloads", format="%d"),
            },
        )

    # Geographic analysis
    if package_data["countries"]:
        st.subheader("🌍 Geographic Distribution")

        # Create countries DataFrame
        countries_df = pd.DataFrame(
            [
                {"country": country, "downloads": downloads}
                for country, downloads in package_data["countries"].items()
            ]
        ).sort_values("downloads", ascending=False)

        col1, col2 = st.columns(2)

        with col1:
            # Top countries bar chart
            top_countries = countries_df.head(15)
            fig = px.bar(
                top_countries,
                x="downloads",
                y="country",
                orientation="h",
                title="Downloads by Country (Top 15)",
                labels={"downloads": "Downloads", "country": "Country"},
            )
            fig.update_layout(height=500, yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, width="stretch")

        with col2:
            # Country distribution pie chart
            top_countries_pie = countries_df.head(8)
            if len(countries_df) > 8:
                others_downloads = countries_df.iloc[8:]["downloads"].sum()
                others_row = pd.DataFrame(
                    [{"country": "Others", "downloads": others_downloads}]
                )
                pie_data = pd.concat([top_countries_pie, others_row], ignore_index=True)
            else:
                pie_data = top_countries_pie

            fig = px.pie(
                pie_data,
                values="downloads",
                names="country",
                title="Download Distribution by Country",
            )
            st.plotly_chart(fig, width="stretch")

        # Countries table
        st.subheader("📋 Country Download Statistics")
        st.dataframe(
            countries_df,
            width="stretch",
            column_config={
                "country": st.column_config.TextColumn("Country"),
                "downloads": st.column_config.NumberColumn("Downloads", format="%d"),
            },
        )

    # Time series analysis if multiple dates
    if len(dates) > 1:
        st.subheader("📅 Download Activity by Period")

        # Get period data for each measurement date
        period_data_raw = []
        sorted_dates = sorted(dates)

        for current_date in sorted_dates:
            # Get downloads for this specific period (data published on this date)
            period_package_data = analyzer.get_package_downloads(
                package_id, [current_date]
            )

            period_data_raw.append(
                {
                    "date": pd.to_datetime(current_date),
                    "period_downloads": period_package_data["total_downloads"],
                    "period_api_hits": period_package_data["api_hits"],
                    "versions": len(period_package_data["versions"]),
                }
            )

        # Calculate cumulative data from period data
        cumulative_data = []
        cumulative_downloads = 0
        cumulative_api_hits = 0

        for item in period_data_raw:
            cumulative_downloads += item["period_downloads"]
            cumulative_api_hits += item["period_api_hits"]

            cumulative_data.append(
                {
                    "date": item["date"],
                    "period_downloads": item["period_downloads"],
                    "period_api_hits": item["period_api_hits"],
                    "cumulative_downloads": cumulative_downloads,
                    "cumulative_api_hits": cumulative_api_hits,
                    "versions": item["versions"],
                }
            )

        ts_df = pd.DataFrame(cumulative_data).sort_values("date")

        if not ts_df.empty and ts_df["period_downloads"].sum() > 0:
            # Create period labels for better display - data is already period-based
            period_display_data = []
            sorted_dates = sorted(dates)

            for i, (idx, row) in enumerate(ts_df.iterrows()):
                current_date = sorted_dates[i]
                if i == 0:
                    period_label = f"Up to {current_date}"
                else:
                    prev_date = sorted_dates[i - 1]
                    period_label = f"{prev_date} to {current_date}"

                period_display_data.append(
                    {
                        "period": period_label,
                        "period_downloads": row["period_downloads"],
                        "period_api_hits": row["period_api_hits"],
                        "cumulative_downloads": row["cumulative_downloads"],
                        "cumulative_api_hits": row["cumulative_api_hits"],
                    }
                )

            period_df = pd.DataFrame(period_display_data)

            # Create period-based column charts
            col1, col2 = st.columns(2)

            with col1:
                # Downloads by period
                fig_downloads = px.bar(
                    period_df,
                    x="period",
                    y="period_downloads",
                    title=f"Downloads by {MEASUREMENT_PERIOD}",
                    labels={
                        "period": MEASUREMENT_PERIOD,
                        "period_downloads": DOWNLOADS_IN_PERIOD,
                    },
                    color="period_downloads",
                    color_continuous_scale="Blues",
                )
                fig_downloads.update_layout(
                    xaxis_tickangle=-45, height=400, showlegend=False
                )
                st.plotly_chart(fig_downloads, width="stretch")

            with col2:
                # API hits by period
                fig_api = px.bar(
                    period_df,
                    x="period",
                    y="period_api_hits",
                    title=f"API Requests by {MEASUREMENT_PERIOD}",
                    labels={
                        "period": MEASUREMENT_PERIOD,
                        "period_api_hits": API_HITS_IN_PERIOD,
                    },
                    color="period_api_hits",
                    color_continuous_scale="Oranges",
                )
                fig_api.update_layout(xaxis_tickangle=-45, height=400, showlegend=False)
                st.plotly_chart(fig_api, width="stretch")

            # Cumulative progression chart
            st.subheader("📈 Cumulative Growth")

            fig_cumulative = make_subplots(
                rows=2,
                cols=1,
                subplot_titles=(CUMULATIVE_DOWNLOADS, "Cumulative API Requests"),
                vertical_spacing=0.1,
            )

            fig_cumulative.add_trace(
                go.Scatter(
                    x=ts_df["date"],
                    y=ts_df["cumulative_downloads"],
                    mode="lines+markers",
                    name=CUMULATIVE_DOWNLOADS,
                    line={"color": "#1f77b4", "width": 3},
                    fill="tozeroy",
                    fillcolor="rgba(31, 119, 180, 0.1)",
                ),
                row=1,
                col=1,
            )

            fig_cumulative.add_trace(
                go.Scatter(
                    x=ts_df["date"],
                    y=ts_df["cumulative_api_hits"],
                    mode="lines+markers",
                    name=CUMULATIVE_API_HITS,
                    line={"color": "#ff7f0e", "width": 3},
                    fill="tozeroy",
                    fillcolor="rgba(255, 127, 14, 0.1)",
                ),
                row=2,
                col=1,
            )

            fig_cumulative.update_layout(height=500, showlegend=False)
            fig_cumulative.update_xaxes(title_text="Measurement Date", row=2, col=1)
            fig_cumulative.update_yaxes(title_text=CUMULATIVE_DOWNLOADS, row=1, col=1)
            fig_cumulative.update_yaxes(title_text=CUMULATIVE_API_HITS, row=2, col=1)

            st.plotly_chart(fig_cumulative, width="stretch")

            # Period data table
            st.subheader("📊 Period-by-Period Breakdown")
            display_period_df = period_df[
                [
                    "period",
                    "period_downloads",
                    "period_api_hits",
                    "cumulative_downloads",
                    "cumulative_api_hits",
                ]
            ].copy()

            display_period_df = display_period_df.rename(
                columns={
                    "period": MEASUREMENT_PERIOD,
                    "period_downloads": DOWNLOADS_IN_PERIOD,
                    "period_api_hits": API_HITS_IN_PERIOD,
                    "cumulative_downloads": TOTAL_DOWNLOADS,
                    "cumulative_api_hits": "Total API Hits",
                }
            )

            st.dataframe(
                display_period_df,
                width="stretch",
                column_config={
                    MEASUREMENT_PERIOD: st.column_config.TextColumn("Period"),
                    DOWNLOADS_IN_PERIOD: st.column_config.NumberColumn(
                        "Period Downloads", format="%d"
                    ),
                    API_HITS_IN_PERIOD: st.column_config.NumberColumn(
                        "Period API Hits", format="%d"
                    ),
                    "Total Downloads": st.column_config.NumberColumn(
                        CUMULATIVE_DOWNLOADS, format="%d"
                    ),
                    "Total API Hits": st.column_config.NumberColumn(
                        CUMULATIVE_API_HITS, format="%d"
                    ),
                },
            )

            # Add explanation
            st.info(
                "📘 **Data Explanation**: Each measurement date represents when data was published. "
                "The download counts show actual downloads that occurred during the period leading up to that publication date. "
                "Cumulative totals are calculated by adding period downloads progressively."
            )

    # Navigation
    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        if st.button("🔙 Back to Package Browser"):
            # Clear query params to go back to package browser
            st.query_params.clear()
            st.rerun()

    with col2:
        if st.button("🔄 Refresh Data"):
            st.rerun()


def show_package_search_and_select(analyzer: AppMetricsAnalyzer, dates: list) -> None:
    """Show package search interface when no specific package is selected."""

    st.subheader("🔍 Search Packages")

    # Get all packages with downloads
    packages_df = get_all_packages_with_downloads_cached(analyzer, dates)

    if packages_df.empty:
        st.warning("No packages with download data found in the selected date range.")
        return

    # Search interface
    search_term = st.text_input(
        "Search for packages:",
        placeholder=f"Enter {PACKAGE_ID.lower()} or partial name...",
        key="package_search",
    )

    # Filter packages based on search
    if search_term:
        filtered_df = packages_df[
            packages_df["package_id"].str.contains(search_term, case=False, na=False)
        ]
    else:
        filtered_df = packages_df.head(50)  # Show top 50 by default

    if filtered_df.empty:
        st.info(f"No packages found matching '{search_term}'")
        return

    # Display packages table with clickable links
    st.write(f"Found {len(filtered_df)} packages with download data:")

    # Create a display DataFrame with action buttons
    display_df = filtered_df.copy()
    display_df = display_df.rename(
        columns={
            "package_id": PACKAGE_ID,
            "total_downloads": TOTAL_DOWNLOADS,
            "total_versions": "Versions",
            "api_hits": API_HITS,
            "dates_active": ACTIVE_DATES,
        }
    )

    # Show the table
    st.dataframe(
        display_df,
        width="stretch",
        column_config={
            "Package ID": st.column_config.TextColumn(PACKAGE_ID),
            TOTAL_DOWNLOADS: st.column_config.NumberColumn(
                TOTAL_DOWNLOADS, format="%d"
            ),
            "Versions": st.column_config.NumberColumn("Versions"),
            "API Hits": st.column_config.NumberColumn(API_HITS, format="%d"),
            "Active Dates": st.column_config.NumberColumn(ACTIVE_DATES),
        },
    )

    # Package selection
    st.subheader("🎯 Select Package for Details")

    if not filtered_df.empty:
        # Create a selectbox for package selection
        package_options = filtered_df["package_id"].tolist()

        def format_package_select_option(x: str) -> str:
            if x == "":
                return "Select a package..."
            elif x in package_options:
                downloads = filtered_df[filtered_df["package_id"] == x][
                    "total_downloads"
                ].iloc[0]
                return f"{x} ({downloads:,} downloads)"
            else:
                return x

        selected_package = st.selectbox(
            "Choose a package to view details:",
            options=[""] + package_options,
            format_func=format_package_select_option,
        )

        if selected_package and selected_package != "":
            if st.button(f"📦 View Details for {selected_package}"):
                # Store package in session state and navigate to package details page
                st.session_state["selected_package"] = selected_package
                st.switch_page("pages/package_details.py")
