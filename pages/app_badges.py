"""
App metrics page for F-Droid dashboard
"""

import re
import streamlit as st


st.set_page_config(page_title="F-Droid App Badges", page_icon="ℹ️", layout="wide")


def display_badge(badge_url: str, label: str) -> None:
    """Display a badge with its raw URL, markdown, and HTML code."""
    st.markdown(f"![{label}]({badge_url})")
    tab1, tab2, tab3 = st.tabs(["Image", "Markdown", "HTML"])
    with tab1:
        st.markdown(f"```\n{badge_url}\n```")
    with tab2:
        st.markdown(f"```\n![{label}]({badge_url})\n```")
    with tab3:
        st.markdown(f"```\n<img src='{badge_url}' alt='{label}'>\n```")


def show_badge_builder() -> None:
    """Show the badge builder interface."""
    st.header("🆒 F-Droid App Badges")

    st.markdown(
        """
        Generate dynamic badges for F-Droid apps to display monthly download and search statistics.
        These badges can be embedded in README files, websites, or any platform that supports image embedding.
        
        Badges are delivered via [Shields.io](https://shields.io/) for easy integration.

        A [GitHub Actions workflow runs daily](https://github.com/kitswas/fdroid-metrics-dashboard/actions/workflows/extract_monthly_package_json.yml) to process raw F-Droid metrics data and generates appwise JSON files.

        First we aggregate monthly data like this:

        ```
        {
        "package_id": "io.github.kitswas.virtualgamepadmobile",
        "total_downloads": 3204,
        "api_hits": 6507,
        "versions": 2,
        "search_count": 0
        }
        ```

        Then, we produce badges like this:

        ![Downloads last month](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fgithub.com%2Fkitswas%2Ffdroid-metrics-dashboard%2Fraw%2Frefs%2Fheads%2Fmain%2Fprocessed%2Fmonthly%2Fio.github.kitswas.virtualgamepadmobile.json&query=%24.total_downloads&logo=fdroid&label=Downloads%20last%20month)
        ![Searches last month](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fgithub.com%2Fkitswas%2Ffdroid-metrics-dashboard%2Fraw%2Frefs%2Fheads%2Fmain%2Fprocessed%2Fmonthly%2Fio.github.kitswas.virtualgamepadmobile.json&query=%24.search_count&logo=fdroid&label=Searches%20last%20month)
        """
    )

    st.subheader("Generate Your Badges")

    app_id = st.text_input("Enter the F-Droid App ID (e.g., org.fdroid.fdroid):")

    # Santitize input, only allow non-empty strings with dots, letters, numbers, underscores, and hyphens
    app_id = app_id.strip()

    if not re.match(r"^[\w.-]+$", app_id):
        st.error(
            "Invalid App ID. Only letters, numbers, dots, underscores, and hyphens are allowed. "
            "If this is wrong, please [file an issue on GitHub.](https://github.com/kitswas/fdroid-metrics-dashboard/issues/new/choose)"
        )
        return

    if app_id:
        downloads_badge_url = (
            "https://img.shields.io/badge/dynamic/json?"
            f"url=https%3A%2F%2Fgithub.com%2Fkitswas%2Ffdroid-metrics-dashboard%2Fraw%2Frefs%2Fheads%2Fmain%2Fprocessed%2Fmonthly%2F{app_id}.json"
            "&query=%24.total_downloads&logo=fdroid&label=Downloads%20last%20month"
        )
        searches_badge_url = (
            "https://img.shields.io/badge/dynamic/json?"
            f"url=https%3A%2F%2Fgithub.com%2Fkitswas%2Ffdroid-metrics-dashboard%2Fraw%2Frefs%2Fheads%2Fmain%2Fprocessed%2Fmonthly%2F{app_id}.json"
            "&query=%24.search_count&logo=fdroid&label=Searches%20last%20month"
        )

        display_badge(downloads_badge_url, "Downloads last month")
        display_badge(searches_badge_url, "Searches last month")


show_badge_builder()
