"""
App metrics page for F-Droid dashboard
"""

import streamlit as st


st.set_page_config(page_title="F-Droid App Badges", page_icon="ℹ️", layout="wide")


def show_badge_builder() -> None:
    """Show the badge builder interface."""
    st.header("🆒 F-Droid App Badges")

    st.markdown(
        """
        Generate dynamic badges for F-Droid apps to display monthly download and search statistics.
        These badges can be embedded in README files, websites, or any platform that supports image embedding.
        
        Badges are delivered via [Shields.io](https://shields.io/) for easy integration.
        """
    )

    """
    From data like this:

    ```
    {
    "package_id": "io.github.kitswas.virtualgamepadmobile",
    "total_downloads": 3204,
    "api_hits": 6507,
    "versions": 2,
    "search_count": 0
    }
    ```

    we produce badges like this:

    ![Downloads last month](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fgithub.com%2Fkitswas%2Ffdroid-metrics-dashboard%2Fraw%2Frefs%2Fheads%2Fmain%2Fprocessed%2Fmonthly%2Fio.github.kitswas.virtualgamepadmobile.json&query=%24.total_downloads&logo=fdroid&label=Downloads%20last%20month)
    ![Searches last month](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fgithub.com%2Fkitswas%2Ffdroid-metrics-dashboard%2Fraw%2Frefs%2Fheads%2Fmain%2Fprocessed%2Fmonthly%2Fio.github.kitswas.virtualgamepadmobile.json&query=%24.search_count&logo=fdroid&label=Searches%20last%20month)
    """

    st.subheader("Generate Your Badges")

    app_id = st.text_input("Enter the F-Droid App ID (e.g., org.fdroid.fdroid):")

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

        st.markdown(f"![Downloads last month]({downloads_badge_url})")
        st.markdown(f"```\n![Downloads last month]({downloads_badge_url})\n```")
        st.markdown(f"![Searches last month]({searches_badge_url})")
        st.markdown(f"```\n![Searches last month]({searches_badge_url})\n```")


show_badge_builder()
