"""
F-Droid Metrics Dashboard - Main Page
"""

import streamlit as st


def main() -> None:
    """Main dashboard application."""
    st.set_page_config(
        page_title="F-Droid Metrics Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Main dashboard welcome page
    st.title("📊 F-Droid Metrics Dashboard")
    st.markdown(
        """
        Welcome to the F-Droid Metrics Dashboard! This application provides comprehensive 
        analytics for the F-Droid app store, including search patterns and app download statistics.
        
        ## 📋 Available Pages
        
        Use the sidebar navigation to explore different sections:
        """
    )

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("### 🔍 Search Metrics")
        st.markdown(
            "Analyze search patterns and usage statistics from the F-Droid app store search."
        )
        if st.button("Go to Search Metrics", key="search_btn"):
            st.switch_page("pages/search_metrics.py")

    with col2:
        st.markdown("### 📱 App Metrics")
        st.markdown(
            "Analyze app download patterns and usage statistics from F-Droid HTTP servers."
        )
        if st.button("Go to App Metrics", key="apps_btn"):
            st.switch_page("pages/app_metrics.py")

    with col3:
        st.markdown("### 📦 Package Details")
        st.markdown(
            "Browse and analyze individual F-Droid packages with detailed download statistics."
        )
        if st.button("Go to Package Details", key="package_btn"):
            st.switch_page("pages/package_details.py")

    st.markdown("---")

    st.markdown("### About")
    st.markdown(
        """
        This dashboard analyzes metrics from the F-Droid app store, providing insights into:
        
        - **Search Patterns**: Understanding what users search for most frequently
        - **App Downloads**: Tracking download trends and popular applications  
        - **Usage Statistics**: Detailed analytics for individual packages
        
        ### Data Sources
        - **Search**: search.f-droid.org
        - **Apps**: http01/02/03.fdroid.net, originserver.f-droid.org
        """
    )


if __name__ == "__main__":
    main()
