# F-Droid Metrics Dashboard

A comprehensive dashboard for analyzing F-Droid app store metrics, including search patterns and app download statistics.

## Key Features

### 🔍 Search Metrics

- Search query analysis and trends
- Geographic distribution of searches
- Error analysis and technical metrics
- Time series visualization

### 📱 App Metrics

- App download patterns from HTTP servers
- Request path analysis (JAR files, repository diffs, etc.)
- Server performance comparison
- Geographic distribution of downloads

## Setup Instructions

You need the UV package/project manager to install the dependencies.  
You can get [UV here](https://docs.astral.sh/uv/getting-started/installation/).

> [!NOTE]
> To change the Python version, change the `requires-python` field in [pyproject.toml](pyproject.toml)
> and the number in [.python-version](.python-version).  
> uv will take care of the rest.

Set up the environment. (Only once)

```bash
uv venv
# .venv/Scripts/activate # Windows
source .venv/bin/activate # Linux/MacOS
uv sync --link-mode=symlink # Install the dependencies, use -U to update
```

## Data Collection

### Search Metrics Data

Download F-Droid search metrics data:

```bash
# Download current month's search data
uv run python -m etl.getdata_search

# Download specific month
uv run python -m etl.getdata_search 2024 6

# Download specific year and month
uv run python -m etl.getdata_search 2024 12
```

### App Metrics Data

Download F-Droid app metrics data from HTTP servers:

```bash
# Download current month's app data
uv run python -m etl.getdata_apps

# Download specific month
uv run python -m etl.getdata_apps 2024 6

# Download specific year and month
uv run python -m etl.getdata_apps 2024 12
```

## Dashboard

Launch the interactive multipage dashboard:

```bash
uv run streamlit run dashboard.py
```

The dashboard will be available at `http://localhost:8501` and includes:

### 🔍 Search Metrics Dashboard

- **Overview**: High-level search metrics and trends
- **Search Queries**: Detailed analysis of search patterns  
- **Geographic**: Geographic distribution of search traffic
- **Technical**: HTTP errors and request path analysis

### 📱 App Metrics Dashboard

- **Overview**: High-level app download metrics and trends
- **Request Paths**: Analysis of requested files and paths
- **Packages**: F-Droid package API request analysis
- **Geographic**: Geographic distribution of app downloads
- **Server Comparison**: Performance comparison across HTTP servers
- **Technical**: Server reliability and error analysis

## Features

- 📊 Interactive charts and visualizations
- 🔍 Search query popularity analysis
- 🌍 Geographic usage patterns
- 📈 Time series analysis
- 🛠️ Technical metrics and error tracking

## Code Formatting and Linting

We have [ruff](https://docs.astral.sh/ruff/) for code formatting and linting.
Install the [VSCode extension](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff)
and enable `Format on Save` for a better experience.

To fix imports:

```bash
uv run ruff check --select I --fix # Sort imports
uv run ruff check --select F401 --fix # Remove unused imports
```

To check for linting errors:

```bash
uv run ruff check # Use --fix to fix the errors
```

To format the code:

```bash
uv run ruff format
```
