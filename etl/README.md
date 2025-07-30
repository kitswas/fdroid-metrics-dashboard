# F-Droid Metrics ETL

Extract, Transform, and Load F-Droid metrics data for analysis.  
Includes data collection tools and an interactive dashboard for visualization.

## How to use

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

Download F-Droid search metrics data:

```bash
# Download current month's data
uv run python search/getdata.py

# Download specific month
uv run python search/getdata.py 2024 6

# Download specific year and month
uv run python search/getdata.py 2024 12
```

## Dashboard

Launch the interactive dashboard to analyze the collected data:

```bash
uv run streamlit run dashboard.py
```

The dashboard will be available at `http://localhost:8501` and includes:

- **Overview**: High-level metrics and trends
- **Search Queries**: Detailed analysis of search patterns
- **Geographic**: Usage by country and region
- **Technical**: Error rates and request path analysis

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
