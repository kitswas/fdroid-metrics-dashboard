# F-Droid Metrics Dashboard

A comprehensive dashboard for analyzing F-Droid app store metrics, including search patterns and app download statistics.

> [!NOTE]  
> The calculations are a bit iffy. _(My calculations always are :P)_  
> And I am unsure whether the data labels mean what I think they mean.  
> Enjoy it while it works! ~~I probably won’t maintain it long-term.~~ Still maintaining it as of Jan 2026. No idea why.

## Key Features

### Search Metrics

- Search query analysis and trends
- Geographic distribution of searches
- Error analysis and technical metrics
- Time series visualization

### App Metrics

- App download patterns from HTTP servers
- Request path analysis (JAR files, repository diffs, etc.)
- Server performance comparison
- Geographic distribution of downloads

### Badges (via Shields.io)

You can add badges to your README file to display various metrics (monthly) for your package.  
A GitHub Actions cronjob runs daily to recompute these metrics.

```markdown
![Downloads last month](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fgithub.com%2Fkitswas%2Ffdroid-metrics-dashboard%2Fraw%2Frefs%2Fheads%2Fmain%2Fprocessed%2Fmonthly%2Fio.github.kitswas.virtualgamepadmobile.json&query=%24.total_downloads&logo=fdroid&label=Downloads%20last%20month)
![Searches last month](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fgithub.com%2Fkitswas%2Ffdroid-metrics-dashboard%2Fraw%2Frefs%2Fheads%2Fmain%2Fprocessed%2Fmonthly%2Fio.github.kitswas.virtualgamepadmobile.json&query=%24.search_count&logo=fdroid&label=Searches%20last%20month)
```

This is how those look ([customizable](https://shields.io/badges/dynamic-json-badge)):
![Downloads last month](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fgithub.com%2Fkitswas%2Ffdroid-metrics-dashboard%2Fraw%2Frefs%2Fheads%2Fmain%2Fprocessed%2Fmonthly%2Fio.github.kitswas.virtualgamepadmobile.json&query=%24.total_downloads&logo=fdroid&label=Downloads%20last%20month)
![Searches last month](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fgithub.com%2Fkitswas%2Ffdroid-metrics-dashboard%2Fraw%2Frefs%2Fheads%2Fmain%2Fprocessed%2Fmonthly%2Fio.github.kitswas.virtualgamepadmobile.json&query=%24.search_count&logo=fdroid&label=Searches%20last%20month)

Replace `io.github.kitswas.virtualgamepadmobile` with your package ID.

You can find the processed data files in the `processed` directory. (Search for your package ID)

## Hosted Dashboard (via Streamlit)

<https://fdroid-metrics.streamlit.app/>

## Local Setup Instructions

You need the uv package/project manager to install the dependencies.  
You can get [uv here](https://docs.astral.sh/uv/getting-started/installation/).

> [!TIP]  
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

Or use the Dockerfile. _(Min 2GB RAM, 2 CPU Cores) (4 CPU Cores recommended)_

### Dashboard

Launch the interactive multipage dashboard:

```bash
uv run streamlit run dashboard.py
```

The dashboard will be available at <http://localhost:8501>.

### Manual Data Fetching

The data fetchers have been revamped to support flexible date range queries. Each data file represents cumulative metrics since the previous date (usually weekly snapshots).

#### Search Metrics Data

Download F-Droid search metrics data:

```bash
# Download data for a specific date range (recommended)
uv run python -m etl.getdata_search --start 2024-09-01 --end 2024-09-30

# Download current month (legacy mode)
uv run python -m etl.getdata_search

# Download specific month (legacy mode)
uv run python -m etl.getdata_search 2024 12

# Get help
uv run python -m etl.getdata_search --help
```

#### App Metrics Data

Download F-Droid app metrics data from HTTP servers:

```bash
# Download data for a specific date range (recommended)
uv run python -m etl.getdata_apps --start 2024-09-01 --end 2024-09-30

# Download current month (legacy mode)
uv run python -m etl.getdata_apps

# Download specific month (legacy mode)
uv run python -m etl.getdata_apps 2024 6

# Get help
uv run python -m etl.getdata_apps --help
```

**Note**: The fetchers will automatically:

1. Fetch the `index.json` from each server
2. Filter available dates that fall within your specified range
3. Download only the matching date files (e.g., `2025-09-29.json`)
4. Skip files that already exist locally

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
