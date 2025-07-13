# python-template

A template for Python projects.  
Includes basic project structure, a test suite, a code formatter and linter, and a dependency manager.

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

You can add other dependencies use `uv add`. The following example adds a valid kernel for Jupyter notebooks in VSCode.

```bash
uv add ipykernel # Similar to pip install ipykernel
```

To run any script, append `uv run` before the `python` command. (If the environment is inactive)

```bash
uv run python src/hello.py
```

Run tests:

```bash
uv run python -m unittest discover
```

Get rid of temporary files: (Use with caution)

```bash
git clean -fdX -n # Remove the -n flag to actually delete the files
```

## Code Formatting and Linting

We have [ruff](https://docs.astral.sh/ruff/) for code formatting and linting.
Install the [VSCode extension](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff)
and enable `Format on Save` for a better experience.

To fix imports:

```bash
uv run ruff check --select I --fix src # Sort imports
uv run ruff check --select F401 --fix src # Remove unused imports
```

To check for linting errors:

```bash
uv run ruff check src # Use --fix to fix the errors
```

To format the code:

```bash
uv run ruff format src
```
