# Seqera IO Usage Calculator

This tool calculates IO usage metrics for workflows run on Seqera Platform. It collects read and write bytes for each workflow and process, allowing you to analyze throughput patterns across your organization.

## Features

- Collects IO metrics (read/write bytes) for workflows in Seqera Platform
- Filters workflows by date range, workspace, user, and status
- Aggregates metrics at both workflow and process levels
- Outputs detailed CSV reports for further analysis
- Provides human-readable summaries of total IO usage

## Requirements

- Python 3.6+
- Required Python packages:
  - requests (external)
  - pandas (external)
  - typer (external)

The other dependencies (logging, argparse, datetime, os) are part of the Python standard library.

## Installation

1. Clone this repository or download the script
2. Install required dependencies:

   ```bash
   pip install requests pandas requests typer
   ```

   or

   ```bash
   pip install -r requirements.txt
   ```

3. Make the script executable:

   ```bash
   chmod +x calculate_seqera_io_usage.py
   ```

Alternatively, you can use [uv](https://docs.astral.sh/uv/) to run within a uv managed environment:

```bash
uv sync
uv run calculate_seqera_io_usage.py --help
```

### Basic Usage

```bash
# Set your API token as an environment variable
export TOWER_ACCESS_TOKEN=your_api_token_here

# Optionally set the API endpoint
export TOWER_API_ENDPOINT=https://api.cloud.seqera.io

# Run help command to see all available options
./calculate_seqera_io_usage.py --help

# Collect IO metrics for all workflows that succeeded between Jan 1, 2025 and today
./calculate_seqera_io_usage.py --from 2025-01-01
```

### Command Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| --from | Start date for workflow collection (YYYY-MM-DD) | Required |
| --to | End date for workflow collection (YYYY-MM-DD) | Today |
| --workspace-id | Specific workspace ID to analyze | All workspaces |
| --user | Filter workflows by specific user | All users |
| --status | Filter workflows by status (e.g., SUCCEEDED, FAILED) | SUCCEEDED |
| --output | Output CSV filename | seqera_io_usage_YYYY-MM-DD.csv |
| --endpoint | Seqera API endpoint | <https://api.cloud.seqera.io> |

### Examples

#### Specifying a Date Range

```bash
# Collect metrics for workflows between specific dates
./calculate_seqera_io_usage.py --from 2025-01-01 --to 2025-03-31
```

#### Filtering by Workspace

```bash
# Collect metrics only for a specific workspace
./calculate_seqera_io_usage.py --from 2025-01-01 --workspace-id 138659136604200
```

#### Filtering by User

```bash
# Collect metrics only for workflows run by a specific user
./calculate_seqera_io_usage.py --from 2025-01-01 --user johndoe
```

#### Filtering by Status

```bash
# Collect metrics only for workflows that failed
./calculate_seqera_io_usage.py --from 2025-01-01 --status FAILED
```

#### Specifying an Output File

```bash
# Collect metrics and save to a specific file
./calculate_seqera_io_usage.py --from 2025-01-01 --output my_workflow_metrics.csv
```

#### Using a Custom API Endpoint

```bash
# Use a custom API endpoint
./calculate_seqera_io_usage.py --from 2025-01-01 --endpoint https://api.myseqera.com
```

#### Combining Filters

```bash
# Combine multiple filters for more specific analysis
./calculate_seqera_io_usage.py --from 2025-01-01 --to 2025-06-30 --workspace-id 138659136604200 --user johndoe --status SUCCEEDED --output johndoe_h1_2025.csv
```

### Output

The script generates two CSV files:

- Workflow Summary CSV (specified by `--output` argument):
  - Contains one row per workflow with aggregated IO metrics
  - Includes workflow ID, name, status, user, start/end times, and total read/write bytes
- Process Detail CSV (prefixed with "process_"):
  - Contains one row per process within each workflow
  - Includes process name and detailed read/write bytes for each process

### How It Works

- The script authenticates with the Seqera Platform API using your access token and your Seqera Platform instance with the API endpoint
- It retrieves a list of workflows matching your specified filters
- For each workflow, it fetches detailed information and metrics
- It extracts and aggregates IO metrics (read/write bytes) from the process-level data
- The aggregated data is saved to CSV files and a summary is printed to the console

### Troubleshooting

- Authentication Issues: Ensure your `TOWER_ACCESS_TOKEN` is valid and has appropriate permissions
- No Data Found: Check your date range and filters to ensure workflows exist in the specified period
- API Rate Limiting: If you encounter rate limiting, try narrowing your date range or adding more specific filters

## Development Setup

This project uses [pre-commit](https://pre-commit.com/) to maintain code quality and consistency. Pre-commit runs various checks and formatters on your code before each commit.

### Installing Pre-commit

1. Install the development dependencies:

   ```bash
   # Using pip
   pip install -e ".[dev]"

   # Or using uv
   uv sync --all-groups
   ```

2. Install the pre-commit hooks:

   ```bash
   pre-commit install
   ```

3. (Optional) Run pre-commit on all files to check current state:

   ```bash
   pre-commit run --all-files
   ```

### Pre-commit Hooks

The following tools are configured to run automatically:

- **black**: Code formatting
- **ruff**: Fast Python linting and formatting
- **isort**: Import sorting
- **mypy**: Type checking
- Various file fixes (trailing whitespace, end of file, etc.)

### Manual Code Checks

You can also run the tools manually:

```bash
# Format code with black
black .

# Run ruff linter
ruff check . --fix

# Sort imports
isort .

# Type check with mypy
mypy .
```

### Contact

For any questions or feedback, please contact your Account Executive at Seqera or <support@seqera.io>.
