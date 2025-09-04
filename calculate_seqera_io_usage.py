#!/usr/bin/env python3

import logging
import os
import sys
from datetime import UTC, datetime
from typing import Annotated, Any

import pandas as pd
import requests
import typer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("io_metrics_api_calls.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

app = typer.Typer(help="Collect IO metrics from Seqera/Tower API")


class APIClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.headers = {"Authorization": f"Bearer {api_key}"}

    def get(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}/{endpoint}"
        try:
            logger.info(f"Making GET request to {url} with params {params}")
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            logger.info(f"Response from {url}: {response.status_code}")
            data: dict[str, Any] = response.json()
            return data
        except requests.exceptions.HTTPError as errh:
            logger.error(f"HTTP Error: {errh}")
        except requests.exceptions.ConnectionError as errc:
            logger.error(f"Error Connecting: {errc}")
        except requests.exceptions.Timeout as errt:
            logger.error(f"Timeout Error: {errt}")
        except requests.exceptions.RequestException as err:
            logger.error(f"Request Exception: {err}")
        return {}

    def organizations(self) -> dict[str, Any]:
        return self.get("orgs", {})

    def workspaces(self, org_id: str) -> dict[str, Any]:
        return self.get(f"orgs/{org_id}/workspaces", {})

    def workflows(
        self,
        workspace_id: str,
        min_time: str,
        max_time: str,
        user: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        workflows = []
        offset = 0
        search_query = f"after:{min_time} before:{max_time}"

        if user:
            search_query += f" user:{user}"

        if status:
            search_query += f" status:{status}"

        while True:
            params = {
                "search": search_query,
                "workspaceId": workspace_id,
                "offset": offset,
            }
            resp = self.get("workflow", params)
            if not resp:
                break

            new_workflows = resp.get("workflows", [])
            workflows.extend(new_workflows)

            if len(workflows) >= resp.get("totalSize", 0) or len(new_workflows) == 0:
                break

            offset += len(new_workflows)

        return workflows

    def workflow_details(self, workflow_id: str, workspace_id: str) -> dict[str, Any]:
        """Get details for a specific workflow"""
        params = {"workspaceId": workspace_id}
        return self.get(f"workflow/{workflow_id}", params)

    def workflow_metrics(self, workflow_id: str, workspace_id: str) -> dict[str, Any]:
        """Get detailed process metrics for a specific workflow"""
        params = {"workspaceId": workspace_id}
        return self.get(f"workflow/{workflow_id}/metrics", params)


def extract_io_metrics(
    workflow_details: dict[str, Any],
    workflow_metrics_data: dict[str, Any],
    org_name: str,
    workspace_name: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Extract IO metrics from workflow details and metrics responses"""
    workflow_data = workflow_details.get("workflow", {})
    workflow_id = workflow_data.get("id")
    workflow_name = workflow_data.get("runName")
    status = workflow_data.get("status")
    user_name = workflow_data.get("userName")
    start_time = workflow_data.get("start")
    end_time = workflow_data.get("complete")

    # Get organization and workspace names from the workflow details response
    org_name = workflow_details.get("orgName", org_name)
    workspace_name = workflow_details.get("workspaceName", workspace_name)

    total_read_bytes = 0
    total_write_bytes = 0

    # Extract process-level metrics from the metrics endpoint
    process_metrics = []
    metrics_list = workflow_metrics_data.get("metrics", [])

    # Sum up read and write bytes from all processes
    for process_metric in metrics_list:
        process_name = process_metric.get("process", "")

        reads_data = process_metric.get("reads", {})
        process_read_bytes = 0
        if reads_data is not None:
            process_read_bytes = reads_data.get("mean", 0)

        writes_data = process_metric.get("writes", {})
        process_write_bytes = 0
        if writes_data is not None:
            process_write_bytes = writes_data.get("mean", 0)

        total_read_bytes += process_read_bytes
        total_write_bytes += process_write_bytes

        process_metrics.append(
            {
                "workflow_id": workflow_id,
                "process_name": process_name,
                "read_bytes": process_read_bytes,
                "write_bytes": process_write_bytes,
                "total_bytes": process_read_bytes + process_write_bytes,
            }
        )

    total_io_bytes = total_read_bytes + total_write_bytes

    summary = {
        "workflow_id": workflow_id,
        "workflow_name": workflow_name,
        "status": status,
        "user_name": user_name,
        "start_time": start_time,
        "end_time": end_time,
        "total_read_bytes": total_read_bytes,
        "total_write_bytes": total_write_bytes,
        "total_io_bytes": total_io_bytes,
        "organization_name": org_name,
        "workspace_name": workspace_name,
    }

    return summary, process_metrics


def check_env_vars(var_name: str, default: str | None = None) -> str:
    """Get environment variable or default value"""
    value = os.getenv(var_name, default)
    if value is None:
        logger.error(f"Environment variable {var_name} is not set")
        sys.exit(1)
    return value


def bytes_to_readable(bytes_value: float) -> str:
    """Convert bytes to human-readable format"""
    if bytes_value == 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    i = 0
    while bytes_value >= 1024 and i < len(units) - 1:
        bytes_value /= 1024
        i += 1
    return f"{bytes_value:.2f} {units[i]}"


def process_workspace(
    client: APIClient,
    workspace_id: str,
    min_time: str,
    max_time: str,
    user: str | None = None,
    status: str | None = None,
    org_name: str = "Unknown",
    workspace_name: str = "Unknown",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Process a single workspace and collect IO metrics for all matching workflows"""
    all_workflow_summaries = []
    all_process_metrics = []

    workflows = client.workflows(workspace_id, min_time, max_time, user, status)
    logger.info(f"Found {len(workflows)} workflows in workspace {workspace_name}")

    for workflow in workflows:
        workflow_data = workflow.get("workflow", {})
        workflow_id = workflow_data.get("id")

        # Get detailed workflow information
        workflow_details = client.workflow_details(workflow_id, workspace_id)

        # Get detailed workflow metrics
        workflow_metrics_data = client.workflow_metrics(workflow_id, workspace_id)

        if workflow_details and workflow_metrics_data:
            summary, process_metrics = extract_io_metrics(
                workflow_details, workflow_metrics_data, org_name, workspace_name
            )
            all_workflow_summaries.append(summary)
            all_process_metrics.extend(process_metrics)
            logger.info(f"Collected IO metrics for workflow {workflow_id}")

    return all_workflow_summaries, all_process_metrics


def display_summary_statistics(df_summary: pd.DataFrame) -> None:
    """Display summary statistics for the collected IO metrics"""
    total_workflows = len(df_summary)
    total_read_bytes = df_summary["total_read_bytes"].sum()
    total_write_bytes = df_summary["total_write_bytes"].sum()
    total_io_bytes = total_read_bytes + total_write_bytes

    readable_read = bytes_to_readable(total_read_bytes)
    readable_write = bytes_to_readable(total_write_bytes)
    readable_total = bytes_to_readable(total_io_bytes)

    typer.echo("\nIO Metrics Summary:")
    typer.echo(f"Total workflows analyzed: {total_workflows}")
    typer.echo(f"Total read volume: {readable_read}")
    typer.echo(f"Total write volume: {readable_write}")
    typer.echo(f"Total IO volume: {readable_total}")

    # Per-user statistics if there are multiple users
    if len(df_summary["user_name"].unique()) > 1:
        typer.echo("\nIO Metrics by User:")
        user_stats = (
            df_summary.groupby("user_name")
            .agg(
                {
                    "total_read_bytes": "sum",
                    "total_write_bytes": "sum",
                    "workflow_id": "count",
                }
            )
            .reset_index()
        )

        user_stats["total_io_bytes"] = (
            user_stats["total_read_bytes"] + user_stats["total_write_bytes"]
        )
        user_stats = user_stats.rename(columns={"workflow_id": "workflow_count"})

        for _, row in user_stats.iterrows():
            user = row["user_name"]
            workflows = row["workflow_count"]
            read_bytes = bytes_to_readable(row["total_read_bytes"])
            write_bytes = bytes_to_readable(row["total_write_bytes"])
            total_bytes = bytes_to_readable(row["total_io_bytes"])

            typer.echo(f"User: {user}")
            typer.echo(f"  Workflows: {workflows}")
            typer.echo(f"  Read: {read_bytes}")
            typer.echo(f"  Write: {write_bytes}")
            typer.echo(f"  Total: {total_bytes}")


@app.command()
def main(
    from_date: Annotated[
        datetime,
        typer.Option(
            ...,
            "--from",
            help="Earliest date to consider (inclusive) in YYYY-MM-DD format",
        ),
    ],
    to_date: Annotated[
        datetime | None,
        typer.Option(
            "--to", help="Last date to consider (inclusive) in YYYY-MM-DD format"
        ),
    ] = None,
    output: Annotated[
        str,
        typer.Option("--output", "-o", help="Output CSV file path"),
    ] = "io_metrics.csv",
    workspace_id: Annotated[
        str | None,
        typer.Option("--workspace-id", "-w", help="Specific workspace ID to analyze"),
    ] = None,
    user: Annotated[
        str | None,
        typer.Option("--user", "-u", help="Filter workflows by user"),
    ] = None,
    status: Annotated[
        str,
        typer.Option(
            "--status",
            "-s",
            help="Filter workflows by status (SUCCEEDED, FAILED, etc.)",
        ),
    ] = "SUCCEEDED",
    endpoint: Annotated[
        str | None,
        typer.Option(
            "--endpoint",
            "-e",
            help="API endpoint URL (defaults to environment variable)",
        ),
    ] = None,
) -> None:
    """Collect IO metrics from Seqera/Tower API for workflows within a date range."""
    if to_date is None:
        to_date = datetime.combine(datetime.now(UTC).date(), datetime.max.time())

    # API setup
    base_url = (
        endpoint
        if endpoint
        else check_env_vars("TOWER_API_ENDPOINT", "https://api.cloud.seqera.io")
    )
    api_key = check_env_vars("TOWER_ACCESS_TOKEN")

    min_time = (
        datetime.combine(from_date.date(), datetime.min.time()).isoformat(
            timespec="milliseconds"
        )
        + "Z"
    )
    max_time = (
        datetime.combine(to_date.date(), datetime.max.time()).isoformat(
            timespec="milliseconds"
        )
        + "Z"
    )

    client = APIClient(base_url, api_key)
    all_workflow_summaries = []
    all_process_metrics = []

    # If workspace ID is provided, only process that workspace
    if workspace_id:
        logger.info(f"Processing only workspace ID: {workspace_id}")
        # We need to find the workspace name and org name
        org_name = "Unknown"
        workspace_name = "Unknown"

        orgs = client.organizations()
        if orgs:
            for org in orgs.get("organizations", []):
                org_id = org.get("orgId")
                workspaces = client.workspaces(org_id).get("workspaces", [])
                for workspace in workspaces:
                    if workspace.get("id") == workspace_id:
                        org_name = org.get("name")
                        workspace_name = workspace.get("name")
                        break
                if workspace_name != "Unknown":
                    break

        summaries, process_metrics = process_workspace(
            client,
            workspace_id,
            min_time,
            max_time,
            user,
            status,
            org_name,
            workspace_name,
        )
        all_workflow_summaries.extend(summaries)
        all_process_metrics.extend(process_metrics)
    else:
        # Iterate over all organizations and workspaces
        orgs = client.organizations()
        if orgs:
            for org in orgs.get("organizations", []):
                org_id = org.get("orgId")
                organization_name = org.get("name")
                workspaces = client.workspaces(org_id).get("workspaces", [])
                for workspace in workspaces:
                    workspace_id = workspace.get("id")
                    workspace_name = workspace.get("name")

                    if workspace_id is None:
                        logger.warning(
                            f"Workspace {workspace_name} has no ID, skipping"
                        )
                        continue

                    summaries, process_metrics = process_workspace(
                        client,
                        workspace_id,
                        min_time,
                        max_time,
                        user,
                        status,
                        organization_name,
                        workspace_name,
                    )
                    all_workflow_summaries.extend(summaries)
                    all_process_metrics.extend(process_metrics)

    if all_workflow_summaries:
        df_summary = pd.DataFrame(all_workflow_summaries)
        summary_output = output
        df_summary.to_csv(summary_output, index=False)
        logger.info(f"Workflow summary IO metrics saved to {summary_output}")

        # Save process-level metrics to a separate file if there are any
        if all_process_metrics:
            process_output = f"process_{output}"
            df_processes = pd.DataFrame(all_process_metrics)
            df_processes.to_csv(process_output, index=False)
            logger.info(f"Process-level IO metrics saved to {process_output}")

        # Display summary statistics
        display_summary_statistics(df_summary)
    else:
        logger.warning("No workflow data collected")
        typer.echo("No workflow data collected. Check your filters or date range.")


if __name__ == "__main__":
    app()
