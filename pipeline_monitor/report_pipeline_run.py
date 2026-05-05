from __future__ import annotations

import argparse
import html
import json
import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key

DEFAULT_ATHENA_PRICE_PER_TB_SCANNED = 5.0
DEFAULT_FARGATE_VCPU_PRICE_PER_SECOND = 0.000011244
DEFAULT_FARGATE_MEMORY_GB_PRICE_PER_SECOND = 0.000001235


@dataclass(frozen=True)
class JobDefinition:
    job_name: str
    state_id: str
    summary_sk: str | None
    cpu_units: int
    memory_mib: int
    processed_candidates: tuple[str, ...]
    failed_candidates: tuple[str, ...]
    athena_bytes_candidates: tuple[str, ...] = ()
    rmse_field: str | None = None
    mae_field: str | None = None
    metric_rows_field: str | None = None


@dataclass
class JobExecution:
    state_name: str
    job_name: str
    start_time: datetime
    end_time: datetime | None = None
    status: str = "RUNNING"
    task_arn: str | None = None
    cluster_arn: str | None = None

    @property
    def duration_seconds(self) -> float | None:
        if self.end_time is None:
            return None
        return max((self.end_time - self.start_time).total_seconds(), 0.0)


@dataclass
class MetricSnapshot:
    cpu_avg: float | None = None
    cpu_peak: float | None = None
    memory_avg: float | None = None
    memory_peak: float | None = None


@dataclass
class JobReportRow:
    job_name: str
    status: str
    duration_seconds: float | None
    processed_count: int | None
    failed_count: int | None
    rmse: float | None
    mae: float | None
    metric_rows: int | None
    cpu_avg: float | None
    cpu_peak: float | None
    memory_avg: float | None
    memory_peak: float | None
    fargate_cost_usd: float
    athena_cost_usd: float
    total_cost_usd: float


JOB_DEFINITIONS: tuple[JobDefinition, ...] = (
    JobDefinition(
        job_name="ingest_pdga_event_pages",
        state_id="IngestPdgaEventPages",
        summary_sk="EVENT_PAGES#SUMMARY",
        cpu_units=512,
        memory_mib=1024,
        processed_candidates=("scraped_total", "scraped"),
        failed_candidates=("failed",),
    ),
    JobDefinition(
        job_name="ingest_pdga_live_results",
        state_id="IngestPdgaLiveResults",
        summary_sk="LIVE_RESULTS#SUMMARY",
        cpu_units=512,
        memory_mib=1024,
        processed_candidates=("processed", "processed_events", "total_events"),
        failed_candidates=("failed", "failed_events"),
    ),
    JobDefinition(
        job_name="silver_pdga_live_results",
        state_id="SilverPdgaLiveResults",
        summary_sk="SILVER_LIVE_RESULTS#SUMMARY",
        cpu_units=1024,
        memory_mib=2048,
        processed_candidates=("processed_events",),
        failed_candidates=("failed_events",),
    ),
    JobDefinition(
        job_name="ingest_weather_observations",
        state_id="IngestWeatherObservations",
        summary_sk="WEATHER_OBS#SUMMARY",
        cpu_units=512,
        memory_mib=1024,
        processed_candidates=("processed_events",),
        failed_candidates=("failed_events",),
    ),
    JobDefinition(
        job_name="silver_weather_observations",
        state_id="SilverWeatherObservations",
        summary_sk="SILVER_WEATHER_OBSERVATIONS#SUMMARY",
        cpu_units=1024,
        memory_mib=2048,
        processed_candidates=("processed_events",),
        failed_candidates=("failed_events",),
    ),
    JobDefinition(
        job_name="silver_weather_enriched",
        state_id="SilverWeatherEnriched",
        summary_sk="SILVER_WEATHER_ENRICHED#SUMMARY",
        cpu_units=1024,
        memory_mib=2048,
        processed_candidates=("processed_events",),
        failed_candidates=("failed_events",),
    ),
    JobDefinition(
        job_name="gold_wind_effects",
        state_id="GoldWindEffects",
        summary_sk="GOLD_WIND_EFFECTS#SUMMARY",
        cpu_units=1024,
        memory_mib=2048,
        processed_candidates=("processed_events",),
        failed_candidates=("failed_events",),
    ),
    JobDefinition(
        job_name="gold_wind_model_inputs",
        state_id="GoldWindModelInputs",
        summary_sk="GOLD_WIND_MODEL_INPUTS#SUMMARY",
        cpu_units=1024,
        memory_mib=2048,
        processed_candidates=("processed_events",),
        failed_candidates=("failed_events",),
    ),
    JobDefinition(
        job_name="train_round_wind_model",
        state_id="TrainRoundWindModel",
        summary_sk="TRAIN_ROUND_WIND_MODEL#SUMMARY",
        cpu_units=2048,
        memory_mib=4096,
        processed_candidates=("processed_trainings",),
        failed_candidates=("failed_trainings",),
    ),
    JobDefinition(
        job_name="score_round_wind_model",
        state_id="ScoreRoundWindModel",
        summary_sk="SCORE_ROUND_WIND_MODEL#SUMMARY",
        cpu_units=2048,
        memory_mib=4096,
        processed_candidates=("processed_events",),
        failed_candidates=("failed_events",),
        athena_bytes_candidates=("athena_scanned_bytes",),
        rmse_field="rmse",
        mae_field="mae",
        metric_rows_field="metric_rows",
    ),
    JobDefinition(
        job_name="report_round_weather_impacts",
        state_id="ReportRoundWeatherImpacts",
        summary_sk="REPORT_ROUND_WEATHER_IMPACTS#SUMMARY",
        cpu_units=1024,
        memory_mib=2048,
        processed_candidates=("refreshed_tables",),
        failed_candidates=("failed_tables",),
        athena_bytes_candidates=("total_scanned_bytes",),
    ),
)

STATE_ID_TO_JOB: dict[str, JobDefinition] = {job.state_id: job for job in JOB_DEFINITIONS}


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _json_load(value: str) -> Any | None:
    try:
        return json.loads(value)
    except Exception:
        return None


def _walk_json(obj: Any):
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from _walk_json(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from _walk_json(value)


def _iter_json_strings(obj: Any):
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, str) and key.lower() in {"input", "output", "cause"}:
                yield value
            yield from _iter_json_strings(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from _iter_json_strings(value)


def _find_first_key(obj: Any, target_key: str) -> Any | None:
    target_key_lower = target_key.lower()
    for node in _walk_json(obj):
        if not isinstance(node, dict):
            continue
        for key, value in node.items():
            if str(key).lower() == target_key_lower:
                return value
    return None


def _first_present(item: dict[str, Any] | None, candidates: tuple[str, ...]) -> Any | None:
    if not item:
        return None
    for key in candidates:
        if key in item and item[key] is not None:
            return item[key]
    return None


def load_execution_history(stepfunctions_client, execution_arn: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    next_token: str | None = None

    while True:
        params: dict[str, Any] = {
            "executionArn": execution_arn,
            "maxResults": 1000,
            "reverseOrder": False,
        }
        if next_token:
            params["nextToken"] = next_token

        response = stepfunctions_client.get_execution_history(**params)
        events.extend(response.get("events", []))
        next_token = response.get("nextToken")
        if not next_token:
            break

    return events


def extract_run_id_from_history(events: list[dict[str, Any]]) -> str | None:
    for event in events:
        for raw in _iter_json_strings(event):
            payload = _json_load(raw)
            if payload is None:
                continue
            run_id = _find_first_key(payload, "run_id")
            if isinstance(run_id, str) and run_id.strip():
                return run_id.strip()
    return None


def extract_pipeline_name_from_history(events: list[dict[str, Any]]) -> str | None:
    for event in events:
        for raw in _iter_json_strings(event):
            payload = _json_load(raw)
            if payload is None:
                continue
            pipeline_name = _find_first_key(payload, "pipeline_name")
            if isinstance(pipeline_name, str) and pipeline_name.strip():
                return pipeline_name.strip()
    return None


def match_job_definition(state_name: str) -> JobDefinition | None:
    for state_id, job in STATE_ID_TO_JOB.items():
        if state_name.endswith(state_id):
            return job
    return None


def extract_task_metadata_from_event(event: dict[str, Any]) -> tuple[str | None, str | None]:
    task_arn: str | None = None
    cluster_arn: str | None = None

    for raw in _iter_json_strings(event):
        payload = _json_load(raw)
        if payload is None:
            continue

        maybe_task_arn = _find_first_key(payload, "taskArn")
        maybe_cluster_arn = _find_first_key(payload, "clusterArn")

        if isinstance(maybe_task_arn, str) and maybe_task_arn.strip():
            task_arn = maybe_task_arn.strip()
        if isinstance(maybe_cluster_arn, str) and maybe_cluster_arn.strip():
            cluster_arn = maybe_cluster_arn.strip()

        if task_arn or cluster_arn:
            break

    return task_arn, cluster_arn


def extract_job_executions(events: list[dict[str, Any]]) -> list[JobExecution]:
    rows: list[JobExecution] = []
    active_job: JobExecution | None = None

    for event in events:
        event_type = event.get("type", "")
        timestamp = event.get("timestamp")

        if event_type == "TaskStateEntered":
            state_name = event.get("stateEnteredEventDetails", {}).get("name", "")
            job = match_job_definition(state_name)
            if job is None:
                active_job = None
                continue

            active_job = JobExecution(
                state_name=state_name,
                job_name=job.job_name,
                start_time=_to_utc(timestamp),
            )
            continue

        if active_job is None:
            continue

        if event_type in {"TaskSucceeded", "TaskFailed", "TaskTimedOut", "TaskSubmitFailed"}:
            task_arn, cluster_arn = extract_task_metadata_from_event(event)
            if task_arn and not active_job.task_arn:
                active_job.task_arn = task_arn
            if cluster_arn and not active_job.cluster_arn:
                active_job.cluster_arn = cluster_arn
            active_job.status = "SUCCEEDED" if event_type == "TaskSucceeded" else "FAILED"

        if event_type == "TaskStateExited":
            state_name = event.get("stateExitedEventDetails", {}).get("name", "")
            if state_name != active_job.state_name:
                continue
            active_job.end_time = _to_utc(timestamp)
            rows.append(active_job)
            active_job = None

    if active_job is not None:
        rows.append(active_job)

    return rows


def load_run_summaries(table_name: str, run_id: str, aws_region: str | None) -> dict[str, dict[str, Any]]:
    dynamodb = boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    response = table.query(KeyConditionExpression=Key("pk").eq(f"RUN#{run_id}"))
    items = response.get("Items", [])

    while "LastEvaluatedKey" in response:
        response = table.query(
            KeyConditionExpression=Key("pk").eq(f"RUN#{run_id}"),
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        items.extend(response.get("Items", []))

    return {str(item["sk"]): item for item in items if "sk" in item}


def task_id_from_arn(task_arn: str | None) -> str | None:
    if not task_arn:
        return None
    return task_arn.rsplit("/", 1)[-1]


def cluster_name_from_arn(cluster_arn: str | None) -> str | None:
    if not cluster_arn:
        return None
    return cluster_arn.rsplit("/", 1)[-1]


def _parse_task_definition_family(task_definition_arn: str | None) -> str | None:
    if not task_definition_arn:
        return None
    resource = task_definition_arn.rsplit("/", 1)[-1]
    family = resource.split(":", 1)[0].strip()
    return family or None


def describe_task_definition_family(
    *,
    cluster_arn: str | None,
    task_arn: str | None,
    aws_region: str | None,
) -> str | None:
    if not cluster_arn or not task_arn:
        return None

    ecs_client = boto3.client("ecs", region_name=aws_region) if aws_region else boto3.client("ecs")
    response = ecs_client.describe_tasks(cluster=cluster_arn, tasks=[task_arn])
    tasks = response.get("tasks", [])
    if not tasks:
        return None

    task_definition_arn = tasks[0].get("taskDefinitionArn")
    return _parse_task_definition_family(task_definition_arn)


def get_cloudwatch_task_metrics(
    *,
    cluster_name: str,
    task_id: str,
    task_definition_family: str,
    start_time: datetime,
    end_time: datetime,
    aws_region: str | None,
) -> MetricSnapshot:
    cloudwatch = boto3.client("cloudwatch", region_name=aws_region) if aws_region else boto3.client("cloudwatch")

    query_start = _to_utc(start_time) - timedelta(minutes=1)
    query_end = _to_utc(end_time) + timedelta(minutes=1)

    dimensions = [
        {"Name": "ClusterName", "Value": cluster_name},
        {"Name": "TaskDefinitionFamily", "Value": task_definition_family},
        {"Name": "TaskId", "Value": task_id},
    ]

    response = cloudwatch.get_metric_data(
        MetricDataQueries=[
            {
                "Id": "cpu_avg",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "ECS/ContainerInsights",
                        "MetricName": "TaskCpuUtilization",
                        "Dimensions": dimensions,
                    },
                    "Period": 60,
                    "Stat": "Average",
                },
                "ReturnData": True,
            },
            {
                "Id": "cpu_peak",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "ECS/ContainerInsights",
                        "MetricName": "TaskCpuUtilization",
                        "Dimensions": dimensions,
                    },
                    "Period": 60,
                    "Stat": "Maximum",
                },
                "ReturnData": True,
            },
            {
                "Id": "mem_avg",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "ECS/ContainerInsights",
                        "MetricName": "MemoryUtilized",
                        "Dimensions": dimensions,
                    },
                    "Period": 60,
                    "Stat": "Average",
                },
                "ReturnData": True,
            },
            {
                "Id": "mem_peak",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "ECS/ContainerInsights",
                        "MetricName": "MemoryUtilized",
                        "Dimensions": dimensions,
                    },
                    "Period": 60,
                    "Stat": "Maximum",
                },
                "ReturnData": True,
            },
        ],
        StartTime=query_start,
        EndTime=query_end,
        ScanBy="TimestampAscending",
    )

    results = {entry["Id"]: entry.get("Values", []) for entry in response.get("MetricDataResults", [])}

    def avg(values: list[float]) -> float | None:
        if not values:
            return None
        return sum(values) / len(values)

    def peak(values: list[float]) -> float | None:
        if not values:
            return None
        return max(values)

    return MetricSnapshot(
        cpu_avg=avg(results.get("cpu_avg", [])),
        cpu_peak=peak(results.get("cpu_peak", [])),
        memory_avg=avg(results.get("mem_avg", [])),
        memory_peak=peak(results.get("mem_peak", [])),
    )


def estimate_fargate_cost(
    *,
    cpu_units: int,
    memory_mib: int,
    duration_seconds: float | None,
    vcpu_price_per_second: float,
    memory_gb_price_per_second: float,
) -> float:
    if duration_seconds is None or duration_seconds <= 0:
        return 0.0

    billable_seconds = max(duration_seconds, 60.0)
    vcpu_count = cpu_units / 1024.0
    memory_gb = memory_mib / 1024.0

    return (vcpu_count * vcpu_price_per_second + memory_gb * memory_gb_price_per_second) * billable_seconds


def estimate_athena_cost(scanned_bytes: int | None, price_per_tb_scanned: float) -> float:
    if scanned_bytes is None or scanned_bytes <= 0:
        return 0.0
    tebibytes = scanned_bytes / float(1024 ** 4)
    return tebibytes * price_per_tb_scanned


def format_seconds(value: float | None) -> str:
    if value is None:
        return "—"
    total = int(round(value))
    hours, remainder = divmod(total, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def format_number(value: int | None) -> str:
    if value is None:
        return "—"
    return f"{value:,}"


def format_float(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "—"
    return f"{value:.{digits}f}"


def build_job_rows(
    *,
    executions: list[JobExecution],
    summaries_by_sk: dict[str, dict[str, Any]],
    aws_region: str | None,
    athena_price_per_tb_scanned: float,
    fargate_vcpu_price_per_second: float,
    fargate_memory_gb_price_per_second: float,
) -> list[JobReportRow]:
    rows: list[JobReportRow] = []

    for execution in executions:
        job = next(item for item in JOB_DEFINITIONS if item.job_name == execution.job_name)
        summary_item = summaries_by_sk.get(job.summary_sk) if job.summary_sk else None

        processed_count = _safe_int(_first_present(summary_item, job.processed_candidates))
        failed_count = _safe_int(_first_present(summary_item, job.failed_candidates))
        scanned_bytes = _safe_int(_first_present(summary_item, job.athena_bytes_candidates))
        rmse = _safe_float(summary_item.get(job.rmse_field)) if summary_item and job.rmse_field else None
        mae = _safe_float(summary_item.get(job.mae_field)) if summary_item and job.mae_field else None
        metric_rows = _safe_int(summary_item.get(job.metric_rows_field)) if summary_item and job.metric_rows_field else None

        metric_snapshot = MetricSnapshot()
        cluster_name = cluster_name_from_arn(execution.cluster_arn)
        task_id = task_id_from_arn(execution.task_arn)
        task_definition_family = describe_task_definition_family(
            cluster_arn=execution.cluster_arn,
            task_arn=execution.task_arn,
            aws_region=aws_region,
        )

        if cluster_name and task_id and task_definition_family and execution.end_time is not None:
            metric_snapshot = get_cloudwatch_task_metrics(
                cluster_name=cluster_name,
                task_id=task_id,
                task_definition_family=task_definition_family,
                start_time=execution.start_time,
                end_time=execution.end_time,
                aws_region=aws_region,
            )

        fargate_cost = estimate_fargate_cost(
            cpu_units=job.cpu_units,
            memory_mib=job.memory_mib,
            duration_seconds=execution.duration_seconds,
            vcpu_price_per_second=fargate_vcpu_price_per_second,
            memory_gb_price_per_second=fargate_memory_gb_price_per_second,
        )
        athena_cost = estimate_athena_cost(scanned_bytes, athena_price_per_tb_scanned)

        rows.append(
            JobReportRow(
                job_name=execution.job_name,
                status=execution.status,
                duration_seconds=execution.duration_seconds,
                processed_count=processed_count,
                failed_count=failed_count,
                rmse=rmse,
                mae=mae,
                metric_rows=metric_rows,
                cpu_avg=metric_snapshot.cpu_avg,
                cpu_peak=metric_snapshot.cpu_peak,
                memory_avg=metric_snapshot.memory_avg,
                memory_peak=metric_snapshot.memory_peak,
                fargate_cost_usd=fargate_cost,
                athena_cost_usd=athena_cost,
                total_cost_usd=fargate_cost + athena_cost,
            )
        )

    return rows


def render_html_report(
    *,
    execution_arn: str,
    pipeline_name: str | None,
    pipeline_status: str,
    pipeline_start: datetime,
    pipeline_end: datetime | None,
    run_id: str,
    rows: list[JobReportRow],
) -> str:
    total_duration = None if pipeline_end is None else max((pipeline_end - pipeline_start).total_seconds(), 0.0)
    total_fargate = sum(row.fargate_cost_usd for row in rows)
    total_athena = sum(row.athena_cost_usd for row in rows)
    total_cost = total_fargate + total_athena

    body_rows = []
    for row in rows:
        body_rows.append(
            "<tr>"
            f"<td>{html.escape(row.job_name)}</td>"
            f"<td>{html.escape(row.status)}</td>"
            f"<td>{format_seconds(row.duration_seconds)}</td>"
            f"<td>{format_number(row.processed_count)}</td>"
            f"<td>{format_number(row.failed_count)}</td>"
            f"<td>{format_float(row.rmse, 4)}</td>"
            f"<td>{format_float(row.mae, 4)}</td>"
            f"<td>{format_number(row.metric_rows)}</td>"
            f"<td>{format_float(row.cpu_avg, 2)}</td>"
            f"<td>{format_float(row.cpu_peak, 2)}</td>"
            f"<td>{format_float(row.memory_avg, 1)}</td>"
            f"<td>{format_float(row.memory_peak, 1)}</td>"
            f"<td>{format_float(row.fargate_cost_usd, 4)}</td>"
            f"<td>{format_float(row.athena_cost_usd, 4)}</td>"
            f"<td>{format_float(row.total_cost_usd, 4)}</td>"
            "</tr>"
        )

    return f"""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Pipeline Run Monitor</title>
<style>
body {{
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  color: #111827;
  margin: 24px;
}}
h1 {{
  font-size: 24px;
  margin: 0 0 8px 0;
}}
p.meta {{
  margin: 4px 0;
  color: #4b5563;
}}
table {{
  border-collapse: collapse;
  width: 100%;
  margin-top: 20px;
}}
th, td {{
  border: 1px solid #d1d5db;
  padding: 8px 10px;
  font-size: 13px;
  text-align: left;
}}
th {{
  background: #f3f4f6;
}}
tfoot td {{
  font-weight: 700;
  background: #f9fafb;
}}
</style>
</head>
<body>
  <h1>Pipeline Run Monitor</h1>
  <p class="meta">Pipeline: {html.escape(pipeline_name or "unknown")}</p>
  <p class="meta">Status: {html.escape(pipeline_status)}</p>
  <p class="meta">Run ID: {html.escape(run_id)}</p>
  <p class="meta">Execution ARN: {html.escape(execution_arn)}</p>
  <p class="meta">Started: {html.escape(pipeline_start.isoformat())}</p>
  <p class="meta">Ended: {html.escape(pipeline_end.isoformat() if pipeline_end else "—")}</p>
  <p class="meta">Total Duration: {format_seconds(total_duration)}</p>

  <table>
    <thead>
      <tr>
        <th>Job</th>
        <th>Status</th>
        <th>Duration</th>
        <th>Processed</th>
        <th>Failed</th>
        <th>RMSE</th>
        <th>MAE</th>
        <th>Metric Rows</th>
        <th>CPU Avg %</th>
        <th>CPU Peak %</th>
        <th>Mem Avg MiB</th>
        <th>Mem Peak MiB</th>
        <th>Fargate $</th>
        <th>Athena $</th>
        <th>Total $</th>
      </tr>
    </thead>
    <tbody>
      {"".join(body_rows)}
    </tbody>
    <tfoot>
      <tr>
        <td>Pipeline Total</td>
        <td>{html.escape(pipeline_status)}</td>
        <td>{format_seconds(total_duration)}</td>
        <td>—</td>
        <td>—</td>
        <td>—</td>
        <td>—</td>
        <td>—</td>
        <td>—</td>
        <td>—</td>
        <td>—</td>
        <td>—</td>
        <td>{format_float(total_fargate, 4)}</td>
        <td>{format_float(total_athena, 4)}</td>
        <td>{format_float(total_cost, 4)}</td>
      </tr>
    </tfoot>
  </table>
</body>
</html>
""".strip()


def send_email_via_ses(
    *,
    ses_region: str | None,
    email_from: str,
    email_to: list[str],
    subject: str,
    html_body: str,
) -> None:
    ses_client = boto3.client("ses", region_name=ses_region) if ses_region else boto3.client("ses")
    ses_client.send_email(
        Source=email_from,
        Destination={"ToAddresses": email_to},
        Message={
            "Subject": {"Data": subject, "Charset": "UTF-8"},
            "Body": {"Html": {"Data": html_body, "Charset": "UTF-8"}},
        },
    )


def build_report(
    *,
    execution_arn: str,
    ddb_table: str,
    aws_region: str | None,
    pipeline_status_override: str | None = None,
    athena_price_per_tb_scanned: float = DEFAULT_ATHENA_PRICE_PER_TB_SCANNED,
    fargate_vcpu_price_per_second: float = DEFAULT_FARGATE_VCPU_PRICE_PER_SECOND,
    fargate_memory_gb_price_per_second: float = DEFAULT_FARGATE_MEMORY_GB_PRICE_PER_SECOND,
) -> dict[str, Any]:
    stepfunctions = boto3.client("stepfunctions", region_name=aws_region) if aws_region else boto3.client("stepfunctions")
    execution = stepfunctions.describe_execution(executionArn=execution_arn)
    history = load_execution_history(stepfunctions, execution_arn)

    run_id = extract_run_id_from_history(history)
    if not run_id:
        raise RuntimeError("Could not determine run_id from execution history.")

    pipeline_name = extract_pipeline_name_from_history(history) or execution["stateMachineArn"].rsplit(":", 1)[-1]
    pipeline_status = pipeline_status_override or execution["status"]
    pipeline_start = _to_utc(execution["startDate"])
    pipeline_end = _to_utc(execution["stopDate"]) if "stopDate" in execution else None

    summaries_by_sk = load_run_summaries(ddb_table, run_id, aws_region)
    executions = extract_job_executions(history)

    rows = build_job_rows(
        executions=executions,
        summaries_by_sk=summaries_by_sk,
        aws_region=aws_region,
        athena_price_per_tb_scanned=athena_price_per_tb_scanned,
        fargate_vcpu_price_per_second=fargate_vcpu_price_per_second,
        fargate_memory_gb_price_per_second=fargate_memory_gb_price_per_second,
    )

    html_report = render_html_report(
        execution_arn=execution_arn,
        pipeline_name=pipeline_name,
        pipeline_status=pipeline_status,
        pipeline_start=pipeline_start,
        pipeline_end=pipeline_end,
        run_id=run_id,
        rows=rows,
    )

    return {
        "run_id": run_id,
        "pipeline_name": pipeline_name,
        "pipeline_status": pipeline_status,
        "pipeline_start": pipeline_start,
        "pipeline_end": pipeline_end,
        "rows": rows,
        "html_report": html_report,
    }


def handle_execution_status_event(event: dict[str, Any], env: dict[str, str] | None = None) -> dict[str, Any]:
    env = env or os.environ
    detail = event.get("detail", {})
    execution_arn = str(detail.get("executionArn", "")).strip()
    status = str(detail.get("status", "")).strip() or None

    if not execution_arn:
        raise ValueError("Event detail.executionArn is required.")

    metric_delay_seconds = int(env.get("PIPELINE_MONITOR_METRIC_DELAY_SECONDS", "120"))
    if metric_delay_seconds > 0:
        time.sleep(metric_delay_seconds)

    ddb_table = str(env["PDGA_DDB_TABLE"]).strip()
    aws_region = str(env.get("AWS_REGION", "")).strip() or None
    ses_region = str(env.get("PIPELINE_MONITOR_SES_REGION", "")).strip() or aws_region
    email_from = str(env.get("PIPELINE_MONITOR_EMAIL_FROM", "")).strip()
    email_to_raw = str(env.get("PIPELINE_MONITOR_EMAIL_TO", "")).strip()

    report = build_report(
        execution_arn=execution_arn,
        ddb_table=ddb_table,
        aws_region=aws_region,
        pipeline_status_override=status,
        athena_price_per_tb_scanned=float(env.get("PIPELINE_MONITOR_ATHENA_PRICE_PER_TB_SCANNED", DEFAULT_ATHENA_PRICE_PER_TB_SCANNED)),
        fargate_vcpu_price_per_second=float(env.get("PIPELINE_MONITOR_FARGATE_VCPU_PRICE_PER_SECOND", DEFAULT_FARGATE_VCPU_PRICE_PER_SECOND)),
        fargate_memory_gb_price_per_second=float(env.get("PIPELINE_MONITOR_FARGATE_MEMORY_GB_PRICE_PER_SECOND", DEFAULT_FARGATE_MEMORY_GB_PRICE_PER_SECOND)),
    )

    if email_from and email_to_raw:
        recipients = [value.strip() for value in email_to_raw.split(",") if value.strip()]
        send_email_via_ses(
            ses_region=ses_region,
            email_from=email_from,
            email_to=recipients,
            subject=f"[{report['pipeline_status']}] {report['pipeline_name']} | run_id={report['run_id']}",
            html_body=report["html_report"],
        )

    return {
        "execution_arn": execution_arn,
        "run_id": report["run_id"],
        "pipeline_name": report["pipeline_name"],
        "pipeline_status": report["pipeline_status"],
        "job_count": len(report["rows"]),
        "email_sent": bool(email_from and email_to_raw),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render and optionally email a pipeline run report.")
    parser.add_argument("--execution-arn", required=True)
    parser.add_argument("--ddb-table", required=True)
    parser.add_argument("--aws-region", default=os.getenv("AWS_REGION"))
    parser.add_argument("--ses-region", default=os.getenv("PIPELINE_MONITOR_SES_REGION") or os.getenv("AWS_REGION"))
    parser.add_argument("--email-from", default=os.getenv("PIPELINE_MONITOR_EMAIL_FROM"))
    parser.add_argument("--email-to", default=os.getenv("PIPELINE_MONITOR_EMAIL_TO"))
    parser.add_argument("--send-email", action="store_true")
    parser.add_argument("--write-html")
    parser.add_argument("--pipeline-status")
    parser.add_argument("--metric-delay-seconds", type=int, default=0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.metric_delay_seconds > 0:
        time.sleep(args.metric_delay_seconds)

    report = build_report(
        execution_arn=args.execution_arn,
        ddb_table=args.ddb_table,
        aws_region=args.aws_region,
        pipeline_status_override=args.pipeline_status,
        athena_price_per_tb_scanned=float(os.getenv("PIPELINE_MONITOR_ATHENA_PRICE_PER_TB_SCANNED", DEFAULT_ATHENA_PRICE_PER_TB_SCANNED)),
        fargate_vcpu_price_per_second=float(os.getenv("PIPELINE_MONITOR_FARGATE_VCPU_PRICE_PER_SECOND", DEFAULT_FARGATE_VCPU_PRICE_PER_SECOND)),
        fargate_memory_gb_price_per_second=float(os.getenv("PIPELINE_MONITOR_FARGATE_MEMORY_GB_PRICE_PER_SECOND", DEFAULT_FARGATE_MEMORY_GB_PRICE_PER_SECOND)),
    )

    if args.write_html:
        with open(args.write_html, "w", encoding="utf-8") as handle:
            handle.write(report["html_report"])

    if args.send_email:
        if not args.email_from or not args.email_to:
            raise ValueError("--send-email requires --email-from and --email-to.")
        recipients = [value.strip() for value in str(args.email_to).split(",") if value.strip()]
        send_email_via_ses(
            ses_region=args.ses_region,
            email_from=args.email_from,
            email_to=recipients,
            subject=f"[{report['pipeline_status']}] {report['pipeline_name']} | run_id={report['run_id']}",
            html_body=report["html_report"],
        )

    print(
        json.dumps(
            {
                "execution_arn": args.execution_arn,
                "run_id": report["run_id"],
                "pipeline_name": report["pipeline_name"],
                "pipeline_status": report["pipeline_status"],
                "job_count": len(report["rows"]),
                "html_written": bool(args.write_html),
                "email_sent": bool(args.send_email),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
