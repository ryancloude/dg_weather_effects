from __future__ import annotations

import time
from typing import Any

import boto3

from score_round_wind_model.models import SCORED_ROUNDS_PREFIX


def _athena_client(aws_region: str | None):
    return boto3.client("athena", region_name=aws_region) if aws_region else boto3.client("athena")


def build_scored_rounds_table_location(*, bucket: str) -> str:
    return f"s3://{bucket}/{SCORED_ROUNDS_PREFIX}"


def build_scored_rounds_storage_location_template(*, bucket: str) -> str:
    return (
        f"s3://{bucket}/{SCORED_ROUNDS_PREFIX}"
        "event_year=${event_year}/"
        "tourn_id=${tourn_id}/"
    )


def build_create_scored_rounds_table_sql(
    *,
    database: str,
    table_name: str,
    table_location: str,
    storage_location_template: str,
    event_year_start: int = 2020,
    event_year_end: int = 2035,
    tourn_id_min: int = 1,
    tourn_id_max: int = 2000000,
) -> str:
    return f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table_name} (
  round_number int,
  player_key string,
  player_name string,
  division string,
  player_rating double,
  event_name string,
  event_city string,
  event_state string,
  event_start_date string,
  event_end_date string,
  round_date string,
  course_id string,
  course_name string,
  layout_id string,
  layout_name string,
  lat double,
  lon double,
  actual_round_strokes double,
  round_strokes_over_par double,
  weather_available_flag boolean,
  hole_count int,
  round_total_hole_length double,
  round_avg_hole_length double,
  round_total_par double,
  round_avg_hole_par double,
  round_length_over_par double,
  round_wind_speed_mps_mean double,
  round_wind_gust_mps_mean double,
  round_temp_c_mean double,
  round_precip_mm_sum double,
  precip_during_round_flag int,
  predicted_round_strokes double,
  predicted_round_strokes_wind_reference double,
  predicted_round_strokes_temperature_reference double,
  predicted_round_strokes_precip_reference double,
  predicted_round_strokes_total_weather_reference double,
  estimated_wind_impact_strokes double,
  estimated_temperature_impact_strokes double,
  estimated_precip_impact_strokes double,
  estimated_total_weather_impact_strokes double,
  model_name string,
  model_version string,
  training_request_fingerprint string,
  scoring_run_id string,
  scored_at_utc string,
  scoring_request_fingerprint string,
  model_artifact_prefix string,
  round_wind_speed_bucket string,
  round_wind_gust_bucket string
)
PARTITIONED BY (
  event_year int,
  tourn_id bigint
)
STORED AS PARQUET
LOCATION '{table_location}'
TBLPROPERTIES (
  'parquet.compression' = 'SNAPPY',
  'projection.enabled' = 'true',
  'projection.event_year.type' = 'integer',
  'projection.event_year.range' = '{int(event_year_start)},{int(event_year_end)}',
  'projection.tourn_id.type' = 'integer',
  'projection.tourn_id.range' = '{int(tourn_id_min)},{int(tourn_id_max)}',
  'storage.location.template' = '{storage_location_template}'
)
""".strip()


def start_athena_query(
    *,
    sql: str,
    database: str,
    workgroup: str,
    output_location: str,
    aws_region: str | None,
) -> str:
    client = _athena_client(aws_region)
    resp = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
        ResultConfiguration={"OutputLocation": output_location},
    )
    return resp["QueryExecutionId"]


def wait_for_query(
    *,
    query_execution_id: str,
    aws_region: str | None,
    poll_seconds: float = 2.0,
    timeout_seconds: float = 1800.0,
) -> dict[str, Any]:
    client = _athena_client(aws_region)
    started = time.perf_counter()

    while True:
        resp = client.get_query_execution(QueryExecutionId=query_execution_id)
        execution = resp["QueryExecution"]
        status = execution["Status"]["State"]

        if status in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            if status != "SUCCEEDED":
                reason = execution["Status"].get("StateChangeReason", "unknown Athena failure")
                raise RuntimeError(f"Athena query {query_execution_id} {status.lower()}: {reason}")

            stats = execution.get("Statistics", {})
            result_cfg = execution.get("ResultConfiguration", {})
            return {
                "query_execution_id": query_execution_id,
                "state": status,
                "scanned_bytes": int(stats.get("DataScannedInBytes", 0) or 0),
                "engine_execution_time_ms": int(stats.get("EngineExecutionTimeInMillis", 0) or 0),
                "total_execution_time_ms": int(stats.get("TotalExecutionTimeInMillis", 0) or 0),
                "output_location": result_cfg.get("OutputLocation", ""),
            }

        if time.perf_counter() - started > timeout_seconds:
            raise TimeoutError(f"Athena query {query_execution_id} timed out after {timeout_seconds} seconds")

        time.sleep(poll_seconds)


def execute_athena_query(
    *,
    sql: str,
    database: str,
    workgroup: str,
    output_location: str,
    aws_region: str | None,
    poll_seconds: float = 2.0,
    timeout_seconds: float = 1800.0,
) -> dict[str, Any]:
    query_execution_id = start_athena_query(
        sql=sql,
        database=database,
        workgroup=workgroup,
        output_location=output_location,
        aws_region=aws_region,
    )
    result = wait_for_query(
        query_execution_id=query_execution_id,
        aws_region=aws_region,
        poll_seconds=poll_seconds,
        timeout_seconds=timeout_seconds,
    )
    result["sql"] = sql
    return result


def ensure_scored_rounds_table(
    *,
    database: str,
    table_name: str,
    workgroup: str,
    output_location: str,
    table_location: str,
    storage_location_template: str,
    aws_region: str | None,
    event_year_start: int = 2020,
    event_year_end: int = 2035,
    tourn_id_min: int = 1,
    tourn_id_max: int = 2000000,
) -> dict[str, Any]:
    sql = build_create_scored_rounds_table_sql(
        database=database,
        table_name=table_name,
        table_location=table_location,
        storage_location_template=storage_location_template,
        event_year_start=event_year_start,
        event_year_end=event_year_end,
        tourn_id_min=tourn_id_min,
        tourn_id_max=tourn_id_max,
    )
    result = execute_athena_query(
        sql=sql,
        database=database,
        workgroup=workgroup,
        output_location=output_location,
        aws_region=aws_region,
    )
    result.update(
        {
            "table_name": table_name,
            "table_location": table_location,
            "storage_location_template": storage_location_template,
        }
    )
    return result
