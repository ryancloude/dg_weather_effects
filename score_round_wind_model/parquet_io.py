from __future__ import annotations

from io import BytesIO
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from score_round_wind_model.models import SCORED_ROUNDS_PREFIX


SCORED_ROUNDS_SCHEMA = pa.schema(
    [
        pa.field("event_year", pa.int64()),
        pa.field("tourn_id", pa.int64()),
        pa.field("round_number", pa.int64()),
        pa.field("player_key", pa.string()),
        pa.field("player_name", pa.string()),
        pa.field("division", pa.string()),
        pa.field("player_rating", pa.float64()),
        pa.field("event_name", pa.string()),
        pa.field("event_city", pa.string()),
        pa.field("event_state", pa.string()),
        pa.field("event_start_date", pa.string()),
        pa.field("event_end_date", pa.string()),
        pa.field("round_date", pa.string()),
        pa.field("course_id", pa.string()),
        pa.field("course_name", pa.string()),
        pa.field("layout_id", pa.string()),
        pa.field("layout_name", pa.string()),
        pa.field("lat", pa.float64()),
        pa.field("lon", pa.float64()),
        pa.field("actual_round_strokes", pa.int64()),
        pa.field("round_strokes_over_par", pa.int64()),
        pa.field("weather_available_flag", pa.bool_()),
        pa.field("hole_count", pa.int64()),
        pa.field("round_total_hole_length", pa.float64()),
        pa.field("round_avg_hole_length", pa.float64()),
        pa.field("round_total_par", pa.int64()),
        pa.field("round_avg_hole_par", pa.float64()),
        pa.field("round_length_over_par", pa.float64()),
        pa.field("round_wind_speed_mps_mean", pa.float64()),
        pa.field("round_wind_speed_mps_max", pa.float64()),
        pa.field("round_wind_gust_mps_mean", pa.float64()),
        pa.field("round_wind_gust_mps_max", pa.float64()),
        pa.field("round_temp_c_mean", pa.float64()),
        pa.field("round_precip_mm_sum", pa.float64()),
        pa.field("round_precip_mm_mean", pa.float64()),
        pa.field("round_pressure_hpa_mean", pa.float64()),
        pa.field("round_humidity_pct_mean", pa.float64()),
        pa.field("precip_during_round_flag", pa.int64()),
        pa.field("predicted_round_strokes", pa.float64()),
        pa.field("predicted_round_strokes_wind_reference", pa.float64()),
        pa.field("predicted_round_strokes_temperature_reference", pa.float64()),
        pa.field("predicted_round_strokes_precip_reference", pa.float64()),
        pa.field("predicted_round_strokes_total_weather_reference", pa.float64()),
        pa.field("estimated_wind_impact_strokes", pa.float64()),
        pa.field("estimated_temperature_impact_strokes", pa.float64()),
        pa.field("estimated_precip_impact_strokes", pa.float64()),
        pa.field("estimated_total_weather_impact_strokes", pa.float64()),
        pa.field("model_name", pa.string()),
        pa.field("model_version", pa.string()),
        pa.field("training_request_fingerprint", pa.string()),
        pa.field("scoring_run_id", pa.string()),
        pa.field("scored_at_utc", pa.string()),
        pa.field("scoring_request_fingerprint", pa.string()),
        pa.field("model_artifact_prefix", pa.string()),
        pa.field("round_wind_speed_bucket", pa.string()),
        pa.field("round_wind_gust_bucket", pa.string()),
    ]
)


def _to_parquet_bytes(rows: list[dict[str, Any]]) -> bytes:
    table = pa.Table.from_pylist(rows, schema=SCORED_ROUNDS_SCHEMA)
    buf = BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def build_scored_round_partition_prefix(*, event_year: int, event_id: int) -> str:
    return (
        f"{SCORED_ROUNDS_PREFIX}"
        f"event_year={int(event_year)}/"
        f"tourn_id={int(event_id)}/"
    )


def build_scored_round_output_key(*, event_year: int, event_id: int) -> str:
    return (
        f"{build_scored_round_partition_prefix(event_year=event_year, event_id=event_id)}"
        "scored_rounds.parquet"
    )


def build_scored_round_partition_location(*, bucket: str, event_year: int, event_id: int) -> str:
    prefix = build_scored_round_partition_prefix(event_year=event_year, event_id=event_id)
    return f"s3://{bucket}/{prefix}"


def overwrite_event_scored_rounds(
    *,
    bucket: str,
    event_year: int,
    event_id: int,
    rows: list[dict[str, Any]],
    s3_client=None,
) -> str:
    s3 = s3_client or boto3.client("s3")
    key = build_scored_round_output_key(event_year=event_year, event_id=event_id)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=_to_parquet_bytes(rows),
        ContentType="application/octet-stream",
    )
    return key
