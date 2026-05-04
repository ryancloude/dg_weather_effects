from __future__ import annotations

import math
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

import boto3

from score_round_wind_model.models import PIPELINE_NAME, SCORE_CHECKPOINT_PK


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _ddb_resource(aws_region: Optional[str]):
    return boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")


def _ddb_client(aws_region: Optional[str]):
    return boto3.client("dynamodb", region_name=aws_region) if aws_region else boto3.client("dynamodb")


def _to_dynamodb_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return Decimal(str(value))
    if hasattr(value, "item") and callable(value.item):
        try:
            return _to_dynamodb_safe(value.item())
        except Exception:
            pass
    if isinstance(value, dict):
        return {str(k): _to_dynamodb_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_dynamodb_safe(v) for v in value]
    return value


def get_score_checkpoint(
    *,
    table_name: str,
    event_id: int,
    training_request_fingerprint: str,
    aws_region: Optional[str],
) -> dict[str, Any] | None:
    table = _ddb_resource(aws_region).Table(table_name)
    resp = table.get_item(
        Key={
            "pk": SCORE_CHECKPOINT_PK,
            "sk": f"EVENT#{int(event_id)}#MODEL#{training_request_fingerprint}",
        },
        ConsistentRead=False,
    )
    return resp.get("Item")


def get_score_checkpoints(
    *,
    table_name: str,
    event_ids: list[int],
    training_request_fingerprint: str,
    aws_region: Optional[str],
) -> dict[int, dict[str, Any]]:
    client = _ddb_client(aws_region)
    unique_event_ids = sorted({int(x) for x in event_ids})
    out: dict[int, dict[str, Any]] = {}

    for start in range(0, len(unique_event_ids), 100):
        chunk = unique_event_ids[start : start + 100]
        request_items = {
            table_name: {
                "Keys": [
                    {
                        "pk": {"S": SCORE_CHECKPOINT_PK},
                        "sk": {"S": f"EVENT#{event_id}#MODEL#{training_request_fingerprint}"},
                    }
                    for event_id in chunk
                ]
            }
        }

        resp = client.batch_get_item(RequestItems=request_items)
        items = resp.get("Responses", {}).get(table_name, [])

        for item in items:
            event_id_raw = item.get("event_id", {}).get("N")
            if event_id_raw is None:
                continue

            event_id = int(event_id_raw)
            decoded = {
                "pk": item.get("pk", {}).get("S", ""),
                "sk": item.get("sk", {}).get("S", ""),
                "pipeline": item.get("pipeline", {}).get("S", ""),
                "event_id": event_id,
                "training_request_fingerprint": item.get("training_request_fingerprint", {}).get("S", ""),
                "status": item.get("status", {}).get("S", ""),
                "last_run_id": item.get("last_run_id", {}).get("S", ""),
                "updated_at": item.get("updated_at", {}).get("S", ""),
                "scoring_request_fingerprint": item.get("scoring_request_fingerprint", {}).get("S", ""),
            }

            for key, value in item.items():
                if key in decoded:
                    continue
                if "S" in value:
                    decoded[key] = value["S"]
                elif "N" in value:
                    decoded[key] = Decimal(value["N"])
                elif "BOOL" in value:
                    decoded[key] = value["BOOL"]

            out[event_id] = decoded

    return out


def put_score_checkpoint(
    *,
    table_name: str,
    event_id: int,
    training_request_fingerprint: str,
    run_id: str,
    status: str,
    aws_region: Optional[str],
    extra_attributes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    table = _ddb_resource(aws_region).Table(table_name)

    checkpoint_item = {
        "pk": SCORE_CHECKPOINT_PK,
        "sk": f"EVENT#{int(event_id)}#MODEL#{training_request_fingerprint}",
        "pipeline": PIPELINE_NAME,
        "event_id": int(event_id),
        "training_request_fingerprint": training_request_fingerprint,
        "status": status,
        "last_run_id": run_id,
        "updated_at": utc_now_iso(),
    }
    if extra_attributes:
        checkpoint_item.update(extra_attributes)

    safe_checkpoint_item = _to_dynamodb_safe(checkpoint_item)
    table.put_item(Item=safe_checkpoint_item)
    return safe_checkpoint_item


def put_score_run_summary(
    *,
    table_name: str,
    run_id: str,
    stats: dict[str, Any],
    aws_region: Optional[str],
) -> dict[str, Any]:
    table = _ddb_resource(aws_region).Table(table_name)

    summary_item = {
        "pk": f"RUN#{run_id}",
        "sk": "SCORE_ROUND_WIND_MODEL#SUMMARY",
        "run_id": run_id,
        "created_at": utc_now_iso(),
        **stats,
    }

    safe_summary_item = _to_dynamodb_safe(summary_item)
    table.put_item(Item=safe_summary_item)
    return safe_summary_item
