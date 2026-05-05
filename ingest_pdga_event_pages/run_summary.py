from __future__ import annotations

import math
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional

import boto3


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _ddb_resource(aws_region: Optional[str]):
    return boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")


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


def put_event_pages_run_summary(
    *,
    table_name: str,
    run_id: str,
    stats: Dict[str, Any],
    aws_region: Optional[str] = None,
) -> Dict[str, Any]:
    table = _ddb_resource(aws_region).Table(table_name)
    item = {
        "pk": f"RUN#{run_id}",
        "sk": "EVENT_PAGES#SUMMARY",
        "run_id": run_id,
        "created_at": utc_now_iso(),
        **stats,
    }
    safe_item = _to_dynamodb_safe(item)
    table.put_item(Item=safe_item)
    return safe_item
