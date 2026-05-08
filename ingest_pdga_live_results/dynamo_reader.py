from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Iterator, Optional

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError


# Required statuses for historical backfill candidate selection.
# You can adjust this list if you want to include additional terminal statuses.
DEFAULT_REQUIRED_HISTORICAL_STATUSES = (
    "Event complete; official ratings processed.",
    "Event complete; unofficial ratings processed.",
    "Event report received; official ratings pending."
)


@dataclass(frozen=True)
class LiveResultsTask:
    event_id: str
    division: str
    round_number: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "division": self.division,
            "round_number": self.round_number,
        }


def _coerce_positive_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, Decimal):
        try:
            if value != value.to_integral_value():
                return None
            parsed = int(value)
            return parsed if parsed > 0 else None
        except (InvalidOperation, ValueError):
            return None
    return None


def has_non_empty_division_rounds(metadata_item: dict[str, Any]) -> bool:
    division_rounds = metadata_item.get("division_rounds")
    return isinstance(division_rounds, dict) and len(division_rounds) > 0


def should_include_metadata_item(
    metadata_item: dict[str, Any],
    *,
    excluded_statuses: set[str] | None = None,
    require_non_empty_division_rounds: bool = False,
    exclude_already_live_results_ingested: bool = False,
) -> bool:
    if exclude_already_live_results_ingested and bool(metadata_item.get("live_results_ingested", False)):
        return False

    if require_non_empty_division_rounds and not has_non_empty_division_rounds(metadata_item):
        return False

    if excluded_statuses:
        status_text = str(metadata_item.get("status_text", "")).strip()
        if status_text in excluded_statuses:
            return False

    return True


def expand_tasks_from_metadata_item(metadata_item: dict[str, Any]) -> list[LiveResultsTask]:
    event_id_raw = metadata_item.get("event_id", "")
    event_id = str(event_id_raw).strip()
    if not event_id:
        return []

    division_rounds = metadata_item.get("division_rounds")
    if not isinstance(division_rounds, dict):
        return []

    tasks: list[LiveResultsTask] = []
    for division, max_round_raw in division_rounds.items():
        division_code = str(division).strip()
        if not division_code:
            continue

        max_round = _coerce_positive_int(max_round_raw)
        if max_round is None:
            continue

        for round_number in range(1, max_round + 1):
            tasks.append(
                LiveResultsTask(
                    event_id=event_id,
                    division=division_code,
                    round_number=round_number,
                )
            )
    return tasks


def expand_tasks_from_metadata_items(metadata_items: Iterable[dict[str, Any]]) -> list[LiveResultsTask]:
    tasks: list[LiveResultsTask] = []
    for item in metadata_items:
        tasks.extend(expand_tasks_from_metadata_item(item))
    return tasks


def _get_metadata_item_by_event_id(table, event_id: int) -> dict[str, Any] | None:
    resp = table.get_item(Key={"pk": f"EVENT#{int(event_id)}", "sk": "METADATA"}, ConsistentRead=False)
    return resp.get("Item")


def _event_has_any_live_results_state(table, event_id: int) -> bool:
    resp = table.query(
        KeyConditionExpression=Key("pk").eq(f"EVENT#{int(event_id)}") & Key("sk").begins_with("LIVE_RESULTS#"),
        ProjectionExpression="pk, sk",
        Limit=1,
        ConsistentRead=False,
    )
    return len(resp.get("Items", [])) > 0


def _resolve_required_statuses(excluded_statuses: set[str]) -> list[str]:
    return [s for s in DEFAULT_REQUIRED_HISTORICAL_STATUSES if s not in excluded_statuses]


def _iter_event_ids_via_status_gsi(
    *,
    table,
    gsi_name: str,
    statuses: list[str],
    end_before_date: str,
    filter_not_ingested_on_gsi: bool,
) -> Iterator[int]:
    dedup: set[int] = set()

    for status in statuses:
        last_evaluated_key = None
        while True:
            query_kwargs: dict[str, Any] = {
                "IndexName": gsi_name,
                "KeyConditionExpression": Key("status_text").eq(status) & Key("end_date").lt(end_before_date),
                "ProjectionExpression": "event_id, #pk, #sk, status_text, end_date, live_results_ingested",
                "ExpressionAttributeNames": {"#pk": "pk", "#sk": "sk"},
            }

            if filter_not_ingested_on_gsi:
                query_kwargs["FilterExpression"] = Attr("live_results_ingested").not_exists() | Attr(
                    "live_results_ingested"
                ).eq(False)

            if last_evaluated_key:
                query_kwargs["ExclusiveStartKey"] = last_evaluated_key

            try:
                resp = table.query(**query_kwargs)
            except ClientError as exc:
                # Fallback if live_results_ingested is not projected in this GSI.
                code = exc.response.get("Error", {}).get("Code", "")
                if code == "ValidationException" and "live_results_ingested" in str(exc):
                    query_kwargs.pop("FilterExpression", None)
                    resp = table.query(**query_kwargs)
                else:
                    raise

            for item in resp.get("Items", []):
                if item.get("sk") != "METADATA":
                    continue
                event_id = item.get("event_id")
                if event_id is None:
                    continue
                value = int(event_id)
                if value in dedup:
                    continue
                dedup.add(value)
                yield value

            last_evaluated_key = resp.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break


def iter_metadata_items(
    *,
    table_name: str,
    event_ids: Optional[list[int]] = None,
    excluded_statuses: Optional[list[str]] = None,
    require_non_empty_division_rounds: bool = False,
    aws_region: Optional[str] = None,
    use_status_end_date_gsi: bool | None = None,
    status_end_date_gsi_name: str = "gsi_status_end_date",
    gsi_start_date: str | None = None,  # kept for API compatibility (unused in simplified GSI mode)
    gsi_end_before_date: str | None = None,
    skip_events_with_live_results_state: bool | None = None,
    exclude_already_live_results_ingested: bool = False,
) -> Iterator[dict[str, Any]]:
    del gsi_start_date  # intentionally unused in simplified mode

    ddb = boto3.resource("dynamodb", region_name=aws_region) if aws_region else boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    excluded_statuses_set = {s.strip() for s in (excluded_statuses or []) if s.strip()}

    if use_status_end_date_gsi is None:
        use_status_end_date_gsi = bool(excluded_statuses_set) and not event_ids

    if skip_events_with_live_results_state is None:
        skip_events_with_live_results_state = False

    if event_ids:
        for event_id in event_ids:
            item = _get_metadata_item_by_event_id(table, int(event_id))
            if not item:
                continue

            if skip_events_with_live_results_state and _event_has_any_live_results_state(table, int(event_id)):
                continue

            if should_include_metadata_item(
                item,
                excluded_statuses=excluded_statuses_set,
                require_non_empty_division_rounds=require_non_empty_division_rounds,
                exclude_already_live_results_ingested=exclude_already_live_results_ingested,
            ):
                yield item
        return

    if use_status_end_date_gsi:
        end_before = gsi_end_before_date or date.today().isoformat()
        required_statuses = _resolve_required_statuses(excluded_statuses_set)

        for event_id in _iter_event_ids_via_status_gsi(
            table=table,
            gsi_name=status_end_date_gsi_name,
            statuses=required_statuses,
            end_before_date=end_before,
            filter_not_ingested_on_gsi=exclude_already_live_results_ingested,
        ):
            if skip_events_with_live_results_state and _event_has_any_live_results_state(table, event_id):
                continue

            item = _get_metadata_item_by_event_id(table, event_id)
            if not item:
                continue

            if should_include_metadata_item(
                item,
                excluded_statuses=excluded_statuses_set,
                require_non_empty_division_rounds=require_non_empty_division_rounds,
                exclude_already_live_results_ingested=exclude_already_live_results_ingested,
            ):
                yield item
        return

    # Fallback full scan path
    last_evaluated_key = None
    while True:
        scan_kwargs: dict[str, Any] = {
            "FilterExpression": Attr("sk").eq("METADATA"),
            "ProjectionExpression": "event_id, division_rounds, status_text, live_results_ingested, pk, sk",
        }
        if last_evaluated_key:
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

        resp = table.scan(**scan_kwargs)
        for item in resp.get("Items", []):
            event_id_raw = item.get("event_id")
            if event_id_raw is None:
                continue
            event_id = int(event_id_raw)

            if skip_events_with_live_results_state and _event_has_any_live_results_state(table, event_id):
                continue

            if should_include_metadata_item(
                item,
                excluded_statuses=excluded_statuses_set,
                require_non_empty_division_rounds=require_non_empty_division_rounds,
                exclude_already_live_results_ingested=exclude_already_live_results_ingested,
            ):
                yield item

        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break


def load_live_results_tasks(
    *,
    table_name: str,
    event_ids: Optional[list[int]] = None,
    excluded_statuses: Optional[list[str]] = None,
    require_non_empty_division_rounds: bool = False,
    aws_region: Optional[str] = None,
    use_status_end_date_gsi: bool | None = None,
    status_end_date_gsi_name: str = "gsi_status_end_date",
    gsi_start_date: str | None = None,
    gsi_end_before_date: str | None = None,
    skip_events_with_live_results_state: bool | None = None,
    exclude_already_live_results_ingested: bool = False,
) -> list[LiveResultsTask]:
    metadata_items = list(
        iter_metadata_items(
            table_name=table_name,
            event_ids=event_ids,
            excluded_statuses=excluded_statuses,
            require_non_empty_division_rounds=require_non_empty_division_rounds,
            aws_region=aws_region,
            use_status_end_date_gsi=use_status_end_date_gsi,
            status_end_date_gsi_name=status_end_date_gsi_name,
            gsi_start_date=gsi_start_date,
            gsi_end_before_date=gsi_end_before_date,
            skip_events_with_live_results_state=skip_events_with_live_results_state,
            exclude_already_live_results_ingested=exclude_already_live_results_ingested,
        )
    )
    return expand_tasks_from_metadata_items(metadata_items)