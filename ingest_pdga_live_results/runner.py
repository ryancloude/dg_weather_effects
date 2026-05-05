from __future__ import annotations

import argparse
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable
from urllib.parse import urlparse
import boto3
from ingest_pdga_event_pages.config import load_config
from ingest_pdga_event_pages.http_client import HttpConfig, build_session, polite_sleep
from ingest_pdga_live_results.dynamo_reader import LiveResultsTask, load_live_results_tasks
from ingest_pdga_live_results.dynamo_writer import (
    get_existing_live_results_sha256,
    mark_event_live_results_ingested,
    put_live_results_run_summary,
    upsert_live_results_state,
)
from ingest_pdga_live_results.http_client import get_live_results_json
from ingest_pdga_live_results.response_handler import classify_response, compute_payload_sha256
from ingest_pdga_live_results.s3_writer import put_live_results_raw

logger = logging.getLogger("pdga_live_results_ingest")

DEFAULT_HISTORICAL_BACKFILL_EXCLUDED_STATUSES = (
    "Sanctioned",
    "Event report received; official ratings pending.",
    "Event complete; waiting for report.",
    "In progress.",
    "Errata pending.",
)


@dataclass(frozen=True)
class ProcessResult:
    task: LiveResultsTask
    source_url: str
    status_code: int | None
    classification: str
    content_sha256: str | None
    changed: bool
    unchanged: bool
    s3_ptrs: Dict[str, Any]


@dataclass
class RunStats:
    attempted: int = 0
    success: int = 0
    not_found_404: int = 0
    empty: int = 0
    changed: int = 0
    unchanged: int = 0
    failed: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "attempted": self.attempted,
            "success": self.success,
            "not_found_404": self.not_found_404,
            "empty": self.empty,
            "changed": self.changed,
            "unchanged": self.unchanged,
            "failed": self.failed,
        }


def parse_args():
    p = argparse.ArgumentParser(description="Ingest PDGA live results API to Bronze S3 + state in DynamoDB")
    p.add_argument("--event-ids", help="Optional comma-separated event IDs, e.g. 86076,86077")
    p.add_argument(
        "--historical-backfill",
        action="store_true",
        help="Backfill all METADATA events except excluded statuses, requiring non-empty division_rounds.",
    )
    p.add_argument(
        "--historical-excluded-statuses",
        help="Optional override for historical backfill excluded statuses, comma-separated.",
    )
    p.add_argument("--bucket")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--timeout", type=int, default=30)
    p.add_argument("--sleep-base", type=float, default=4.0)
    p.add_argument("--sleep-jitter", type=float, default=2.0)
    p.add_argument("--progress-every", type=int, default=50, help="Emit progress every N attempted tasks.")
    p.add_argument("--event-ids-s3-uri", help="S3 URI to a CSV/TXT file containing event IDs")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()

def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    return parsed.netloc, parsed.path.lstrip("/")

def resolve_run_id(default_factory) -> str:
    env_value = str(os.getenv("RUN_ID", "") or "").strip()
    if env_value:
        return env_value
    return default_factory()


def load_event_ids_from_s3_uri(*, s3_uri: str, aws_region: str | None) -> list[int]:
    bucket, key = parse_s3_uri(s3_uri)
    s3 = boto3.client("s3", region_name=aws_region) if aws_region else boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8")

    # Supports either newline-delimited IDs or comma-delimited CSV.
    raw_tokens = []
    for line in body.splitlines():
        raw_tokens.extend(line.split(","))

    event_ids = []
    for token in raw_tokens:
        token = token.strip()
        if not token:
            continue
        event_ids.append(int(token))
    return event_ids


def resolve_event_ids(args, aws_region: str | None) -> list[int] | None:
    if args.event_ids and args.event_ids_s3_uri:
        raise ValueError("Use either --event-ids or --event-ids-s3-uri, not both")
    if args.event_ids_s3_uri:
        return load_event_ids_from_s3_uri(s3_uri=args.event_ids_s3_uri, aws_region=aws_region)
    return parse_event_ids(args.event_ids)


def parse_event_ids(raw: str | None) -> list[int] | None:
    if not raw:
        return None
    return [int(x.strip()) for x in raw.split(",") if x.strip()]


def parse_status_list(raw_statuses: str) -> list[str]:
    values = [value.strip() for value in raw_statuses.split(",") if value.strip()]
    if not values:
        raise ValueError("--historical-excluded-statuses requires at least one status_text value")
    return values


def resolve_historical_excluded_statuses(args) -> list[str]:
    if args.historical_excluded_statuses:
        return parse_status_list(args.historical_excluded_statuses)
    return list(DEFAULT_HISTORICAL_BACKFILL_EXCLUDED_STATUSES)


def make_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"live-results-{ts}"


def process_task(
    *,
    task: LiveResultsTask,
    bucket: str,
    dry_run: bool,
    app_cfg,
    session,
    http_cfg: HttpConfig,
    run_id: str,
) -> ProcessResult:
    source_url = ""
    status_code: int | None = None
    payload = None
    error: Exception | None = None

    try:
        status_code, payload, source_url = get_live_results_json(session, http_cfg, task)
    except Exception as exc:
        error = exc

    classification = classify_response(status_code=status_code, payload=payload, error=error)
    content_sha256: str | None = None
    changed = False
    unchanged = False
    s3_ptrs: Dict[str, Any] = {}

    if classification in ("success", "empty"):
        content_sha256 = compute_payload_sha256(payload if payload is not None else {})
        if not dry_run:
            existing_hash = get_existing_live_results_sha256(
                table_name=app_cfg.ddb_table,
                event_id=int(task.event_id),
                division=task.division,
                round_number=task.round_number,
                aws_region=app_cfg.aws_region,
            )
            unchanged = bool(existing_hash and existing_hash == content_sha256)
            changed = not unchanged

            if changed:
                s3_ptrs = put_live_results_raw(
                    bucket=bucket,
                    task=task,
                    source_url=source_url,
                    payload=payload if payload is not None else {},
                    http_status=int(status_code or 200),
                    content_sha256=content_sha256,
                    run_id=run_id,
                )

            upsert_live_results_state(
                table_name=app_cfg.ddb_table,
                event_id=int(task.event_id),
                division=task.division,
                round_number=task.round_number,
                source_url=source_url,
                status_text=classification,
                content_sha256=content_sha256,
                s3_ptrs=s3_ptrs,
                run_id=run_id,
                aws_region=app_cfg.aws_region,
            )

    if error is not None:
        raise error

    return ProcessResult(
        task=task,
        source_url=source_url,
        status_code=status_code,
        classification=classification,
        content_sha256=content_sha256,
        changed=changed,
        unchanged=unchanged,
        s3_ptrs=s3_ptrs,
    )


def run_task_sequence(
    *,
    tasks: Iterable[LiveResultsTask],
    total_tasks: int,
    bucket: str,
    dry_run: bool,
    app_cfg,
    session,
    http_cfg: HttpConfig,
    run_id: str,
    progress_every: int,
) -> RunStats:
    stats = RunStats()
    started_at = time.monotonic()
    progress_every = max(progress_every, 1)

    for task in tasks:
        stats.attempted += 1

        try:
            result = process_task(
                task=task,
                bucket=bucket,
                dry_run=dry_run,
                app_cfg=app_cfg,
                session=session,
                http_cfg=http_cfg,
                run_id=run_id,
            )

            if result.classification == "success":
                stats.success += 1
            elif result.classification == "empty":
                stats.empty += 1
            elif result.classification == "not_found":
                stats.not_found_404 += 1

            if result.changed:
                stats.changed += 1
            if result.unchanged:
                stats.unchanged += 1

            logger.info(
                "live_results_task_ok",
                extra={
                    "event_id": task.event_id,
                    "division": task.division,
                    "round_number": task.round_number,
                    "classification": result.classification,
                    "changed": result.changed,
                    "unchanged": result.unchanged,
                    "s3_json_key": result.s3_ptrs.get("s3_json_key"),
                },
            )

        except Exception as exc:
            stats.failed += 1
            logger.exception(
                "live_results_task_failed",
                extra={
                    "event_id": task.event_id,
                    "division": task.division,
                    "round_number": task.round_number,
                    "error": str(exc),
                },
            )

        if total_tasks > 0 and (stats.attempted % progress_every == 0 or stats.attempted == total_tasks):
            elapsed = max(time.monotonic() - started_at, 1e-6)
            rate = stats.attempted / elapsed
            remaining = max(total_tasks - stats.attempted, 0)
            eta_seconds = int(remaining / rate) if rate > 0 else -1
            progress = {
                "run_id": run_id,
                "attempted": stats.attempted,
                "total_tasks": total_tasks,
                "pct_complete": round((stats.attempted / total_tasks) * 100.0, 2),
                "rate_tasks_per_sec": round(rate, 3),
                "eta_seconds": eta_seconds,
                "success": stats.success,
                "empty": stats.empty,
                "not_found_404": stats.not_found_404,
                "changed": stats.changed,
                "unchanged": stats.unchanged,
                "failed": stats.failed,
            }
            logger.info("live_results_progress", extra=progress)
            print({"live_results_progress": progress})

        polite_sleep(http_cfg)

    return stats


def main() -> int:
    args = parse_args()

    if args.historical_backfill and args.event_ids:
        raise ValueError("Use either --historical-backfill or --event-ids, not both")

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    app_cfg = load_config()
    bucket = args.bucket or app_cfg.s3_bucket
    http_cfg = HttpConfig(timeout_s=args.timeout, base_sleep_s=args.sleep_base, jitter_s=args.sleep_jitter)
    session = build_session(http_cfg)

    event_ids = resolve_event_ids(args, app_cfg.aws_region)

    excluded_statuses: list[str] | None = None
    require_non_empty_division_rounds = False
    use_status_end_date_gsi = False
    skip_events_with_live_results_state = False
    exclude_already_live_results_ingested = False

    if args.historical_backfill:
        excluded_statuses = resolve_historical_excluded_statuses(args)
        require_non_empty_division_rounds = True
        use_status_end_date_gsi = True
        skip_events_with_live_results_state = False
        exclude_already_live_results_ingested = True

        logger.info(
            "historical_backfill_mode",
            extra={
                "excluded_statuses": excluded_statuses,
                "require_non_empty_division_rounds": True,
            },
        )
        logger.info(
            "historical_backfill_controls",
            extra={
                "use_status_end_date_gsi": True,
                "status_end_date_gsi_name": app_cfg.ddb_status_end_date_gsi,
                "skip_events_with_live_results_state": False,
                "exclude_already_live_results_ingested": True,
            },
        )

    tasks = load_live_results_tasks(
        table_name=app_cfg.ddb_table,
        event_ids=event_ids,
        excluded_statuses=excluded_statuses,
        require_non_empty_division_rounds=require_non_empty_division_rounds,
        aws_region=app_cfg.aws_region,
        use_status_end_date_gsi=use_status_end_date_gsi,
        status_end_date_gsi_name=app_cfg.ddb_status_end_date_gsi,
        skip_events_with_live_results_state=skip_events_with_live_results_state,
        exclude_already_live_results_ingested=exclude_already_live_results_ingested,
    )

    total_tasks = len(tasks)
    total_events = len({task.event_id for task in tasks})
    run_id = resolve_run_id(make_run_id)

    logger.info(
        "live_results_run_plan",
        extra={
            "run_id": run_id,
            "historical_backfill": bool(args.historical_backfill),
            "total_events": total_events,
            "total_tasks": total_tasks,
            "dry_run": bool(args.dry_run),
            "progress_every": args.progress_every,
        },
    )
    print(
        {
            "live_results_run_plan": {
                "run_id": run_id,
                "historical_backfill": bool(args.historical_backfill),
                "total_events": total_events,
                "total_tasks": total_tasks,
                "dry_run": bool(args.dry_run),
                "progress_every": args.progress_every,
            }
        }
    )

    stats = run_task_sequence(
        tasks=tasks,
        total_tasks=total_tasks,
        bucket=bucket,
        dry_run=args.dry_run,
        app_cfg=app_cfg,
        session=session,
        http_cfg=http_cfg,
        run_id=run_id,
        progress_every=args.progress_every,
    )

    if not args.dry_run:
        put_live_results_run_summary(
            table_name=app_cfg.ddb_table,
            run_id=run_id,
            stats=stats.to_dict(),
            aws_region=app_cfg.aws_region,
        )

        if stats.failed == 0:
            for event_id in sorted({int(task.event_id) for task in tasks}):
                mark_event_live_results_ingested(
                    table_name=app_cfg.ddb_table,
                    event_id=event_id,
                    run_id=run_id,
                    aws_region=app_cfg.aws_region,
                )

    logger.info("live_results_summary", extra={"run_id": run_id, **stats.to_dict()})
    print({"run_id": run_id, "summary": stats.to_dict()})
    return 0 if stats.failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())