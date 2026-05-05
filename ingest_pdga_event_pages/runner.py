from __future__ import annotations

import argparse
import itertools
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable

import requests

from ingest_pdga_event_pages.config import load_config
from ingest_pdga_event_pages.dynamo_reader import (
    get_existing_content_sha256,
    get_max_event_id,
    iter_rescrape_event_ids_via_gsi,
)
from ingest_pdga_event_pages.dynamo_writer import (
    record_event_fetch_failure,
    touch_event_fetch_success,
    upsert_event_metadata,
)
from ingest_pdga_event_pages.event_page_parser import parse_event_page
from ingest_pdga_event_pages.http_client import HttpConfig, build_session, get_event_page_html, polite_sleep
from ingest_pdga_event_pages.run_summary import put_event_pages_run_summary
from ingest_pdga_event_pages.s3_writer import put_event_page_raw

logger = logging.getLogger("pdga_ingest")

DEFAULT_INCREMENTAL_WINDOW_DAYS = 183
DEFAULT_INCREMENTAL_REFETCH_HOURS = 48
DEFAULT_FAILED_REFETCH_COOLDOWN_HOURS = 72
DEFAULT_INCREMENTAL_STATUSES = (
    "Sanctioned",
    "Event report received; official ratings pending.",
    "Event complete; waiting for report.",
    "In progress.",
    "Errata pending.",
)
DEFAULT_MAX_FAILURE_RATE = 0.5
DEFAULT_PROGRESS_EVERY = 50


@dataclass(frozen=True)
class ProcessResult:
    event_id: int
    parsed: Dict[str, Any]
    http_status: int
    s3_ptrs: Dict[str, Any]
    ddb_attrs: Dict[str, Any]
    unchanged: bool
    change_type: str


@dataclass
class RunStats:
    scraped: int = 0
    new_scraped: int = 0
    updated_scraped: int = 0
    unchanged_scraped: int = 0
    not_found_404: int = 0
    failed: int = 0

    def merge(self, other: "RunStats") -> None:
        self.scraped += other.scraped
        self.new_scraped += other.new_scraped
        self.updated_scraped += other.updated_scraped
        self.unchanged_scraped += other.unchanged_scraped
        self.not_found_404 += other.not_found_404
        self.failed += other.failed

    def to_dict(self) -> Dict[str, int]:
        return {
            "scraped": self.scraped,
            "new_scraped": self.new_scraped,
            "updated_scraped": self.updated_scraped,
            "unchanged_scraped": self.unchanged_scraped,
            "not_found_404": self.not_found_404,
            "failed": self.failed,
        }

    def attempted_total(self) -> int:
        return self.scraped + self.not_found_404 + self.failed

    def failure_rate(self) -> float:
        attempted = self.attempted_total()
        if attempted == 0:
            return 0.0
        return self.failed / attempted


def make_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"event-pages-{ts}"


def resolve_run_id(default_factory) -> str:
    env_value = str(os.getenv("RUN_ID", "") or "").strip()
    if env_value:
        return env_value
    return default_factory()


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def probability(value: str) -> float:
    parsed = float(value)
    if not 0.0 <= parsed <= 1.0:
        raise argparse.ArgumentTypeError("value must be between 0.0 and 1.0")
    return parsed


def parse_args():
    p = argparse.ArgumentParser(description="Ingest PDGA event pages (raw HTML to S3 + parse discovery fields)")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--ids", help="Comma-separated event ids, e.g. 90009,90010")
    group.add_argument("--range", help="Inclusive range like 90000-90050")
    group.add_argument("--backfill-start-id", type=positive_int)
    group.add_argument("--incremental", action="store_true", help="Run incremental rescrape + forward scan mode.")

    p.add_argument(
        "--incremental-statuses",
        help="Optional override for incremental statuses, comma-separated. If omitted, defaults are used.",
    )
    p.add_argument("--incremental-window-days", type=positive_int, default=DEFAULT_INCREMENTAL_WINDOW_DAYS)
    p.add_argument(
        "--incremental-refetch-hours",
        type=positive_int,
        default=DEFAULT_INCREMENTAL_REFETCH_HOURS,
        help="Only rescrape incremental candidates when last_fetched_at is older than this many hours.",
    )
    p.add_argument(
        "--failed-refetch-cooldown-hours",
        type=positive_int,
        default=DEFAULT_FAILED_REFETCH_COOLDOWN_HOURS,
        help="Skip incremental rescrape for recently failed events until this many hours have passed.",
    )
    p.add_argument(
        "--max-failure-rate",
        type=probability,
        default=DEFAULT_MAX_FAILURE_RATE,
        help="Exit non-zero only when failed event attempts are at or above this fraction of total attempts.",
    )
    p.add_argument(
        "--progress-every",
        type=positive_int,
        default=DEFAULT_PROGRESS_EVERY,
        help="Emit progress every N attempted events.",
    )

    p.add_argument("--bucket")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--timeout", type=int, default=30)
    p.add_argument("--sleep-base", type=float, default=2.0)
    p.add_argument("--sleep-jitter", type=float, default=1.0)
    p.add_argument("--log-level", default="INFO")
    p.add_argument("--backfill-stop-after-unscheduled", type=positive_int, default=5)
    p.add_argument("--backfill-max-event-id", type=positive_int)
    return p.parse_args()


def iter_explicit_event_ids(args) -> Iterable[int]:
    if args.ids:
        return [int(x.strip()) for x in args.ids.split(",") if x.strip()]

    start_s, end_s = args.range.split("-", 1)
    start, end = int(start_s), int(end_s)
    if end < start:
        raise ValueError(f"range end must be >= start: {args.range}")
    return list(range(start, end + 1))


def parse_status_list(raw_statuses: str) -> list[str]:
    values = [value.strip() for value in raw_statuses.split(",") if value.strip()]
    if not values:
        raise ValueError("--incremental-statuses requires at least one status_text value")
    return values


def resolve_incremental_statuses(args) -> list[str]:
    if args.incremental_statuses:
        return parse_status_list(args.incremental_statuses)
    return list(DEFAULT_INCREMENTAL_STATUSES)


def update_no_event_streak(
    current_streak: int,
    *,
    is_unscheduled_placeholder: bool = False,
    is_not_found_404: bool = False,
) -> int:
    if is_unscheduled_placeholder or is_not_found_404:
        return current_streak + 1
    return 0


def should_stop_backfill(streak: int, threshold: int) -> bool:
    return streak >= threshold


def should_exit_nonzero(*, stats: RunStats, max_failure_rate: float) -> bool:
    return stats.failure_rate() >= max_failure_rate


def process_event(
    *,
    event_id: int,
    bucket: str,
    dry_run: bool,
    app_cfg,
    session,
    http_cfg: HttpConfig,
) -> ProcessResult:
    url = f"https://www.pdga.com/tour/event/{event_id}"

    status_code, html = get_event_page_html(session, http_cfg, event_id)
    parsed = parse_event_page(event_id=event_id, html=html, source_url=url)

    unchanged = False
    s3_ptrs: Dict[str, Any] = {}
    ddb_attrs: Dict[str, Any] = {}
    change_type = "unknown"

    if not dry_run:
        existing_hash = get_existing_content_sha256(
            table_name=app_cfg.ddb_table,
            event_id=event_id,
            aws_region=app_cfg.aws_region,
        )

        unchanged = bool(existing_hash and existing_hash == parsed["idempotency_sha256"])

        if unchanged:
            change_type = "unchanged"
            touch_event_fetch_success(
                table_name=app_cfg.ddb_table,
                event_id=event_id,
                aws_region=app_cfg.aws_region,
            )
        else:
            change_type = "updated" if existing_hash else "new"
            s3_ptrs = put_event_page_raw(
                bucket=bucket,
                event_id=event_id,
                source_url=url,
                html=html,
                http_status=status_code,
                content_sha256=parsed["content_sha256"],
                parser_version=parsed["parser_version"],
            )
            ddb_attrs = upsert_event_metadata(
                table_name=app_cfg.ddb_table,
                parsed=parsed,
                s3_ptrs=s3_ptrs,
                aws_region=app_cfg.aws_region,
            )

    return ProcessResult(
        event_id=event_id,
        parsed=parsed,
        http_status=status_code,
        s3_ptrs=s3_ptrs,
        ddb_attrs=ddb_attrs,
        unchanged=unchanged,
        change_type=change_type,
    )


def log_event_result(result: ProcessResult) -> None:
    parsed = result.parsed
    logger.info(
        "event_ok",
        extra={
            "event_id": result.event_id,
            "change_type": result.change_type,
            "is_unscheduled_placeholder": parsed["is_unscheduled_placeholder"],
            "status_text": parsed["status_text"],
            "divisions": len(parsed["division_rounds"]),
            "s3_html_key": result.s3_ptrs.get("s3_html_key"),
        },
    )


def run_event_sequence(
    *,
    event_ids: Iterable[int],
    bucket: str,
    dry_run: bool,
    app_cfg,
    session,
    http_cfg: HttpConfig,
    run_id: str,
    progress_every: int,
) -> RunStats:
    stats = RunStats()
    event_id_list = list(event_ids)
    total_events = len(event_id_list)
    progress_every = max(progress_every, 1)

    for idx, event_id in enumerate(event_id_list, start=1):
        try:
            result = process_event(
                event_id=event_id,
                bucket=bucket,
                dry_run=dry_run,
                app_cfg=app_cfg,
                session=session,
                http_cfg=http_cfg,
            )
            log_event_result(result)

            stats.scraped += 1
            if result.change_type == "new":
                stats.new_scraped += 1
            elif result.change_type == "updated":
                stats.updated_scraped += 1
            elif result.change_type == "unchanged":
                stats.unchanged_scraped += 1

        except Exception as exc:
            stats.failed += 1
            logger.exception("event_failed", extra={"event_id": event_id, "error": str(exc)})
            if not dry_run:
                try:
                    record_event_fetch_failure(
                        table_name=app_cfg.ddb_table,
                        event_id=event_id,
                        error_message=str(exc),
                        aws_region=app_cfg.aws_region,
                    )
                except Exception:
                    logger.exception("event_failure_metadata_write_failed", extra={"event_id": event_id})

        if idx % progress_every == 0 or idx == total_events:
            progress = {
                "run_id": run_id,
                "processed_events": idx,
                "total_events": total_events,
                "pct_complete": round((idx / total_events) * 100.0, 2) if total_events else 100.0,
                **stats.to_dict(),
                "attempted_total": stats.attempted_total(),
                "failure_rate": round(stats.failure_rate(), 4),
            }
            logger.info("incremental_progress", extra=progress)
            print({"incremental_progress": progress})

        polite_sleep(http_cfg)

    return stats


def run_forward_scan(
    *,
    start_event_id: int,
    stop_after_unscheduled: int,
    max_event_id: int | None,
    bucket: str,
    dry_run: bool,
    app_cfg,
    session,
    http_cfg: HttpConfig,
    run_id: str,
    progress_every: int,
) -> RunStats:
    stats = RunStats()
    no_event_streak = 0
    progress_every = max(progress_every, 1)

    for event_id in itertools.count(start_event_id):
        if max_event_id is not None and event_id > max_event_id:
            break

        try:
            result = process_event(
                event_id=event_id,
                bucket=bucket,
                dry_run=dry_run,
                app_cfg=app_cfg,
                session=session,
                http_cfg=http_cfg,
            )
            log_event_result(result)

            stats.scraped += 1
            if result.change_type == "new":
                stats.new_scraped += 1
            elif result.change_type == "updated":
                stats.updated_scraped += 1
            elif result.change_type == "unchanged":
                stats.unchanged_scraped += 1

            no_event_streak = update_no_event_streak(
                no_event_streak,
                is_unscheduled_placeholder=result.parsed["is_unscheduled_placeholder"],
            )
            if should_stop_backfill(no_event_streak, stop_after_unscheduled):
                break

        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code == 404:
                stats.not_found_404 += 1
                no_event_streak = update_no_event_streak(no_event_streak, is_not_found_404=True)
                if should_stop_backfill(no_event_streak, stop_after_unscheduled):
                    break
            else:
                stats.failed += 1
                no_event_streak = 0
                logger.exception("event_failed", extra={"event_id": event_id, "error": str(exc)})
                if not dry_run:
                    try:
                        record_event_fetch_failure(
                            table_name=app_cfg.ddb_table,
                            event_id=event_id,
                            error_message=str(exc),
                            aws_region=app_cfg.aws_region,
                        )
                    except Exception:
                        logger.exception("event_failure_metadata_write_failed", extra={"event_id": event_id})

        except Exception as exc:
            stats.failed += 1
            no_event_streak = 0
            logger.exception("event_failed", extra={"event_id": event_id, "error": str(exc)})
            if not dry_run:
                try:
                    record_event_fetch_failure(
                        table_name=app_cfg.ddb_table,
                        event_id=event_id,
                        error_message=str(exc),
                        aws_region=app_cfg.aws_region,
                    )
                except Exception:
                    logger.exception("event_failure_metadata_write_failed", extra={"event_id": event_id})

        attempted = stats.attempted_total()
        if attempted > 0 and attempted % progress_every == 0:
            progress = {
                "run_id": run_id,
                "mode": "forward_scan",
                "last_event_id": event_id,
                **stats.to_dict(),
                "attempted_total": stats.attempted_total(),
                "failure_rate": round(stats.failure_rate(), 4),
                "no_event_streak": no_event_streak,
            }
            logger.info("forward_scan_progress", extra=progress)
            print({"forward_scan_progress": progress})

        polite_sleep(http_cfg)

    return stats


def main() -> int:
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    app_cfg = load_config()
    bucket = args.bucket or app_cfg.s3_bucket
    http_cfg = HttpConfig(timeout_s=args.timeout, base_sleep_s=args.sleep_base, jitter_s=args.sleep_jitter)
    session = build_session(http_cfg)

    total = RunStats()
    run_id = resolve_run_id(make_run_id)
    progress_every = max(args.progress_every, 1)

    if args.incremental:
        statuses = resolve_incremental_statuses(args)
        now_utc = datetime.now(timezone.utc)
        today = now_utc.date()
        window_start = today - timedelta(days=args.incremental_window_days)
        refetch_cutoff_dt = now_utc - timedelta(hours=args.incremental_refetch_hours)
        failed_refetch_cutoff_dt = now_utc - timedelta(hours=args.failed_refetch_cooldown_hours)
        refetch_cutoff_ts = refetch_cutoff_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        failed_refetch_cutoff_ts = failed_refetch_cutoff_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")

        candidate_ids = list(
            iter_rescrape_event_ids_via_gsi(
                table_name=app_cfg.ddb_table,
                gsi_name=app_cfg.ddb_status_end_date_gsi,
                status_texts=statuses,
                start_date=window_start.isoformat(),
                end_before_date=today.isoformat(),
                older_than_ts=refetch_cutoff_ts,
                failed_older_than_ts=failed_refetch_cutoff_ts,
                aws_region=app_cfg.aws_region,
            )
        )

        candidate_count = len(candidate_ids)
        logger.info(
            "incremental_rescrape_candidate_count",
            extra={
                "candidate_count": candidate_count,
                "incremental_window_days": args.incremental_window_days,
                "incremental_refetch_hours": args.incremental_refetch_hours,
                "failed_refetch_cooldown_hours": args.failed_refetch_cooldown_hours,
                "incremental_statuses": statuses,
                "window_start": window_start.isoformat(),
                "end_before_date": today.isoformat(),
                "older_than_ts": refetch_cutoff_ts,
                "failed_older_than_ts": failed_refetch_cutoff_ts,
            },
        )
        print(
            {
                "incremental_rescrape_candidate_count": candidate_count,
                "incremental_window_days": args.incremental_window_days,
                "incremental_refetch_hours": args.incremental_refetch_hours,
                "failed_refetch_cooldown_hours": args.failed_refetch_cooldown_hours,
                "incremental_statuses": statuses,
                "window_start": window_start.isoformat(),
                "end_before_date": today.isoformat(),
                "older_than_ts": refetch_cutoff_ts,
                "failed_older_than_ts": failed_refetch_cutoff_ts,
            }
        )

        rescrape_stats = run_event_sequence(
            event_ids=candidate_ids,
            bucket=bucket,
            dry_run=args.dry_run,
            app_cfg=app_cfg,
            session=session,
            http_cfg=http_cfg,
            run_id=run_id,
            progress_every=progress_every,
        )
        total.merge(rescrape_stats)

        max_known_event_id = get_max_event_id(table_name=app_cfg.ddb_table, aws_region=app_cfg.aws_region)
        if max_known_event_id is None:
            raise RuntimeError("Could not determine max known event_id for incremental forward scan")

        forward_stats = run_forward_scan(
            start_event_id=max_known_event_id + 1,
            stop_after_unscheduled=args.backfill_stop_after_unscheduled,
            max_event_id=args.backfill_max_event_id,
            bucket=bucket,
            dry_run=args.dry_run,
            app_cfg=app_cfg,
            session=session,
            http_cfg=http_cfg,
            run_id=run_id,
            progress_every=progress_every,
        )
        total.merge(forward_stats)

        summary = {
            "updated_scraped": total.updated_scraped,
            "new_scraped": total.new_scraped,
            "unchanged_scraped": total.unchanged_scraped,
            "scraped_total": total.scraped,
            "not_found_404": total.not_found_404,
            "failed": total.failed,
            "attempted_total": total.attempted_total(),
            "failure_rate": round(total.failure_rate(), 4),
            "max_failure_rate": args.max_failure_rate,
        }
        logger.info("incremental_summary", extra=summary)
        print({"incremental_summary": summary})

    elif args.backfill_start_id is not None:
        stats = run_forward_scan(
            start_event_id=args.backfill_start_id,
            stop_after_unscheduled=args.backfill_stop_after_unscheduled,
            max_event_id=args.backfill_max_event_id,
            bucket=bucket,
            dry_run=args.dry_run,
            app_cfg=app_cfg,
            session=session,
            http_cfg=http_cfg,
            run_id=run_id,
            progress_every=progress_every,
        )
        total.merge(stats)

    else:
        stats = run_event_sequence(
            event_ids=iter_explicit_event_ids(args),
            bucket=bucket,
            dry_run=args.dry_run,
            app_cfg=app_cfg,
            session=session,
            http_cfg=http_cfg,
            run_id=run_id,
            progress_every=progress_every,
        )
        total.merge(stats)

    exit_nonzero = should_exit_nonzero(stats=total, max_failure_rate=args.max_failure_rate)
    run_mode = (
        "incremental"
        if bool(args.incremental)
        else "backfill"
        if args.backfill_start_id is not None
        else "explicit"
    )
    final_summary = {
        "run_id": run_id,
        "run_mode": run_mode,
        **total.to_dict(),
        "attempted_total": total.attempted_total(),
        "failure_rate": round(total.failure_rate(), 4),
        "max_failure_rate": args.max_failure_rate,
        "exit_nonzero": exit_nonzero,
    }

    logger.info("summary", extra=final_summary)
    print({"summary": final_summary})

    if not args.dry_run:
        put_event_pages_run_summary(
            table_name=app_cfg.ddb_table,
            run_id=run_id,
            stats=final_summary,
            aws_region=app_cfg.aws_region,
        )

    return 2 if exit_nonzero else 0


if __name__ == "__main__":
    raise SystemExit(main())
