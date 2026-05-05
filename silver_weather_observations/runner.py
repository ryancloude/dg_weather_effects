from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
import os

from silver_weather_observations.bronze_io import (
    build_weather_round_sources,
    compute_event_source_fingerprint,
)
from silver_weather_observations.config import load_config
from silver_weather_observations.dynamo_io import (
    get_event_metadata,
    get_silver_weather_event_checkpoint,
    load_silver_weather_event_checkpoints,
    load_weather_event_summaries,
    load_weather_state_items,
    put_silver_weather_event_checkpoint,
    put_silver_weather_run_summary,
    utc_now_iso,
)
from silver_weather_observations.models import OBS_PK_COLS, OBS_TIEBREAK_COLS
from silver_weather_observations.normalize import normalize_event_records
from silver_weather_observations.parquet_io import overwrite_event_table, put_quarantine_report
from silver_weather_observations.quality import validate_quality



logger = logging.getLogger("silver_weather_observations")


@dataclass
class RunStats:
    attempted_events: int = 0
    processed_events: int = 0
    skipped_unchanged_events: int = 0
    failed_events: int = 0
    dq_failed_events: int = 0
    observation_rows_written: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "attempted_events": self.attempted_events,
            "processed_events": self.processed_events,
            "skipped_unchanged_events": self.skipped_unchanged_events,
            "failed_events": self.failed_events,
            "dq_failed_events": self.dq_failed_events,
            "observation_rows_written": self.observation_rows_written,
        }

    def failure_rate(self) -> float:
        if self.attempted_events <= 0:
            return 0.0
        return self.failed_events / self.attempted_events


def make_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"silver-weather-observations-{ts}"


def probability(value: str) -> float:
    parsed = float(value)
    if not 0.0 <= parsed <= 1.0:
        raise argparse.ArgumentTypeError("value must be between 0.0 and 1.0")
    return parsed

def resolve_run_id(default_factory) -> str:
    env_value = str(os.getenv("RUN_ID", "") or "").strip()
    if env_value:
        return env_value
    return default_factory()

def parse_args():
    p = argparse.ArgumentParser(description="Build Silver weather observations_hourly from Bronze weather payloads.")
    p.add_argument("--event-ids", help="Optional comma-separated event IDs")
    p.add_argument("--bucket", help="Override S3 bucket")
    p.add_argument("--ddb-table", help="Override DDB table")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--force-events", action="store_true", help="Process events even when fingerprint unchanged")
    p.add_argument(
        "--run-mode",
        choices=("pending_only", "full_check"),
        default="pending_only",
        help="pending_only: process only events without success checkpoints; full_check: evaluate all candidates",
    )
    p.add_argument(
        "--include-failed-events",
        action="store_true",
        help="When run-mode=pending_only, include events with checkpoint status=failed.",
    )
    p.add_argument(
        "--include-dq-failed-in-pending",
        action="store_true",
        help="When run-mode=pending_only, include events with checkpoint status=dq_failed.",
    )
    p.add_argument("--progress-every", type=int, default=25)
    p.add_argument(
        "--max-failure-rate",
        type=probability,
        default=0.5,
        help="Exit non-zero only when failed events are at or above this fraction of attempted events.",
    )
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def parse_event_ids(raw: str | None) -> list[int] | None:
    if not raw:
        return None
    return [int(x.strip()) for x in raw.split(",") if x.strip()]


def _rank_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    return str(value)


def _is_newer_row(candidate: dict[str, Any], current: dict[str, Any], tie_cols: tuple[str, ...]) -> bool:
    cand_rank = tuple(_rank_value(candidate.get(col)) for col in tie_cols)
    curr_rank = tuple(_rank_value(current.get(col)) for col in tie_cols)
    return cand_rank > curr_rank


def dedup_rows(rows: list[dict[str, Any]], key_cols: tuple[str, ...], tie_cols: tuple[str, ...]) -> list[dict[str, Any]]:
    best: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = tuple(row.get(col) for col in key_cols)
        current = best.get(key)
        if current is None or _is_newer_row(row, current, tie_cols):
            best[key] = row

    out = list(best.values())
    out.sort(key=lambda row: tuple(_rank_value(row.get(col)) for col in key_cols))
    return out


def _is_pending_event(
    event_id: int,
    checkpoints: dict[int, dict[str, Any]],
    *,
    include_failed: bool,
    include_dq_failed: bool,
) -> bool:
    checkpoint = checkpoints.get(event_id)
    if not checkpoint:
        return True

    status = str(checkpoint.get("status", "")).strip().lower()

    if status == "":
        return True

    if status == "failed":
        return bool(include_failed)

    if status == "dq_failed":
        return bool(include_dq_failed)

    if status == "success":
        fp = str(checkpoint.get("event_source_fingerprint", "")).strip()
        return fp == ""

    return True


def _should_exit_nonzero(*, stats: RunStats, max_failure_rate: float) -> bool:
    return stats.failure_rate() >= max_failure_rate


def main() -> int:
    args = parse_args()
    run_mode = getattr(args, "run_mode", "pending_only")
    include_failed_events = bool(getattr(args, "include_failed_events", False))
    include_dq_failed_in_pending = bool(getattr(args, "include_dq_failed_in_pending", False))
    max_failure_rate = float(getattr(args, "max_failure_rate", 0.5))

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    cfg = load_config()
    bucket = args.bucket or cfg.s3_bucket
    ddb_table = args.ddb_table or cfg.ddb_table
    event_ids = parse_event_ids(args.event_ids)

    run_id = resolve_run_id(make_run_id)

    stats = RunStats()
    progress_every = max(int(args.progress_every), 1)

    summaries = load_weather_event_summaries(
        table_name=ddb_table,
        aws_region=cfg.aws_region,
        event_ids=event_ids,
    )

    candidate_event_ids = sorted({int(item.get("event_id")) for item in summaries if item.get("event_id") is not None})

    selected_event_ids = candidate_event_ids
    if event_ids is None and run_mode == "pending_only":
        checkpoints = load_silver_weather_event_checkpoints(
            table_name=ddb_table,
            aws_region=cfg.aws_region,
        )
        selected_event_ids = [
            event_id
            for event_id in candidate_event_ids
            if _is_pending_event(
                event_id,
                checkpoints,
                include_failed=include_failed_events,
                include_dq_failed=include_dq_failed_in_pending,
            )
        ]

    logger.info(
        "silver_weather_run_plan",
        extra={
            "run_id": run_id,
            "run_mode": run_mode,
            "include_failed_events": include_failed_events,
            "include_dq_failed_in_pending": include_dq_failed_in_pending,
            "candidate_event_count": len(candidate_event_ids),
            "selected_event_count": len(selected_event_ids),
            "dry_run": bool(args.dry_run),
            "force_events": bool(args.force_events),
            "max_failure_rate": max_failure_rate,
        },
    )
    print(
        {
            "silver_weather_run_plan": {
                "run_id": run_id,
                "run_mode": run_mode,
                "include_failed_events": include_failed_events,
                "include_dq_failed_in_pending": include_dq_failed_in_pending,
                "candidate_event_count": len(candidate_event_ids),
                "selected_event_count": len(selected_event_ids),
                "dry_run": bool(args.dry_run),
                "force_events": bool(args.force_events),
                "max_failure_rate": max_failure_rate,
            }
        }
    )

    for idx, event_id in enumerate(selected_event_ids, start=1):
        stats.attempted_events += 1

        try:
            event_metadata = get_event_metadata(
                table_name=ddb_table,
                event_id=event_id,
                aws_region=cfg.aws_region,
            )
            if not event_metadata:
                raise ValueError("missing METADATA item")

            state_items = load_weather_state_items(
                table_name=ddb_table,
                event_id=event_id,
                aws_region=cfg.aws_region,
            )
            round_sources = build_weather_round_sources(
                bucket=bucket,
                event_id=event_id,
                state_items=state_items,
            )
            if not round_sources:
                raise ValueError("no Bronze weather round sources resolved for event")

            event_fingerprint = compute_event_source_fingerprint(round_sources)
            checkpoint = get_silver_weather_event_checkpoint(
                table_name=ddb_table,
                event_id=event_id,
                aws_region=cfg.aws_region,
            )

            if (
                not args.force_events
                and checkpoint
                and str(checkpoint.get("status", "")).strip().lower() == "success"
                and checkpoint.get("event_source_fingerprint") == event_fingerprint
            ):
                stats.skipped_unchanged_events += 1
                logger.info("silver_weather_event_skipped_unchanged", extra={"event_id": event_id, "run_id": run_id})
                continue

            processed_at = utc_now_iso()
            rows = normalize_event_records(
                event_metadata=event_metadata,
                round_sources=round_sources,
                event_source_fingerprint=event_fingerprint,
                run_id=run_id,
                silver_processed_at_utc=processed_at,
            )
            rows = dedup_rows(rows, OBS_PK_COLS, OBS_TIEBREAK_COLS)

            if not rows:
                raise ValueError("normalized observations_hourly is empty")

            dq_errors = validate_quality(rows)
            if dq_errors:
                stats.failed_events += 1
                stats.dq_failed_events += 1

                quarantine_key = ""
                if not args.dry_run:
                    event_year = int(rows[0].get("event_year", 0))
                    quarantine_key = put_quarantine_report(
                        bucket=bucket,
                        event_year=event_year,
                        event_id=event_id,
                        run_id=run_id,
                        errors=dq_errors,
                    )
                    put_silver_weather_event_checkpoint(
                        table_name=ddb_table,
                        event_id=event_id,
                        run_id=run_id,
                        status="dq_failed",
                        event_source_fingerprint=event_fingerprint,
                        aws_region=cfg.aws_region,
                        extra_attributes={
                            "event_year": event_year,
                            "error_count": len(dq_errors),
                            "errors": dq_errors[:20],
                            "quarantine_key": quarantine_key,
                        },
                    )

                logger.error(
                    "silver_weather_event_dq_failed",
                    extra={
                        "event_id": event_id,
                        "run_id": run_id,
                        "error_count": len(dq_errors),
                        "quarantine_key": quarantine_key,
                    },
                )
                continue

            s3_keys: dict[str, str] = {}
            if not args.dry_run:
                event_year = int(rows[0].get("event_year", 0))
                s3_keys = overwrite_event_table(
                    bucket=bucket,
                    event_year=event_year,
                    event_id=event_id,
                    run_id=run_id,
                    observation_rows=rows,
                )
                put_silver_weather_event_checkpoint(
                    table_name=ddb_table,
                    event_id=event_id,
                    run_id=run_id,
                    status="success",
                    event_source_fingerprint=event_fingerprint,
                    aws_region=cfg.aws_region,
                    extra_attributes={
                        "event_year": event_year,
                        "observation_rows": len(rows),
                        "observations_s3_key": s3_keys.get("observations_key", ""),
                    },
                )

            stats.processed_events += 1
            stats.observation_rows_written += len(rows)

            logger.info(
                "silver_weather_event_processed",
                extra={
                    "event_id": event_id,
                    "run_id": run_id,
                    "observation_rows": len(rows),
                    "observations_key": s3_keys.get("observations_key", ""),
                },
            )

        except Exception as exc:
            stats.failed_events += 1
            logger.exception("silver_weather_event_failed", extra={"event_id": event_id, "run_id": run_id, "error": str(exc)})
            try:
                if not args.dry_run:
                    put_silver_weather_event_checkpoint(
                        table_name=ddb_table,
                        event_id=event_id,
                        run_id=run_id,
                        status="failed",
                        event_source_fingerprint="",
                        aws_region=cfg.aws_region,
                        extra_attributes={"error_message": str(exc)},
                    )
            except Exception:
                logger.exception("silver_weather_checkpoint_write_failed", extra={"event_id": event_id, "run_id": run_id})

        if idx % progress_every == 0 or idx == len(selected_event_ids):
            progress = {
                "run_id": run_id,
                "processed_events": idx,
                "total_events": len(selected_event_ids),
                **stats.to_dict(),
                "failure_rate": round(stats.failure_rate(), 4),
                "max_failure_rate": max_failure_rate,
            }
            logger.info("silver_weather_progress", extra=progress)
            print({"silver_weather_progress": progress})

    exit_nonzero = _should_exit_nonzero(stats=stats, max_failure_rate=max_failure_rate)
    summary = {
        "run_id": run_id,
        **stats.to_dict(),
        "failure_rate": round(stats.failure_rate(), 4),
        "max_failure_rate": max_failure_rate,
        "exit_nonzero": exit_nonzero,
    }
    logger.info("silver_weather_summary", extra=summary)
    print({"silver_weather_summary": summary})

    if not args.dry_run:
        put_silver_weather_run_summary(
            table_name=ddb_table,
            run_id=run_id,
            stats=stats.to_dict(),
            aws_region=cfg.aws_region,
        )

    return 2 if exit_nonzero else 0


if __name__ == "__main__":
    raise SystemExit(main())
