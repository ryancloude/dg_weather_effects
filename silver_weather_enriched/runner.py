from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
import os

from silver_weather_enriched.config import load_config
from silver_weather_enriched.dynamo_io import (
    EnrichedEventCandidate,
    get_enriched_event_checkpoint,
    load_enriched_event_candidates,
    load_enriched_event_checkpoints,
    put_enriched_event_checkpoint,
    put_enriched_run_summary,
    utc_now_iso,
)
from silver_weather_enriched.join import (
    build_weather_lookup,
    compute_enriched_event_fingerprint,
    enrich_player_hole_rows,
    enrich_player_round_rows,
)
from silver_weather_enriched.parquet_io import overwrite_event_tables, put_quarantine_report
from silver_weather_enriched.quality import validate_enriched_quality
from silver_weather_enriched.silver_io import load_event_input_tables



logger = logging.getLogger("silver_weather_enriched")


@dataclass
class RunStats:
    attempted_events: int = 0
    processed_events: int = 0
    skipped_unchanged_events: int = 0
    failed_events: int = 0
    dq_failed_events: int = 0
    round_rows_written: int = 0
    hole_rows_written: int = 0
    missing_weather_round_rows: int = 0
    missing_weather_hole_rows: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "attempted_events": self.attempted_events,
            "processed_events": self.processed_events,
            "skipped_unchanged_events": self.skipped_unchanged_events,
            "failed_events": self.failed_events,
            "dq_failed_events": self.dq_failed_events,
            "round_rows_written": self.round_rows_written,
            "hole_rows_written": self.hole_rows_written,
            "missing_weather_round_rows": self.missing_weather_round_rows,
            "missing_weather_hole_rows": self.missing_weather_hole_rows,
        }

    def failure_rate(self) -> float:
        if self.attempted_events <= 0:
            return 0.0
        return self.failed_events / self.attempted_events


def make_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"silver-weather-enriched-{ts}"


def probability(value: str) -> float:
    parsed = float(value)
    if not 0.0 <= parsed <= 1.0:
        raise argparse.ArgumentTypeError("value must be between 0.0 and 1.0")
    return parsed


def parse_args():
    p = argparse.ArgumentParser(description="Enrich Silver player_rounds/player_holes with Silver weather observations.")
    p.add_argument("--event-ids", help="Optional comma-separated event IDs")
    p.add_argument("--bucket", help="Override S3 bucket")
    p.add_argument("--ddb-table", help="Override DynamoDB table")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--force-events", action="store_true")
    p.add_argument(
        "--run-mode",
        choices=("pending_only", "full_check"),
        default="pending_only",
        help="pending_only: process events without success checkpoint; full_check: evaluate all candidates",
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


def _is_pending_event(
    event_id: int,
    checkpoints: dict[int, dict],
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

def resolve_run_id(default_factory) -> str:
    env_value = str(os.getenv("RUN_ID", "") or "").strip()
    if env_value:
        return env_value
    return default_factory()

def _event_year(candidate: EnrichedEventCandidate, round_rows: list[dict], hole_rows: list[dict]) -> int:
    if candidate.event_year > 0:
        return int(candidate.event_year)

    for rows in (round_rows, hole_rows):
        if rows:
            val = rows[0].get("event_year")
            try:
                parsed = int(val)
                if parsed > 0:
                    return parsed
            except (TypeError, ValueError):
                pass
    return 0


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

    candidates = load_enriched_event_candidates(
        table_name=ddb_table,
        aws_region=cfg.aws_region,
        event_ids=event_ids,
    )

    selected = candidates
    if event_ids is None and run_mode == "pending_only":
        checkpoints = load_enriched_event_checkpoints(table_name=ddb_table, aws_region=cfg.aws_region)
        selected = [
            c
            for c in candidates
            if _is_pending_event(
                c.event_id,
                checkpoints,
                include_failed=include_failed_events,
                include_dq_failed=include_dq_failed_in_pending,
            )
        ]

    logger.info(
        "silver_weather_enriched_run_plan",
        extra={
            "run_id": run_id,
            "run_mode": run_mode,
            "include_failed_events": include_failed_events,
            "include_dq_failed_in_pending": include_dq_failed_in_pending,
            "candidate_event_count": len(candidates),
            "selected_event_count": len(selected),
            "dry_run": bool(args.dry_run),
            "force_events": bool(args.force_events),
            "max_failure_rate": max_failure_rate,
        },
    )
    print(
        {
            "silver_weather_enriched_run_plan": {
                "run_id": run_id,
                "run_mode": run_mode,
                "include_failed_events": include_failed_events,
                "include_dq_failed_in_pending": include_dq_failed_in_pending,
                "candidate_event_count": len(candidates),
                "selected_event_count": len(selected),
                "dry_run": bool(args.dry_run),
                "force_events": bool(args.force_events),
                "max_failure_rate": max_failure_rate,
            }
        }
    )

    for idx, candidate in enumerate(selected, start=1):
        stats.attempted_events += 1
        event_id = candidate.event_id

        try:
            round_rows, hole_rows, weather_rows = load_event_input_tables(
                bucket=bucket,
                round_s3_key=candidate.round_s3_key,
                hole_s3_key=candidate.hole_s3_key,
                weather_s3_key=candidate.weather_s3_key,
            )

            source_fp = compute_enriched_event_fingerprint(
                round_rows=round_rows,
                hole_rows=hole_rows,
                weather_rows=weather_rows,
            )

            checkpoint = get_enriched_event_checkpoint(
                table_name=ddb_table,
                event_id=event_id,
                aws_region=cfg.aws_region,
            )

            if (
                not args.force_events
                and checkpoint
                and str(checkpoint.get("status", "")).strip().lower() == "success"
                and str(checkpoint.get("event_source_fingerprint", "")) == source_fp
            ):
                stats.skipped_unchanged_events += 1
                logger.info("silver_weather_enriched_event_skipped_unchanged", extra={"event_id": event_id, "run_id": run_id})
                continue

            weather_lookup = build_weather_lookup(weather_rows)
            round_out = enrich_player_round_rows(round_rows, weather_lookup)
            hole_out = enrich_player_hole_rows(hole_rows, weather_lookup)

            dq_errors = validate_enriched_quality(
                round_input_rows=round_rows,
                hole_input_rows=hole_rows,
                round_output_rows=round_out,
                hole_output_rows=hole_out,
            )

            if dq_errors:
                stats.failed_events += 1
                stats.dq_failed_events += 1

                event_year = _event_year(candidate, round_rows, hole_rows)
                quarantine_key = ""
                if not args.dry_run:
                    quarantine_key = put_quarantine_report(
                        bucket=bucket,
                        event_year=event_year,
                        event_id=event_id,
                        run_id=run_id,
                        errors=dq_errors,
                    )
                    put_enriched_event_checkpoint(
                        table_name=ddb_table,
                        event_id=event_id,
                        run_id=run_id,
                        status="dq_failed",
                        event_source_fingerprint=source_fp,
                        aws_region=cfg.aws_region,
                        extra_attributes={
                            "event_year": event_year,
                            "error_count": len(dq_errors),
                            "errors": dq_errors[:20],
                            "quarantine_key": quarantine_key,
                        },
                    )

                logger.error(
                    "silver_weather_enriched_event_dq_failed",
                    extra={
                        "event_id": event_id,
                        "run_id": run_id,
                        "error_count": len(dq_errors),
                    },
                )
                continue

            keys = {}
            if not args.dry_run:
                event_year = _event_year(candidate, round_rows, hole_rows)
                keys = overwrite_event_tables(
                    bucket=bucket,
                    event_year=event_year,
                    event_id=event_id,
                    round_rows=round_out,
                    hole_rows=hole_out,
                )
                put_enriched_event_checkpoint(
                    table_name=ddb_table,
                    event_id=event_id,
                    run_id=run_id,
                    status="success",
                    event_source_fingerprint=source_fp,
                    aws_region=cfg.aws_region,
                    extra_attributes={
                        "event_year": event_year,
                        "round_rows": len(round_out),
                        "hole_rows": len(hole_out),
                        "missing_weather_round_rows": sum(1 for r in round_out if bool(r.get("wx_weather_missing_flag"))),
                        "missing_weather_hole_rows": sum(1 for r in hole_out if bool(r.get("wx_weather_missing_flag"))),
                        "round_s3_key": keys.get("round_key", ""),
                        "hole_s3_key": keys.get("hole_key", ""),
                    },
                )

            stats.processed_events += 1
            stats.round_rows_written += len(round_out)
            stats.hole_rows_written += len(hole_out)
            stats.missing_weather_round_rows += sum(1 for r in round_out if bool(r.get("wx_weather_missing_flag")))
            stats.missing_weather_hole_rows += sum(1 for r in hole_out if bool(r.get("wx_weather_missing_flag")))

            logger.info(
                "silver_weather_enriched_event_processed",
                extra={
                    "event_id": event_id,
                    "run_id": run_id,
                    "round_rows": len(round_out),
                    "hole_rows": len(hole_out),
                    "round_key": keys.get("round_key", ""),
                    "hole_key": keys.get("hole_key", ""),
                },
            )

        except Exception as exc:
            stats.failed_events += 1
            logger.exception("silver_weather_enriched_event_failed", extra={"event_id": event_id, "run_id": run_id, "error": str(exc)})

            try:
                if not args.dry_run:
                    put_enriched_event_checkpoint(
                        table_name=ddb_table,
                        event_id=event_id,
                        run_id=run_id,
                        status="failed",
                        event_source_fingerprint="",
                        aws_region=cfg.aws_region,
                        extra_attributes={"error_message": str(exc)},
                    )
            except Exception:
                logger.exception(
                    "silver_weather_enriched_checkpoint_write_failed",
                    extra={"event_id": event_id, "run_id": run_id},
                )

        if idx % progress_every == 0 or idx == len(selected):
            progress = {
                "run_id": run_id,
                "processed_events": idx,
                "total_events": len(selected),
                **stats.to_dict(),
                "failure_rate": round(stats.failure_rate(), 4),
                "max_failure_rate": max_failure_rate,
            }
            logger.info("silver_weather_enriched_progress", extra=progress)
            print({"silver_weather_enriched_progress": progress})

    exit_nonzero = _should_exit_nonzero(stats=stats, max_failure_rate=max_failure_rate)
    summary = {
        "run_id": run_id,
        **stats.to_dict(),
        "failure_rate": round(stats.failure_rate(), 4),
        "max_failure_rate": max_failure_rate,
        "exit_nonzero": exit_nonzero,
    }
    logger.info("silver_weather_enriched_summary", extra=summary)
    print({"silver_weather_enriched_summary": summary})

    if not args.dry_run:
        put_enriched_run_summary(
            table_name=ddb_table,
            run_id=run_id,
            stats=stats.to_dict(),
            aws_region=cfg.aws_region,
        )

    return 2 if exit_nonzero else 0


if __name__ == "__main__":
    raise SystemExit(main())
