from __future__ import annotations

import argparse
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from score_round_wind_model.athena_io import (
    build_scored_rounds_storage_location_template,
    build_scored_rounds_table_location,
    ensure_scored_rounds_table,
)
from score_round_wind_model.config import load_config
from score_round_wind_model.dynamo_io import (
    get_score_checkpoints,
    put_score_checkpoint,
    put_score_run_summary,
)
from score_round_wind_model.gold_io import (
    list_model_input_round_objects,
    load_event_dataframe,
)
from score_round_wind_model.model_io import load_model_bundle
from score_round_wind_model.parquet_io import overwrite_event_scored_rounds
from score_round_wind_model.scoring import score_round_rows

logger = logging.getLogger("score_round_wind_model")

_EVENT_YEAR_RE = re.compile(r"event_year=(\d+)")
_EVENT_ID_RE = re.compile(r"tourn_id=(\d+)")


@dataclass
class RunStats:
    candidate_events: int = 0
    selected_events: int = 0
    skipped_success_events: int = 0
    skipped_failed_events: int = 0
    attempted_events: int = 0
    processed_events: int = 0
    failed_events: int = 0
    rows_scored: int = 0
    athena_queries_executed: int = 0
    athena_scanned_bytes: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "candidate_events": self.candidate_events,
            "selected_events": self.selected_events,
            "skipped_success_events": self.skipped_success_events,
            "skipped_failed_events": self.skipped_failed_events,
            "attempted_events": self.attempted_events,
            "processed_events": self.processed_events,
            "failed_events": self.failed_events,
            "rows_scored": self.rows_scored,
            "athena_queries_executed": self.athena_queries_executed,
            "athena_scanned_bytes": self.athena_scanned_bytes,
        }


def make_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"score-round-wind-model-{ts}"


def parse_args():
    p = argparse.ArgumentParser(
        description="Score round-level wind/weather impacts from model_inputs_round using a specific trained model artifact."
    )
    p.add_argument(
        "--training-request-fingerprint",
        required=True,
        help="Required training artifact fingerprint from train_round_wind_model.",
    )
    p.add_argument("--event-ids", help="Optional comma-separated event IDs")
    p.add_argument("--bucket", help="Override S3 bucket")
    p.add_argument("--ddb-table", help="Override DynamoDB table")
    p.add_argument("--athena-database", help="Override Athena database")
    p.add_argument("--athena-workgroup", help="Override Athena workgroup")
    p.add_argument("--athena-results-s3-uri", help="Override Athena query results S3 URI")
    p.add_argument("--source-table", help="Override Athena scored-rounds table name")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--force-events", action="store_true")
    p.add_argument(
        "--include-failures",
        action="store_true",
        help="Include events whose most recent checkpoint status is failed.",
    )
    p.add_argument("--progress-every", type=int, default=25)
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def parse_event_ids(raw: str | None) -> list[int] | None:
    if not raw:
        return None
    return [int(x.strip()) for x in raw.split(",") if x.strip()]


def _log_phase_timing(*, run_id: str, phase: str, started_at: float, extra: dict | None = None) -> None:
    elapsed_s = round(time.perf_counter() - started_at, 3)
    payload = {"run_id": run_id, "phase": phase, "elapsed_s": elapsed_s}
    if extra:
        payload.update(extra)
    logger.info("score_round_wind_model_phase_complete", extra=payload)
    print({"score_round_wind_model_phase_complete": payload})


def _event_year_from_key(key: str) -> int:
    match = _EVENT_YEAR_RE.search(key)
    if not match:
        raise ValueError(f"Could not parse event_year from key: {key}")
    return int(match.group(1))


def _event_id_from_key(key: str) -> int:
    match = _EVENT_ID_RE.search(key)
    if not match:
        raise ValueError(f"Could not parse tourn_id from key: {key}")
    return int(match.group(1))


def _checkpoint_status(checkpoint: dict[str, Any] | None) -> str:
    if not checkpoint:
        return ""
    return str(checkpoint.get("status", "")).strip().lower()


def _should_select_event(
    *,
    checkpoint: dict[str, Any] | None,
    force_events: bool,
    include_failures: bool,
) -> bool:
    if force_events:
        return True

    status = _checkpoint_status(checkpoint)
    if status == "success":
        return False
    if status == "failed":
        return bool(include_failures)
    return True


def _prefilter_event_objects(
    *,
    event_objects: list[dict[str, Any]],
    table_name: str,
    training_request_fingerprint: str,
    aws_region: str | None,
    force_events: bool,
    include_failures: bool,
) -> tuple[list[dict[str, Any]], int, int]:
    if force_events or not event_objects:
        selected = [
            {
                "event_id": _event_id_from_key(str(obj["key"])),
                "event_year": _event_year_from_key(str(obj["key"])),
                "event_object": obj,
            }
            for obj in event_objects
        ]
        return selected, 0, 0

    event_ids = [_event_id_from_key(str(obj["key"])) for obj in event_objects]
    checkpoints = get_score_checkpoints(
        table_name=table_name,
        event_ids=event_ids,
        training_request_fingerprint=training_request_fingerprint,
        aws_region=aws_region,
    )

    selected: list[dict[str, Any]] = []
    skipped_success = 0
    skipped_failed = 0

    for obj in event_objects:
        key = str(obj["key"])
        event_id = _event_id_from_key(key)
        event_year = _event_year_from_key(key)
        checkpoint = checkpoints.get(event_id)
        status = _checkpoint_status(checkpoint)

        if not _should_select_event(
            checkpoint=checkpoint,
            force_events=False,
            include_failures=include_failures,
        ):
            if status == "success":
                skipped_success += 1
            elif status == "failed":
                skipped_failed += 1
            continue

        selected.append(
            {
                "event_id": event_id,
                "event_year": event_year,
                "event_object": obj,
            }
        )

    return selected, skipped_success, skipped_failed


def main() -> int:
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    cfg = load_config()
    bucket = args.bucket or cfg.s3_bucket
    ddb_table = args.ddb_table or cfg.ddb_table
    athena_database = args.athena_database or cfg.athena_database
    athena_workgroup = args.athena_workgroup or cfg.athena_workgroup
    athena_results_s3_uri = args.athena_results_s3_uri or cfg.athena_results_s3_uri
    source_table = args.source_table or cfg.athena_source_scored_table
    event_ids = parse_event_ids(args.event_ids)

    run_id = make_run_id()
    stats = RunStats()
    progress_every = max(int(args.progress_every), 1)
    training_request_fingerprint = args.training_request_fingerprint

    try:
        t0 = time.perf_counter()
        model_bundle = load_model_bundle(
            bucket=bucket,
            training_request_fingerprint=training_request_fingerprint,
        )
        _log_phase_timing(
            run_id=run_id,
            phase="load_model_bundle",
            started_at=t0,
            extra={
                "training_request_fingerprint": training_request_fingerprint,
                "artifact_prefix": model_bundle["artifact_prefix"],
            },
        )

        t1 = time.perf_counter()
        event_objects = list_model_input_round_objects(
            bucket=bucket,
            event_ids=event_ids,
        )
        stats.candidate_events = len(event_objects)
        _log_phase_timing(
            run_id=run_id,
            phase="list_model_input_round_objects",
            started_at=t1,
            extra={"candidate_event_count": len(event_objects)},
        )

        t2 = time.perf_counter()
        selected_events, skipped_success, skipped_failed = _prefilter_event_objects(
            event_objects=event_objects,
            table_name=ddb_table,
            training_request_fingerprint=training_request_fingerprint,
            aws_region=cfg.aws_region,
            force_events=bool(args.force_events),
            include_failures=bool(args.include_failures),
        )
        stats.selected_events = len(selected_events)
        stats.skipped_success_events = skipped_success
        stats.skipped_failed_events = skipped_failed
        _log_phase_timing(
            run_id=run_id,
            phase="prefilter_events",
            started_at=t2,
            extra={
                "candidate_event_count": stats.candidate_events,
                "selected_event_count": stats.selected_events,
                "skipped_success_events": stats.skipped_success_events,
                "skipped_failed_events": stats.skipped_failed_events,
                "force_events": bool(args.force_events),
                "include_failures": bool(args.include_failures),
            },
        )

        logger.info(
            "score_round_wind_model_run_plan",
            extra={
                "run_id": run_id,
                "training_request_fingerprint": training_request_fingerprint,
                "candidate_event_count": stats.candidate_events,
                "selected_event_count": stats.selected_events,
                "skipped_success_events": stats.skipped_success_events,
                "skipped_failed_events": stats.skipped_failed_events,
                "dry_run": bool(args.dry_run),
                "force_events": bool(args.force_events),
                "include_failures": bool(args.include_failures),
            },
        )
        print(
            {
                "score_round_wind_model_run_plan": {
                    "run_id": run_id,
                    "training_request_fingerprint": training_request_fingerprint,
                    "candidate_event_count": stats.candidate_events,
                    "selected_event_count": stats.selected_events,
                    "skipped_success_events": stats.skipped_success_events,
                    "skipped_failed_events": stats.skipped_failed_events,
                    "dry_run": bool(args.dry_run),
                    "force_events": bool(args.force_events),
                    "include_failures": bool(args.include_failures),
                }
            }
        )

        if not args.dry_run and selected_events:
            t3 = time.perf_counter()
            ensure_result = ensure_scored_rounds_table(
                database=athena_database,
                table_name=source_table,
                workgroup=athena_workgroup,
                output_location=athena_results_s3_uri,
                table_location=build_scored_rounds_table_location(bucket=bucket),
                storage_location_template=build_scored_rounds_storage_location_template(bucket=bucket),
                aws_region=cfg.aws_region,
            )
            stats.athena_queries_executed += 1
            stats.athena_scanned_bytes += int(ensure_result.get("scanned_bytes", 0) or 0)
            _log_phase_timing(
                run_id=run_id,
                phase="ensure_scored_rounds_table",
                started_at=t3,
                extra={"table_name": source_table},
            )

        for idx, selected in enumerate(selected_events, start=1):
            stats.attempted_events += 1

            event_id = int(selected["event_id"])
            event_year = int(selected["event_year"])
            event_object = selected["event_object"]
            event_key = str(event_object["key"])

            try:
                t_event_load = time.perf_counter()
                df = load_event_dataframe(bucket=bucket, key=event_key)
                _log_phase_timing(
                    run_id=run_id,
                    phase="load_event_dataframe",
                    started_at=t_event_load,
                    extra={"event_id": event_id, "input_rows": int(len(df))},
                )

                scored_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

                t_score = time.perf_counter()
                scoring_result = score_round_rows(
                    df=df,
                    model=model_bundle["model"],
                    training_manifest=model_bundle["training_manifest"],
                    feature_columns=model_bundle["feature_columns"],
                    categorical_feature_columns=model_bundle["categorical_feature_columns"],
                    training_request_fingerprint=training_request_fingerprint,
                    scoring_run_id=run_id,
                    scored_at_utc=scored_at_utc,
                    scoring_request_fingerprint="",
                    model_artifact_prefix=model_bundle["artifact_prefix"],
                )
                _log_phase_timing(
                    run_id=run_id,
                    phase="score_round_rows",
                    started_at=t_score,
                    extra={"event_id": event_id, "rows_scored": int(len(scoring_result.scored_df))},
                )

                scored_rounds_key = ""

                if not args.dry_run:
                    t_write = time.perf_counter()
                    scored_rounds_key = overwrite_event_scored_rounds(
                        bucket=bucket,
                        event_year=event_year,
                        event_id=event_id,
                        rows=scoring_result.scored_df.to_dict(orient="records"),
                    )
                    _log_phase_timing(
                        run_id=run_id,
                        phase="overwrite_event_scored_rounds",
                        started_at=t_write,
                        extra={"event_id": event_id, "scored_rounds_key": scored_rounds_key},
                    )

                    put_score_checkpoint(
                        table_name=ddb_table,
                        event_id=event_id,
                        training_request_fingerprint=training_request_fingerprint,
                        run_id=run_id,
                        status="success",
                        aws_region=cfg.aws_region,
                        extra_attributes={
                            "event_year": event_year,
                            "rows_scored": int(len(scoring_result.scored_df)),
                            "scored_rounds_key": scored_rounds_key,
                            "model_name": scoring_result.scoring_manifest["model_name"],
                            "model_version": scoring_result.scoring_manifest["model_version"],
                            "model_artifact_prefix": model_bundle["artifact_prefix"],
                        },
                    )

                stats.processed_events += 1
                stats.rows_scored += int(len(scoring_result.scored_df))

                logger.info(
                    "score_round_wind_model_event_processed",
                    extra={
                        "run_id": run_id,
                        "event_id": event_id,
                        "event_year": event_year,
                        "rows_scored": int(len(scoring_result.scored_df)),
                        "scored_rounds_key": scored_rounds_key,
                    },
                )

            except Exception as exc:
                stats.failed_events += 1
                logger.exception(
                    "score_round_wind_model_event_failed",
                    extra={
                        "run_id": run_id,
                        "event_id": event_id,
                        "training_request_fingerprint": training_request_fingerprint,
                        "error": str(exc),
                    },
                )

                try:
                    if not args.dry_run:
                        put_score_checkpoint(
                            table_name=ddb_table,
                            event_id=event_id,
                            training_request_fingerprint=training_request_fingerprint,
                            run_id=run_id,
                            status="failed",
                            aws_region=cfg.aws_region,
                            extra_attributes={
                                "event_year": event_year,
                                "error_message": str(exc),
                            },
                        )
                except Exception:
                    logger.exception(
                        "score_round_wind_model_checkpoint_write_failed",
                        extra={"run_id": run_id, "event_id": event_id},
                    )

            if idx % progress_every == 0 or idx == len(selected_events):
                progress = {
                    "run_id": run_id,
                    "processed_events": idx,
                    "total_events": len(selected_events),
                    **stats.to_dict(),
                }
                logger.info("score_round_wind_model_progress", extra=progress)
                print({"score_round_wind_model_progress": progress})

        summary = {
            "run_id": run_id,
            "training_request_fingerprint": training_request_fingerprint,
            **stats.to_dict(),
        }
        logger.info("score_round_wind_model_summary", extra=summary)
        print({"score_round_wind_model_summary": summary})

        if not args.dry_run:
            put_score_run_summary(
                table_name=ddb_table,
                run_id=run_id,
                stats=summary,
                aws_region=cfg.aws_region,
            )

        return 0 if stats.failed_events == 0 else 2

    except Exception as exc:
        logger.exception(
            "score_round_wind_model_failed",
            extra={
                "run_id": run_id,
                "training_request_fingerprint": training_request_fingerprint,
                "error": str(exc),
            },
        )
        print(
            {
                "score_round_wind_model_summary": {
                    "run_id": run_id,
                    "training_request_fingerprint": training_request_fingerprint,
                    **stats.to_dict(),
                    "error_message": str(exc),
                }
            }
        )

        try:
            if not args.dry_run:
                put_score_run_summary(
                    table_name=ddb_table,
                    run_id=run_id,
                    stats={
                        "training_request_fingerprint": training_request_fingerprint,
                        **stats.to_dict(),
                        "error_message": str(exc),
                    },
                    aws_region=cfg.aws_region,
                )
        except Exception:
            logger.exception("score_round_wind_model_run_summary_write_failed", extra={"run_id": run_id})

        return 2


if __name__ == "__main__":
    raise SystemExit(main())
