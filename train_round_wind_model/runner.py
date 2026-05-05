from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
import os
from train_round_wind_model.artifact_io import write_training_artifacts
from train_round_wind_model.config import load_config
from train_round_wind_model.dynamo_io import (
    get_training_checkpoint,
    put_training_checkpoint,
    put_training_run_summary,
)
from train_round_wind_model.gold_io import load_model_input_round_dataframe
from train_round_wind_model.models import MODEL_NAME, MODEL_VERSION
from train_round_wind_model.ssm_io import put_string_parameter
from train_round_wind_model.training import (
    compute_dataset_fingerprint,
    compute_training_request_fingerprint,
    train_round_model,
)

logger = logging.getLogger("train_round_wind_model")


@dataclass
class RunStats:
    attempted_trainings: int = 0
    processed_trainings: int = 0
    skipped_unchanged_trainings: int = 0
    failed_trainings: int = 0
    input_rows: int = 0
    source_key_count: int = 0
    train_rows: int = 0
    valid_rows: int = 0
    test_rows: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "attempted_trainings": self.attempted_trainings,
            "processed_trainings": self.processed_trainings,
            "skipped_unchanged_trainings": self.skipped_unchanged_trainings,
            "failed_trainings": self.failed_trainings,
            "input_rows": self.input_rows,
            "source_key_count": self.source_key_count,
            "train_rows": self.train_rows,
            "valid_rows": self.valid_rows,
            "test_rows": self.test_rows,
        }


def make_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"train-round-wind-model-{ts}"


def resolve_run_id(default_factory) -> str:
    env_value = str(os.getenv("RUN_ID", "") or "").strip()
    if env_value:
        return env_value
    return default_factory()

def parse_args():
    p = argparse.ArgumentParser(description="Train the production round-level one-stage CatBoost wind model.")
    p.add_argument("--event-ids", help="Optional comma-separated event IDs to train on a subset of model_inputs_round")
    p.add_argument("--bucket", help="Override S3 bucket")
    p.add_argument("--ddb-table", help="Override DynamoDB table")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--force-train", action="store_true")
    p.add_argument(
        "--set-production-fingerprint",
        action="store_true",
        help="Write the resulting training_request_fingerprint to the configured production SSM parameter.",
    )
    p.add_argument(
        "--production-fingerprint-parameter-name",
        help="Exact SSM parameter name to update when --set-production-fingerprint is used.",
    )
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
    logger.info("train_round_wind_model_phase_complete", extra=payload)
    print({"train_round_wind_model_phase_complete": payload})


def _resolve_production_parameter_name(args: argparse.Namespace) -> str | None:
    raw = getattr(args, "production_fingerprint_parameter_name", None)
    if raw is None:
        return None
    value = str(raw).strip()
    return value or None


def _maybe_update_production_fingerprint(
    *,
    args: argparse.Namespace,
    aws_region: str | None,
    run_id: str,
    training_request_fingerprint: str,
) -> dict[str, str | int] | None:
    if not bool(getattr(args, "set_production_fingerprint", False)):
        return None

    parameter_name = _resolve_production_parameter_name(args)
    if not parameter_name:
        raise ValueError(
            "--production-fingerprint-parameter-name is required when --set-production-fingerprint is used."
        )

    if bool(getattr(args, "dry_run", False)):
        return {
            "parameter_name": parameter_name,
            "version": 0,
            "value": training_request_fingerprint,
        }

    started = time.perf_counter()
    result = put_string_parameter(
        parameter_name=parameter_name,
        value=training_request_fingerprint,
        aws_region=aws_region,
    )
    _log_phase_timing(
        run_id=run_id,
        phase="set_production_training_request_fingerprint",
        started_at=started,
        extra={
            "parameter_name": result["parameter_name"],
            "version": result["version"],
        },
    )
    return result


def main() -> int:
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    cfg = load_config()
    bucket = args.bucket or cfg.s3_bucket
    ddb_table = args.ddb_table or cfg.ddb_table
    event_ids = parse_event_ids(args.event_ids)

    run_id = resolve_run_id(make_run_id)
    stats = RunStats(attempted_trainings=1)

    try:
        t0 = time.perf_counter()
        df, source_objects = load_model_input_round_dataframe(
            bucket=bucket,
            event_ids=event_ids,
        )
        stats.input_rows = int(len(df))
        stats.source_key_count = int(len(source_objects))
        _log_phase_timing(
            run_id=run_id,
            phase="load_model_input_round_dataframe",
            started_at=t0,
            extra={"input_rows": stats.input_rows, "source_key_count": stats.source_key_count},
        )

        t1 = time.perf_counter()
        dataset_fingerprint = compute_dataset_fingerprint(source_objects)
        training_request_fingerprint = compute_training_request_fingerprint(
            dataset_fingerprint=dataset_fingerprint,
            event_ids=event_ids,
        )
        _log_phase_timing(
            run_id=run_id,
            phase="compute_fingerprints",
            started_at=t1,
            extra={"training_request_fingerprint": training_request_fingerprint},
        )

        logger.info(
            "train_round_wind_model_run_plan",
            extra={
                "run_id": run_id,
                "model_name": MODEL_NAME,
                "model_version": MODEL_VERSION,
                "input_rows": len(df),
                "source_key_count": len(source_objects),
                "training_request_fingerprint": training_request_fingerprint,
                "dry_run": bool(args.dry_run),
                "force_train": bool(args.force_train),
                "set_production_fingerprint": bool(args.set_production_fingerprint),
            },
        )
        print(
            {
                "train_round_wind_model_run_plan": {
                    "run_id": run_id,
                    "model_name": MODEL_NAME,
                    "model_version": MODEL_VERSION,
                    "input_rows": len(df),
                    "source_key_count": len(source_objects),
                    "training_request_fingerprint": training_request_fingerprint,
                    "dry_run": bool(args.dry_run),
                    "force_train": bool(args.force_train),
                    "set_production_fingerprint": bool(args.set_production_fingerprint),
                }
            }
        )

        t2 = time.perf_counter()
        checkpoint = get_training_checkpoint(
            table_name=ddb_table,
            training_request_fingerprint=training_request_fingerprint,
            aws_region=cfg.aws_region,
        )
        _log_phase_timing(run_id=run_id, phase="get_training_checkpoint", started_at=t2)

        if (
            not args.force_train
            and checkpoint
            and str(checkpoint.get("status", "")).strip().lower() == "success"
        ):
            stats.skipped_unchanged_trainings += 1

            production_result = _maybe_update_production_fingerprint(
                args=args,
                aws_region=cfg.aws_region,
                run_id=run_id,
                training_request_fingerprint=training_request_fingerprint,
            )

            summary = {
                "run_id": run_id,
                "model_name": MODEL_NAME,
                "model_version": MODEL_VERSION,
                "training_request_fingerprint": training_request_fingerprint,
                "production_fingerprint_updated": bool(production_result),
                "production_fingerprint_parameter_name": (
                    production_result["parameter_name"] if production_result else ""
                ),
                **stats.to_dict(),
            }

            logger.info(
                "train_round_wind_model_skipped_unchanged",
                extra={
                    "run_id": run_id,
                    "training_request_fingerprint": training_request_fingerprint,
                },
            )
            print({"train_round_wind_model_summary": summary})

            if not args.dry_run:
                put_training_run_summary(
                    table_name=ddb_table,
                    run_id=run_id,
                    stats=summary,
                    aws_region=cfg.aws_region,
                )
            return 0

        t3 = time.perf_counter()
        result = train_round_model(
            df=df,
            dataset_fingerprint=dataset_fingerprint,
            training_request_fingerprint=training_request_fingerprint,
            source_key_count=len(source_objects),
            event_ids=event_ids,
        )
        _log_phase_timing(
            run_id=run_id,
            phase="train_round_model",
            started_at=t3,
            extra={
                "train_rows": result.metrics["train_rows"],
                "valid_rows": result.metrics["valid_rows"],
                "test_rows": result.metrics["test_rows"],
                "rmse": result.metrics["rmse"],
                "mae": result.metrics["mae"],
                "r2": result.metrics["r2"],
            },
        )

        stats.processed_trainings += 1
        stats.train_rows = int(result.metrics["train_rows"])
        stats.valid_rows = int(result.metrics["valid_rows"])
        stats.test_rows = int(result.metrics["test_rows"])

        artifact_keys: dict[str, str] = {}
        if not args.dry_run:
            t4 = time.perf_counter()
            artifact_keys = write_training_artifacts(
                bucket=bucket,
                training_request_fingerprint=training_request_fingerprint,
                model=result.model,
                training_manifest=result.training_manifest,
                metrics=result.metrics,
                feature_importance_rows=result.feature_importance_rows,
            )
            _log_phase_timing(
                run_id=run_id,
                phase="write_training_artifacts",
                started_at=t4,
                extra={"artifact_prefix": artifact_keys.get("artifact_prefix", "")},
            )

            t5 = time.perf_counter()
            put_training_checkpoint(
                table_name=ddb_table,
                training_request_fingerprint=training_request_fingerprint,
                run_id=run_id,
                status="success",
                aws_region=cfg.aws_region,
                extra_attributes={
                    "model_name": MODEL_NAME,
                    "model_version": MODEL_VERSION,
                    "dataset_fingerprint": dataset_fingerprint,
                    "artifact_prefix": artifact_keys.get("artifact_prefix", ""),
                    "model_key": artifact_keys.get("model_key", ""),
                    "metrics_key": artifact_keys.get("metrics_key", ""),
                    "manifest_key": artifact_keys.get("manifest_key", ""),
                    "mae": result.metrics["mae"],
                    "rmse": result.metrics["rmse"],
                    "r2": result.metrics["r2"],
                    "best_iteration": result.metrics["best_iteration"],
                },
            )
            _log_phase_timing(run_id=run_id, phase="put_training_checkpoint", started_at=t5)

        production_result = _maybe_update_production_fingerprint(
            args=args,
            aws_region=cfg.aws_region,
            run_id=run_id,
            training_request_fingerprint=training_request_fingerprint,
        )

        summary = {
            "run_id": run_id,
            "model_name": MODEL_NAME,
            "model_version": MODEL_VERSION,
            "training_request_fingerprint": training_request_fingerprint,
            "artifact_prefix": artifact_keys.get("artifact_prefix", ""),
            "production_fingerprint_updated": bool(production_result),
            "production_fingerprint_parameter_name": (
                production_result["parameter_name"] if production_result else ""
            ),
            **stats.to_dict(),
            **result.metrics,
        }
        logger.info("train_round_wind_model_summary", extra=summary)
        print({"train_round_wind_model_summary": summary})

        if not args.dry_run:
            t6 = time.perf_counter()
            put_training_run_summary(
                table_name=ddb_table,
                run_id=run_id,
                stats=summary,
                aws_region=cfg.aws_region,
            )
            _log_phase_timing(run_id=run_id, phase="put_training_run_summary", started_at=t6)

        return 0

    except Exception as exc:
        stats.failed_trainings += 1
        logger.exception("train_round_wind_model_failed", extra={"run_id": run_id, "error": str(exc)})

        try:
            if not args.dry_run:
                put_training_run_summary(
                    table_name=ddb_table,
                    run_id=run_id,
                    stats={"error_message": str(exc), **stats.to_dict()},
                    aws_region=cfg.aws_region,
                )
        except Exception:
            logger.exception("train_round_wind_model_checkpoint_write_failed", extra={"run_id": run_id})

        print({"train_round_wind_model_summary": {"run_id": run_id, **stats.to_dict(), "error_message": str(exc)}})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
