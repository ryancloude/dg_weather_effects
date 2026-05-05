from __future__ import annotations

import argparse
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
import os

from report_round_weather_impacts.athena_io import delete_s3_prefix, execute_athena_query
from report_round_weather_impacts.config import load_config
from report_round_weather_impacts.dynamo_io import put_report_run_summary, put_report_table_checkpoint
from report_round_weather_impacts.models import (
    ATHENA_BASE_PREFIX,
    PUBLISHED_BASE_PREFIX,
    REPORT_POLICY_VERSION,
    REPORT_TABLES,
)
from report_round_weather_impacts.queries import (
    build_drop_table_sql,
    build_report_ctas_sql,
    build_reporting_base_ctas_sql,
)

logger = logging.getLogger("report_round_weather_impacts")

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass
class RunStats:
    refreshed_tables: int = 0
    failed_tables: int = 0
    athena_queries_executed: int = 0
    total_scanned_bytes: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "refreshed_tables": self.refreshed_tables,
            "failed_tables": self.failed_tables,
            "athena_queries_executed": self.athena_queries_executed,
            "total_scanned_bytes": self.total_scanned_bytes,
        }


def make_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"report-round-weather-impacts-{ts}"

def resolve_run_id(default_factory) -> str:
    env_value = str(os.getenv("RUN_ID", "") or "").strip()
    if env_value:
        return env_value
    return default_factory()

def parse_args():
    p = argparse.ArgumentParser(description="Build dashboard reporting tables with Athena from scored round outputs.")
    p.add_argument("--tables", help="Optional comma-separated report tables to rebuild")
    p.add_argument("--bucket", help="Override S3 bucket")
    p.add_argument("--ddb-table", help="Override DynamoDB table")
    p.add_argument("--athena-database", help="Override Athena database")
    p.add_argument("--athena-workgroup", help="Override Athena workgroup")
    p.add_argument("--athena-results-s3-uri", help="Override Athena query results S3 URI")
    p.add_argument("--source-table", help="Override Athena source scored table name")
    p.add_argument("--base-table-name", help="Override Athena reporting base table name")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def parse_report_tables(raw: str | None) -> list[str]:
    if not raw:
        return list(REPORT_TABLES)

    selected = [value.strip() for value in raw.split(",") if value.strip()]
    invalid = [value for value in selected if value not in REPORT_TABLES]
    if invalid:
        raise ValueError(f"unsupported report tables: {invalid}")
    return selected


def _validate_identifier(value: str, label: str) -> str:
    if not _IDENTIFIER_RE.match(value):
        raise ValueError(f"invalid {label}: {value}")
    return value


def _s3_uri(bucket: str, prefix: str) -> str:
    clean = prefix.strip("/")
    return f"s3://{bucket}/{clean}/"


def _log_phase_timing(*, run_id: str, phase: str, started_at: float, extra: dict | None = None) -> None:
    elapsed_s = round(time.perf_counter() - started_at, 3)
    payload = {"run_id": run_id, "phase": phase, "elapsed_s": elapsed_s}
    if extra:
        payload.update(extra)
    logger.info("report_round_weather_impacts_phase_complete", extra=payload)
    print({"report_round_weather_impacts_phase_complete": payload})


def _execute_statement(
    *,
    run_id: str,
    phase: str,
    sql: str,
    database: str,
    workgroup: str,
    output_location: str,
    aws_region: str | None,
    stats: RunStats,
) -> dict:
    started = time.perf_counter()
    result = execute_athena_query(
        sql=sql,
        database=database,
        workgroup=workgroup,
        output_location=output_location,
        aws_region=aws_region,
    )
    stats.athena_queries_executed += 1
    stats.total_scanned_bytes += int(result.get("scanned_bytes", 0) or 0)
    _log_phase_timing(
        run_id=run_id,
        phase=phase,
        started_at=started,
        extra={
            "query_execution_id": result["query_execution_id"],
            "scanned_bytes": result["scanned_bytes"],
            "engine_execution_time_ms": result["engine_execution_time_ms"],
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
    athena_database = _validate_identifier(args.athena_database or cfg.athena_database, "Athena database")
    athena_workgroup = args.athena_workgroup or cfg.athena_workgroup
    athena_results_s3_uri = args.athena_results_s3_uri or cfg.athena_results_s3_uri
    source_table = _validate_identifier(args.source_table or cfg.athena_source_scored_table, "source table")
    base_table_name = _validate_identifier(args.base_table_name or cfg.athena_reporting_base_table, "base table name")
    selected_tables = parse_report_tables(args.tables)

    run_id = resolve_run_id(make_run_id)

    stats = RunStats()

    try:
        base_external_location = _s3_uri(bucket, f"{ATHENA_BASE_PREFIX}{base_table_name}")
        report_external_locations = {
            table_name: _s3_uri(bucket, f"{PUBLISHED_BASE_PREFIX}{table_name}")
            for table_name in selected_tables
        }

        if args.dry_run:
            print(
                {
                    "report_round_weather_impacts_plan": {
                        "run_id": run_id,
                        "athena_database": athena_database,
                        "athena_workgroup": athena_workgroup,
                        "source_table": source_table,
                        "base_table_name": base_table_name,
                        "selected_tables": selected_tables,
                        "base_external_location": base_external_location,
                        "report_external_locations": report_external_locations,
                    }
                }
            )
            print(build_drop_table_sql(database=athena_database, table_name=base_table_name))
            print(
                build_reporting_base_ctas_sql(
                    database=athena_database,
                    source_table=source_table,
                    base_table_name=base_table_name,
                    external_location=base_external_location,
                )
            )
            for table_name in selected_tables:
                print(build_drop_table_sql(database=athena_database, table_name=table_name))
                print(
                    build_report_ctas_sql(
                        database=athena_database,
                        base_table_name=base_table_name,
                        report_table_name=table_name,
                        external_location=report_external_locations[table_name],
                    )
                )
            return 0

        t_base_cleanup = time.perf_counter()
        deleted_base = delete_s3_prefix(s3_uri=base_external_location, aws_region=cfg.aws_region)
        _log_phase_timing(
            run_id=run_id,
            phase="delete_reporting_base_prefix",
            started_at=t_base_cleanup,
            extra={"deleted_objects": deleted_base, "s3_uri": base_external_location},
        )

        _execute_statement(
            run_id=run_id,
            phase="drop_reporting_base_table",
            sql=build_drop_table_sql(database=athena_database, table_name=base_table_name),
            database=athena_database,
            workgroup=athena_workgroup,
            output_location=athena_results_s3_uri,
            aws_region=cfg.aws_region,
            stats=stats,
        )

        _execute_statement(
            run_id=run_id,
            phase="create_reporting_base_table",
            sql=build_reporting_base_ctas_sql(
                database=athena_database,
                source_table=source_table,
                base_table_name=base_table_name,
                external_location=base_external_location,
            ),
            database=athena_database,
            workgroup=athena_workgroup,
            output_location=athena_results_s3_uri,
            aws_region=cfg.aws_region,
            stats=stats,
        )

        for table_name in selected_tables:
            try:
                target_s3_uri = report_external_locations[table_name]

                t_delete = time.perf_counter()
                deleted = delete_s3_prefix(s3_uri=target_s3_uri, aws_region=cfg.aws_region)
                _log_phase_timing(
                    run_id=run_id,
                    phase="delete_published_prefix",
                    started_at=t_delete,
                    extra={"report_table": table_name, "deleted_objects": deleted, "s3_uri": target_s3_uri},
                )

                _execute_statement(
                    run_id=run_id,
                    phase=f"drop_{table_name}",
                    sql=build_drop_table_sql(database=athena_database, table_name=table_name),
                    database=athena_database,
                    workgroup=athena_workgroup,
                    output_location=athena_results_s3_uri,
                    aws_region=cfg.aws_region,
                    stats=stats,
                )

                result = _execute_statement(
                    run_id=run_id,
                    phase=f"create_{table_name}",
                    sql=build_report_ctas_sql(
                        database=athena_database,
                        base_table_name=base_table_name,
                        report_table_name=table_name,
                        external_location=target_s3_uri,
                    ),
                    database=athena_database,
                    workgroup=athena_workgroup,
                    output_location=athena_results_s3_uri,
                    aws_region=cfg.aws_region,
                    stats=stats,
                )

                put_report_table_checkpoint(
                    table_name=ddb_table,
                    report_table=table_name,
                    report_policy_version=REPORT_POLICY_VERSION,
                    run_id=run_id,
                    status="success",
                    aws_region=cfg.aws_region,
                    extra_attributes={
                        "athena_database": athena_database,
                        "athena_workgroup": athena_workgroup,
                        "source_table": source_table,
                        "base_table_name": base_table_name,
                        "query_execution_id": result["query_execution_id"],
                        "scanned_bytes": result["scanned_bytes"],
                        "engine_execution_time_ms": result["engine_execution_time_ms"],
                        "published_s3_uri": target_s3_uri,
                    },
                )
                stats.refreshed_tables += 1

            except Exception as exc:
                stats.failed_tables += 1
                logger.exception(
                    "report_round_weather_impacts_table_failed",
                    extra={"run_id": run_id, "report_table": table_name, "error": str(exc)},
                )
                put_report_table_checkpoint(
                    table_name=ddb_table,
                    report_table=table_name,
                    report_policy_version=REPORT_POLICY_VERSION,
                    run_id=run_id,
                    status="failed",
                    aws_region=cfg.aws_region,
                    extra_attributes={
                        "athena_database": athena_database,
                        "athena_workgroup": athena_workgroup,
                        "source_table": source_table,
                        "base_table_name": base_table_name,
                        "error_message": str(exc),
                    },
                )

        put_report_run_summary(
            table_name=ddb_table,
            run_id=run_id,
            stats={
                **stats.to_dict(),
                "athena_database": athena_database,
                "athena_workgroup": athena_workgroup,
                "source_table": source_table,
                "base_table_name": base_table_name,
                "report_tables_requested": selected_tables,
            },
            aws_region=cfg.aws_region,
        )

        summary = {"run_id": run_id, **stats.to_dict(), "selected_tables": selected_tables}
        logger.info("report_round_weather_impacts_summary", extra=summary)
        print({"report_round_weather_impacts_summary": summary})
        return 0 if stats.failed_tables == 0 else 2

    except Exception as exc:
        logger.exception("report_round_weather_impacts_failed", extra={"run_id": run_id, "error": str(exc)})
        print({"report_round_weather_impacts_summary": {"run_id": run_id, "error_message": str(exc)}})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
