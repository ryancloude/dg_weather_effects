from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any
import os


from ingest_weather_observations.config import load_config
from ingest_weather_observations.dynamo_reader import (
    WeatherEventCandidate,
    get_cached_geocode,
    get_event_weather_summaries,
    load_weather_event_candidates,
)
from ingest_weather_observations.dynamo_writer import (
    get_existing_weather_state,
    put_cached_geocode,
    put_weather_run_summary,
    upsert_event_geocode_resolution,
    upsert_event_weather_summary,
    upsert_weather_state,
)
from ingest_weather_observations.geocode import (
    build_geocode_query,
    build_geocode_search_candidates,
    pick_best_geocode_result,
)
from ingest_weather_observations.http_client import (
    HttpConfig,
    build_session,
    get_open_meteo_archive_json,
    get_open_meteo_geocoding_json,
)
from ingest_weather_observations.location import extract_geopoint
from ingest_weather_observations.models import GeoPoint, WeatherObservationTask
from ingest_weather_observations.open_meteo import build_archive_request
from ingest_weather_observations.response_handler import compute_payload_sha256, extract_daylight_hourly_rows
from ingest_weather_observations.s3_writer import put_weather_raw
from ingest_weather_observations.silver_reader import compute_tee_time_source_fingerprint, load_player_round_rows
from ingest_weather_observations.utils import build_request_fingerprint
from ingest_weather_observations.windowing import build_fetch_window, infer_round_date

logger = logging.getLogger("weather_observations_ingest")


@dataclass
class RunStats:
    attempted_events: int = 0
    processed_events: int = 0
    skipped_incremental_events: int = 0
    skipped_failed_events: int = 0
    failed_events: int = 0

    attempted_round_tasks: int = 0
    processed_round_tasks: int = 0
    changed_round_tasks: int = 0
    unchanged_round_tasks: int = 0
    failed_round_tasks: int = 0
    daylight_hours_total: int = 0

    point_from_metadata: int = 0
    point_from_cache: int = 0
    point_from_geocode_api: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "attempted_events": self.attempted_events,
            "processed_events": self.processed_events,
            "skipped_incremental_events": self.skipped_incremental_events,
            "skipped_failed_events": self.skipped_failed_events,
            "failed_events": self.failed_events,
            "attempted_round_tasks": self.attempted_round_tasks,
            "processed_round_tasks": self.processed_round_tasks,
            "changed_round_tasks": self.changed_round_tasks,
            "unchanged_round_tasks": self.unchanged_round_tasks,
            "failed_round_tasks": self.failed_round_tasks,
            "daylight_hours_total": self.daylight_hours_total,
            "point_from_metadata": self.point_from_metadata,
            "point_from_cache": self.point_from_cache,
            "point_from_geocode_api": self.point_from_geocode_api,
        }

    def failure_rate(self) -> float:
        if self.attempted_events <= 0:
            return 0.0
        return self.failed_events / self.attempted_events


def make_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"weather-observations-{ts}"


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
    p = argparse.ArgumentParser(description="Ingest Open-Meteo archive observations to Bronze S3 + DynamoDB state.")
    p.add_argument("--event-ids", help="Optional comma-separated event IDs")
    p.add_argument("--incremental", action="store_true", help="Process only events with updated Silver checkpoints")
    p.add_argument("--historical-backfill", action="store_true", help="Process all candidate events from Silver success checkpoints")
    p.add_argument("--bucket", help="Override S3 bucket")
    p.add_argument("--ddb-table", help="Override DDB table")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--force-events", action="store_true", help="Ignore incremental summary skip and process selected events")
    p.add_argument("--include-failed-events", action="store_true", help="Include events previously marked failed in the weather summary")
    p.add_argument("--round-padding-days", type=int, default=0, help="Date padding around inferred round date")
    p.add_argument("--timeout", type=int, default=30)
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


def _parse_local_date_from_ts(value: Any) -> date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt.date()


def _derive_round_date_overrides(rows: list[dict[str, Any]]) -> dict[int, date]:
    counts: dict[int, dict[date, int]] = {}
    for row in rows:
        try:
            round_number = int(row.get("round_number"))
        except (TypeError, ValueError):
            continue
        if round_number <= 0:
            continue

        d = _parse_local_date_from_ts(row.get("tee_time_join_ts"))
        if d is None:
            continue

        round_counts = counts.setdefault(round_number, {})
        round_counts[d] = round_counts.get(d, 0) + 1

    out: dict[int, date] = {}
    for round_number, date_counts in counts.items():
        ranked = sorted(date_counts.items(), key=lambda x: (-x[1], x[0].isoformat()))
        out[round_number] = ranked[0][0]
    return out


def _derive_round_play_dates(rows: list[dict[str, Any]]) -> dict[int, set[date]]:
    out: dict[int, set[date]] = {}
    for row in rows:
        try:
            round_number = int(row.get("round_number"))
        except (TypeError, ValueError):
            continue
        if round_number <= 0:
            continue
        d = _parse_local_date_from_ts(row.get("tee_time_join_ts"))
        if d is None:
            continue
        out.setdefault(round_number, set()).add(d)
    return out


def _max_round_from_metadata(metadata: dict[str, Any]) -> int:
    division_rounds = metadata.get("division_rounds")
    if not isinstance(division_rounds, dict):
        return 0

    max_round = 0
    for value in division_rounds.values():
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        if parsed > max_round:
            max_round = parsed
    return max_round


def _build_round_tasks(
    *,
    candidate: WeatherEventCandidate,
    rows: list[dict[str, Any]],
    point: GeoPoint,
    round_padding_days: int,
) -> tuple[list[WeatherObservationTask], dict[int, set[date]]]:
    metadata = candidate.event_metadata
    round_date_overrides = _derive_round_date_overrides(rows)
    round_play_dates = _derive_round_play_dates(rows)

    max_round = _max_round_from_metadata(metadata)
    if max_round <= 0:
        max_round = max(round_date_overrides.keys(), default=0)

    if max_round <= 0:
        raise ValueError("unable to resolve max round count from metadata or Silver rows")

    tasks: list[WeatherObservationTask] = []
    for round_number in range(1, max_round + 1):
        round_date = round_date_overrides.get(round_number)
        if round_date is None:
            round_date = infer_round_date(
                start_date_str=str(metadata.get("start_date", "")),
                end_date_str=str(metadata.get("end_date", "")),
                round_number=round_number,
                max_rounds=max_round,
            )

        window = build_fetch_window(
            round_number=round_number,
            round_date=round_date,
            padding_days=round_padding_days,
        )

        tasks.append(
            WeatherObservationTask(
                event_id=candidate.event_id,
                event_name=str(metadata.get("name", "")),
                point=point,
                window=window,
                city=str(metadata.get("city", "")),
                state=str(metadata.get("state", "")),
                country=str(metadata.get("country", "")),
            )
        )

    return tasks, round_play_dates


def _is_incremental_skip(
    *,
    candidate: WeatherEventCandidate,
    summary_item: dict[str, Any] | None,
) -> bool:
    if not summary_item:
        return False

    last_silver = str(summary_item.get("last_silver_checkpoint_updated_at", "")).strip()
    candidate_silver = candidate.silver_checkpoint_updated_at
    return bool(last_silver and candidate_silver and last_silver == candidate_silver)


def _is_failed_summary(summary_item: dict[str, Any] | None) -> bool:
    if not summary_item:
        return False
    return str(summary_item.get("status", "")).strip().lower() == "failed"


def _select_candidates_for_run(
    *,
    candidates: list[WeatherEventCandidate],
    summaries_by_event_id: dict[int, dict[str, Any]],
    mode_incremental: bool,
    force_events: bool,
    include_failed_events: bool,
) -> tuple[list[WeatherEventCandidate], int, int]:
    if not mode_incremental or force_events:
        return candidates, 0, 0

    selected: list[WeatherEventCandidate] = []
    skipped_incremental = 0
    skipped_failed = 0

    for candidate in candidates:
        summary_item = summaries_by_event_id.get(candidate.event_id)

        if not include_failed_events and _is_failed_summary(summary_item):
            skipped_failed += 1
            continue

        if _is_incremental_skip(candidate=candidate, summary_item=summary_item):
            skipped_incremental += 1
            continue

        selected.append(candidate)

    return selected, skipped_incremental, skipped_failed


def _coerce_cached_point(cache_item: dict[str, Any] | None) -> GeoPoint | None:
    if not cache_item:
        return None
    try:
        lat = float(cache_item.get("latitude"))
        lon = float(cache_item.get("longitude"))
    except (TypeError, ValueError):
        return None
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        return None
    return GeoPoint(latitude=lat, longitude=lon)


def _resolve_event_geopoint(
    *,
    candidate: WeatherEventCandidate,
    table_name: str,
    aws_region: str | None,
    session,
    http_cfg: HttpConfig,
    dry_run: bool,
    run_id: str,
    geocode_memo: dict[str, GeoPoint | None] | None = None,
) -> tuple[GeoPoint, str]:
    if geocode_memo is None:
        geocode_memo = {}

    metadata = candidate.event_metadata

    point = extract_geopoint(metadata)
    if point is not None:
        return point, "metadata"

    query = build_geocode_query(metadata)
    if query is None:
        raise ValueError("event metadata missing coordinates and usable location fields")

    if query.fingerprint in geocode_memo:
        memo_point = geocode_memo[query.fingerprint]
        if memo_point is None:
            raise ValueError(f"geocoding memo miss for query='{query.query_text}'")
        return memo_point, "cache"

    cache_item = get_cached_geocode(
        table_name=table_name,
        query_fingerprint=query.fingerprint,
        aws_region=aws_region,
    )
    cache_point = _coerce_cached_point(cache_item)
    if cache_point is not None:
        geocode_memo[query.fingerprint] = cache_point
        return cache_point, "cache"

    resolution = None
    for search_name, country_code in build_geocode_search_candidates(query):
        _, geocode_payload, _, _ = get_open_meteo_geocoding_json(
            session=session,
            cfg=http_cfg,
            query_text=search_name,
            country_code=country_code,
        )
        resolution = pick_best_geocode_result(geocode_payload, query=query)
        if resolution is not None:
            break

    if resolution is None:
        geocode_memo[query.fingerprint] = None
        raise ValueError(f"geocoding produced no valid result for query='{query.query_text}'")

    if not dry_run:
        put_cached_geocode(
            table_name=table_name,
            query_fingerprint=query.fingerprint,
            query_text=query.query_text,
            latitude=resolution.point.latitude,
            longitude=resolution.point.longitude,
            source_name=resolution.source_name,
            source_admin1=resolution.source_admin1,
            source_country=resolution.source_country,
            source_country_code=resolution.source_country_code,
            run_id=run_id,
            aws_region=aws_region,
        )
        upsert_event_geocode_resolution(
            table_name=table_name,
            event_id=candidate.event_id,
            query_fingerprint=query.fingerprint,
            query_text=query.query_text,
            latitude=resolution.point.latitude,
            longitude=resolution.point.longitude,
            resolution_source="open_meteo_geocoding",
            run_id=run_id,
            aws_region=aws_region,
        )

    geocode_memo[query.fingerprint] = resolution.point
    return resolution.point, "geocode_api"


def _should_exit_nonzero(*, stats: RunStats, max_failure_rate: float) -> bool:
    return stats.failure_rate() >= max_failure_rate


def main() -> int:
    args = parse_args()

    if args.event_ids and args.historical_backfill:
        raise ValueError("Use either --event-ids or --historical-backfill, not both")

    mode_incremental = bool(args.incremental)
    if not args.event_ids and not args.historical_backfill and not mode_incremental:
        mode_incremental = True

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    cfg = load_config()
    bucket = args.bucket or cfg.s3_bucket
    ddb_table = args.ddb_table or cfg.ddb_table
    event_ids = parse_event_ids(args.event_ids)

    http_cfg = HttpConfig(timeout_s=args.timeout)
    session = build_session(http_cfg)
    geocode_memo: dict[str, GeoPoint | None] = {}

    run_id = resolve_run_id(make_run_id)
    stats = RunStats()
    progress_every = max(int(args.progress_every), 1)

    candidates = load_weather_event_candidates(
        table_name=ddb_table,
        aws_region=cfg.aws_region,
        event_ids=event_ids,
    )

    logger.info(
        "weather_candidates_loaded",
        extra={
            "run_id": run_id,
            "candidate_event_count": len(candidates),
            "mode_incremental": mode_incremental,
            "historical_backfill": bool(args.historical_backfill),
            "force_events": bool(args.force_events),
            "include_failed_events": bool(args.include_failed_events),
        },
    )
    print(
        {
            "weather_candidates_loaded": {
                "run_id": run_id,
                "candidate_event_count": len(candidates),
                "mode_incremental": mode_incremental,
                "historical_backfill": bool(args.historical_backfill),
                "force_events": bool(args.force_events),
                "include_failed_events": bool(args.include_failed_events),
            }
        }
    )

    summaries_by_event_id = get_event_weather_summaries(
        table_name=ddb_table,
        event_ids=[candidate.event_id for candidate in candidates],
        aws_region=cfg.aws_region,
    )

    selected_candidates, skipped_incremental_count, skipped_failed_count = _select_candidates_for_run(
        candidates=candidates,
        summaries_by_event_id=summaries_by_event_id,
        mode_incremental=mode_incremental,
        force_events=bool(args.force_events),
        include_failed_events=bool(args.include_failed_events),
    )
    stats.skipped_incremental_events = skipped_incremental_count
    stats.skipped_failed_events = skipped_failed_count

    logger.info(
        "weather_run_plan",
        extra={
            "run_id": run_id,
            "mode_incremental": mode_incremental,
            "historical_backfill": bool(args.historical_backfill),
            "candidate_event_count": len(candidates),
            "selected_event_count": len(selected_candidates),
            "skipped_incremental_event_count": skipped_incremental_count,
            "skipped_failed_event_count": skipped_failed_count,
            "dry_run": bool(args.dry_run),
            "force_events": bool(args.force_events),
            "include_failed_events": bool(args.include_failed_events),
            "max_failure_rate": float(args.max_failure_rate),
        },
    )
    print(
        {
            "weather_run_plan": {
                "run_id": run_id,
                "mode_incremental": mode_incremental,
                "historical_backfill": bool(args.historical_backfill),
                "candidate_event_count": len(candidates),
                "selected_event_count": len(selected_candidates),
                "skipped_incremental_event_count": skipped_incremental_count,
                "skipped_failed_event_count": skipped_failed_count,
                "dry_run": bool(args.dry_run),
                "force_events": bool(args.force_events),
                "include_failed_events": bool(args.include_failed_events),
                "max_failure_rate": float(args.max_failure_rate),
            }
        }
    )

    for idx, candidate in enumerate(selected_candidates, start=1):
        stats.attempted_events += 1
        event_id = candidate.event_id
        event_stats = {
            "attempted_round_tasks": 0,
            "processed_round_tasks": 0,
            "changed_round_tasks": 0,
            "unchanged_round_tasks": 0,
            "failed_round_tasks": 0,
            "daylight_hours_total": 0,
        }

        try:
            point, point_source = _resolve_event_geopoint(
                candidate=candidate,
                table_name=ddb_table,
                aws_region=cfg.aws_region,
                session=session,
                http_cfg=http_cfg,
                dry_run=bool(args.dry_run),
                run_id=run_id,
                geocode_memo=geocode_memo,
            )
            if point_source == "metadata":
                stats.point_from_metadata += 1
            elif point_source == "cache":
                stats.point_from_cache += 1
            else:
                stats.point_from_geocode_api += 1

            rows = load_player_round_rows(bucket=bucket, key=candidate.round_s3_key)
            tee_time_source_fingerprint = compute_tee_time_source_fingerprint(rows)

            round_tasks, round_play_dates = _build_round_tasks(
                candidate=candidate,
                rows=rows,
                point=point,
                round_padding_days=max(0, int(args.round_padding_days)),
            )

            for task in round_tasks:
                stats.attempted_round_tasks += 1
                event_stats["attempted_round_tasks"] += 1

                try:
                    request = build_archive_request(point=task.point, window=task.window)
                    status_code, payload, source_url, request_params = get_open_meteo_archive_json(
                        session=session,
                        cfg=http_cfg,
                        request=request,
                    )
                    request_fingerprint = build_request_fingerprint(url=source_url, params=request_params)
                    content_sha256 = compute_payload_sha256(payload)

                    existing = get_existing_weather_state(
                        table_name=ddb_table,
                        event_id=task.event_id,
                        round_number=task.window.round_number,
                        provider=task.provider,
                        source_id=task.source_id,
                        aws_region=cfg.aws_region,
                    )

                    same_as_existing = bool(
                        existing
                        and str(existing.get("content_sha256", "")) == content_sha256
                        and str(existing.get("request_fingerprint", "")) == request_fingerprint
                        and str(existing.get("tee_time_source_fingerprint", "")) == tee_time_source_fingerprint
                    )

                    target_dates = round_play_dates.get(task.window.round_number, {task.window.round_date})
                    daylight_rows = extract_daylight_hourly_rows(payload=payload, target_dates=target_dates)
                    stats.daylight_hours_total += len(daylight_rows)
                    event_stats["daylight_hours_total"] += len(daylight_rows)

                    s3_ptrs: dict[str, Any] = {}
                    if not args.dry_run and not same_as_existing:
                        s3_ptrs = put_weather_raw(
                            bucket=bucket,
                            task=task,
                            source_url=source_url,
                            request_params=request_params,
                            request_fingerprint=request_fingerprint,
                            payload=payload,
                            daylight_hour_count=len(daylight_rows),
                            content_sha256=content_sha256,
                            http_status=status_code,
                            run_id=run_id,
                            tee_time_source_fingerprint=tee_time_source_fingerprint,
                        )
                    elif existing:
                        s3_ptrs = {
                            "s3_json_key": existing.get("latest_s3_json_key", ""),
                            "s3_meta_key": existing.get("latest_s3_meta_key", ""),
                            "fetched_at": existing.get("last_fetched_at", ""),
                        }

                    if not args.dry_run:
                        upsert_weather_state(
                            table_name=ddb_table,
                            event_id=task.event_id,
                            round_number=task.window.round_number,
                            provider=task.provider,
                            source_id=task.source_id,
                            source_url=source_url,
                            request_fingerprint=request_fingerprint,
                            tee_time_source_fingerprint=tee_time_source_fingerprint,
                            fetch_status="unchanged" if same_as_existing else "success",
                            content_sha256=content_sha256,
                            s3_ptrs=s3_ptrs,
                            run_id=run_id,
                            aws_region=cfg.aws_region,
                        )

                    stats.processed_round_tasks += 1
                    event_stats["processed_round_tasks"] += 1
                    if same_as_existing:
                        stats.unchanged_round_tasks += 1
                        event_stats["unchanged_round_tasks"] += 1
                    else:
                        stats.changed_round_tasks += 1
                        event_stats["changed_round_tasks"] += 1

                except Exception as round_exc:
                    stats.failed_round_tasks += 1
                    event_stats["failed_round_tasks"] += 1
                    logger.exception(
                        "weather_round_task_failed",
                        extra={
                            "event_id": task.event_id,
                            "round_number": task.window.round_number,
                            "run_id": run_id,
                            "error": str(round_exc),
                        },
                    )

            event_status = "success"
            error_type = ""
            error_message = ""
            if event_stats["failed_round_tasks"] > 0:
                stats.failed_events += 1
                event_status = "failed"
                error_type = "round_task_failed"
                error_message = f"{event_stats['failed_round_tasks']} round tasks failed"
            else:
                stats.processed_events += 1

            if not args.dry_run:
                upsert_event_weather_summary(
                    table_name=ddb_table,
                    event_id=event_id,
                    run_id=run_id,
                    silver_checkpoint_updated_at=candidate.silver_checkpoint_updated_at,
                    status=event_status,
                    stats=event_stats,
                    error_type=error_type,
                    error_message=error_message,
                    aws_region=cfg.aws_region,
                )

            logger.info(
                "weather_event_processed",
                extra={
                    "event_id": event_id,
                    "run_id": run_id,
                    "point_source": point_source,
                    "event_status": event_status,
                    **event_stats,
                },
            )

        except Exception as exc:
            stats.failed_events += 1
            logger.exception("weather_event_failed", extra={"event_id": event_id, "run_id": run_id, "error": str(exc)})

            if not args.dry_run:
                upsert_event_weather_summary(
                    table_name=ddb_table,
                    event_id=event_id,
                    run_id=run_id,
                    silver_checkpoint_updated_at=candidate.silver_checkpoint_updated_at,
                    status="failed",
                    stats=event_stats,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    aws_region=cfg.aws_region,
                )

        if idx % progress_every == 0 or idx == len(selected_candidates):
            progress = {
                "run_id": run_id,
                "processed_events": idx,
                "total_events": len(selected_candidates),
                **stats.to_dict(),
                "failure_rate": round(stats.failure_rate(), 4),
                "max_failure_rate": float(args.max_failure_rate),
            }
            logger.info("weather_progress", extra=progress)
            print({"weather_progress": progress})

    exit_nonzero = _should_exit_nonzero(stats=stats, max_failure_rate=float(args.max_failure_rate))
    summary = {
        "run_id": run_id,
        **stats.to_dict(),
        "failure_rate": round(stats.failure_rate(), 4),
        "max_failure_rate": float(args.max_failure_rate),
        "exit_nonzero": exit_nonzero,
    }
    logger.info("weather_summary", extra=summary)
    print({"weather_summary": summary})

    if not args.dry_run:
        put_weather_run_summary(
            table_name=ddb_table,
            run_id=run_id,
            stats=summary,
            aws_region=cfg.aws_region,
        )

    return 2 if exit_nonzero else 0


if __name__ == "__main__":
    raise SystemExit(main())
