from __future__ import annotations

from dataclasses import dataclass
from typing import Final


COMMON_ENV_KEYS: Final[tuple[str, ...]] = (
    "PDGA_S3_BUCKET",
    "PDGA_DDB_TABLE",
    "AWS_REGION",
)

COMMON_WITH_GSI_ENV_KEYS: Final[tuple[str, ...]] = (
    "PDGA_S3_BUCKET",
    "PDGA_DDB_TABLE",
    "AWS_REGION",
    "PDGA_DDB_STATUS_END_DATE_GSI",
)

REPORTING_ENV_KEYS: Final[tuple[str, ...]] = (
    "PDGA_S3_BUCKET",
    "PDGA_DDB_TABLE",
    "AWS_REGION",
    "ATHENA_DATABASE",
    "ATHENA_WORKGROUP",
    "ATHENA_RESULTS_S3_URI",
    "ATHENA_SOURCE_SCORED_TABLE",
    "ATHENA_REPORTING_BASE_TABLE",
)

SCORING_ENV_KEYS: Final[tuple[str, ...]] = (
    *REPORTING_ENV_KEYS,
    "PRODUCTION_TRAINING_REQUEST_FINGERPRINT",
)


@dataclass(frozen=True)
class PipelineJobDefinition:
    job_name: str
    state_id: str
    ecr_repo_name: str
    container_name: str
    cpu: int
    memory_mib: int
    timeout_minutes: int
    default_command: tuple[str, ...]
    env_keys: tuple[str, ...]
    needs_athena: bool = False
    ssm_write_parameter_keys: tuple[str, ...] = ()


JOB_DEFINITIONS: Final[tuple[PipelineJobDefinition, ...]] = (
    PipelineJobDefinition(
        job_name="ingest_pdga_event_pages",
        state_id="IngestPdgaEventPages",
        ecr_repo_name="dgwe/ingest-pdga-event-pages",
        container_name="ingest-pdga-event-pages",
        cpu=512,
        memory_mib=1024,
        timeout_minutes=120,
        default_command=(
            "--incremental",
            "--progress-every", "50",
            "--incremental-refetch-hours", "48",
            "--failed-refetch-cooldown-hours", "72",
            "--max-failure-rate", "0.5",
            "--log-level", "INFO",
        ),
        env_keys=COMMON_WITH_GSI_ENV_KEYS,
    ),
    PipelineJobDefinition(
        job_name="ingest_pdga_live_results",
        state_id="IngestPdgaLiveResults",
        ecr_repo_name="dgwe/ingest-pdga-live-results",
        container_name="ingest-pdga-live-results",
        cpu=512,
        memory_mib=1024,
        timeout_minutes=120,
        default_command=("--historical-backfill", "--log-level", "INFO"),
        env_keys=COMMON_ENV_KEYS,
    ),
    PipelineJobDefinition(
        job_name="ingest_weather_observations",
        state_id="IngestWeatherObservations",
        ecr_repo_name="dgwe/ingest-weather-observations",
        container_name="ingest-weather-observations",
        cpu=512,
        memory_mib=1024,
        timeout_minutes=120,
        default_command=("--incremental", "--log-level", "INFO"),
        env_keys=COMMON_WITH_GSI_ENV_KEYS,
    ),
    PipelineJobDefinition(
        job_name="silver_pdga_live_results",
        state_id="SilverPdgaLiveResults",
        ecr_repo_name="dgwe/silver-pdga-live-results",
        container_name="silver-pdga-live-results",
        cpu=1024,
        memory_mib=2048,
        timeout_minutes=300,
        default_command=("--log-level", "INFO"),
        env_keys=COMMON_WITH_GSI_ENV_KEYS,
    ),
    PipelineJobDefinition(
        job_name="silver_weather_observations",
        state_id="SilverWeatherObservations",
        ecr_repo_name="dgwe/silver-weather-observations",
        container_name="silver-weather-observations",
        cpu=1024,
        memory_mib=2048,
        timeout_minutes=300,
        default_command=("--log-level", "INFO"),
        env_keys=COMMON_ENV_KEYS,
    ),
    PipelineJobDefinition(
        job_name="silver_weather_enriched",
        state_id="SilverWeatherEnriched",
        ecr_repo_name="dgwe/silver-weather-enriched",
        container_name="silver-weather-enriched",
        cpu=1024,
        memory_mib=2048,
        timeout_minutes=300,
        default_command=("--log-level", "INFO"),
        env_keys=COMMON_ENV_KEYS,
    ),
    PipelineJobDefinition(
        job_name="gold_wind_effects",
        state_id="GoldWindEffects",
        ecr_repo_name="dgwe/gold-wind-effects",
        container_name="gold-wind-effects",
        cpu=1024,
        memory_mib=2048,
        timeout_minutes=300,
        default_command=("--log-level", "INFO"),
        env_keys=COMMON_ENV_KEYS,
    ),
    PipelineJobDefinition(
        job_name="gold_wind_model_inputs",
        state_id="GoldWindModelInputs",
        ecr_repo_name="dgwe/gold-wind-model-inputs",
        container_name="gold-wind-model-inputs",
        cpu=1024,
        memory_mib=2048,
        timeout_minutes=300,
        default_command=("--log-level", "INFO"),
        env_keys=COMMON_ENV_KEYS,
    ),
    PipelineJobDefinition(
        job_name="train_round_wind_model",
        state_id="TrainRoundWindModel",
        ecr_repo_name="dgwe/train-round-wind-model",
        container_name="train-round-wind-model",
        cpu=2048,
        memory_mib=4096,
        timeout_minutes=300,
        default_command=("--log-level", "INFO"),
        env_keys=COMMON_ENV_KEYS,
        ssm_write_parameter_keys=("PRODUCTION_TRAINING_REQUEST_FINGERPRINT",),
    ),
    PipelineJobDefinition(
        job_name="score_round_wind_model",
        state_id="ScoreRoundWindModel",
        ecr_repo_name="dgwe/score-round-wind-model",
        container_name="score-round-wind-model",
        cpu=2048,
        memory_mib=4096,
        timeout_minutes=300,
        default_command=("--log-level", "INFO"),
        env_keys=SCORING_ENV_KEYS,
        needs_athena=True,
    ),
    PipelineJobDefinition(
        job_name="report_round_weather_impacts",
        state_id="ReportRoundWeatherImpacts",
        ecr_repo_name="dgwe/report-round-weather-impacts",
        container_name="report-round-weather-impacts",
        cpu=1024,
        memory_mib=2048,
        timeout_minutes=60,
        default_command=("--log-level", "INFO"),
        env_keys=REPORTING_ENV_KEYS,
        needs_athena=True,
    ),
)
