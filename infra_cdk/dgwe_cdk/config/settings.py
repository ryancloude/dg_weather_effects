from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping
from urllib.parse import urlparse

from dotenv import dotenv_values


@dataclass(frozen=True)
class PipelineSettings:
    app_name: str
    app_env: str
    aws_region: str
    image_tag: str
    log_level: str
    schedule_expression: str
    weekly_retrain_schedule_expression: str
    pdga_s3_bucket: str
    pdga_ddb_table: str
    pdga_ddb_status_end_date_gsi: str
    athena_database: str
    athena_workgroup: str
    athena_results_s3_uri: str
    athena_source_scored_table: str
    athena_reporting_base_table: str
    production_training_request_fingerprint: str

    @property
    def stack_prefix(self) -> str:
        return f"{self.resource_prefix}-"

    @property
    def resource_prefix(self) -> str:
        return f"{self.app_name}-{self.app_env}"

    @property
    def parameter_prefix(self) -> str:
        return f"/{self.app_name}/{self.app_env}"

    @property
    def pipeline_name(self) -> str:
        return f"{self.resource_prefix}-incremental"

    @property
    def weekly_retrain_pipeline_name(self) -> str:
        return f"{self.resource_prefix}-weekly-retrain"

    @property
    def state_machine_name(self) -> str:
        return f"{self.resource_prefix}-incremental-pipeline"

    @property
    def weekly_retrain_state_machine_name(self) -> str:
        return f"{self.resource_prefix}-weekly-retrain-pipeline"

    @property
    def cluster_name(self) -> str:
        return f"{self.resource_prefix}-ecs"

    @property
    def pipeline_runs_table_name(self) -> str:
        return f"{self.resource_prefix}-pipeline-runs"

    @property
    def athena_results_bucket(self) -> str | None:
        if not self.athena_results_s3_uri:
            return None
        parsed = urlparse(self.athena_results_s3_uri)
        if parsed.scheme != "s3":
            return None
        return parsed.netloc or None

    def parameter_name(self, env_var_name: str) -> str:
        return f"{self.parameter_prefix}/{env_var_name}"

    @classmethod
    def from_sources(
        cls,
        *,
        env_file: Path,
        shell_env: Mapping[str, str],
    ) -> "PipelineSettings":
        file_values = {
            key: value
            for key, value in dotenv_values(env_file).items()
            if value is not None
        }

        def resolve(name: str, default: str | None = None) -> str:
            value = shell_env.get(name) or file_values.get(name) or default
            if value is None or str(value).strip() == "":
                raise ValueError(
                    f"Missing required configuration value '{name}'. "
                    f"Set it in {env_file} or your shell environment."
                )
            return str(value).strip()

        return cls(
            app_name=resolve("CDK_APP_NAME", "dgwe"),
            app_env=resolve("APP_ENV", "dev"),
            aws_region=resolve("AWS_REGION"),
            image_tag=resolve("PIPELINE_IMAGE_TAG", "latest"),
            log_level=resolve("PIPELINE_LOG_LEVEL", "INFO"),
            schedule_expression=resolve("PIPELINE_SCHEDULE_EXPRESSION", "rate(1 day)"),
            weekly_retrain_schedule_expression=resolve(
                "PIPELINE_WEEKLY_RETRAIN_SCHEDULE_EXPRESSION",
                "cron(0 12 ? * SUN *)",
            ),
            pdga_s3_bucket=resolve("PDGA_S3_BUCKET"),
            pdga_ddb_table=resolve("PDGA_DDB_TABLE"),
            pdga_ddb_status_end_date_gsi=resolve(
                "PDGA_DDB_STATUS_END_DATE_GSI",
                "gsi_status_end_date",
            ),
            athena_database=resolve("ATHENA_DATABASE", "pdga_analytics"),
            athena_workgroup=resolve("ATHENA_WORKGROUP", "pdga-analytics"),
            athena_results_s3_uri=resolve("ATHENA_RESULTS_S3_URI"),
            athena_source_scored_table=resolve(
                "ATHENA_SOURCE_SCORED_TABLE",
                "scored_rounds",
            ),
            athena_reporting_base_table=resolve(
                "ATHENA_REPORTING_BASE_TABLE",
                "reporting_base_rounds",
            ),
            production_training_request_fingerprint=resolve(
                "PRODUCTION_TRAINING_REQUEST_FINGERPRINT"
            ),
        )
