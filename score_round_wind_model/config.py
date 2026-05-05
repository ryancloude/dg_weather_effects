from __future__ import annotations

import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass


@dataclass(frozen=True)
class Config:
    s3_bucket: str
    ddb_table: str
    athena_database: str
    athena_workgroup: str
    athena_results_s3_uri: str
    athena_source_scored_table: str = "scored_rounds"
    aws_region: str | None = None
    production_training_request_fingerprint: str | None = None


def load_config() -> Config:
    return Config(
        s3_bucket=os.environ["PDGA_S3_BUCKET"],
        ddb_table=os.environ["PDGA_DDB_TABLE"],
        athena_database=os.environ["ATHENA_DATABASE"],
        athena_workgroup=os.environ["ATHENA_WORKGROUP"],
        athena_results_s3_uri=os.environ["ATHENA_RESULTS_S3_URI"],
        athena_source_scored_table=os.getenv("ATHENA_SOURCE_SCORED_TABLE", "scored_rounds"),
        aws_region=os.getenv("AWS_REGION"),
        production_training_request_fingerprint=os.getenv("PRODUCTION_TRAINING_REQUEST_FINGERPRINT"),
    )
