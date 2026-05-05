from datetime import datetime, timezone

from pipeline_monitor.report_pipeline_run import (
    estimate_athena_cost,
    estimate_fargate_cost,
    extract_run_id_from_history,
    format_seconds,
    match_job_definition,
)


def test_match_job_definition():
    job = match_job_definition("WeeklyRetrainScoreRoundWindModel")
    assert job is not None
    assert job.job_name == "score_round_wind_model"
    assert job.summary_sk == "SCORE_ROUND_WIND_MODEL#SUMMARY"


def test_extract_run_id_from_history():
    events = [
        {
            "type": "PassStateExited",
            "timestamp": datetime.now(timezone.utc),
            "stateExitedEventDetails": {
                "name": "IncrementalInitializeContext",
                "output": '{"run_id":"abc-123","pipeline_name":"dgwe-dev-incremental"}',
            },
        }
    ]
    assert extract_run_id_from_history(events) == "abc-123"


def test_estimate_fargate_cost_minimum_billing_window():
    cost = estimate_fargate_cost(
        cpu_units=1024,
        memory_mib=2048,
        duration_seconds=10.0,
        vcpu_price_per_second=0.000011244,
        memory_gb_price_per_second=0.000001235,
    )
    assert cost > 0.0


def test_estimate_athena_cost():
    one_tb = 1024 ** 4
    assert round(estimate_athena_cost(one_tb, 5.0), 2) == 5.00


def test_format_seconds():
    assert format_seconds(3661.0) == "01:01:01"
