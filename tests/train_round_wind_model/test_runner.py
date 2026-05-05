from types import SimpleNamespace

import pandas as pd

import train_round_wind_model.runner as runner
from train_round_wind_model.training import TrainingResult


class _FakeModel:
    pass


def test_main_skips_when_checkpoint_exists(monkeypatch):
    args = SimpleNamespace(
        event_ids=None,
        bucket=None,
        ddb_table=None,
        dry_run=True,
        force_train=False,
        set_production_fingerprint=False,
        production_fingerprint_parameter_name=None,
        log_level="INFO",
    )

    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(
        runner,
        "load_config",
        lambda: SimpleNamespace(s3_bucket="bucket", ddb_table="table", aws_region="us-east-1"),
    )
    monkeypatch.setattr(
        runner,
        "load_model_input_round_dataframe",
        lambda **kwargs: (
            pd.DataFrame([{"row_hash_sha256": "a"}]),
            [{"key": "k1", "etag": "e1", "size": 1, "last_modified": "x"}],
        ),
    )
    monkeypatch.setattr(runner, "compute_dataset_fingerprint", lambda objects: "dataset-fp")
    monkeypatch.setattr(runner, "compute_training_request_fingerprint", lambda **kwargs: "train-fp")
    monkeypatch.setattr(
        runner,
        "get_training_checkpoint",
        lambda **kwargs: {"status": "success", "training_request_fingerprint": "train-fp"},
    )

    exit_code = runner.main()
    assert exit_code == 0


def test_main_skips_and_still_updates_production_fingerprint(monkeypatch):
    args = SimpleNamespace(
        event_ids=None,
        bucket=None,
        ddb_table=None,
        dry_run=False,
        force_train=False,
        set_production_fingerprint=True,
        production_fingerprint_parameter_name="/dgwe/dev/PRODUCTION_TRAINING_REQUEST_FINGERPRINT",
        log_level="INFO",
    )

    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(
        runner,
        "load_config",
        lambda: SimpleNamespace(s3_bucket="bucket", ddb_table="table", aws_region="us-east-1"),
    )
    monkeypatch.setattr(
        runner,
        "load_model_input_round_dataframe",
        lambda **kwargs: (
            pd.DataFrame([{"row_hash_sha256": "a"}]),
            [{"key": "k1", "etag": "e1", "size": 1, "last_modified": "x"}],
        ),
    )
    monkeypatch.setattr(runner, "compute_dataset_fingerprint", lambda objects: "dataset-fp")
    monkeypatch.setattr(runner, "compute_training_request_fingerprint", lambda **kwargs: "train-fp")
    monkeypatch.setattr(
        runner,
        "get_training_checkpoint",
        lambda **kwargs: {"status": "success", "training_request_fingerprint": "train-fp"},
    )

    parameter_calls = []
    run_summary_calls = []

    monkeypatch.setattr(runner, "put_string_parameter", lambda **kwargs: parameter_calls.append(kwargs) or {
        "parameter_name": kwargs["parameter_name"],
        "version": 7,
        "value": kwargs["value"],
    })
    monkeypatch.setattr(runner, "put_training_run_summary", lambda **kwargs: run_summary_calls.append(kwargs))

    exit_code = runner.main()

    assert exit_code == 0
    assert len(parameter_calls) == 1
    assert parameter_calls[0]["parameter_name"] == "/dgwe/dev/PRODUCTION_TRAINING_REQUEST_FINGERPRINT"
    assert parameter_calls[0]["value"] == "train-fp"
    assert len(run_summary_calls) == 1
    assert run_summary_calls[0]["stats"]["production_fingerprint_updated"] is True


def test_main_trains_and_writes(monkeypatch):
    args = SimpleNamespace(
        event_ids=None,
        bucket=None,
        ddb_table=None,
        dry_run=False,
        force_train=False,
        set_production_fingerprint=True,
        production_fingerprint_parameter_name="/dgwe/dev/PRODUCTION_TRAINING_REQUEST_FINGERPRINT",
        log_level="INFO",
    )

    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(
        runner,
        "load_config",
        lambda: SimpleNamespace(s3_bucket="bucket", ddb_table="table", aws_region="us-east-1"),
    )
    monkeypatch.setattr(
        runner,
        "load_model_input_round_dataframe",
        lambda **kwargs: (
            pd.DataFrame([{"row_hash_sha256": "a"}, {"row_hash_sha256": "b"}]),
            [
                {"key": "k1", "etag": "e1", "size": 1, "last_modified": "x"},
                {"key": "k2", "etag": "e2", "size": 1, "last_modified": "y"},
            ],
        ),
    )
    monkeypatch.setattr(runner, "compute_dataset_fingerprint", lambda objects: "dataset-fp")
    monkeypatch.setattr(runner, "compute_training_request_fingerprint", lambda **kwargs: "train-fp")
    monkeypatch.setattr(runner, "get_training_checkpoint", lambda **kwargs: None)

    fake_result = TrainingResult(
        model=_FakeModel(),
        metrics={
            "mae": 1.2,
            "rmse": 1.5,
            "r2": 0.4,
            "best_iteration": 123,
            "input_rows": 100,
            "rows_after_weather_filter": 90,
            "rows_after_hole_filter": 84,
            "rows_after_numeric_not_null_filter": 80,
            "train_rows": 50,
            "valid_rows": 15,
            "test_rows": 15,
        },
        training_manifest={
            "model_name": "round_one_stage_catboost_monotone",
            "model_version": "v4",
            "feature_cols": ["player_rating", "round_wind_speed_mps_mean", "precip_during_round_flag"],
            "categorical_features": ["course_id", "division"],
        },
        feature_importance_rows=[{"feature": "player_rating", "importance": 10.0}],
    )
    monkeypatch.setattr(runner, "train_round_model", lambda **kwargs: fake_result)
    monkeypatch.setattr(
        runner,
        "write_training_artifacts",
        lambda **kwargs: {
            "artifact_prefix": "artifacts/prefix/",
            "model_key": "model.cbm",
            "metrics_key": "metrics.json",
            "manifest_key": "manifest.json",
        },
    )

    checkpoint_calls = []
    run_summary_calls = []
    parameter_calls = []

    monkeypatch.setattr(runner, "put_training_checkpoint", lambda **kwargs: checkpoint_calls.append(kwargs))
    monkeypatch.setattr(runner, "put_training_run_summary", lambda **kwargs: run_summary_calls.append(kwargs))
    monkeypatch.setattr(runner, "put_string_parameter", lambda **kwargs: parameter_calls.append(kwargs) or {
        "parameter_name": kwargs["parameter_name"],
        "version": 11,
        "value": kwargs["value"],
    })

    exit_code = runner.main()

    assert exit_code == 0
    assert len(checkpoint_calls) == 1
    assert checkpoint_calls[0]["status"] == "success"
    assert len(parameter_calls) == 1
    assert parameter_calls[0]["value"] == "train-fp"
    assert len(run_summary_calls) == 1
    assert run_summary_calls[0]["stats"]["production_fingerprint_updated"] is True
