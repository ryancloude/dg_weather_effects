from types import SimpleNamespace

import pandas as pd

import score_round_wind_model.runner as runner


def test_prefilter_event_objects_skips_success_and_failed_by_default(monkeypatch):
    event_objects = [
        {
            "key": "gold/pdga/wind_effects/model_inputs_round/event_year=2026/tourn_id=90008/model_inputs_round.parquet",
            "etag": "etag-1",
            "size": 123,
            "last_modified": "2026-05-01T12:00:00Z",
        },
        {
            "key": "gold/pdga/wind_effects/model_inputs_round/event_year=2026/tourn_id=90009/model_inputs_round.parquet",
            "etag": "etag-2",
            "size": 456,
            "last_modified": "2026-05-01T12:05:00Z",
        },
        {
            "key": "gold/pdga/wind_effects/model_inputs_round/event_year=2026/tourn_id=90010/model_inputs_round.parquet",
            "etag": "etag-3",
            "size": 789,
            "last_modified": "2026-05-01T12:10:00Z",
        },
    ]

    monkeypatch.setattr(
        runner,
        "get_score_checkpoints",
        lambda **kwargs: {
            90008: {"status": "success"},
            90009: {"status": "failed"},
        },
    )

    selected, skipped_success, skipped_failed = runner._prefilter_event_objects(
        event_objects=event_objects,
        table_name="table",
        training_request_fingerprint="train-fp",
        aws_region="us-east-2",
        force_events=False,
        include_failures=False,
    )

    assert skipped_success == 1
    assert skipped_failed == 1
    assert len(selected) == 1
    assert selected[0]["event_id"] == 90010


def test_prefilter_event_objects_includes_failed_when_enabled(monkeypatch):
    event_objects = [
        {
            "key": "gold/pdga/wind_effects/model_inputs_round/event_year=2026/tourn_id=90008/model_inputs_round.parquet",
            "etag": "etag-1",
            "size": 123,
            "last_modified": "2026-05-01T12:00:00Z",
        },
        {
            "key": "gold/pdga/wind_effects/model_inputs_round/event_year=2026/tourn_id=90009/model_inputs_round.parquet",
            "etag": "etag-2",
            "size": 456,
            "last_modified": "2026-05-01T12:05:00Z",
        },
    ]

    monkeypatch.setattr(
        runner,
        "get_score_checkpoints",
        lambda **kwargs: {
            90008: {"status": "success"},
            90009: {"status": "failed"},
        },
    )

    selected, skipped_success, skipped_failed = runner._prefilter_event_objects(
        event_objects=event_objects,
        table_name="table",
        training_request_fingerprint="train-fp",
        aws_region="us-east-2",
        force_events=False,
        include_failures=True,
    )

    assert skipped_success == 1
    assert skipped_failed == 0
    assert len(selected) == 1
    assert selected[0]["event_id"] == 90009


def test_main_skips_when_prefilter_selects_nothing(monkeypatch):
    args = SimpleNamespace(
        training_request_fingerprint="train-fp",
        event_ids=None,
        bucket=None,
        ddb_table=None,
        athena_database=None,
        athena_workgroup=None,
        athena_results_s3_uri=None,
        source_table=None,
        dry_run=True,
        force_events=False,
        include_failures=False,
        progress_every=10,
        log_level="INFO",
    )

    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(
        runner,
        "load_config",
        lambda: SimpleNamespace(
            s3_bucket="bucket",
            ddb_table="table",
            athena_database="pdga_analytics",
            athena_workgroup="pdga-analytics",
            athena_results_s3_uri="s3://athena-results/query-results/",
            athena_source_scored_table="scored_rounds",
            aws_region="us-east-2",
        ),
    )
    monkeypatch.setattr(
        runner,
        "load_model_bundle",
        lambda **kwargs: {
            "artifact_prefix": "artifacts/prefix/",
            "model": object(),
            "training_manifest": {},
            "feature_columns": [],
            "categorical_feature_columns": [],
        },
    )
    monkeypatch.setattr(
        runner,
        "list_model_input_round_objects",
        lambda **kwargs: [
            {
                "key": "gold/pdga/wind_effects/model_inputs_round/event_year=2026/tourn_id=90008/model_inputs_round.parquet",
                "etag": "etag-1",
                "size": 123,
                "last_modified": "2026-05-01T12:00:00Z",
            }
        ],
    )
    monkeypatch.setattr(
        runner,
        "_prefilter_event_objects",
        lambda **kwargs: ([], 1, 0),
    )

    exit_code = runner.main()

    assert exit_code == 0


def test_main_scores_selected_events_only_without_partition_registration(monkeypatch):
    args = SimpleNamespace(
        training_request_fingerprint="train-fp",
        event_ids=None,
        bucket=None,
        ddb_table=None,
        athena_database=None,
        athena_workgroup=None,
        athena_results_s3_uri=None,
        source_table=None,
        dry_run=False,
        force_events=False,
        include_failures=False,
        progress_every=10,
        log_level="INFO",
    )

    selected_event = {
        "event_id": 90008,
        "event_year": 2026,
        "event_object": {
            "key": "gold/pdga/wind_effects/model_inputs_round/event_year=2026/tourn_id=90008/model_inputs_round.parquet",
            "etag": "etag-1",
            "size": 123,
            "last_modified": "2026-05-01T12:00:00Z",
        },
    }

    monkeypatch.setattr(runner, "parse_args", lambda: args)
    monkeypatch.setattr(
        runner,
        "load_config",
        lambda: SimpleNamespace(
            s3_bucket="bucket",
            ddb_table="table",
            athena_database="pdga_analytics",
            athena_workgroup="pdga-analytics",
            athena_results_s3_uri="s3://athena-results/query-results/",
            athena_source_scored_table="scored_rounds",
            aws_region="us-east-2",
        ),
    )
    monkeypatch.setattr(
        runner,
        "load_model_bundle",
        lambda **kwargs: {
            "artifact_prefix": "artifacts/prefix/",
            "model": object(),
            "training_manifest": {
                "model_name": "round_one_stage_catboost_monotone",
                "model_version": "v4",
            },
            "feature_columns": ["player_rating", "division"],
            "categorical_feature_columns": ["division"],
        },
    )
    monkeypatch.setattr(
        runner,
        "list_model_input_round_objects",
        lambda **kwargs: [selected_event["event_object"]],
    )
    monkeypatch.setattr(
        runner,
        "_prefilter_event_objects",
        lambda **kwargs: ([selected_event], 0, 0),
    )
    monkeypatch.setattr(
        runner,
        "ensure_scored_rounds_table",
        lambda **kwargs: {
            "query_execution_id": "qe-create",
            "scanned_bytes": 0,
        },
    )
    monkeypatch.setattr(
        runner,
        "load_event_dataframe",
        lambda **kwargs: pd.DataFrame(
            [
                {
                    "event_year": 2026,
                    "tourn_id": 90008,
                    "round_number": 1,
                    "player_key": "P1",
                }
            ]
        ),
    )

    fake_scored_df = pd.DataFrame(
        [
            {
                "event_year": 2026,
                "tourn_id": 90008,
                "round_number": 1,
                "player_key": "P1",
                "predicted_round_strokes": 60.0,
            }
        ]
    )

    monkeypatch.setattr(
        runner,
        "score_round_rows",
        lambda **kwargs: SimpleNamespace(
            scored_df=fake_scored_df,
            scoring_manifest={
                "model_name": "round_one_stage_catboost_monotone",
                "model_version": "v4",
                "rows_scored": 1,
            },
        ),
    )
    monkeypatch.setattr(
        runner,
        "overwrite_event_scored_rounds",
        lambda **kwargs: "gold/pdga/wind_effects/scored_rounds/event_year=2026/tourn_id=90008/scored_rounds.parquet",
    )

    checkpoint_calls = []
    run_summary_calls = []

    monkeypatch.setattr(runner, "put_score_checkpoint", lambda **kwargs: checkpoint_calls.append(kwargs))
    monkeypatch.setattr(runner, "put_score_run_summary", lambda **kwargs: run_summary_calls.append(kwargs))

    exit_code = runner.main()

    assert exit_code == 0
    assert len(checkpoint_calls) == 1
    assert checkpoint_calls[0]["status"] == "success"
    assert checkpoint_calls[0]["extra_attributes"]["rows_scored"] == 1
    assert len(run_summary_calls) == 1
    assert run_summary_calls[0]["stats"]["candidate_events"] == 1
    assert run_summary_calls[0]["stats"]["selected_events"] == 1
    assert run_summary_calls[0]["stats"]["athena_queries_executed"] == 1
