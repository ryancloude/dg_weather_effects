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
    assert selected[0]["scoring_request_fingerprint"]


def test_merge_projection_bounds_uses_s3_and_selected_events():
    merged = runner._merge_projection_bounds(
        discovered_bounds={
            "event_year_start": 2025,
            "event_year_end": 2026,
            "tourn_id_min": 90008,
            "tourn_id_max": 100656,
        },
        selected_events=[
            {
                "event_id": 100900,
                "event_year": 2026,
                "event_object": {"key": "ignored"},
                "scoring_request_fingerprint": "score-fp-1",
            }
        ],
    )

    assert merged == {
        "event_year_start": 2025,
        "event_year_end": 2026,
        "tourn_id_min": 90008,
        "tourn_id_max": 100900,
    }


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


def test_main_uses_production_fingerprint_when_arg_missing(monkeypatch):
    args = SimpleNamespace(
        training_request_fingerprint=None,
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
        max_failure_rate=0.5,
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
        "scoring_request_fingerprint": "score-fp-1",
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
            production_training_request_fingerprint="prod-train-fp",
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
        "discover_scored_round_projection_bounds",
        lambda **kwargs: {
            "event_year_start": 2025,
            "event_year_end": 2026,
            "tourn_id_min": 90008,
            "tourn_id_max": 100656,
        },
    )

    recreate_calls = []

    monkeypatch.setattr(
        runner,
        "recreate_scored_rounds_table",
        lambda **kwargs: recreate_calls.append(kwargs) or {
            "queries_executed": 2,
            "scanned_bytes": 0,
            "drop_query_execution_id": "qe-drop",
            "create_query_execution_id": "qe-create",
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
    assert checkpoint_calls[0]["training_request_fingerprint"] == "prod-train-fp"
    assert len(recreate_calls) == 1
    assert recreate_calls[0]["event_year_start"] == 2025
    assert recreate_calls[0]["event_year_end"] == 2026
    assert recreate_calls[0]["tourn_id_min"] == 90008
    assert recreate_calls[0]["tourn_id_max"] == 100656
    assert len(run_summary_calls) == 1


def test_main_returns_zero_when_failures_are_not_greater_than_half(monkeypatch):
    args = SimpleNamespace(
        training_request_fingerprint=None,
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
        max_failure_rate=0.5,
        progress_every=10,
        log_level="INFO",
    )

    selected_events = [
        {
            "event_id": 90008,
            "event_year": 2026,
            "event_object": {
                "key": "gold/pdga/wind_effects/model_inputs_round/event_year=2026/tourn_id=90008/model_inputs_round.parquet",
            },
            "scoring_request_fingerprint": "score-fp-1",
        },
        {
            "event_id": 90009,
            "event_year": 2026,
            "event_object": {
                "key": "gold/pdga/wind_effects/model_inputs_round/event_year=2026/tourn_id=90009/model_inputs_round.parquet",
            },
            "scoring_request_fingerprint": "score-fp-2",
        },
    ]

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
            production_training_request_fingerprint="prod-train-fp",
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
        lambda **kwargs: [item["event_object"] for item in selected_events],
    )
    monkeypatch.setattr(
        runner,
        "_prefilter_event_objects",
        lambda **kwargs: (selected_events, 0, 0),
    )
    monkeypatch.setattr(
        runner,
        "discover_scored_round_projection_bounds",
        lambda **kwargs: {
            "event_year_start": 2026,
            "event_year_end": 2026,
            "tourn_id_min": 90008,
            "tourn_id_max": 90009,
        },
    )
    monkeypatch.setattr(
        runner,
        "recreate_scored_rounds_table",
        lambda **kwargs: {
            "queries_executed": 2,
            "scanned_bytes": 0,
            "drop_query_execution_id": "qe-drop",
            "create_query_execution_id": "qe-create",
        },
    )

    def fake_load_event_dataframe(**kwargs):
        if "tourn_id=90008" in kwargs["key"]:
            raise RuntimeError("test failure")
        return pd.DataFrame(
            [
                {
                    "event_year": 2026,
                    "tourn_id": 90009,
                    "round_number": 1,
                    "player_key": "P1",
                }
            ]
        )

    monkeypatch.setattr(runner, "load_event_dataframe", fake_load_event_dataframe)

    fake_scored_df = pd.DataFrame(
        [
            {
                "event_year": 2026,
                "tourn_id": 90009,
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
        lambda **kwargs: "gold/pdga/wind_effects/scored_rounds/event_year=2026/tourn_id=90009/scored_rounds.parquet",
    )

    checkpoint_calls = []
    run_summary_calls = []

    monkeypatch.setattr(runner, "put_score_checkpoint", lambda **kwargs: checkpoint_calls.append(kwargs))
    monkeypatch.setattr(runner, "put_score_run_summary", lambda **kwargs: run_summary_calls.append(kwargs))

    exit_code = runner.main()

    assert exit_code == 0
    assert len(checkpoint_calls) == 2
    assert run_summary_calls[0]["stats"]["failed_events"] == 1
    assert run_summary_calls[0]["stats"]["attempted_events"] == 2
    assert run_summary_calls[0]["stats"]["failure_rate"] == 0.5
    assert run_summary_calls[0]["stats"]["exit_nonzero"] is False
