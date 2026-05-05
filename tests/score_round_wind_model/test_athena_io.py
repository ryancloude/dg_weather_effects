import score_round_wind_model.athena_io as athena_io


def test_build_scored_rounds_storage_location_template():
    template = athena_io.build_scored_rounds_storage_location_template(bucket="bucket")
    assert template == "s3://bucket/gold/pdga/wind_effects/scored_rounds/event_year=${event_year}/tourn_id=${tourn_id}/"


def test_discover_scored_round_projection_bounds_from_s3(monkeypatch):
    prefix_root = "gold/pdga/wind_effects/scored_rounds/"
    prefix_2025 = "gold/pdga/wind_effects/scored_rounds/event_year=2025/"
    prefix_2026 = "gold/pdga/wind_effects/scored_rounds/event_year=2026/"

    pages = {
        prefix_root: [
            {
                "CommonPrefixes": [
                    {"Prefix": prefix_2025},
                    {"Prefix": prefix_2026},
                ]
            }
        ],
        prefix_2025: [
            {
                "CommonPrefixes": [
                    {"Prefix": f"{prefix_2025}tourn_id=90008/"},
                    {"Prefix": f"{prefix_2025}tourn_id=90010/"},
                ]
            }
        ],
        prefix_2026: [
            {
                "CommonPrefixes": [
                    {"Prefix": f"{prefix_2026}tourn_id=100168/"},
                    {"Prefix": f"{prefix_2026}tourn_id=100656/"},
                ]
            }
        ],
    }

    class FakePaginator:
        def paginate(self, **kwargs):
            return pages[kwargs["Prefix"]]

    class FakeS3Client:
        def get_paginator(self, name):
            assert name == "list_objects_v2"
            return FakePaginator()

    monkeypatch.setattr(athena_io, "_s3_client", lambda aws_region: FakeS3Client())

    bounds = athena_io.discover_scored_round_projection_bounds(
        bucket="bucket",
        aws_region="us-east-2",
    )

    assert bounds == {
        "event_year_start": 2025,
        "event_year_end": 2026,
        "tourn_id_min": 90008,
        "tourn_id_max": 100656,
    }


def test_build_create_scored_rounds_table_sql_uses_partition_projection_and_bigint_fields():
    sql = athena_io.build_create_scored_rounds_table_sql(
        database="pdga_analytics",
        table_name="scored_rounds",
        table_location="s3://bucket/gold/pdga/wind_effects/scored_rounds/",
        storage_location_template="s3://bucket/gold/pdga/wind_effects/scored_rounds/event_year=${event_year}/tourn_id=${tourn_id}/",
    )

    assert "CREATE EXTERNAL TABLE pdga_analytics.scored_rounds" in sql
    assert "actual_round_strokes bigint" in sql
    assert "round_strokes_over_par bigint" in sql
    assert "hole_count bigint" in sql
    assert "'projection.enabled' = 'true'" in sql
    assert "'storage.location.template' = 's3://bucket/gold/pdga/wind_effects/scored_rounds/event_year=${event_year}/tourn_id=${tourn_id}/'" in sql


def test_recreate_scored_rounds_table_executes_drop_then_create(monkeypatch):
    execute_calls = []

    def fake_execute_athena_query(**kwargs):
        execute_calls.append(kwargs)
        return {
            "query_execution_id": f"qe-{len(execute_calls)}",
            "state": "SUCCEEDED",
            "sql": kwargs["sql"],
            "scanned_bytes": 0,
        }

    monkeypatch.setattr(athena_io, "execute_athena_query", fake_execute_athena_query)

    result = athena_io.recreate_scored_rounds_table(
        database="pdga_analytics",
        table_name="scored_rounds",
        workgroup="pdga-analytics",
        output_location="s3://athena-results/query-results/",
        table_location="s3://bucket/gold/pdga/wind_effects/scored_rounds/",
        storage_location_template="s3://bucket/gold/pdga/wind_effects/scored_rounds/event_year=${event_year}/tourn_id=${tourn_id}/",
        aws_region="us-east-2",
    )

    assert len(execute_calls) == 2
    assert execute_calls[0]["sql"] == "DROP TABLE IF EXISTS pdga_analytics.scored_rounds"
    assert "CREATE EXTERNAL TABLE pdga_analytics.scored_rounds" in execute_calls[1]["sql"]
    assert result["queries_executed"] == 2
    assert result["drop_query_execution_id"] == "qe-1"
    assert result["create_query_execution_id"] == "qe-2"
