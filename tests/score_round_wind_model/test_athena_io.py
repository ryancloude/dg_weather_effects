import score_round_wind_model.athena_io as athena_io


def test_build_scored_rounds_storage_location_template():
    template = athena_io.build_scored_rounds_storage_location_template(bucket="bucket")
    assert template == "s3://bucket/gold/pdga/wind_effects/scored_rounds/event_year=${event_year}/tourn_id=${tourn_id}/"


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
