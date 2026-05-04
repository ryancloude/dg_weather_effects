import score_round_wind_model.athena_io as athena_io


def test_build_scored_rounds_storage_location_template():
    template = athena_io.build_scored_rounds_storage_location_template(bucket="bucket")
    assert template == "s3://bucket/gold/pdga/wind_effects/scored_rounds/event_year=${event_year}/tourn_id=${tourn_id}/"


def test_build_create_scored_rounds_table_sql_uses_partition_projection():
    sql = athena_io.build_create_scored_rounds_table_sql(
        database="pdga_analytics",
        table_name="scored_rounds",
        table_location="s3://bucket/gold/pdga/wind_effects/scored_rounds/",
        storage_location_template="s3://bucket/gold/pdga/wind_effects/scored_rounds/event_year=${event_year}/tourn_id=${tourn_id}/",
    )

    assert "CREATE EXTERNAL TABLE IF NOT EXISTS pdga_analytics.scored_rounds" in sql
    assert "'projection.enabled' = 'true'" in sql
    assert "'projection.event_year.type' = 'integer'" in sql
    assert "'projection.tourn_id.type' = 'integer'" in sql
    assert "'storage.location.template' = 's3://bucket/gold/pdga/wind_effects/scored_rounds/event_year=${event_year}/tourn_id=${tourn_id}/'" in sql


def test_ensure_scored_rounds_table_executes_athena_query(monkeypatch):
    execute_calls = []

    def fake_execute_athena_query(**kwargs):
        execute_calls.append(kwargs)
        return {
            "query_execution_id": "qe-create",
            "state": "SUCCEEDED",
            "sql": kwargs["sql"],
            "scanned_bytes": 0,
        }

    monkeypatch.setattr(athena_io, "execute_athena_query", fake_execute_athena_query)

    result = athena_io.ensure_scored_rounds_table(
        database="pdga_analytics",
        table_name="scored_rounds",
        workgroup="pdga-analytics",
        output_location="s3://athena-results/query-results/",
        table_location="s3://bucket/gold/pdga/wind_effects/scored_rounds/",
        storage_location_template="s3://bucket/gold/pdga/wind_effects/scored_rounds/event_year=${event_year}/tourn_id=${tourn_id}/",
        aws_region="us-east-2",
    )

    assert len(execute_calls) == 1
    call = execute_calls[0]
    assert call["database"] == "pdga_analytics"
    assert call["workgroup"] == "pdga-analytics"
    assert "CREATE EXTERNAL TABLE IF NOT EXISTS pdga_analytics.scored_rounds" in call["sql"]
    assert "'projection.enabled' = 'true'" in call["sql"]
    assert result["table_name"] == "scored_rounds"
    assert result["storage_location_template"] == "s3://bucket/gold/pdga/wind_effects/scored_rounds/event_year=${event_year}/tourn_id=${tourn_id}/"


def test_execute_athena_query_returns_wait_result(monkeypatch):
    monkeypatch.setattr(
        athena_io,
        "start_athena_query",
        lambda **kwargs: "qe-456",
    )
    monkeypatch.setattr(
        athena_io,
        "wait_for_query",
        lambda **kwargs: {
            "query_execution_id": kwargs["query_execution_id"],
            "state": "SUCCEEDED",
            "scanned_bytes": 0,
            "engine_execution_time_ms": 12,
            "total_execution_time_ms": 20,
            "output_location": "s3://athena-results/query-results/qe-456.csv",
        },
    )

    result = athena_io.execute_athena_query(
        sql="SELECT 1",
        database="pdga_analytics",
        workgroup="pdga-analytics",
        output_location="s3://athena-results/query-results/",
        aws_region="us-east-2",
    )

    assert result["query_execution_id"] == "qe-456"
    assert result["state"] == "SUCCEEDED"
    assert result["sql"] == "SELECT 1"
