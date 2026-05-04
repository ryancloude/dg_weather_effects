from io import BytesIO

import pyarrow.parquet as pq

import score_round_wind_model.parquet_io as parquet_io


def test_to_parquet_bytes_uses_stable_schema():
    rows = [
        {
            "event_year": 2026,
            "tourn_id": 100168,
            "round_number": 1,
            "player_key": "P1",
            "division": "MA3",
            "player_rating": 915.0,
            "actual_round_strokes": 57,
            "round_strokes_over_par": -3,
            "weather_available_flag": True,
            "hole_count": 18,
            "round_total_hole_length": 9000.0,
            "round_avg_hole_length": 500.0,
            "round_total_par": 60,
            "round_avg_hole_par": 3.33,
            "round_length_over_par": 150.0,
            "round_wind_speed_mps_mean": 4.0,
            "round_wind_gust_mps_mean": 6.0,
            "round_temp_c_mean": 18.0,
            "round_precip_mm_sum": 0.0,
            "precip_during_round_flag": 0,
            "predicted_round_strokes": 60.0,
            "predicted_round_strokes_wind_reference": 58.5,
            "predicted_round_strokes_temperature_reference": 59.25,
            "predicted_round_strokes_precip_reference": 59.75,
            "predicted_round_strokes_total_weather_reference": 57.75,
            "estimated_wind_impact_strokes": 1.5,
            "estimated_temperature_impact_strokes": 0.75,
            "estimated_precip_impact_strokes": 0.25,
            "estimated_total_weather_impact_strokes": 2.25,
            "model_name": "round_one_stage_catboost_monotone",
            "model_version": "v4",
            "training_request_fingerprint": "train-fp",
            "scoring_run_id": "score-run-1",
            "scored_at_utc": "2026-05-04T17:00:00Z",
            "scoring_request_fingerprint": "score-fp-1",
            "model_artifact_prefix": "artifacts/prefix/",
            "course_id": "101",
            "layout_id": "201",
            "round_wind_speed_bucket": "light",
            "round_wind_gust_bucket": "mild",
        }
    ]

    payload = parquet_io._to_parquet_bytes(rows)
    table = pq.read_table(BytesIO(payload))
    schema = table.schema

    assert schema.field("actual_round_strokes").type == parquet_io.pa.int64()
    assert schema.field("round_strokes_over_par").type == parquet_io.pa.int64()
    assert schema.field("hole_count").type == parquet_io.pa.int64()
    assert schema.field("course_id").type == parquet_io.pa.string()
    assert schema.field("layout_id").type == parquet_io.pa.string()
    assert schema.field("predicted_round_strokes").type == parquet_io.pa.float64()
