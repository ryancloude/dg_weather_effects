from decimal import Decimal

import score_round_wind_model.dynamo_io as dio


class FakeTable:
    def __init__(self):
        self.items = {}

    def get_item(self, **kwargs):
        key = (kwargs["Key"]["pk"], kwargs["Key"]["sk"])
        if key in self.items:
            return {"Item": self.items[key]}
        return {}

    def put_item(self, Item):
        key = (Item["pk"], Item["sk"])
        self.items[key] = Item


class FakeDdb:
    def __init__(self, table):
        self._table = table

    def Table(self, name):
        return self._table


class FakeClient:
    def __init__(self, table):
        self._table = table

    def batch_get_item(self, RequestItems):
        table_name = next(iter(RequestItems.keys()))
        keys = RequestItems[table_name]["Keys"]
        items = []

        for key in keys:
            lookup = (key["pk"]["S"], key["sk"]["S"])
            item = self._table.items.get(lookup)
            if not item:
                continue

            encoded = {}
            for field, value in item.items():
                if isinstance(value, bool):
                    encoded[field] = {"BOOL": value}
                elif isinstance(value, int):
                    encoded[field] = {"N": str(value)}
                elif isinstance(value, Decimal):
                    encoded[field] = {"N": str(value)}
                else:
                    encoded[field] = {"S": str(value)}
            items.append(encoded)

        return {"Responses": {table_name: items}}


def test_put_and_get_score_checkpoint(monkeypatch):
    table = FakeTable()
    monkeypatch.setattr(dio, "_ddb_resource", lambda aws_region: FakeDdb(table))

    dio.put_score_checkpoint(
        table_name="table",
        event_id=90008,
        training_request_fingerprint="train-fp",
        run_id="run-1",
        status="success",
        aws_region="us-east-1",
        extra_attributes={"rows_scored": 10, "rmse": 1.2},
    )

    item = dio.get_score_checkpoint(
        table_name="table",
        event_id=90008,
        training_request_fingerprint="train-fp",
        aws_region="us-east-1",
    )

    assert item is not None
    assert item["status"] == "success"
    assert item["rmse"] == Decimal("1.2")


def test_get_score_checkpoints_batch(monkeypatch):
    table = FakeTable()
    monkeypatch.setattr(dio, "_ddb_resource", lambda aws_region: FakeDdb(table))
    monkeypatch.setattr(dio, "_ddb_client", lambda aws_region: FakeClient(table))

    dio.put_score_checkpoint(
        table_name="table",
        event_id=90008,
        training_request_fingerprint="train-fp",
        run_id="run-1",
        status="success",
        aws_region="us-east-1",
        extra_attributes={"scoring_request_fingerprint": "score-fp-1"},
    )
    dio.put_score_checkpoint(
        table_name="table",
        event_id=90009,
        training_request_fingerprint="train-fp",
        run_id="run-2",
        status="failed",
        aws_region="us-east-1",
        extra_attributes={"error_message": "boom"},
    )

    items = dio.get_score_checkpoints(
        table_name="table",
        event_ids=[90008, 90009, 90010],
        training_request_fingerprint="train-fp",
        aws_region="us-east-1",
    )

    assert set(items.keys()) == {90008, 90009}
    assert items[90008]["status"] == "success"
    assert items[90008]["scoring_request_fingerprint"] == "score-fp-1"
    assert items[90009]["status"] == "failed"
    assert items[90009]["error_message"] == "boom"
