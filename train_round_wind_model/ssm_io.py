from __future__ import annotations

from typing import Any, Optional

import boto3


def _ssm_client(aws_region: Optional[str]):
    return boto3.client("ssm", region_name=aws_region) if aws_region else boto3.client("ssm")


def put_string_parameter(
    *,
    parameter_name: str,
    value: str,
    aws_region: Optional[str],
) -> dict[str, Any]:
    client = _ssm_client(aws_region)
    response = client.put_parameter(
        Name=parameter_name,
        Value=value,
        Type="String",
        Overwrite=True,
    )
    return {
        "parameter_name": parameter_name,
        "version": int(response["Version"]),
        "value": value,
    }
